# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Bytecode injection engine for live debugger.

This module handles the injection of breakpoint calls into Python functions
using bytecode injection for Python 3.9-3.11.
"""

import inspect
import logging
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from types import CodeType, FunctionType
from typing import Any, Dict, List, Optional, Set, Tuple

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED
from amazon.opentelemetry.distro.debugger._data_models import (
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_STRING_LENGTH,
    CaptureConfig,
)
from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter
from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    CapturedThrowable,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    StackFrame,
    ThreadInfo,
    TraceContext,
)
from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer
from amazon.opentelemetry.distro.debugger._stack_utils import capture_stack_frames
from amazon.opentelemetry.distro.debugger.instrumentation_engine._instrumentation_engine import InstrumentationEngine
from amazon.opentelemetry.distro.debugger.instrumentation_engine._undecorate import undecorated
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.semconv._incubating.attributes.deployment_attributes import DEPLOYMENT_ENVIRONMENT_NAME
from opentelemetry.semconv.resource import ResourceAttributes

logger = logging.getLogger(__name__)

# Global name for the breakpoint handler function injected into function globals
_HANDLER_NAME = "_breakpoint_handler"
# Global name for the locals() builtin injected into function globals
_LOCALS_NAME = "_breakpoint_locals"
# Global name for the function-level (PROBE / function-level BREAKPOINT)
# event dispatcher injected into function globals.
_FUNCTION_HANDLER_NAME = "_function_event_handler"

# Code-flag mask the bytecode-rewrite path declines to instrument: rewriting
# these in place would corrupt .send() / .throw() / await semantics. The
# manager observes the False return from enable_function_level_instrumentation
# and routes these through _function_wrapper.instrument_function instead,
# which has dedicated coroutine support via _create_async_wrapper. So async
# functions ARE instrumented end-to-end — just not by the bytecode engine.
# Plain generators and async generators don't have a dedicated wrapper path
# yet; they fall through to the sync wrapper and only fire on the
# generator-construction call.
_DECLINE_BYTECODE_REWRITE_MASK = (
    inspect.CO_GENERATOR | inspect.CO_COROUTINE | inspect.CO_ASYNC_GENERATOR | inspect.CO_ITERABLE_COROUTINE
)

# Import bytecode classes if available
if IS_BYTECODE_INSTALLED:
    from bytecode import Bytecode, Instr, Label, TryBegin, TryEnd
else:
    Bytecode = None  # type: ignore[misc, assignment]
    Instr = None  # type: ignore[misc, assignment]
    Label = None  # type: ignore[misc, assignment]
    TryBegin = None  # type: ignore[misc, assignment]
    TryEnd = None  # type: ignore[misc, assignment]


@dataclass
class InjectionState:
    """
    Tracks bytecode injection state for a function.

    Only stores what's needed for restoration and callback:
    - original_code: To restore the function's original bytecode
    - function_ref: To access the function for restoration and cleanup
    - function_key: For constructing breakpoint_key in callback
    - function_metadata: Populated only when function-level instrumentation
      is armed; line-only callers leave this None. Bag fields:
      module_name, qualified_name, capture_config, location_hash,
      instrumentation_type, file_path, unique_local_names (3-tuple).
    """

    original_code: CodeType
    function_ref: Optional[FunctionType]
    function_key: str
    function_metadata: Optional[Dict[str, Any]] = None


class BytecodeInjectionEngine(InstrumentationEngine):
    """
    Bytecode injection engine for line-level debugging on Python 3.9-3.11.

    Safety guarantees:
    - Never crashes the application (comprehensive exception handling)
    - Stores original code for restoration
    - Thread-safe for concurrent operations
    - Gracefully degrades on errors
    """

    def __init__(self):
        """Initialize the bytecode injection engine."""
        self._lock = threading.RLock()
        self._injection_states: Dict[int, InjectionState] = {}
        self._initialized = False
        # Callback for hit count tracking
        self._hit_count_callback = None
        # Re-entrancy guard for function-level handlers. Snapshot serialization
        # may invoke user __repr__ / OTel resource lookup; if either is itself
        # PROBE'd, the inner call must short-circuit on the same thread.
        self._function_reentrancy_guard = threading.local()
        # Maps (function_key, line_number) to location_hash for span events
        self._location_hashes: Dict[tuple, str] = {}
        # Maps (function_key, line_number) to CaptureConfig for filtering captured data
        self._capture_configs: Dict[tuple, CaptureConfig] = {}
        # Maps (function_key, 0) to "PROBE" or "BREAKPOINT" for function-level
        # snapshots. O(1) lookup in the hot _handle_function_event path.
        self._instrumentation_types: Dict[tuple, str] = {}

        if not IS_BYTECODE_INSTALLED:
            logger.warning(
                "bytecode library not available. "
                "Debugger will not function on Python 3.9-3.11. "
                "Install with: pip install bytecode"
            )

    def initialize(self, hit_count_callback=None) -> None:
        """
        Initialize the bytecode injection engine.

        Args:
            hit_count_callback: Optional callback to be called when a breakpoint is hit
        """
        try:
            if not IS_BYTECODE_INSTALLED:
                logger.warning("Cannot initialize: bytecode library not available")
                return

            logger.debug(
                "BytecodeInjectionEngine initialized for Python %d.%d",
                sys.version_info.major,
                sys.version_info.minor,
            )
            self._initialized = True
            self._hit_count_callback = hit_count_callback
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to initialize BytecodeInjectionEngine: %s", exc, exc_info=True)

    @staticmethod
    def supports_runtime() -> bool:
        """Check if bytecode injection is supported (Python 3.9-3.11)."""
        return (3, 9) <= sys.version_info < (3, 12) and IS_BYTECODE_INSTALLED

    @staticmethod
    def _should_decline_bytecode_rewrite(code: CodeType) -> bool:
        """True if this code object's body cannot safely be rewritten by the
        bytecode-injection path: generators, coroutines, async generators,
        and iterable coroutines.

        A False return from enable_function_level_instrumentation is the
        manager's cue to fall back to _function_wrapper.instrument_function,
        which DOES support coroutines via _create_async_wrapper. So an
        ``async def`` function is still instrumented end-to-end; just not
        by this engine."""
        return bool(code.co_flags & _DECLINE_BYTECODE_REWRITE_MASK)

    def enable_breakpoints_for_function(
        self,
        code: CodeType,
        func: FunctionType,
        line_numbers: Set[int],
        function_key: str,
        line_location_hashes: Optional[Dict[int, str]] = None,
        line_capture_configs: Optional[Dict[int, Any]] = None,
    ) -> None:
        """
        Enable multiple breakpoints for a function atomically.

        Injects all breakpoints into the function's bytecode in a single operation.

        Args:
            code: Code object of the function
            func: Function object (needed for bytecode injection)
            line_numbers: Set of line numbers to enable breakpoints on
            function_key: Function key (module.function) for constructing breakpoint_key
            line_location_hashes: Optional mapping of line_number -> location_hash for span events
        """
        if not self._initialized:
            logger.warning("Engine not initialized")
            return

        if not line_numbers:
            logger.debug("No line numbers provided, nothing to enable")
            return

        try:
            func_id = id(func)

            # Create new bytecode with injected breakpoints outside the lock since it doesn't modify shared state
            new_code, actually_injected = self._create_code_with_breakpoints(code, line_numbers, function_key)

            if new_code is None:
                logger.warning("Failed to inject breakpoints for %s", function_key)
                return

            with self._lock:
                if func_id not in self._injection_states:
                    self._injection_states[func_id] = InjectionState(
                        original_code=code, function_ref=func, function_key=function_key
                    )
                    logger.debug("%s entry added to _injection_states for %s", func_id, function_key)

                # Update function code directly
                func.__code__ = new_code
                # Ensure handler is in globals
                func.__globals__[_HANDLER_NAME] = self._breakpoint_handler
                func.__globals__[_LOCALS_NAME] = locals

                # Store location hashes and capture configs for each line
                if line_location_hashes:
                    for line_num, location_hash in line_location_hashes.items():
                        self._location_hashes[(function_key, line_num)] = location_hash
                if line_capture_configs:
                    for line_num, capture_config in line_capture_configs.items():
                        self._capture_configs[(function_key, line_num)] = capture_config

                logger.debug(
                    "Enabled %d breakpoints for %s at lines: %s",
                    len(actually_injected),
                    function_key,
                    actually_injected,
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error enabling breakpoints for %s: %s", function_key, exc, exc_info=True)

    def disable_breakpoints_for_function(self, code: CodeType, func: FunctionType) -> None:
        """
        Disable ALL breakpoints for a function and restore it to original state.

        This is an all-or-nothing operation that:
        - Removes ALL active breakpoints for the function
        - Restores the function to its original (uninstrumented) bytecode
        - Cleans up injected globals and engine-specific state

        Args:
            code: Code object of the function
            func: Function object (needed for bytecode restoration)
        """
        if not self._initialized:
            logger.warning("Engine not initialized")
            return

        try:
            func_id = id(func)

            with self._lock:
                if func_id not in self._injection_states:
                    logger.debug("No injection state for %s", code.co_name)
                    return

                function_key = self._injection_states[func_id].function_key

                state = self._injection_states[func_id]

                # Restore original code (removes all breakpoints)
                func.__code__ = state.original_code

                # Clean up injected globals, but only if no OTHER instrumented function
                # shares this module's globals dict. Two functions defined in the same
                # module share one __globals__; popping the handler while a sibling is
                # still instrumented would make the sibling's injected LOAD_GLOBAL raise
                # NameError inside user code. Compare by identity since dicts aren't hashable.
                target_globals_id = id(func.__globals__)
                shared_by_other = any(
                    other_id != func_id
                    and other_state.function_ref is not None
                    and id(other_state.function_ref.__globals__) == target_globals_id
                    for other_id, other_state in self._injection_states.items()
                )
                if not shared_by_other:
                    func.__globals__.pop(_HANDLER_NAME, None)
                    func.__globals__.pop(_LOCALS_NAME, None)

                # Clean up location hashes and capture configs for this function
                keys_to_remove = [key for key in self._location_hashes if key[0] == function_key]
                for key in keys_to_remove:
                    del self._location_hashes[key]
                config_keys_to_remove = [key for key in self._capture_configs if key[0] == function_key]
                for key in config_keys_to_remove:
                    del self._capture_configs[key]
                self._instrumentation_types.pop((function_key, 0), None)

                # Clean up state
                del self._injection_states[func_id]
                logger.debug("Disabled all breakpoints for %s", function_key)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error disabling breakpoints for %s: %s", code.co_name, exc, exc_info=True)

    def enable_function_level_instrumentation(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        code: CodeType,
        func: FunctionType,
        function_key: str,
        module_name: str,
        qualified_name: str,
        capture_config: Optional[CaptureConfig] = None,
        location_hash: Optional[str] = None,
        instrumentation_type: Optional[str] = None,
    ) -> bool:
        """Arm function-level (PROBE / function-level BREAKPOINT) on `func`.

        Refuses generators, coroutines, async generators (CO_* flag mask) since
        wrapping their bodies with try/finally would corrupt .send()/.throw()/
        await semantics. 3.11 is also refused in this commit; ConcreteBytecode
        + exception_table support lands in a follow-up.

        Returns True on successful arm, False on refusal or rewrite failure.
        """
        if not self._initialized:
            return False
        if self._should_decline_bytecode_rewrite(code):
            # The manager will fall back to _function_wrapper.instrument_function,
            # which has a working coroutine path. Return False, not raise.
            logger.debug(
                "Engine declines bytecode rewrite for %s; manager will route through wrapper instead",
                function_key,
            )
            return False

        # Resolve past decorator wrappers (functools.wraps, partial, closures)
        # to the real user function. Without this, instrumenting a decorated
        # symbol like `@login_required def my_view(...)` rewrites the
        # decorator's wrapper, not my_view, and snapshots report the wrong
        # location. Falls back to func when no deeper match is found.
        resolved = undecorated(func, name=qualified_name.rsplit(".", 1)[-1], path=code.co_filename)
        if isinstance(resolved, FunctionType) and resolved is not func:
            func = resolved
            code = func.__code__

        try:
            with self._lock:
                func_id = id(func)
                # Idempotent re-arm: if already armed, restore original first
                # so we can rewrite cleanly with possibly-updated config.
                existing = self._injection_states.get(func_id)
                if existing is not None and existing.function_metadata is not None:
                    try:
                        if existing.function_ref is not None:
                            existing.function_ref.__code__ = existing.original_code
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.debug("Re-arm restore-original failed for %s: %s", function_key, exc)

                rewrite = self._create_code_with_function_wrap(code, function_key)
                if rewrite is None:
                    return False
                new_code, unique_local_names = rewrite

                # Inject globals (idempotent). Line-BP path may also inject
                # _HANDLER_NAME / _LOCALS_NAME for the same func; share without
                # clobbering.
                func.__globals__.setdefault(_FUNCTION_HANDLER_NAME, self._function_event_handler)
                func.__globals__.setdefault(_LOCALS_NAME, locals)

                func.__code__ = new_code

                # Record state. If a line-BP InjectionState already exists for
                # this func, just attach metadata; otherwise create a new one.
                if existing is None:
                    self._injection_states[func_id] = InjectionState(
                        original_code=code,
                        function_ref=func,
                        function_key=function_key,
                        function_metadata={
                            "module_name": module_name,
                            "qualified_name": qualified_name,
                            "capture_config": capture_config,
                            "location_hash": location_hash,
                            "instrumentation_type": instrumentation_type,
                            "file_path": code.co_filename,
                            "unique_local_names": unique_local_names,
                        },
                    )
                else:
                    existing.function_metadata = {
                        "module_name": module_name,
                        "qualified_name": qualified_name,
                        "capture_config": capture_config,
                        "location_hash": location_hash,
                        "instrumentation_type": instrumentation_type,
                        "file_path": code.co_filename,
                        "unique_local_names": unique_local_names,
                    }

                if capture_config is not None:
                    self._capture_configs[(function_key, 0)] = capture_config
                else:
                    self._capture_configs.pop((function_key, 0), None)
                if location_hash:
                    self._location_hashes[(function_key, 0)] = location_hash
                else:
                    self._location_hashes.pop((function_key, 0), None)
                self._instrumentation_types[(function_key, 0)] = instrumentation_type or "PROBE"

                logger.debug("Armed function-level instrumentation for %s", function_key)
                return True

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error arming function-level instrumentation for %s: %s", function_key, exc, exc_info=True)
            return False

    def disable_function_level_instrumentation(self, code: CodeType, func: Optional[FunctionType] = None) -> None:
        """Disarm function-level instrumentation. Restores the original
        __code__ and prunes function-level config entries; preserves any
        line-level BP state armed independently on the same function."""
        if not self._initialized:
            return
        if func is None:
            return
        try:
            with self._lock:
                # State is keyed by id(real_user_func). The manager passes us
                # the wrapper that getattr(module, name) returned, so on a
                # decorated function id(func) != id(real_user_func). Fall
                # back to scanning by function_key recorded at enable time.
                func_id = id(func)
                state = self._injection_states.get(func_id)
                if state is None or state.function_metadata is None:
                    for sid, candidate in self._injection_states.items():
                        if candidate.function_metadata is not None:
                            md = candidate.function_metadata
                            qualname = md.get("qualified_name")
                            if (
                                qualname
                                and md.get("file_path") == code.co_filename
                                and candidate.original_code.co_name == qualname.rsplit(".", 1)[-1]
                            ):
                                # Probabilistic match — for the manager's call
                                # contract, the wrapper's resolved-code lookup
                                # uses qualified_name as co_name. Defensive: we
                                # only match if the wrapper points at the same
                                # globals object as the candidate's function_ref.
                                if (
                                    candidate.function_ref is not None
                                    and candidate.function_ref.__globals__ is func.__globals__
                                ):
                                    func_id = sid
                                    state = candidate
                                    break
                    if state is None or state.function_metadata is None:
                        return  # not armed for function-level

                function_key = state.function_key
                try:
                    if state.function_ref is not None:
                        state.function_ref.__code__ = state.original_code
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.debug("Restore-original failed during disable for %s: %s", function_key, exc)

                self._capture_configs.pop((function_key, 0), None)
                self._location_hashes.pop((function_key, 0), None)
                self._instrumentation_types.pop((function_key, 0), None)
                state.function_metadata = None

                # If no line-BPs armed for this func, drop the InjectionState.
                line_keys = [k for k in self._capture_configs if k[0] == function_key]
                if not line_keys:
                    del self._injection_states[func_id]
                    # Refcount-clear injected globals only when no other
                    # state for the same module needs them.
                    if state.function_ref is not None:
                        globals_obj = state.function_ref.__globals__
                        still_used = any(
                            other.function_ref is not None and other.function_ref.__globals__ is globals_obj
                            for other in self._injection_states.values()
                        )
                        if not still_used:
                            globals_obj.pop(_FUNCTION_HANDLER_NAME, None)
                            globals_obj.pop(_LOCALS_NAME, None)
                            globals_obj.pop(_HANDLER_NAME, None)

                logger.debug("Disarmed function-level instrumentation for %s", function_key)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error disarming function-level instrumentation: %s", exc, exc_info=True)

    @staticmethod
    def _get_service_name():
        try:
            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                return provider.resource.attributes.get(ResourceAttributes.SERVICE_NAME)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    @staticmethod
    def _get_environment():
        try:
            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                attrs = provider.resource.attributes
                return attrs.get(DEPLOYMENT_ENVIRONMENT_NAME) or attrs.get(ResourceAttributes.DEPLOYMENT_ENVIRONMENT)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    def _breakpoint_handler(  # pylint: disable=too-many-locals
        self, function_key: str, line_number: int, local_vars: dict
    ) -> None:
        """
        Handler called from injected bytecode when breakpoint is hit.

        Produces a line-level Snapshot instead of a span event.

        Args:
            function_key: Fully qualified function name (e.g., "mymodule.MyClass.method")
            line_number: Line number of breakpoint
            local_vars: Local variables captured via locals() in injected bytecode
        """
        try:
            # Check rate limit FIRST — skip all expensive work if rate-limited
            if self._hit_count_callback:
                breakpoint_key = f"{function_key}:{line_number}"
                if not self._hit_count_callback(breakpoint_key):
                    return

            # Look up capture config for this breakpoint
            capture_config = self._capture_configs.get((function_key, line_number))

            # Apply capture_locals filtering:
            # None = field absent (do not capture locals)
            # [] = capture all locals
            # ["a", "b"] = capture only those
            capture_locals_list = capture_config.capture_locals if capture_config else None

            # Use per-breakpoint limits if available, otherwise defaults
            serializer = SnapshotSerializer(
                max_fields=capture_config.max_fields_per_object if capture_config else DEFAULT_MAX_FIELDS_PER_OBJECT,
                max_string_length=capture_config.max_string_length if capture_config else DEFAULT_MAX_STRING_LENGTH,
                max_depth=capture_config.max_object_depth if capture_config else 3,
                max_collection_size=capture_config.max_collection_width if capture_config else 10,
            )

            # Serialize local variables into CapturedValue map
            locals_captured = {}
            if capture_locals_list is not None and local_vars:
                try:
                    # Filter out functions, modules, classes, etc.
                    filtered = {
                        k: v
                        for k, v in local_vars.items()
                        if not (
                            inspect.isfunction(v)
                            or inspect.ismodule(v)
                            or inspect.isclass(v)
                            or inspect.ismethod(v)
                            or inspect.isbuiltin(v)
                        )
                    }
                    # Filter by capture_locals list if specific names given
                    if len(capture_locals_list) > 0:
                        filtered = {k: v for k, v in filtered.items() if k in capture_locals_list}
                    locals_captured = serializer.serialize_variables(filtered)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.debug("Failed to capture variables at %s:%s: %s", function_key, line_number, exc)

            # Build line-level captures
            line_context = CapturedContext(locals=locals_captured if locals_captured else None)
            captures = Captures(lines={line_number: line_context})

            # Build instrumentation details
            location_hash = self._location_hashes.get((function_key, line_number), "")
            method_name = function_key.split(".")[-1]
            module_parts = function_key.split(".")
            code_unit = ".".join(module_parts[:-1]) if len(module_parts) > 1 else function_key
            class_name = code_unit  # For Python, className = module path

            instrumentation = InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit=code_unit,
                    class_name=class_name,
                    method_name=method_name,
                    line_number=line_number,
                    language="python",
                ),
            )

            # Read current OTel trace context
            trace_ctx = None
            try:
                span = otel_trace.get_current_span()
                if span and span.get_span_context().is_valid:
                    ctx = span.get_span_context()
                    trace_ctx = TraceContext(
                        trace_id=format(ctx.trace_id, "032x"),
                        span_id=format(ctx.span_id, "016x"),
                    )
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            # Thread info
            current_thread = threading.current_thread()
            thread_info = ThreadInfo(id=threading.get_ident(), name=current_thread.name)

            # Stack trace (only if configured — stack walk is expensive)
            stack = None
            if capture_config and capture_config.capture_stack_trace:
                stack = capture_stack_frames(capture_config.max_stack_frames)

            snapshot = Snapshot(
                timestamp=int(time.time() * 1000),
                location_hash=location_hash or None,
                service=self._get_service_name(),
                environment=self._get_environment(),
                instrumentation=instrumentation,
                trace=trace_ctx,
                thread=thread_info,
                stack=stack,
                captures=captures,
                instrumentation_type="BREAKPOINT",  # Line-level is always BREAKPOINT
            )

            # Emit snapshot (rate limit already checked at top of handler)
            try:
                # pylint: disable=import-outside-toplevel
                from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter

                emitter = get_snapshot_emitter()
                if emitter:
                    emitter.emit_snapshot(snapshot)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            logger.debug("Created line snapshot for breakpoint at %s:%s", function_key, line_number)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in breakpoint handler at %s:%s: %s", function_key, line_number, exc, exc_info=True)

    # Function-level (PROBE / function-level BREAKPOINT) runtime hooks.
    # Single dispatcher named in injected globals; private handlers below.
    # All three short-circuit on the per-thread reentrancy guard so user
    # __repr__ / OTel resource lookup invoked during snapshot serialization
    # cannot recursively re-enter the handler chain on the same thread.

    def _function_event_handler(self, function_key: str, event: str, *args):
        """Single dispatch surface installed in instrumented function globals.

        Returns:
            entry: (start_ns: int, entry_context: Optional[CapturedContext])
                   The bytecode UNPACK_SEQUENCE-2 stores both into local slots.
            exit / unwind: None (POP_TOP discards in injected bytecode).
        """
        if event == "entry":
            return self._function_entry_event_handler(function_key, args[0])
        if event == "exit":
            return self._function_exit_event_handler(function_key, args[0], args[1], args[2])
        if event == "unwind":
            return self._function_unwind_event_handler(function_key, args[0], args[1])
        return None

    def _function_entry_event_handler(
        self, function_key: str, locals_dict: Dict[str, Any]
    ) -> Tuple[int, Optional[CapturedContext]]:
        """Stamp start_ns and capture entry args. Always returns a non-zero
        start_ns so the exit/unwind handlers can compute a duration; the
        entry_context may be None on serializer error or reentrancy."""
        start_ns = time.time_ns()
        if getattr(self._function_reentrancy_guard, "active", False):
            return start_ns, None
        try:
            self._function_reentrancy_guard.active = True
            entry_context = self._build_entry_context(function_key, locals_dict)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Error capturing entry context for %s: %s", function_key, exc)
            entry_context = None
        finally:
            self._function_reentrancy_guard.active = False
        return start_ns, entry_context

    def _function_exit_event_handler(
        self,
        function_key: str,
        retval: Any,
        start_ns: int,
        entry_context: Optional[CapturedContext],
    ) -> None:
        """Build and emit a normal-return Snapshot for function-level."""
        if getattr(self._function_reentrancy_guard, "active", False):
            return
        try:
            self._function_reentrancy_guard.active = True
            self._handle_function_event(
                function_key=function_key,
                start_ns=start_ns,
                entry_context=entry_context,
                retval=retval,
                thrown=None,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in PY_RETURN-equivalent handler for %s: %s", function_key, exc, exc_info=True)
        finally:
            self._function_reentrancy_guard.active = False

    def _function_unwind_event_handler(
        self,
        function_key: str,
        start_ns: int,
        entry_context: Optional[CapturedContext],
    ) -> None:
        """SETUP_FINALLY handler runtime: capture sys.exc_info BEFORE the guard
        check (so reentrant calls still see the live exception via the bytecode
        path's RERAISE)."""
        exc_type, exc_value, exc_tb = sys.exc_info()
        if getattr(self._function_reentrancy_guard, "active", False):
            return
        try:
            self._function_reentrancy_guard.active = True
            self._handle_function_event(
                function_key=function_key,
                start_ns=start_ns,
                entry_context=entry_context,
                retval=None,
                thrown=(exc_type, exc_value, exc_tb),
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in PY_UNWIND-equivalent handler for %s: %s", function_key, exc, exc_info=True)
        finally:
            self._function_reentrancy_guard.active = False

    def _build_entry_context(self, function_key: str, locals_dict: Dict[str, Any]) -> Optional[CapturedContext]:
        """Filter & serialize argument locals at function entry. Mirrors the
        line-BP handler's variable filtering and serializer-config logic."""
        capture_config = self._capture_configs.get((function_key, 0))
        capture_locals_list = capture_config.capture_locals if capture_config else None
        if capture_locals_list is None:
            return None

        serializer = SnapshotSerializer(
            max_fields=capture_config.max_fields_per_object if capture_config else DEFAULT_MAX_FIELDS_PER_OBJECT,
            max_string_length=capture_config.max_string_length if capture_config else DEFAULT_MAX_STRING_LENGTH,
            max_depth=capture_config.max_object_depth if capture_config else 3,
            max_collection_size=capture_config.max_collection_width if capture_config else 10,
        )

        filtered = {
            k: v
            for k, v in locals_dict.items()
            if not (
                k.startswith("_di_")  # exclude our own injected slots
                or inspect.isfunction(v)
                or inspect.ismodule(v)
                or inspect.isclass(v)
                or inspect.ismethod(v)
                or inspect.isbuiltin(v)
            )
        }
        if len(capture_locals_list) > 0:
            filtered = {k: v for k, v in filtered.items() if k in capture_locals_list}
        try:
            arguments = serializer.serialize_variables(filtered)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to serialize entry args for %s: %s", function_key, exc)
            arguments = {}
        return CapturedContext(arguments=arguments if arguments else None)

    def _handle_function_event(  # pylint: disable=too-many-locals,too-many-arguments
        self,
        function_key: str,
        start_ns: int,
        entry_context: Optional[CapturedContext],
        retval: Any,
        thrown: Optional[Tuple[Any, Any, Any]],
    ) -> None:
        """Build and emit a function-level Snapshot. Mutually exclusive:
        retval is set on the normal path, thrown on the exception path."""
        # Hit-count gate first — skip all expensive work if rate-limited.
        breakpoint_key = f"{function_key}:0"
        if self._hit_count_callback and not self._hit_count_callback(breakpoint_key):
            return

        capture_config = self._capture_configs.get((function_key, 0))
        location_hash = self._location_hashes.get((function_key, 0))

        instrumentation_type = self._instrumentation_types.get((function_key, 0), "PROBE")

        method_name = function_key.split(".")[-1]
        module_parts = function_key.split(".")
        code_unit = ".".join(module_parts[:-1]) if len(module_parts) > 1 else function_key
        instrumentation = InstrumentationDetails(
            location=InstrumentationLocation(
                code_unit=code_unit,
                class_name=code_unit,
                method_name=method_name,
                line_number=0,
                language="python",
            ),
        )

        # Build return_context: either a serialized return value or a
        # CapturedThrowable with the live exception's type/message/stack.
        return_context: Optional[CapturedContext] = None
        if thrown is not None:
            exc_type, exc_value, exc_tb = thrown
            return_context = CapturedContext(throwable=self._build_throwable(exc_type, exc_value, exc_tb))
        elif capture_config and capture_config.capture_return:
            try:
                serializer = SnapshotSerializer(
                    max_fields=capture_config.max_fields_per_object,
                    max_string_length=capture_config.max_string_length,
                    max_depth=capture_config.max_object_depth,
                    max_collection_size=capture_config.max_collection_width,
                )
                return_context = CapturedContext(return_value=serializer.serialize(retval))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.debug("Failed to serialize return value for %s: %s", function_key, exc)

        captures = Captures(entry=entry_context, return_context=return_context)

        trace_ctx = None
        try:
            span = otel_trace.get_current_span()
            if span and span.get_span_context().is_valid:
                ctx = span.get_span_context()
                trace_ctx = TraceContext(
                    trace_id=format(ctx.trace_id, "032x"),
                    span_id=format(ctx.span_id, "016x"),
                )
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        current_thread = threading.current_thread()
        thread_info = ThreadInfo(id=threading.get_ident(), name=current_thread.name)

        stack = None
        if capture_config and capture_config.capture_stack_trace:
            stack = capture_stack_frames(capture_config.max_stack_frames)

        duration_ns = max(0, time.time_ns() - start_ns) if start_ns else 0

        snapshot = Snapshot(
            timestamp=int(time.time() * 1000),
            location_hash=location_hash or None,
            service=self._get_service_name(),
            environment=self._get_environment(),
            instrumentation=instrumentation,
            trace=trace_ctx,
            thread=thread_info,
            stack=stack,
            captures=captures,
            instrumentation_type=instrumentation_type,
            duration=duration_ns // 1_000_000 if duration_ns else None,
        )

        try:
            emitter = get_snapshot_emitter()
            if emitter:
                emitter.emit_snapshot(snapshot)
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        logger.debug("Emitted function-level snapshot for %s (thrown=%s)", function_key, thrown is not None)

    @staticmethod
    def _build_throwable(
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> Optional[CapturedThrowable]:
        """Materialize a CapturedThrowable. Merges the in-function traceback
        (extract_tb) with the caller chain (extract_stack), reversed so the
        throw site appears first."""
        if exc_type is None or exc_value is None:
            return None
        try:
            in_func: List[StackFrame] = []
            if exc_tb is not None:
                for frame_summary in traceback.extract_tb(exc_tb):
                    in_func.append(
                        StackFrame(
                            file_name=frame_summary.filename,
                            function=frame_summary.name,
                            line_number=frame_summary.lineno or 0,
                        )
                    )
            caller: List[StackFrame] = []
            for frame_summary in traceback.extract_stack():
                caller.append(
                    StackFrame(
                        file_name=frame_summary.filename,
                        function=frame_summary.name,
                        line_number=frame_summary.lineno or 0,
                    )
                )
            stacktrace = list(reversed(in_func)) + list(reversed(caller))
            return CapturedThrowable(
                type=exc_type.__name__,
                message=str(exc_value) if exc_value is not None else "",
                stacktrace=stacktrace,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to build throwable: %s", exc)
            return None

    def _create_code_with_breakpoints(
        self, code: CodeType, line_numbers: Set[int], function_key: str
    ) -> tuple[Optional[CodeType], Set[int]]:
        """
        Create a new code object with breakpoint handler calls inserted at specified lines.
        Breakpoints are inserted BEFORE the target line executes.

        Note: This function does not modify the original code object.

        Args:
            code: Original code object
            line_numbers: Set of line numbers to insert breakpoints at
            function_key: Fully qualified function name (e.g., "mymodule.MyClass.method")

        Returns:
            Tuple of (new_code, injected_lines):
            - new_code: New code object with breakpoints, or None on failure
            - injected_lines: Set of lines where breakpoints were successfully inserted
        """
        try:
            bc = Bytecode.from_code(code)
            new_instructions = []
            injected_lines = set()

            for instr in bc:
                # Check if we should inject BEFORE this instruction
                if hasattr(instr, "lineno") and instr.lineno in line_numbers and instr.lineno not in injected_lines:

                    # Generate and inject breakpoint instructions BEFORE the line
                    bp_instructions = self._create_breakpoint_instructions(function_key, instr.lineno)

                    if bp_instructions:
                        new_instructions.extend(bp_instructions)
                        injected_lines.add(instr.lineno)
                        logger.debug("Injected breakpoint BEFORE line %s in %s", instr.lineno, code.co_name)

                # Add original instruction AFTER breakpoint
                new_instructions.append(instr)

            # Report missing lines
            missing_lines = line_numbers - injected_lines
            if missing_lines:
                logger.warning(
                    "Could not inject breakpoints at lines %s in %s. Lines may not have executable code.",
                    sorted(missing_lines),
                    code.co_name,
                )

            if not injected_lines:
                logger.warning("No breakpoints were injected in %s", code.co_name)
                return None, set()

            # Create new bytecode and convert to code object
            new_bc = bc.copy()
            new_bc.clear()
            new_bc.extend(new_instructions)

            return new_bc.to_code(), injected_lines

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error injecting bytecode for %s: %s", code.co_name, exc, exc_info=True)
            return None, set()

    def _create_breakpoint_instructions(self, function_key: str, line_number: int) -> Optional[list]:
        """
        Create version-specific breakpoint instructions.

        Args:
            function_key: Fully qualified function name (e.g., "mymodule.MyClass.method")
            line_number: Line number of the breakpoint

        Returns:
            List of bytecode.Instr objects, or None on error
        """
        try:
            if sys.version_info >= (3, 11):
                return self._create_breakpoint_instructions_py311(function_key, line_number)
            if sys.version_info >= (3, 9):
                return self._create_breakpoint_instructions_py39_py310(function_key, line_number)
            logger.error("Unsupported Python version: %s", sys.version_info)
            return None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating breakpoint instructions: %s", exc, exc_info=True)
            return None

    @staticmethod
    def _create_breakpoint_instructions_py311(function_key: str, line_number: int) -> list:
        """
        Create Python 3.11-specific breakpoint instructions.

        Generates bytecode that calls:
        _breakpoint_handler(function_key, line_number, locals())

        Python 3.11 calling convention:
        - PUSH_NULL (for method calls)
        - LOAD_GLOBAL (load _breakpoint_handler)
        - LOAD_CONST (load arguments)
        - LOAD_GLOBAL + PRECALL + CALL (call locals())
        - PRECALL + CALL (call handler)
        - POP_TOP (discard return value)
        """
        return [
            # Setup for method call (_breakpoint_handler)
            Instr("PUSH_NULL"),
            Instr("LOAD_GLOBAL", (False, _HANDLER_NAME)),
            # Load arguments for handler
            Instr("LOAD_CONST", function_key),  # Arg 1: function_key
            Instr("LOAD_CONST", line_number),  # Arg 2: line_number
            # Call locals() to get local variables as Arg 3
            Instr("LOAD_GLOBAL", (True, _LOCALS_NAME)),  # Load locals builtin
            Instr("PRECALL", 0),  # Prepare call to locals() with 0 args
            Instr("CALL", 0),  # Call locals() -> returns dict
            # Call the handler with 3 arguments
            Instr("PRECALL", 3),  # Prepare call to handler with 3 args
            Instr("CALL", 3),  # Call handler(function_key, line_number, locals_dict)
            # Discard return value (handler returns None)
            Instr("POP_TOP"),
        ]

    @staticmethod
    def _create_breakpoint_instructions_py39_py310(function_key: str, line_number: int) -> list:
        """
        Create Python 3.9/3.10-specific breakpoint instructions.

        Generates bytecode that calls:
        _breakpoint_handler(function_key, line_number, locals())

        Python 3.9/3.10 calling convention:
        - LOAD_GLOBAL (load _breakpoint_handler)
        - LOAD_CONST (load arguments)
        - LOAD_GLOBAL + CALL_FUNCTION (call locals())
        - CALL_FUNCTION (call handler)
        - POP_TOP (discard return value)
        """
        return [
            # Load the breakpoint handler function
            Instr("LOAD_GLOBAL", _HANDLER_NAME),
            # Load arguments for handler
            Instr("LOAD_CONST", function_key),  # Arg 1: function_key
            Instr("LOAD_CONST", line_number),  # Arg 2: line_number
            # Call locals() to get local variables as Arg 3
            Instr("LOAD_GLOBAL", _LOCALS_NAME),  # Load locals builtin
            Instr("CALL_FUNCTION", 0),  # Call locals() -> returns dict
            # Call the handler with 3 arguments
            Instr("CALL_FUNCTION", 3),  # Call handler(function_key, line_number, locals_dict)
            # Discard return value (handler returns None)
            Instr("POP_TOP"),
        ]

    # Function-level instrumentation bytecode rewrite (3.9 / 3.10).
    # 3.11 lands in a follow-up commit that uses ConcreteBytecode +
    # exception_table since the high-level API forbids nested TryBegin.

    @staticmethod
    def _unique_local_name(code: CodeType, base: str, taken: Set[str]) -> str:
        """Return a local-slot name not already used by `code` and not in `taken`."""
        existing = set(code.co_varnames) | set(code.co_freevars) | set(code.co_cellvars) | taken
        if base not in existing:
            return base
        i = 1
        while f"{base}_{i}" in existing:
            i += 1
        return f"{base}_{i}"

    def _create_code_with_function_wrap(  # pylint: disable=too-many-locals
        self,
        code: CodeType,
        function_key: str,
    ) -> Optional[tuple]:
        """Wrap a function body with entry / exit / unwind hooks.

        Returns (new_code, unique_local_names) on success, None on failure or
        refusal. unique_local_names is the 3-tuple (start_ns, entry_ctx, retval)
        of slot names to be stored on InjectionState for restoration audits.

        Bytecode shape (3.9/3.10):
            <pre-init three locals to None>
            SETUP_FINALLY @handler_label
            <call entry hook -> (start_ns, entry_ctx); store both>
            <user body, with each RETURN_VALUE replaced by:
                STORE_FAST retval
                <call exit hook(retval, start_ns, entry_ctx)>
                POP_TOP
                POP_BLOCK
                LOAD_FAST retval
                RETURN_VALUE
            >
            handler_label:
                <call unwind hook(start_ns, entry_ctx)>
                POP_TOP
                RERAISE   (no oparg on 3.9; RERAISE 0 on 3.10)
        """
        if sys.version_info >= (3, 11):
            return self._create_code_with_function_wrap_311(code, function_key)
        if not (3, 9) <= sys.version_info < (3, 11):
            return None

        try:
            # pylint: disable=import-outside-toplevel
            from bytecode import Label

            bc = Bytecode.from_code(code)

            # Allocate three uniquely-named local slots. Pre-init them to None
            # (right after the SETUP_FINALLY arm point) so the unwind hook can
            # LOAD_FAST them even if the entry hook itself raised.
            taken: Set[str] = set()
            start_ns_slot = self._unique_local_name(code, "_di_start_ns", taken)
            taken.add(start_ns_slot)
            entry_ctx_slot = self._unique_local_name(code, "_di_entry_ctx", taken)
            taken.add(entry_ctx_slot)
            retval_slot = self._unique_local_name(code, "_di_retval", taken)
            taken.add(retval_slot)

            handler_label = Label()
            prologue = self._build_function_prologue(function_key, start_ns_slot, entry_ctx_slot, retval_slot)
            prologue.append(Instr("SETUP_FINALLY", handler_label))
            prologue.extend(self._build_function_entry_call(function_key, start_ns_slot, entry_ctx_slot))

            # Walk user instructions; replace RETURN_VALUE with exit-call sequence.
            new_instructions = list(prologue)
            for instr in bc:
                if hasattr(instr, "name") and instr.name == "RETURN_VALUE":
                    new_instructions.extend(
                        self._build_function_exit_call(function_key, start_ns_slot, entry_ctx_slot, retval_slot)
                    )
                else:
                    new_instructions.append(instr)

            # Append unwind handler at end.
            new_instructions.append(handler_label)
            new_instructions.extend(self._build_function_unwind_call(function_key, start_ns_slot, entry_ctx_slot))

            new_bc = bc.copy()
            new_bc.clear()
            new_bc.extend(new_instructions)
            return new_bc.to_code(), (start_ns_slot, entry_ctx_slot, retval_slot)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error wrapping function %s with function-level hooks: %s", code.co_name, exc, exc_info=True)
            return None

    @staticmethod
    def _build_function_prologue(
        function_key: str,
        start_ns_slot: str,
        entry_ctx_slot: str,
        retval_slot: str,
    ) -> list:
        """Pre-init the three injected slots to None so unwind LOAD_FAST is safe
        even if the entry hook crashes before storing them. function_key is
        unused but accepted for symmetry with the other builders."""
        del function_key
        return [
            Instr("LOAD_CONST", None),
            Instr("STORE_FAST", start_ns_slot),
            Instr("LOAD_CONST", None),
            Instr("STORE_FAST", entry_ctx_slot),
            Instr("LOAD_CONST", None),
            Instr("STORE_FAST", retval_slot),
        ]

    @staticmethod
    def _build_function_entry_call(function_key: str, start_ns_slot: str, entry_ctx_slot: str) -> list:
        """Emit: tup = handler(function_key, "entry", locals()); start_ns, entry_ctx = tup."""
        return [
            Instr("LOAD_GLOBAL", _FUNCTION_HANDLER_NAME),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_CONST", "entry"),
            Instr("LOAD_GLOBAL", _LOCALS_NAME),
            Instr("CALL_FUNCTION", 0),
            Instr("CALL_FUNCTION", 3),
            Instr("UNPACK_SEQUENCE", 2),
            Instr("STORE_FAST", start_ns_slot),
            Instr("STORE_FAST", entry_ctx_slot),
        ]

    @staticmethod
    def _build_function_exit_call(
        function_key: str,
        start_ns_slot: str,
        entry_ctx_slot: str,
        retval_slot: str,
    ) -> list:
        """Replace RETURN_VALUE with: store retval, call exit hook, POP_BLOCK,
        re-load retval, RETURN_VALUE. POP_BLOCK must come BEFORE the final
        RETURN_VALUE so the SETUP_FINALLY block is unwound on the normal path."""
        return [
            Instr("STORE_FAST", retval_slot),
            Instr("LOAD_GLOBAL", _FUNCTION_HANDLER_NAME),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_CONST", "exit"),
            Instr("LOAD_FAST", retval_slot),
            Instr("LOAD_FAST", start_ns_slot),
            Instr("LOAD_FAST", entry_ctx_slot),
            Instr("CALL_FUNCTION", 5),
            Instr("POP_TOP"),
            Instr("POP_BLOCK"),
            Instr("LOAD_FAST", retval_slot),
            Instr("RETURN_VALUE"),
        ]

    @staticmethod
    def _build_function_unwind_call(function_key: str, start_ns_slot: str, entry_ctx_slot: str) -> list:
        """SETUP_FINALLY handler body: call unwind hook then RERAISE.

        3.9: RERAISE has no oparg.
        3.10: RERAISE takes oparg 0 (re-raise top exception, no f_lasti restore).
        """
        instrs = [
            Instr("LOAD_GLOBAL", _FUNCTION_HANDLER_NAME),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_CONST", "unwind"),
            Instr("LOAD_FAST", start_ns_slot),
            Instr("LOAD_FAST", entry_ctx_slot),
            Instr("CALL_FUNCTION", 4),
            Instr("POP_TOP"),
        ]
        if sys.version_info >= (3, 10):
            instrs.append(Instr("RERAISE", 0))
        else:
            instrs.append(Instr("RERAISE"))
        return instrs

    # Function-level instrumentation bytecode rewrite (3.11).
    #
    # 3.11 removed SETUP_FINALLY / block stack: protected regions are described
    # by PEP 657's co_exceptiontable. The high-level bytecode API forbids
    # nested TryBegin, so we use a "splice walk": every time a user TryBegin is
    # encountered we close our outer protected region with a TryEnd before it,
    # and on the matching user TryEnd we reopen a fresh outer TryBegin after.
    # The resulting layout is N adjacent (never nested) outer regions all
    # targeting one shared except-label foot.

    @staticmethod
    def _build_function_prologue_311(start_ns_slot: str, entry_ctx_slot: str, retval_slot: str) -> list:
        """Pre-init the three injected slots to None so unwind LOAD_FAST is safe
        even if the entry hook crashes before storing them."""
        return [
            Instr("LOAD_CONST", None),
            Instr("STORE_FAST", start_ns_slot),
            Instr("LOAD_CONST", None),
            Instr("STORE_FAST", entry_ctx_slot),
            Instr("LOAD_CONST", None),
            Instr("STORE_FAST", retval_slot),
        ]

    @staticmethod
    def _build_function_entry_call_311(function_key: str, start_ns_slot: str, entry_ctx_slot: str) -> list:
        """3.11 calling convention: PUSH_NULL + LOAD_GLOBAL((push_null, name))
        + args + PRECALL n + CALL n. LOAD_GLOBAL takes a TUPLE oparg
        (push_null: bool, name: str); the True flag pushes NULL implicitly so
        no separate PUSH_NULL is needed for the locals() builtin call."""
        return [
            Instr("PUSH_NULL"),
            Instr("LOAD_GLOBAL", (False, _FUNCTION_HANDLER_NAME)),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_CONST", "entry"),
            Instr("LOAD_GLOBAL", (True, _LOCALS_NAME)),
            Instr("PRECALL", 0),
            Instr("CALL", 0),
            Instr("PRECALL", 3),
            Instr("CALL", 3),
            Instr("UNPACK_SEQUENCE", 2),
            Instr("STORE_FAST", start_ns_slot),
            Instr("STORE_FAST", entry_ctx_slot),
        ]

    @staticmethod
    def _build_function_exit_call_311(
        function_key: str,
        start_ns_slot: str,
        entry_ctx_slot: str,
        retval_slot: str,
    ) -> list:
        """Replace RETURN_VALUE with: store retval, call exit hook, reload
        retval, RETURN_VALUE. No POP_BLOCK on 3.11 — protected regions live in
        co_exceptiontable, not on a runtime block stack."""
        return [
            Instr("STORE_FAST", retval_slot),
            Instr("PUSH_NULL"),
            Instr("LOAD_GLOBAL", (False, _FUNCTION_HANDLER_NAME)),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_CONST", "exit"),
            Instr("LOAD_FAST", retval_slot),
            Instr("LOAD_FAST", start_ns_slot),
            Instr("LOAD_FAST", entry_ctx_slot),
            Instr("PRECALL", 5),
            Instr("CALL", 5),
            Instr("POP_TOP"),
            Instr("LOAD_FAST", retval_slot),
            Instr("RETURN_VALUE"),
        ]

    @staticmethod
    def _build_function_unwind_foot_311(function_key: str, start_ns_slot: str, entry_ctx_slot: str) -> list:
        """Shared except-label foot for the N adjacent outer protected regions.

        With push_lasti=True the handler is entered with stack [..., lasti, exc].
        We must:
          1. PUSH_EXC_INFO so the hook's sys.exc_info() call sees the live
             exception (without this, sys.exc_info() returns the prior frame's
             exception state and the captured throwable comes back None).
             Stack after: [..., lasti, prev_exc, exc].
          2. Call our unwind hook on top of that state (POP_TOP the result).
          3. RERAISE 0 — pops exc from TOS and reraises. Frame unwinds and the
             runtime cleans up remaining stack items (prev_exc, lasti).

        We don't preserve lasti for f_lasti restoration — the exception object
        already carries its own traceback chain, which is what the hook reads."""
        return [
            Instr("PUSH_EXC_INFO"),  # [..., lasti, prev_exc, exc]; sys.exc_info -> exc
            Instr("PUSH_NULL"),
            Instr("LOAD_GLOBAL", (False, _FUNCTION_HANDLER_NAME)),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_CONST", "unwind"),
            Instr("LOAD_FAST", start_ns_slot),
            Instr("LOAD_FAST", entry_ctx_slot),
            Instr("PRECALL", 4),
            Instr("CALL", 4),
            Instr("POP_TOP"),  # discard hook return -> [..., lasti, prev_exc, exc]
            Instr("RERAISE", 0),  # pop exc, reraise; runtime unwinds remaining stack
        ]

    def _create_code_with_function_wrap_311(  # pylint: disable=too-many-locals,too-many-statements
        self,
        code: CodeType,
        function_key: str,
    ) -> Optional[tuple]:
        """3.11 function-level rewrite via PEP 657 exception_table.

        Algorithm:
          1. Allocate three unique local slots (start_ns, entry_ctx, retval).
          2. Insert prologue + entry-call AFTER the preamble (RESUME 0 plus any
             COPY_FREE_VARS / MAKE_CELL) so closures and free-vars are set up
             before our entry hook fires.
          3. Open an outer TryBegin(handler_label, push_lasti=True).
          4. Splice-walk the user instruction stream: on every user TryBegin
             close our outer region with TryEnd before it; on the matching
             user TryEnd reopen with a fresh TryBegin after it. This produces
             N adjacent (never nested) outer regions all targeting one shared
             handler label.
          5. Replace each user RETURN_VALUE with the exit-call sequence
             (store retval, call exit hook, reload retval, RETURN_VALUE).
          6. Close the final outer region with TryEnd, append the handler
             label, and emit the unwind foot (POP_TOP lasti, call unwind hook,
             RERAISE 1).
          7. Build via Bytecode.to_code(compute_exception_stack_depths=True).

        Returns (new_code, (start_ns_slot, entry_ctx_slot, retval_slot)) on
        success, None on refusal/failure.
        """
        try:
            bc = Bytecode.from_code(code)

            # Allocate three uniquely-named local slots, pre-inited to None.
            taken: Set[str] = set()
            start_ns_slot = self._unique_local_name(code, "_di_start_ns", taken)
            taken.add(start_ns_slot)
            entry_ctx_slot = self._unique_local_name(code, "_di_entry_ctx", taken)
            taken.add(entry_ctx_slot)
            retval_slot = self._unique_local_name(code, "_di_retval", taken)
            taken.add(retval_slot)

            # Find preamble end: skip past RESUME and any COPY_FREE_VARS /
            # MAKE_CELL so our entry hook fires only after closures are armed.
            preamble_end = 0
            for idx, instr in enumerate(bc):
                if isinstance(instr, Instr) and instr.name in (
                    "RESUME",
                    "COPY_FREE_VARS",
                    "MAKE_CELL",
                ):
                    preamble_end = idx + 1

            handler_label = Label()

            new_instructions: list = []

            # Copy preamble unchanged.
            new_instructions.extend(bc[:preamble_end])

            # Prologue + entry call (must run after preamble, before outer
            # protected region opens — entry-hook failure is logged by the
            # handler itself; pre-inited None slots keep the unwind path safe
            # if the entry call itself raises after store).
            new_instructions.extend(self._build_function_prologue_311(start_ns_slot, entry_ctx_slot, retval_slot))
            new_instructions.extend(self._build_function_entry_call_311(function_key, start_ns_slot, entry_ctx_slot))

            # Open outer protected region.
            current_outer_begin = TryBegin(handler_label, push_lasti=True)
            new_instructions.append(current_outer_begin)

            # Splice-walk body: maintain "outer is open" invariant by closing
            # our outer region before each user TryBegin and reopening after
            # the matching user TryEnd. Replace RETURN_VALUE with exit-call
            # sequence. Each TryEnd needs a reference to the matching
            # TryBegin — for outer closes we pass our current_outer_begin;
            # user TryEnd objects already carry their own back-reference and
            # are passed through unchanged.
            for instr in bc[preamble_end:]:
                if isinstance(instr, TryBegin):
                    # Close outer before user try, then emit user TryBegin.
                    new_instructions.append(TryEnd(current_outer_begin))
                    new_instructions.append(instr)
                elif isinstance(instr, TryEnd):
                    # Pass user TryEnd through, then reopen outer.
                    new_instructions.append(instr)
                    current_outer_begin = TryBegin(handler_label, push_lasti=True)
                    new_instructions.append(current_outer_begin)
                elif isinstance(instr, Instr) and instr.name == "RETURN_VALUE":
                    new_instructions.extend(
                        self._build_function_exit_call_311(function_key, start_ns_slot, entry_ctx_slot, retval_slot)
                    )
                else:
                    new_instructions.append(instr)

            # Close the final outer protected region.
            new_instructions.append(TryEnd(current_outer_begin))

            # Handler label + foot.
            new_instructions.append(handler_label)
            new_instructions.extend(self._build_function_unwind_foot_311(function_key, start_ns_slot, entry_ctx_slot))

            new_bc = bc.copy()
            new_bc.clear()
            new_bc.extend(new_instructions)
            return (
                new_bc.to_code(compute_exception_stack_depths=True),
                (start_ns_slot, entry_ctx_slot, retval_slot),
            )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug(
                "Error wrapping function %s with 3.11 function-level hooks: %s",
                code.co_name,
                exc,
                exc_info=True,
            )
            return None

    def cleanup(self) -> None:
        """
        Clean up all bytecode modifications and restore original code.

        Restores all modified functions to their original state and cleans up
        injected globals. Never raises exceptions.

        This method iterates through all injection states and calls the same
        restoration logic used by disable_breakpoints_for_function.
        """
        try:
            if not self._initialized:
                logger.debug("Engine not initialized, nothing to clean up")
                return

            with self._lock:
                # Restore all modified functions
                restored_count = 0
                failed_count = 0

                for _, state in list(self._injection_states.items()):
                    try:
                        if state.function_ref and state.original_code:
                            # Restore original code (same logic as disable_breakpoints_for_function)
                            state.function_ref.__code__ = state.original_code

                            # Clean up injected globals
                            state.function_ref.__globals__.pop(_HANDLER_NAME, None)
                            state.function_ref.__globals__.pop(_LOCALS_NAME, None)

                            restored_count += 1
                            logger.debug("Restored function %s", state.original_code.co_name)

                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        failed_count += 1
                        logger.warning("Failed to restore function during cleanup: %s", exc, exc_info=True)
                        # Continue cleanup even if one function fails
                        continue

                # Clear all state
                self._injection_states.clear()
                self._location_hashes.clear()
                self._capture_configs.clear()
                self._instrumentation_types.clear()

                logger.debug(
                    "BytecodeInjectionEngine cleaned up: %d functions restored, %d failed", restored_count, failed_count
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error during cleanup: %s", exc, exc_info=True)
        finally:
            with self._lock:
                self._initialized = False
                self._hit_count_callback = None
