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
from collections import deque
from dataclasses import dataclass
from functools import partial
from types import CodeType, FunctionType
from typing import Any, Dict, List, Optional, Set

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED
from amazon.opentelemetry.distro.debugger._data_models import (
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_STRING_LENGTH,
    CaptureConfig,
)
from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    ThreadInfo,
    TraceContext,
)
from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer
from amazon.opentelemetry.distro.debugger._stack_utils import capture_stack_frames
from amazon.opentelemetry.distro.debugger.instrumentation_engine._instrumentation_engine import InstrumentationEngine

logger = logging.getLogger(__name__)

# Global name for the breakpoint handler function injected into function globals
_HANDLER_NAME = "_breakpoint_handler"
# Global name for the locals() builtin injected into function globals
_LOCALS_NAME = "_breakpoint_locals"

# Global names used by the function-entry/exit injection (PROBE / function-level
# BREAKPOINT). Distinct from the line-level breakpoint handler so both can
# coexist on the same function via a single combined bytecode rewrite.
_ENTRY_HANDLER_NAME = "_di_function_entry_handler"
_EXIT_HANDLER_NAME = "_di_function_exit_handler"

# Temp local-variable name used by the exit injection to stash the return value
# across the handler call. Resolved against ``code.co_varnames`` at injection
# time and made unique if it collides with a user local. Mirrors Datadog's
# ddtrace.internal.wrapping.context approach.
_RETVAL_LOCAL_NAME = "__otel_di_retval__"

# Import bytecode classes if available
if IS_BYTECODE_INSTALLED:
    from bytecode import Bytecode, Instr
else:
    Bytecode = None  # type: ignore[misc, assignment]
    Instr = None  # type: ignore[misc, assignment]


@dataclass
class InjectionState:
    """
    Tracks bytecode injection state for a function.

    Only stores what's needed for restoration and callback:
    - original_code: To restore the function's original bytecode
    - function_ref: To access the function for restoration and cleanup
    - function_key: For constructing breakpoint_key in callback
    """

    original_code: CodeType
    function_ref: Optional[FunctionType]
    function_key: str


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
        # Maps (function_key, line_number) to location_hash for span events
        self._location_hashes: Dict[tuple, str] = {}
        # Maps (function_key, line_number) to CaptureConfig for filtering captured data
        self._capture_configs: Dict[tuple, CaptureConfig] = {}

        # Function-entry instrumentation state (PROBE / function-level BREAKPOINT).
        # Mirrors SysMonitoringEngine for behavioral parity. Keyed by id(func) —
        # the bytecode rewrite mutates that function's __code__ in place, so
        # every existing reference (Django URLPattern.callback, Flask
        # view_functions, decorator closures, framework registries) executes
        # the new bytecode on its next call without registry traversal.
        # Per Datadog ddtrace.internal.wrapping.context.
        self._function_entries: Dict[int, Dict[str, Any]] = {}
        # Per-thread LIFO stack pairs PY_START-equivalent entry callbacks with
        # PY_RETURN-equivalent exit callbacks across recursion / nested calls.
        self._tls = threading.local()
        # Reentrancy guard so a snapshot built from inside a handler cannot
        # itself recursively trigger another snapshot via the wrapper path.
        self._reentrancy_guard = threading.local()

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

                # Clean up state
                del self._injection_states[func_id]
                logger.debug("Disabled all breakpoints for %s", function_key)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error disabling breakpoints for %s: %s", code.co_name, exc, exc_info=True)

    # ------------------------------------------------------------------
    # Function-entry instrumentation (PROBE / function-level BREAKPOINT)
    # ------------------------------------------------------------------
    # Mirrors SysMonitoringEngine.enable_function_entry for behavioral parity
    # across Python versions. The interception layer is the function's __code__
    # object: we rewrite it via ``func.__code__ = new_code``. Because
    # ``__code__`` is read off the FunctionType at every call dispatch (CPython
    # data model), every existing reference (Django ``URLPattern.callback``,
    # Flask ``view_functions[endpoint]``, decorator closures, framework
    # registries) executes the rewritten bytecode on its next invocation —
    # without any registry traversal. This is the same architecture Datadog
    # ships in production via ``ddtrace.internal.wrapping.context``.

    def enable_function_entry(  # pylint: disable=too-many-arguments
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
        """
        Inject function-entry/exit bytecode hooks into a function.

        Walks ``func.__wrapped__`` / ``partial.func`` / closure cells to find
        the user's underlying function under ``functools.wraps`` decorators
        (Django ``@login_required``, ``@cache_page``, custom ``@wraps`` decorators)
        before rewriting bytecode — otherwise we'd instrument the auth/cache
        check rather than the user code.

        Args:
            code: Code object of the function (the immediate target — may be
                a wrapper if the function is decorated; ``_undecorated`` tries
                to resolve to the inner function first).
            func: Function object — used for ``__code__`` rewrite, decorator
                resolution, and as a key into ``_function_entries``.
            function_key: ``module.qualname`` for snapshot routing / hit-count keys.
            module_name: Module name (component of the snapshot's CodeUnit).
            qualified_name: Qualified function name (component of MethodName).
            capture_config: Controls argument / return / stack capture.
            location_hash: LocationHash to attach to the emitted snapshot.
            instrumentation_type: ``"PROBE"`` or ``"BREAKPOINT"`` — surfaced
                as ``aws.di.instrumentation_type`` on the snapshot.

        Returns:
            True on success, False if the engine isn't initialized, the target
            is a generator/async-generator (we don't instrument those — see
            below), or the bytecode rewrite failed.
        """
        if not self._initialized:
            logger.warning("BytecodeInjectionEngine not initialized, cannot enable function entry")
            return False

        if not IS_BYTECODE_INSTALLED:
            logger.warning("bytecode library not available, cannot enable function entry for %s", function_key)
            return False

        try:
            # Resolve through @functools.wraps / partial / closure cells to the
            # user function. If the user wrote @login_required at the route,
            # ``func`` is the wrapper; the actual view body is reachable via
            # ``func.__wrapped__``. Without this, we'd rewrite the wrapper's
            # bytecode (the auth check) — instrumentation would fire but at
            # the wrong layer.
            target_func = self._undecorated(func, qualified_name.split(".")[-1])
            target_code = target_func.__code__ if hasattr(target_func, "__code__") else code

            # Skip generators / async-generators / iterable-coroutines. Their
            # exit point is YIELD_VALUE (not RETURN_VALUE), and instrumenting
            # YIELD_VALUE would corrupt the .send() value protocol. Regular
            # async functions (CO_COROUTINE without CO_ASYNC_GENERATOR) only
            # hit RETURN_VALUE on coroutine completion — they're safe.
            generator_flags = (
                inspect.CO_GENERATOR | inspect.CO_ASYNC_GENERATOR | inspect.CO_ITERABLE_COROUTINE
            )
            if target_code.co_flags & generator_flags:
                logger.debug(
                    "Skipping function-entry injection for generator/async-generator %s",
                    function_key,
                )
                return False

            # Build new bytecode outside the lock — pure function over `code`.
            new_code = self._create_code_with_function_entry_exit(target_code, function_key)
            if new_code is None:
                logger.warning("Failed to inject function entry/exit bytecode for %s", function_key)
                return False

            with self._lock:
                func_id = id(target_func)
                self._function_entries[func_id] = {
                    "func": target_func,
                    "function_key": function_key,
                    "module_name": module_name,
                    "qualified_name": qualified_name,
                    "capture_config": capture_config,
                    "location_hash": location_hash,
                    "instrumentation_type": instrumentation_type,
                    "original_code": target_code,
                }

                target_func.__code__ = new_code
                # Make handlers + locals reachable via LOAD_GLOBAL in the
                # injected bytecode. ``self._function_entry_handler`` is a
                # bound method — the bytecode CALL passes only the explicit
                # args (function_key + locals_dict on entry; function_key +
                # retval on exit).
                target_func.__globals__[_ENTRY_HANDLER_NAME] = self._function_entry_handler
                target_func.__globals__[_EXIT_HANDLER_NAME] = self._function_exit_handler
                # Reuse the locals builtin global (same as line breakpoints).
                target_func.__globals__[_LOCALS_NAME] = locals

                logger.debug(
                    "Enabled function entry for %s (func_id=%s, code_id=%s, type=%s)",
                    function_key,
                    func_id,
                    id(target_code),
                    instrumentation_type,
                )
                return True

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to enable function entry for %s: %s", function_key, exc, exc_info=True)
            return False

    def disable_function_entry(self, code: CodeType, func: Optional[FunctionType] = None) -> None:
        """
        Tear down the function-entry hook installed for a function.

        Restores the function's original bytecode via ``func.__code__ =
        original_code``. The caller passes ``func`` so we can look up by
        ``id(func)`` directly (the engine's tracking-dict key); ``code`` is
        kept in the signature for API symmetry with
        ``disable_breakpoints_for_function`` and is unused here.

        Once restored, every stale reference (registries, decorator closures)
        immediately dispatches back to the uninstrumented code on next call —
        the same property that made the install transparent.
        """
        if not self._initialized:
            return
        try:
            with self._lock:
                # Primary lookup: by id(func) when caller provides it.
                target_func_id = None
                if func is not None and id(func) in self._function_entries:
                    target_func_id = id(func)
                else:
                    # Fallback: scan for an entry whose function's __code__ has
                    # the same id as `code`. This handles callers that only
                    # have a code reference (e.g., the line-level path's call
                    # site that mirrors disable_breakpoints_for_function's
                    # signature). Note: the entry's func.__code__ is the
                    # REWRITTEN code after enable, so we also accept matches
                    # against entry["original_code"].
                    for func_id, entry in self._function_entries.items():
                        entry_func = entry.get("func")
                        if entry_func is None:
                            continue
                        if (
                            entry_func.__code__ is code
                            or entry.get("original_code") is code
                        ):
                            target_func_id = func_id
                            break
                if target_func_id is None:
                    return
                entry = self._function_entries.pop(target_func_id)
                target_func = entry["func"]
                try:
                    target_func.__code__ = entry["original_code"]
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to restore __code__ for %s: %s",
                        entry.get("function_key"),
                        exc,
                    )
                logger.debug("Disabled function entry for %s", entry.get("function_key"))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to disable function entry: %s", exc, exc_info=True)

    def suppress_function_entry_for_thread(self) -> None:
        """
        Mark the current thread as 'inside the FunctionWrapper' so the
        injected entry/exit handlers skip — preventing double-emission when
        the caller went through the legacy wrapper path (module attribute)
        instead of via a stale reference.

        Called by ``FunctionWrapper`` before invoking ``original_func``.
        """
        self._reentrancy_guard.active = True

    def release_function_entry_suppression(self) -> None:
        """Counterpart to ``suppress_function_entry_for_thread``."""
        self._reentrancy_guard.active = False

    def _function_entry_handler(self, function_key: str, local_vars: dict) -> None:
        """
        Bytecode-injected callback at function entry.

        Captures arguments (filtered by capture_config), pushes a per-thread
        frame onto ``self._tls.stack`` with the start time. The companion
        ``_function_exit_handler`` pops that frame and emits the snapshot.

        CRITICAL: must never raise — the injected bytecode does not catch.
        """
        try:
            # Re-entrancy guard — wrapper path is in flight, skip.
            if getattr(self._reentrancy_guard, "active", False):
                return

            # Look up the entry. The handler is the SAME bound method for every
            # instrumented function; we need function_key to disambiguate.
            entry = self._find_entry_by_key(function_key)
            if entry is None:
                return

            capture_config = entry.get("capture_config")
            entry_context = self._build_entry_context(local_vars, capture_config)

            stack = getattr(self._tls, "stack", None)
            if stack is None:
                stack = []
                self._tls.stack = stack
            stack.append(
                {
                    "function_key": function_key,
                    "start_ns": time.time_ns(),
                    "entry_context": entry_context,
                }
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Critical error in entry handler for %s: %s", function_key, exc, exc_info=True)

    def _function_exit_handler(self, function_key: str, retval: Any) -> None:
        """
        Bytecode-injected callback before each RETURN_VALUE.

        Pops the matching entry frame, computes duration, builds the
        function-entry snapshot, and emits via the global emitter.

        CRITICAL: must never raise. The injected bytecode discards this
        function's return value via POP_TOP and re-pushes the user's retval
        from the temp local before the original RETURN_VALUE — so we don't
        need to return retval here.
        """
        try:
            if getattr(self._reentrancy_guard, "active", False):
                return

            entry = self._find_entry_by_key(function_key)
            if entry is None:
                return

            stack = getattr(self._tls, "stack", None)
            if not stack:
                return

            # Pop the most recent frame matching this function_key. Recursive
            # calls through the same function will produce nested frames; the
            # LIFO scan handles them correctly.
            frame_info = None
            for i in range(len(stack) - 1, -1, -1):
                if stack[i]["function_key"] == function_key:
                    frame_info = stack.pop(i)
                    break
            if frame_info is None:
                return

            self._reentrancy_guard.active = True
            try:
                self._handle_function_entry(entry, frame_info, retval)
            finally:
                self._reentrancy_guard.active = False
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Critical error in exit handler for %s: %s", function_key, exc, exc_info=True)

    def _find_entry_by_key(self, function_key: str) -> Optional[Dict[str, Any]]:
        """Lookup the registered entry by function_key. Lock-free read.

        ``self._function_entries`` is keyed by ``id(func)`` for fast disable,
        but the injected bytecode only knows ``function_key``. We linear-scan
        — this is fine because the dict typically has a few dozen entries
        and the read is O(1) per probe. CPython dict iteration is GIL-atomic.
        """
        try:
            for entry in self._function_entries.values():
                if entry.get("function_key") == function_key:
                    return entry
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    @staticmethod
    def _build_entry_context(
        local_vars: dict, capture_config: Optional[CaptureConfig]
    ) -> Optional[CapturedContext]:
        """
        Filter ``local_vars`` (which is the full ``locals()`` dict captured by
        injected bytecode) by ``capture_config.capture_arguments`` and serialize.
        """
        if capture_config is None or capture_config.capture_arguments is None:
            return None
        try:
            args_dict = local_vars
            if capture_config.capture_arguments:
                args_dict = {k: v for k, v in args_dict.items() if k in capture_config.capture_arguments}
            if not args_dict:
                return None
            serializer = SnapshotSerializer(
                max_fields=capture_config.max_fields_per_object or DEFAULT_MAX_FIELDS_PER_OBJECT,
                max_string_length=capture_config.max_string_length or DEFAULT_MAX_STRING_LENGTH,
                max_depth=capture_config.max_object_depth or 3,
                max_collection_size=capture_config.max_collection_width or 10,
            )
            return CapturedContext(arguments=serializer.serialize_variables(args_dict))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to build entry context: %s", exc)
            return None

    def _handle_function_entry(  # pylint: disable=too-many-locals
        self,
        entry: Dict[str, Any],
        frame_info: Dict[str, Any],
        retval: Any,
    ) -> None:
        """Build and emit the function-entry snapshot. Mirrors SysMonitoringEngine."""
        try:
            function_key = entry["function_key"]
            capture_config = entry.get("capture_config")

            # Hit-count rate limit (key = function_key:0 for function-level).
            if self._hit_count_callback is not None:
                if not self._hit_count_callback(f"{function_key}:0"):
                    return

            duration_ns = time.time_ns() - frame_info["start_ns"]
            entry_context = frame_info.get("entry_context")

            return_context = None
            if capture_config is not None and capture_config.capture_return and retval is not None:
                serializer = SnapshotSerializer(
                    max_fields=capture_config.max_fields_per_object or DEFAULT_MAX_FIELDS_PER_OBJECT,
                    max_string_length=capture_config.max_string_length or DEFAULT_MAX_STRING_LENGTH,
                    max_depth=capture_config.max_object_depth or 3,
                    max_collection_size=capture_config.max_collection_width or 10,
                )
                return_context = CapturedContext()
                return_context.return_value = serializer.serialize(retval)

            module_name = entry["module_name"]
            qualified_name = entry["qualified_name"]
            method_name = qualified_name.split(".")[-1]
            class_part = ".".join(qualified_name.split(".")[:-1]) if "." in qualified_name else None
            class_name_fq = f"{module_name}.{class_part}" if class_part else module_name
            file_path = getattr(entry["original_code"], "co_filename", None)

            instrumentation = InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit=module_name,
                    class_name=class_name_fq,
                    method_name=method_name,
                    line_number=0,  # 0 = function-level per spec
                    file_path=file_path,
                    language="python",
                ),
            )

            current_thread = threading.current_thread()
            thread_info = ThreadInfo(id=threading.get_ident(), name=current_thread.name)

            # Trace context (read-only — never start a span here). Mirrors
            # SysMonitoringEngine so snapshots correlate with the active span.
            trace_ctx = None
            try:
                from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel

                span = otel_trace.get_current_span()
                if span and span.get_span_context().is_valid:
                    sctx = span.get_span_context()
                    trace_ctx = TraceContext(
                        trace_id=format(sctx.trace_id, "032x"),
                        span_id=format(sctx.span_id, "016x"),
                    )
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            # Stack trace (only when configured — stack walk is expensive).
            stack = None
            if capture_config is not None and capture_config.capture_stack_trace:
                stack = capture_stack_frames(capture_config.max_stack_frames)

            captures = Captures(entry=entry_context, return_context=return_context)

            duration_ms = duration_ns // 1_000_000 if duration_ns else None

            snapshot = Snapshot(
                timestamp=int(time.time() * 1000),
                duration=duration_ms,
                service=self._get_service_name(),
                environment=self._get_environment(),
                location_hash=entry.get("location_hash"),
                instrumentation=instrumentation,
                trace=trace_ctx,
                thread=thread_info,
                stack=stack,
                captures=captures,
                instrumentation_type=entry.get("instrumentation_type"),
            )

            try:
                # pylint: disable=import-outside-toplevel
                from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter

                emitter = get_snapshot_emitter()
                if emitter is not None:
                    emitter.emit_snapshot(snapshot)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error handling function entry for %s: %s", entry.get("function_key"), exc, exc_info=True)

    @staticmethod
    def _undecorated(f: Any, expected_name: str, max_depth: int = 10) -> Any:
        """
        BFS over ``__wrapped__`` / ``partial.func`` / closure cells to find the
        innermost user function under ``functools.wraps`` decorators.

        Port of Datadog's algorithm at ``ddtrace/internal/utils/inspection.py:42-100``.
        Matches on ``co_name`` so we resolve to the function whose source name
        agrees with the user's declaration — not, e.g., a sibling helper that
        happens to live in the same closure.

        Returns ``f`` itself if no inner match is found.
        """
        # Accept FunctionType or partial as input. Other callables (built-ins,
        # bound methods, classes) are returned unchanged because they have no
        # __wrapped__ / closure / partial.func chain to traverse.
        if not isinstance(f, (FunctionType, partial)):
            return f
        if isinstance(f, FunctionType) and f.__code__.co_name == expected_name:
            return f
        seen = {id(f)}
        q: deque = deque([f])
        depth = 0
        while q and depth < max_depth:
            depth += 1
            level_size = len(q)
            for _ in range(level_size):
                g = q.popleft()
                # __wrapped__ chain (set by functools.wraps)
                wrapped = getattr(g, "__wrapped__", None)
                if wrapped is not None and id(wrapped) not in seen:
                    seen.add(id(wrapped))
                    if isinstance(wrapped, FunctionType) and wrapped.__code__.co_name == expected_name:
                        return wrapped
                    q.append(wrapped)
                # functools.partial wraps a callable in .func
                if isinstance(g, partial):
                    inner = g.func
                    if id(inner) not in seen and isinstance(inner, FunctionType):
                        seen.add(id(inner))
                        if inner.__code__.co_name == expected_name:
                            return inner
                        q.append(inner)
                # closure cells — captures the inner user function for plain
                # decorators that don't use functools.wraps
                if isinstance(g, FunctionType):
                    for cell in (g.__closure__ or ()):
                        try:
                            obj = cell.cell_contents
                        except ValueError:
                            continue
                        if isinstance(obj, FunctionType) and id(obj) not in seen:
                            seen.add(id(obj))
                            if obj.__code__.co_name == expected_name:
                                return obj
                            q.append(obj)
        return f

    def _create_code_with_function_entry_exit(
        self, code: CodeType, function_key: str
    ) -> Optional[CodeType]:
        """
        Driver: rewrite a code object's bytecode to call the entry handler
        AFTER the leading RESUME (3.11) / at offset 0 (3.9/3.10), and the
        exit handler before every RETURN_VALUE.
        """
        try:
            bc = Bytecode.from_code(code)

            # Pick a non-colliding temp local for the exit pattern.
            retval_local = _RETVAL_LOCAL_NAME
            while retval_local in code.co_varnames:
                retval_local = "_" + retval_local

            entry_instrs = self._create_function_entry_instructions(function_key)
            exit_instrs = self._create_function_exit_instructions(function_key, retval_local)
            if entry_instrs is None or exit_instrs is None:
                return None

            new_instrs: List[Any] = []
            entry_inserted = False

            for instr in bc:
                # Entry: directly after RESUME on 3.11 (verifier expects RESUME
                # at offset 0). On 3.9/3.10 there is no RESUME — we insert
                # before the first instruction.
                if (
                    not entry_inserted
                    and sys.version_info >= (3, 11)
                    and isinstance(instr, Instr)
                    and instr.name == "RESUME"
                ):
                    new_instrs.append(instr)
                    new_instrs.extend(entry_instrs)
                    entry_inserted = True
                    continue

                # Exit: BEFORE each RETURN_VALUE.
                if isinstance(instr, Instr) and instr.name == "RETURN_VALUE":
                    new_instrs.extend(exit_instrs)
                    new_instrs.append(instr)
                    continue

                new_instrs.append(instr)

            # Fallback for 3.9/3.10 where there is no RESUME — prepend.
            if not entry_inserted:
                new_instrs = list(entry_instrs) + new_instrs

            new_bc = bc.copy()
            new_bc.clear()
            new_bc.extend(new_instrs)
            return new_bc.to_code()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error injecting entry/exit bytecode for %s: %s", code.co_name, exc, exc_info=True)
            return None

    def _create_function_entry_instructions(self, function_key: str) -> Optional[list]:
        """Dispatch to the version-specific entry-instruction builder."""
        try:
            if sys.version_info >= (3, 11):
                return self._create_function_entry_instructions_py311(function_key)
            if sys.version_info >= (3, 9):
                return self._create_function_entry_instructions_py39_py310(function_key)
            logger.error("Unsupported Python version for entry injection: %s", sys.version_info)
            return None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating entry instructions: %s", exc, exc_info=True)
            return None

    def _create_function_exit_instructions(self, function_key: str, retval_local: str) -> Optional[list]:
        """Dispatch to the version-specific exit-instruction builder."""
        try:
            if sys.version_info >= (3, 11):
                return self._create_function_exit_instructions_py311(function_key, retval_local)
            if sys.version_info >= (3, 9):
                return self._create_function_exit_instructions_py39_py310(function_key, retval_local)
            logger.error("Unsupported Python version for exit injection: %s", sys.version_info)
            return None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating exit instructions: %s", exc, exc_info=True)
            return None

    @staticmethod
    def _create_function_entry_instructions_py311(function_key: str) -> list:
        """
        Python 3.11 entry pattern. Equivalent to::

            _di_function_entry_handler(function_key, locals())

        Mirrors the existing line-breakpoint pattern at line 535 of this file
        (PUSH_NULL + LOAD_GLOBAL + LOAD_CONST + LOAD_GLOBAL + PRECALL/CALL pairs
        + POP_TOP), with the line_number argument dropped.
        """
        return [
            Instr("PUSH_NULL"),
            Instr("LOAD_GLOBAL", (False, _ENTRY_HANDLER_NAME)),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_GLOBAL", (True, _LOCALS_NAME)),  # builtin: pre-pushes NULL
            Instr("PRECALL", 0),
            Instr("CALL", 0),  # locals() -> dict
            Instr("PRECALL", 2),
            Instr("CALL", 2),  # entry_handler(fk, dict) -> None
            Instr("POP_TOP"),
        ]

    @staticmethod
    def _create_function_entry_instructions_py39_py310(function_key: str) -> list:
        """
        Python 3.9/3.10 entry pattern. No PRECALL, no PUSH_NULL — uses
        ``CALL_FUNCTION``. Equivalent to::

            _di_function_entry_handler(function_key, locals())
        """
        return [
            Instr("LOAD_GLOBAL", _ENTRY_HANDLER_NAME),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_GLOBAL", _LOCALS_NAME),
            Instr("CALL_FUNCTION", 0),  # locals() -> dict
            Instr("CALL_FUNCTION", 2),  # entry_handler(fk, dict) -> None
            Instr("POP_TOP"),
        ]

    @staticmethod
    def _create_function_exit_instructions_py311(function_key: str, retval_local: str) -> list:
        """
        Python 3.11 exit pattern. Inserted BEFORE each RETURN_VALUE.

        Stack discipline (TOS shown, top is right):

        - on entry to the injected sequence: ``[..., retval]``
        - after STORE_FAST: ``[...]`` (retval stashed in ``retval_local``)
        - we build the call frame ``[..., NULL, handler, fk, retval]`` and
          invoke ``handler(fk, retval)`` -> None
        - POP_TOP discards the handler return; ``LOAD_FAST`` re-pushes retval
        - the ORIGINAL ``RETURN_VALUE`` (which follows our injected sequence)
          consumes retval as it would have without injection

        We use the temp-local pattern instead of ``COPY 1`` + stack juggling
        because the call frame requires ``NULL, handler, fk, retval`` in that
        order — copying with ``COPY 1`` and then rotating with ``SWAP n`` is
        fragile across Python versions. This is what Datadog uses.
        """
        return [
            Instr("STORE_FAST", retval_local),
            Instr("PUSH_NULL"),
            Instr("LOAD_GLOBAL", (False, _EXIT_HANDLER_NAME)),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_FAST", retval_local),
            Instr("PRECALL", 2),
            Instr("CALL", 2),  # exit_handler(fk, retval) -> None
            Instr("POP_TOP"),  # discard handler return
            Instr("LOAD_FAST", retval_local),  # restore retval to TOS for RETURN_VALUE
        ]

    @staticmethod
    def _create_function_exit_instructions_py39_py310(function_key: str, retval_local: str) -> list:
        """Python 3.9/3.10 exit pattern. No PRECALL/PUSH_NULL on these versions."""
        return [
            Instr("STORE_FAST", retval_local),
            Instr("LOAD_GLOBAL", _EXIT_HANDLER_NAME),
            Instr("LOAD_CONST", function_key),
            Instr("LOAD_FAST", retval_local),
            Instr("CALL_FUNCTION", 2),  # exit_handler(fk, retval) -> None
            Instr("POP_TOP"),
            Instr("LOAD_FAST", retval_local),
        ]

    @staticmethod
    def _get_service_name():
        try:
            from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel
            from opentelemetry.sdk.trace import TracerProvider  # pylint: disable=import-outside-toplevel

            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                return provider.resource.attributes.get("service.name")
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    @staticmethod
    def _get_environment():
        try:
            from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel
            from opentelemetry.sdk.trace import TracerProvider  # pylint: disable=import-outside-toplevel

            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                return provider.resource.attributes.get("deployment.environment.name")
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
                from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel

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

                logger.debug(
                    "BytecodeInjectionEngine cleaned up: %d functions restored, %d failed", restored_count, failed_count
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error during cleanup: %s", exc, exc_info=True)
        finally:
            with self._lock:
                self._initialized = False
                self._hit_count_callback = None
