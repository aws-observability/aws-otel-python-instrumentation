# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-member
# Disabling no-member from linter as sys.monitoring is only available in Python 3.12+

import inspect
import logging
import sys
import threading
import time
from types import CodeType, FunctionType
from typing import Any, Dict, Optional, Set

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
from amazon.opentelemetry.distro.debugger.instrumentation_engine._undecorate import undecorated

logger = logging.getLogger(__name__)

_TOOL_NAME = "AwsLiveDebugger"


class SysMonitoringEngine(InstrumentationEngine):
    """
    Production-ready sys.monitoring engine for line-level debugging.

    Supports line-specific breakpoints that create span events within
    the parent function span.

    Safety guarantees:
    - Never crashes the application (comprehensive exception handling)
    - Detects and warns about tool ID conflicts
    - Gracefully degrades on errors
    - Thread-safe for concurrent breakpoint hits
    """

    def __init__(self):
        super().__init__()
        self.tool_id = sys.monitoring.DEBUGGER_ID
        self._initialized = False

        # Line-specific breakpoint tracking
        # Maps code object ID to set of line numbers with breakpoints
        self._breakpoints: Dict[int, Set[int]] = {}
        # Maps code object ID to function_key for constructing breakpoint_key
        self._function_keys: Dict[int, str] = {}
        # Maps (code_id, line_number) to location_hash for span events
        self._location_hashes: Dict[tuple, str] = {}
        # Maps (code_id, line_number) to CaptureConfig for filtering captured data
        self._capture_configs: Dict[tuple, CaptureConfig] = {}
        # Callback for hit count tracking
        self._hit_count_callback = None

        # Function-entry instrumentation tracking (PROBE / function-level BREAKPOINT).
        # Hooks PY_START + PY_RETURN on the code object so the instrumentation fires
        # regardless of which reference invokes it — the caller's binding (module
        # attribute, framework registry, decorator capture) is irrelevant.
        # Maps code_id -> dict with the per-function metadata the handlers need.
        self._function_entries: Dict[int, Dict[str, Any]] = {}
        # Per-thread LIFO stack of in-progress entry frames keyed by code_id.
        # PY_START pushes; PY_RETURN pops and emits.
        self._tls = threading.local()
        # Re-entrancy guard so a snapshot built from inside a callback cannot
        # itself trigger another snapshot via PY_START on the helper code path.
        self._reentrancy_guard = threading.local()
        self._lock = threading.RLock()

    def initialize(self, hit_count_callback=None):
        """
        One-time setup with error handling and conflict detection.

        Detects if another tool is using DEBUGGER_ID and logs warning.

        Args:
            hit_count_callback: Optional callback to be called when a breakpoint is hit
        """
        try:
            # Check for tool ID conflicts. Note that after os.fork() (gunicorn/uWSGI
            # prefork), the child inherits the parent's tool-id registration. That is
            # OURS, not a real conflict — so only bail when the id is held by a DIFFERENT
            # tool. When it is already registered to our own name, reuse it (re-registering
            # the callback below rebinds it to this fresh engine instance).
            existing_tool = sys.monitoring.get_tool(self.tool_id)
            if existing_tool is not None and existing_tool != _TOOL_NAME:
                logger.error(
                    "Cannot initialize %s: DEBUGGER_ID (%s) "
                    "is already in use by '%s'. Please disable the existing tool first.",
                    _TOOL_NAME,
                    sys.monitoring.DEBUGGER_ID,
                    existing_tool,
                )
                return

            # Register tool, unless we already own the id (e.g. inherited across fork).
            if existing_tool is None:
                sys.monitoring.use_tool_id(self.tool_id, _TOOL_NAME)
                logger.debug("Successfully registered %s with sys.monitoring", _TOOL_NAME)
            else:
                logger.debug("Reusing existing %s sys.monitoring registration", _TOOL_NAME)

            # Register LINE event callback
            sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.LINE, self._line_event_handler)

            # Register PY_START/PY_RETURN callbacks for function-entry instrumentation.
            # These coexist with LINE on the same tool_id — events are armed per code
            # object via set_local_events, so a code object only fires the events it
            # has been individually configured for.
            sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_START, self._py_start_handler)
            sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_RETURN, self._py_return_handler)

            self._initialized = True
            self._hit_count_callback = hit_count_callback
            logger.debug("SysMonitoringEngine initialized successfully")

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to initialize SysMonitoringEngine: %s", exc, exc_info=True)
            self.cleanup()

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

        This enables LINE event monitoring for the code object and registers
        all specified line numbers as breakpoints.

        Args:
            code: Code object of the function
            func: Function object (not used by sys.monitoring engine, kept for interface consistency)
            line_numbers: Set of line numbers to enable breakpoints on
            function_key: Function key (module.function) for constructing breakpoint_key
            line_location_hashes: Optional mapping of line_number -> location_hash for span events
            line_capture_configs: Optional mapping of line_number -> CaptureConfig for capture filtering
        """
        if not self._initialized:
            logger.warning("SysMonitoringEngine not initialized, cannot enable breakpoints")
            return

        if not line_numbers:
            logger.debug("No line numbers provided, nothing to enable")
            return

        try:
            code_id = id(code)

            with self._lock:
                if code_id not in self._breakpoints:
                    # Enable LINE monitoring before updating state — if set_local_events fails,
                    # _breakpoints remains unchanged and no partial state is left behind
                    sys.monitoring.set_local_events(self.tool_id, code, sys.monitoring.events.LINE)
                    logger.debug("Enabled LINE monitoring for %s", function_key)
                    self._breakpoints[code_id] = set()
                else:
                    # New lines may have been DISABLE'd during previous training.
                    # Reset LINE events for this code object to allow re-training.
                    sys.monitoring.set_local_events(self.tool_id, code, sys.monitoring.events.NO_EVENTS)
                    sys.monitoring.set_local_events(self.tool_id, code, sys.monitoring.events.LINE)
                    logger.debug("Reset LINE monitoring for %s to retrain with new breakpoints", function_key)

                self._function_keys[code_id] = function_key

                # Add all lines to the breakpoint set
                self._breakpoints[code_id].update(line_numbers)

                # Store location hashes and capture configs for each line
                if line_location_hashes:
                    for line_num, location_hash in line_location_hashes.items():
                        self._location_hashes[(code_id, line_num)] = location_hash
                if line_capture_configs:
                    for line_num, capture_config in line_capture_configs.items():
                        self._capture_configs[(code_id, line_num)] = capture_config

                logger.debug(
                    "Enabled %d breakpoints for %s at lines: %s", len(line_numbers), function_key, line_numbers
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            # For the new-function path (if branch): set_local_events is called before state is
            # modified, so failure leaves _breakpoints unchanged.
            # For the already-monitored path (else branch): if NO_EVENTS succeeds but LINE fails,
            # existing breakpoints become dormant until the next reconfiguration cycle resets them.
            # This is acceptable because set_local_events practically never fails for valid inputs.
            logger.error("Failed to enable breakpoints for %s: %s", function_key, exc, exc_info=True)

    def disable_breakpoints_for_function(self, code: CodeType, func: FunctionType) -> None:
        """
        Disable ALL breakpoints for a function and restore it to original state.

        This is an all-or-nothing operation that:
        - Removes ALL active breakpoints for the function
        - Disables LINE event monitoring for the code object
        - Cleans up all engine-specific state for this function

        Args:
            code: Code object of the function
            func: Function object (not used by sys.monitoring engine, but kept for interface consistency)
        """
        if not self._initialized:
            logger.warning("SysMonitoringEngine not initialized, cannot disable breakpoints")
            return

        try:
            code_id = id(code)

            with self._lock:
                if code_id not in self._breakpoints:
                    logger.debug("No breakpoints found for %s", code.co_name)
                    return

                # Disable LINE events first to stop the handler from firing during cleanup.
                # If this fails, proceed with state cleanup anyway — the handler will find
                # no breakpoints and return DISABLE.
                try:
                    sys.monitoring.set_local_events(self.tool_id, code, sys.monitoring.events.NO_EVENTS)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning("Failed to disable LINE events for %s: %s", code.co_name, exc)

                # Clean up all state for this function
                line_numbers = self._breakpoints.pop(code_id, set())
                function_key = self._function_keys.pop(code_id, None)

                # Clean up location hashes and capture configs for all lines
                for line_num in line_numbers:
                    self._location_hashes.pop((code_id, line_num), None)
                    self._capture_configs.pop((code_id, line_num), None)

                logger.debug("Disabled all breakpoints for %s", function_key or code.co_name)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to disable breakpoints for %s: %s",
                code.co_name,
                exc,
                exc_info=True,
            )

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
        Hook PY_START + PY_RETURN on a code object for function-entry instrumentation.

        Unlike module-attribute replacement (setattr), this binds to the code object
        the interpreter actually executes, so it survives stale references — Django's
        URL resolver, Flask's view_functions, decorators capturing the function,
        ``from x import y``, and any other pattern that holds a direct reference.

        Args:
            code: Code object of the function — the actual hook target.
            func: Function object — used for ``inspect.signature`` and as a
                  fallback when the snapshot needs the original callable.
            function_key: ``module.qualname`` for snapshot routing / hit-count keys.
            module_name: Module name (component of the snapshot's CodeUnit).
            qualified_name: Qualified function name (component of MethodName).
            capture_config: Controls argument / return / stack capture.
            location_hash: LocationHash to attach to the emitted snapshot.
            instrumentation_type: ``"PROBE"`` or ``"BREAKPOINT"`` — surfaced as
                                  ``aws.di.instrumentation_type`` on the snapshot.

        Returns:
            True on success, False if the engine isn't initialized or the hook
            could not be installed (state is left unchanged on failure).
        """
        if not self._initialized:
            logger.warning("SysMonitoringEngine not initialized, cannot enable function entry")
            return False

        try:
            # Resolve through @functools.wraps / partial / closure cells so we
            # arm PY_START on the user's function (not the auth/cache wrapper).
            # Shared with the bytecode engine — same algorithm, same
            # behavior across Python versions.
            target_func = undecorated(
                func,
                qualified_name.split(".")[-1],
                getattr(code, "co_filename", None),
            )
            target_code = getattr(target_func, "__code__", code) if target_func is not None else code

            code_id = id(target_code)
            with self._lock:
                self._function_entries[code_id] = {
                    "func": target_func,
                    "function_key": function_key,
                    "module_name": module_name,
                    "qualified_name": qualified_name,
                    "capture_config": capture_config,
                    "location_hash": location_hash,
                    "instrumentation_type": instrumentation_type,
                }
                # Combine with any existing events on this code object (LINE may
                # already be armed for line-level breakpoints on the same function).
                existing = sys.monitoring.get_local_events(self.tool_id, target_code)
                sys.monitoring.set_local_events(
                    self.tool_id,
                    target_code,
                    existing | sys.monitoring.events.PY_START | sys.monitoring.events.PY_RETURN,
                )
                logger.debug(
                    "Enabled function entry for %s (code_id=%s, type=%s)",
                    function_key,
                    code_id,
                    instrumentation_type,
                )
                return True
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to enable function entry for %s: %s", function_key, exc, exc_info=True)
            # Best-effort rollback so we don't leave a half-armed entry behind.
            self._function_entries.pop(id(code), None)
            return False

    def disable_function_entry(self, code: CodeType, func: Optional[FunctionType] = None) -> None:
        """Tear down PY_START / PY_RETURN hooks for a code object.

        ``func`` is unused on this engine — sys.monitoring keys state by
        ``id(code)`` directly. The parameter is in the signature for API
        symmetry with BytecodeInjectionEngine.
        """
        del func  # unused
        if not self._initialized:
            return
        try:
            code_id = id(code)
            with self._lock:
                if code_id not in self._function_entries:
                    return
                # Drop only PY_START/PY_RETURN — preserve LINE if it was set.
                existing = sys.monitoring.get_local_events(self.tool_id, code)
                remaining = existing & ~(sys.monitoring.events.PY_START | sys.monitoring.events.PY_RETURN)
                try:
                    sys.monitoring.set_local_events(self.tool_id, code, remaining)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning("Failed to clear PY_START/PY_RETURN for %s: %s", code.co_name, exc)
                self._function_entries.pop(code_id, None)
                logger.debug("Disabled function entry for %s", code.co_name)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to disable function entry for %s: %s", code.co_name, exc, exc_info=True)

    def _py_start_handler(self, code: CodeType, instruction_offset: int):
        """
        Called on every function entry for code objects with PY_START armed.

        CRITICAL: Must NOT raise. Returns ``sys.monitoring.DISABLE`` to suppress
        future PY_START callbacks for code objects we don't track (one-time
        training cost; the interpreter's callback dispatch then skips them).
        """
        try:
            entry = self._function_entries.get(id(code))
            if entry is None:
                return sys.monitoring.DISABLE

            # Re-entrancy guard: if we're already inside the engine emitting a
            # snapshot, the helper code we run (serialization, OTel resource
            # lookup, etc.) must not trigger more snapshots. CPython's GIL means
            # a single thread cannot be in two PY_START callbacks at once, but
            # the helper code may invoke other instrumented functions.
            if getattr(self._reentrancy_guard, "active", False):
                return None

            stack = getattr(self._tls, "stack", None)
            if stack is None:
                stack = []
                self._tls.stack = stack

            entry_context = SysMonitoringEngine._capture_entry_arguments(code, entry.get("capture_config"))
            stack.append(
                {
                    "code_id": id(code),
                    "start_ns": time.time_ns(),
                    "entry_context": entry_context,
                }
            )
            return None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Critical error in PY_START handler for %s: %s", code.co_name, exc, exc_info=True)
            return None

    def _py_return_handler(self, code: CodeType, instruction_offset: int, retval: Any):
        """
        Called on every function return for code objects with PY_RETURN armed.

        Pairs with ``_py_start_handler`` via the per-thread LIFO stack: pops the
        matching entry frame, computes duration, captures return value, and emits
        the function-entry snapshot.
        """
        try:
            entry = self._function_entries.get(id(code))
            if entry is None:
                return sys.monitoring.DISABLE

            if getattr(self._reentrancy_guard, "active", False):
                return None

            stack = getattr(self._tls, "stack", None)
            if not stack:
                # No matching PY_START — the caller entered the function before
                # the hook was armed (e.g. a generator's .send() after creation,
                # which we don't yet support). Skip silently rather than emit a
                # malformed snapshot.
                return None

            # Pop the most recent matching frame. Generators/coroutines can
            # interleave PY_START/PY_RETURN across threads in non-LIFO order,
            # but for plain sync calls (the only case we currently support) the
            # top-of-stack always matches.
            frame_info = None
            for idx in range(len(stack) - 1, -1, -1):
                if stack[idx]["code_id"] == id(code):
                    frame_info = stack.pop(idx)
                    break
            if frame_info is None:
                return None

            self._reentrancy_guard.active = True
            try:
                self._handle_function_entry(code, entry, frame_info, retval, thrown=None)
            finally:
                self._reentrancy_guard.active = False
            return None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Critical error in PY_RETURN handler for %s: %s", code.co_name, exc, exc_info=True)
            return None

    @staticmethod
    def _capture_entry_arguments(code: CodeType, capture_config: Optional[CaptureConfig]) -> Optional[CapturedContext]:
        """
        Capture function arguments from the calling frame.

        sys.monitoring callbacks don't receive the executing frame; we walk the
        call stack to find the frame whose ``f_code is code``. This mirrors how
        ``_get_local_vars`` recovers locals for line-level breakpoints.
        """
        if capture_config is None or capture_config.capture_arguments is None:
            return None

        frame = inspect.currentframe()
        try:
            target_frame = None
            while frame is not None:
                if frame.f_code is code:
                    target_frame = frame
                    break
                frame = frame.f_back
            if target_frame is None:
                return None

            # Argument names are co_varnames[: co_argcount + co_kwonlyargcount].
            arg_count = code.co_argcount + code.co_kwonlyargcount
            arg_names = code.co_varnames[:arg_count]
            args_dict = {name: target_frame.f_locals[name] for name in arg_names if name in target_frame.f_locals}

            if not args_dict:
                return None

            # Apply capture_arguments filter: [] means all, ["a","b"] means subset.
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
            logger.warning("Failed to capture entry arguments: %s", exc)
            return None
        finally:
            del frame

    def _handle_function_entry(
        self,
        code: CodeType,
        entry: Dict[str, Any],
        frame_info: Dict[str, Any],
        retval: Any,
        thrown: Optional[Exception],
    ) -> None:
        """Build and emit the function-entry snapshot via the shared factory."""
        try:
            function_key = entry["function_key"]

            # Hit-count rate limit (key = function_key:0 for function-level).
            if self._hit_count_callback is not None:
                if not self._hit_count_callback(f"{function_key}:0"):
                    return

            # pylint: disable=import-outside-toplevel
            from amazon.opentelemetry.distro.debugger._snapshot_factory import (
                build_function_entry_snapshot,
                emit_snapshot,
            )

            # On the exception path PY_RETURN doesn't fire, so this engine
            # never invokes _handle_function_entry with thrown!=None today.
            # Pass retval=None when thrown is set so the snapshot semantics
            # match the bytecode engine's exception path.
            effective_retval = None if thrown is not None else retval
            snapshot = build_function_entry_snapshot(
                entry=entry,
                frame_info=frame_info,
                retval=effective_retval,
                file_path=getattr(code, "co_filename", None),
            )
            emit_snapshot(snapshot)

            logger.debug(
                "Created function-entry snapshot for %s (type=%s)",
                function_key,
                entry.get("instrumentation_type"),
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error handling function entry for %s: %s", code.co_name, exc, exc_info=True)

    def _line_event_handler(self, code: CodeType, line_number: int):
        """
        Called on every LINE event for monitored code objects.

        CRITICAL: This method MUST NOT throw exceptions.

        Uses sys.monitoring.DISABLE to suppress callbacks for non-target lines.
        After a one-time "training" pass (first function call after enable), only
        breakpoint target lines continue to fire. This reduces overhead from O(lines)
        to O(breakpoints) per function call.

        Returns:
            sys.monitoring.DISABLE: Suppress future callbacks for this (code, offset) pair
            None: Continue monitoring this line (used for active breakpoint lines)
        """
        try:
            code_id = id(code)

            # Fast path (lock-free): dict.get() and set.__contains__ are GIL-atomic on CPython.
            # On free-threaded builds (PEP 703), worst case is a stale read resulting in
            # one missed or extra snapshot, corrected on next call.
            breakpoint_set = self._breakpoints.get(code_id)
            if breakpoint_set is None:
                return sys.monitoring.DISABLE
            if line_number not in breakpoint_set:
                return sys.monitoring.DISABLE

            # This line IS a breakpoint - capture context and create span event
            self._handle_breakpoint(code, line_number)
            return None

        except Exception as exc:  # pylint: disable=broad-exception-caught
            # CRITICAL: Never let exceptions escape to application code.
            # Return None to keep this breakpoint line active. Returning DISABLE would
            # suppress it permanently since reconfiguration (which resets DISABLE state)
            # only triggers on actual config changes, not on every poll cycle. A transient
            # error should not permanently kill a valid breakpoint.
            logger.error(
                "Critical error in debugger line handler for %s:%s: %s", code.co_name, line_number, exc, exc_info=True
            )
            return None

    @staticmethod
    def _get_service_name() -> Optional[str]:
        """Get service name from OTel resource."""
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
    def _get_environment() -> Optional[str]:
        """Get deployment environment from OTel resource."""
        try:
            from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel
            from opentelemetry.sdk.trace import TracerProvider  # pylint: disable=import-outside-toplevel

            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                return provider.resource.attributes.get("deployment.environment.name")
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    def _handle_breakpoint(
        self, code: CodeType, line_number: int
    ):  # pylint: disable=too-many-locals,too-many-statements
        """
        Handle a breakpoint hit by creating a line-level Snapshot.

        Args:
            code: Code object where breakpoint was hit
            line_number: Line number of the breakpoint
        """
        try:
            # Resolve function_key once, with fallback for unregistered code objects
            code_id = id(code)
            function_key = self._function_keys.get(code_id, code.co_name)

            # Check rate limit FIRST — skip stack walk and serialization if rate-limited
            if self._hit_count_callback:
                breakpoint_key = f"{function_key}:{line_number}"
                if not self._hit_count_callback(breakpoint_key):
                    return

            # Look up capture config for this breakpoint
            capture_config = self._capture_configs.get((code_id, line_number))

            # Apply capture_locals filtering:
            # None = field absent (do not capture locals)
            # [] = capture all locals
            # ["a", "b"] = capture only those
            capture_locals = capture_config.capture_locals if capture_config else None

            local_vars = {}
            locals_captured = {}
            if capture_locals is not None:
                # Capture local variables (expensive stack walk — only when capture is enabled)
                local_vars = self._get_local_vars(code)

                # Use per-breakpoint limits if available, otherwise defaults
                serializer = SnapshotSerializer(
                    max_fields=(
                        capture_config.max_fields_per_object if capture_config else DEFAULT_MAX_FIELDS_PER_OBJECT
                    ),
                    max_string_length=capture_config.max_string_length if capture_config else DEFAULT_MAX_STRING_LENGTH,
                    max_depth=capture_config.max_object_depth if capture_config else 3,
                    max_collection_size=capture_config.max_collection_width if capture_config else 10,
                )

                if local_vars:
                    # Filter by capture_locals list if specific names given
                    if len(capture_locals) > 0:
                        local_vars = {k: v for k, v in local_vars.items() if k in capture_locals}
                    locals_captured = serializer.serialize_variables(local_vars)

            # Build line-level captures
            line_context = CapturedContext(locals=locals_captured if locals_captured else None)
            captures = Captures(lines={line_number: line_context})
            location_hash = self._location_hashes.get((code_id, line_number))

            method_name = function_key.split(".")[-1]
            module_parts = function_key.split(".")
            code_unit = ".".join(module_parts[:-1]) if len(module_parts) > 1 else function_key
            class_name = code_unit  # For Python, className = module path

            instrumentation = InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit=code_unit,
                    class_name=class_name,
                    method_name=method_name,
                    file_path=code.co_filename,
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
                location_hash=location_hash,
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

            logger.debug(
                "Created line snapshot for breakpoint at %s:%s with %d variables",
                code.co_name,
                line_number,
                len(local_vars),
            )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error handling breakpoint at %s:%s: %s", code.co_name, line_number, exc, exc_info=True)

    @staticmethod
    def _get_local_vars(code: CodeType) -> Dict:
        """
        Extract local variables from the execution frame.

        sys.monitoring callbacks don't receive the frame, so we walk the call stack
        to find the frame executing the target code object.
        """
        frame = inspect.currentframe()
        try:
            # Walk up the stack to find the frame executing the breakpoint code
            while frame:
                if frame.f_code is code:
                    # Filter out functions, modules, classes, methods, builtins (likely imports)
                    return {
                        k: v
                        for k, v in frame.f_locals.items()
                        if not (
                            inspect.isfunction(v)
                            or inspect.ismodule(v)
                            or inspect.isclass(v)
                            or inspect.ismethod(v)
                            or inspect.isbuiltin(v)
                        )
                    }
                frame = frame.f_back
            return {}
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in _get_local_vars: %s", exc, exc_info=True)
            return {}
        finally:
            # Prevent reference cycles
            # https://docs.python.org/3/library/inspect.html
            del frame

    @staticmethod
    def supports_runtime() -> bool:
        """Check if sys.monitoring is available (Python 3.12+)."""
        try:
            return sys.version_info >= (3, 12)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error checking runtime support: %s", exc)
            return False

    def cleanup(self) -> None:
        """Clean up sys.monitoring resources."""
        try:
            with self._lock:
                self._initialized = False
                self._hit_count_callback = None
                self._breakpoints.clear()
                self._function_keys.clear()
                self._location_hashes.clear()
                self._capture_configs.clear()

                self._function_entries.clear()

                # Always try to unregister callbacks and free tool if we own it
                if sys.monitoring.get_tool(self.tool_id) == _TOOL_NAME:
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.LINE, None)
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_START, None)
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_RETURN, None)
                    try:
                        sys.monitoring.free_tool_id(self.tool_id)
                        logger.debug("Freed tool ID %s", self.tool_id)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.warning("Failed to free tool ID: %s", exc)

            logger.debug("SysMonitoringEngine cleaned up")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error during cleanup: %s", exc, exc_info=True)
