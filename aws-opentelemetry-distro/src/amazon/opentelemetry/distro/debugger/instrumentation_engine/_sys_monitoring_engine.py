# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-member
# Disabling no-member from linter as sys.monitoring is only available in Python 3.12+

import inspect
import logging
import sys
import threading
import time
import traceback
from types import CodeType, FunctionType
from typing import Any, Dict, Optional, Set

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
from opentelemetry import trace as otel_trace

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
        # Maps (code_id, 0) to "PROBE" or "BREAKPOINT". Function-level
        # instrumentation can be either; line-level (line>0) is always
        # BREAKPOINT and bypasses this dict.
        self._instrumentation_types: Dict[tuple, str] = {}
        # Callback for hit count tracking
        self._hit_count_callback = None
        # Per-call start_ns stash, keyed by (code_id, thread_id). Set on
        # PY_START, popped on PY_RETURN / PY_UNWIND, used to compute
        # aws.di.duration_ms in the snapshot.
        self._call_start_ns: Dict[tuple, int] = {}
        # Re-entrancy guard for our own snapshot-building code. If the
        # serializer runs a user __repr__ that calls back into another
        # instrumented function, this short-circuits the inner call.
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

            # Register LINE event callback (line-level breakpoints).
            sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.LINE, self._line_event_handler)

            # Register function-entry callbacks (PROBE / function-level BREAKPOINT).
            # PY_START / PY_RETURN are armed per-code-object via set_local_events
            # in enable_function_level_instrumentation. PY_UNWIND must be set GLOBALLY (it isn't
            # local-event-capable on 3.12-3.14, see CPython gh-142186); the
            # _function_unwind_event_handler filters by code via the existing _function_keys
            # dict so the cost on uninstrumented frames is one dict lookup.
            sys.monitoring.register_callback(
                self.tool_id, sys.monitoring.events.PY_START, self._function_start_event_handler
            )
            sys.monitoring.register_callback(
                self.tool_id, sys.monitoring.events.PY_RETURN, self._function_return_event_handler
            )
            sys.monitoring.register_callback(
                self.tool_id, sys.monitoring.events.PY_UNWIND, self._function_unwind_event_handler
            )
            sys.monitoring.set_events(self.tool_id, sys.monitoring.events.PY_UNWIND)

            self._initialized = True
            self._hit_count_callback = hit_count_callback
            logger.debug("SysMonitoringEngine initialized successfully")

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to initialize SysMonitoringEngine: %s", exc, exc_info=True)
            self.cleanup()

    # Function-entry instrumentation (PROBE / function-level BREAKPOINT).
    # Arms PY_START / PY_RETURN per-code-object via set_local_events; the
    # global PY_UNWIND from initialize() covers the exception path. Per-call
    # event dispatch goes through _function_start_event_handler / _function_return_event_handler /
    # _function_unwind_event_handler, which look up config in the existing _function_keys
    # / _capture_configs / _location_hashes / _instrumentation_types dicts
    # using (code_id, 0) — line=0 marks function-level (matches manager's
    # bp_set.breakpoints[0] convention).

    def enable_function_level_instrumentation(  # pylint: disable=too-many-arguments
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
        Arm function-entry instrumentation on a code object.

        Tells sys.monitoring to fire PY_START / PY_RETURN events on this
        specific code object, and stashes the customer's config in the
        engine's per-(code_id, 0) state so handlers can build a snapshot
        when those events fire.

        Returns True on success, False if the engine isn't initialized.
        """
        del func, module_name, qualified_name  # commit 4 wires undecorate
        if not self._initialized:
            logger.warning("SysMonitoringEngine not initialized, cannot enable function-level instrumentation")
            return False
        try:
            code_id = id(code)
            with self._lock:
                # Combine PY_START | PY_RETURN with whatever LINE events may
                # already be armed for this code (line BPs and function-level
                # PROBE can coexist on the same function).
                existing = sys.monitoring.get_local_events(self.tool_id, code)
                sys.monitoring.set_local_events(
                    self.tool_id,
                    code,
                    existing | sys.monitoring.events.PY_START | sys.monitoring.events.PY_RETURN,
                )
                self._function_keys[code_id] = function_key
                if code_id not in self._breakpoints:
                    self._breakpoints[code_id] = set()
                self._breakpoints[code_id].add(0)
                if capture_config is not None:
                    self._capture_configs[(code_id, 0)] = capture_config
                if location_hash:
                    self._location_hashes[(code_id, 0)] = location_hash
                self._instrumentation_types[(code_id, 0)] = instrumentation_type or "PROBE"
                logger.debug("Enabled function-level instrumentation for %s", function_key)
            return True
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to enable function-level instrumentation for %s: %s", function_key, exc, exc_info=True)
            return False

    def disable_function_level_instrumentation(self, code: CodeType, func: Optional[FunctionType] = None) -> None:
        """
        Disarm function-entry instrumentation on a code object.

        Drops PY_START | PY_RETURN from this code's local-events mask
        while preserving LINE if line BPs remain. Clears the engine's
        per-(code_id, 0) state. PY_UNWIND stays globally armed in
        initialize() — the unwind handler filters by code, so deleting
        the entries here is sufficient to stop emitting unwind snapshots
        for this function.
        """
        del func  # not needed for sys.monitoring (events are bound to code)
        if not self._initialized:
            return
        try:
            code_id = id(code)
            with self._lock:
                # Drop PY_START | PY_RETURN; keep LINE (and anything else).
                existing = sys.monitoring.get_local_events(self.tool_id, code)
                remaining = existing & ~(sys.monitoring.events.PY_START | sys.monitoring.events.PY_RETURN)
                try:
                    sys.monitoring.set_local_events(self.tool_id, code, remaining)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to clear PY_START/PY_RETURN for %s: %s",
                        code.co_name,
                        exc,
                    )

                # Clear function-level state at line=0.
                self._capture_configs.pop((code_id, 0), None)
                self._location_hashes.pop((code_id, 0), None)
                self._instrumentation_types.pop((code_id, 0), None)
                # Drain in-flight PY_START stamps for this code across all threads.
                # Once we strip PY_START/PY_RETURN above, already-running frames may
                # never get a PY_RETURN — these entries would leak otherwise.
                for stale_key in [k for k in self._call_start_ns if k[0] == code_id]:
                    self._call_start_ns.pop(stale_key, None)
                line_set = self._breakpoints.get(code_id)
                if line_set is not None:
                    line_set.discard(0)
                    # If no line BPs remain either, drop the code-level entries.
                    if not line_set:
                        self._breakpoints.pop(code_id, None)
                        self._function_keys.pop(code_id, None)
                logger.debug("Disabled function-level instrumentation for %s", code.co_name)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to disable function-level instrumentation for %s: %s", code.co_name, exc, exc_info=True
            )

    def _function_start_event_handler(self, code: CodeType, instruction_offset: int):
        """PY_START callback. Records call start_ns; no snapshot yet.

        The snapshot is built on PY_RETURN / PY_UNWIND so it can include
        the return value or exception.
        """
        del instruction_offset
        if getattr(self._reentrancy_guard, "active", False):
            return
        code_id = id(code)
        if code_id not in self._function_keys or 0 not in self._breakpoints.get(code_id, ()):
            return sys.monitoring.DISABLE
        try:
            self._reentrancy_guard.active = True
            self._call_start_ns[(code_id, threading.get_ident())] = time.time_ns()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in PY_START handler for %s: %s", code.co_name, exc, exc_info=True)
        finally:
            self._reentrancy_guard.active = False

    def _function_return_event_handler(self, code: CodeType, instruction_offset: int, retval: Any):
        """PY_RETURN callback. Builds and emits a normal-return snapshot."""
        del instruction_offset
        code_id = id(code)
        # Pop unconditionally so reentrancy short-circuit or disable-mid-flight
        # cannot orphan the start_ns stamp written by PY_START.
        start_ns = self._call_start_ns.pop((code_id, threading.get_ident()), 0)
        if getattr(self._reentrancy_guard, "active", False):
            return
        if code_id not in self._function_keys or 0 not in self._breakpoints.get(code_id, ()):
            return sys.monitoring.DISABLE
        try:
            self._reentrancy_guard.active = True
            self._handle_function_level_instrumentation(code, retval=retval, thrown=None, start_ns=start_ns)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in PY_RETURN handler for %s: %s", code.co_name, exc, exc_info=True)
        finally:
            self._reentrancy_guard.active = False

    def _function_unwind_event_handler(self, code: CodeType, instruction_offset: int, exception: BaseException):
        """PY_UNWIND callback. Builds and emits an exception-path snapshot.

        Globally armed (PY_UNWIND isn't local-event-capable on 3.12-3.14),
        so we filter by code first. The third arg IS the live BaseException;
        do NOT call sys.exc_info() here — CPython nulls the current exception
        around the callback. Cannot return DISABLE on PY_UNWIND (raises
        ValueError); just return None for non-ours codes.
        """
        del instruction_offset
        code_id = id(code)
        # Pop unconditionally so reentrancy short-circuit or disable-mid-flight
        # cannot orphan the start_ns stamp written by PY_START.
        start_ns = self._call_start_ns.pop((code_id, threading.get_ident()), 0)
        if getattr(self._reentrancy_guard, "active", False):
            return
        if code_id not in self._function_keys or 0 not in self._breakpoints.get(code_id, ()):
            return
        try:
            self._reentrancy_guard.active = True
            self._handle_function_level_instrumentation(code, retval=None, thrown=exception, start_ns=start_ns)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in PY_UNWIND handler for %s: %s", code.co_name, exc, exc_info=True)
        finally:
            self._reentrancy_guard.active = False

    def _handle_function_level_instrumentation(
        self,
        code: CodeType,
        retval: Any,
        thrown: Optional[BaseException],
        start_ns: int,
    ) -> None:
        """Build and emit a snapshot for a function-level event.

        Called from PY_RETURN (thrown=None) and PY_UNWIND (retval=None,
        thrown=<exc>). Mutually exclusive: exactly one of retval / thrown
        is meaningful per call.
        """
        try:
            code_id = id(code)
            function_key = self._function_keys.get(code_id, code.co_name)
            breakpoint_key = f"{function_key}:0"

            # Rate limit before doing any expensive work.
            if self._hit_count_callback and not self._hit_count_callback(breakpoint_key):
                return

            capture_config = self._capture_configs.get((code_id, 0))
            location_hash = self._location_hashes.get((code_id, 0))
            instrumentation_type = self._instrumentation_types.get((code_id, 0), "PROBE")

            # Capture user locals from the calling user frame.
            local_vars: Dict[str, Any] = {}
            if capture_config and capture_config.capture_arguments is not None:
                local_vars = self._get_local_vars(code)

            serializer = SnapshotSerializer(
                max_fields=(capture_config.max_fields_per_object if capture_config else DEFAULT_MAX_FIELDS_PER_OBJECT),
                max_string_length=capture_config.max_string_length if capture_config else DEFAULT_MAX_STRING_LENGTH,
                max_depth=capture_config.max_object_depth if capture_config else 3,
                max_collection_size=capture_config.max_collection_width if capture_config else 10,
            )

            # Filter args by capture_arguments list (None = skip args, [] = all).
            args_captured: Dict[str, Any] = {}
            if capture_config and capture_config.capture_arguments is not None and local_vars:
                wanted = capture_config.capture_arguments
                filtered = local_vars if not wanted else {k: v for k, v in local_vars.items() if k in wanted}
                args_captured = serializer.serialize_variables(filtered)

            entry_context = CapturedContext(arguments=args_captured if args_captured else None)

            # Build return_context only when there is something to capture.
            return_context: Optional[CapturedContext] = None
            if thrown is not None:
                # Exception path. Always build a context so the consumer
                # can detect the exception even if capture_return is False.
                return_context = CapturedContext(throwable=self._build_throwable(thrown))
            elif capture_config and capture_config.capture_return and retval is not None:
                return_context = CapturedContext(return_value=serializer.serialize(retval))

            captures = Captures(entry=entry_context, return_context=return_context)

            method_name = function_key.split(".")[-1]
            module_parts = function_key.split(".")
            code_unit = ".".join(module_parts[:-1]) if len(module_parts) > 1 else function_key
            instrumentation = InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit=code_unit,
                    class_name=code_unit,
                    method_name=method_name,
                    file_path=code.co_filename,
                    line_number=0,
                    language="python",
                ),
            )

            # OTel trace context.
            trace_context: Optional[TraceContext] = None
            try:
                span = otel_trace.get_current_span()
                ctx = span.get_span_context()
                if ctx.is_valid:
                    trace_context = TraceContext(
                        trace_id=format(ctx.trace_id, "032x"),
                        span_id=format(ctx.span_id, "016x"),
                    )
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            current_thread = threading.current_thread()
            thread = ThreadInfo(id=threading.get_ident(), name=current_thread.name)

            stack = None
            if capture_config and capture_config.capture_stack_trace:
                stack = capture_stack_frames(capture_config.max_stack_frames)

            duration_ms = (time.time_ns() - start_ns) // 1_000_000 if start_ns else 0

            snapshot = Snapshot(
                timestamp=int(time.time() * 1000),
                duration=duration_ms,
                location_hash=location_hash or None,
                service=self._get_service_name(),
                environment=self._get_environment(),
                instrumentation=instrumentation,
                trace=trace_context,
                thread=thread,
                stack=stack,
                captures=captures,
                instrumentation_type=instrumentation_type,
            )

            # Emit via the global snapshot emitter (set up by debugger init).
            try:
                emitter = get_snapshot_emitter()
                if emitter:
                    emitter.emit_snapshot(snapshot)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            logger.debug(
                "Emitted %s snapshot for %s (duration_ms=%d, exc=%s)",
                instrumentation_type,
                function_key,
                duration_ms,
                type(thrown).__name__ if thrown else None,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error handling function-level instrumentation: %s", exc, exc_info=True)

    @staticmethod
    def _build_throwable(thrown: BaseException) -> CapturedThrowable:
        """Build a CapturedThrowable from a live exception.

        Walks ``thrown.__traceback__`` (do NOT call sys.exc_info() inside
        a sys.monitoring callback — CPython nulls the current exception
        around the call).
        """
        frames: list = []
        tb = thrown.__traceback__
        if tb is not None:
            try:
                extracted = traceback.extract_tb(tb)
                frames = [
                    StackFrame(file_name=f.filename, function=f.name, line_number=f.lineno or 0) for f in extracted
                ]
            except Exception:  # pylint: disable=broad-exception-caught
                frames = []
        return CapturedThrowable(
            type=type(thrown).__name__,
            message=str(thrown) or "",
            stacktrace=frames,
        )

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
                # (line=0 is included if function-level was also armed).
                for line_num in line_numbers:
                    self._location_hashes.pop((code_id, line_num), None)
                    self._capture_configs.pop((code_id, line_num), None)

                # _instrumentation_types is keyed at (code_id, 0) only and is NOT
                # iterated above; clear it explicitly in case function-level
                # was armed on this code.
                self._instrumentation_types.pop((code_id, 0), None)

                # Drain in-flight PY_START stamps across all threads. We just
                # popped _function_keys, so PY_RETURN/PY_UNWIND will short-circuit
                # on the membership guard before reaching the pop.
                for stale_key in [k for k in self._call_start_ns if k[0] == code_id]:
                    self._call_start_ns.pop(stale_key, None)

                logger.debug("Disabled all breakpoints for %s", function_key or code.co_name)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to disable breakpoints for %s: %s",
                code.co_name,
                exc,
                exc_info=True,
            )

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
                self._instrumentation_types.clear()
                self._call_start_ns.clear()

                # Always try to unregister callback and free tool if we own it
                if sys.monitoring.get_tool(self.tool_id) == _TOOL_NAME:
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.LINE, None)
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_START, None)
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_RETURN, None)
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.PY_UNWIND, None)
                    # Disarm global PY_UNWIND. LINE / PY_START / PY_RETURN
                    # were armed per-code via set_local_events and release
                    # automatically when the code is GC'd.
                    try:
                        sys.monitoring.set_events(self.tool_id, sys.monitoring.events.NO_EVENTS)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.warning("Failed to clear global events: %s", exc)
                    try:
                        sys.monitoring.free_tool_id(self.tool_id)
                        logger.debug("Freed tool ID %s", self.tool_id)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.warning("Failed to free tool ID: %s", exc)

            logger.debug("SysMonitoringEngine cleaned up")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error during cleanup: %s", exc, exc_info=True)
