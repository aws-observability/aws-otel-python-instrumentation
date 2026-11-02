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
    DEFAULT_MAX_COLLECTION_WIDTH,
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_OBJECT_DEPTH,
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
        # Callback for hit count tracking
        self._hit_count_callback = None
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
                    max_depth=capture_config.max_object_depth if capture_config else DEFAULT_MAX_OBJECT_DEPTH,
                    max_collection_size=(
                        capture_config.max_collection_width if capture_config else DEFAULT_MAX_COLLECTION_WIDTH
                    ),
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

                # Always try to unregister callback and free tool if we own it
                if sys.monitoring.get_tool(self.tool_id) == _TOOL_NAME:
                    sys.monitoring.register_callback(self.tool_id, sys.monitoring.events.LINE, None)
                    try:
                        sys.monitoring.free_tool_id(self.tool_id)
                        logger.debug("Freed tool ID %s", self.tool_id)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.warning("Failed to free tool ID: %s", exc)

            logger.debug("SysMonitoringEngine cleaned up")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error during cleanup: %s", exc, exc_info=True)
