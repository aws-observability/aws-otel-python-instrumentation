# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Bytecode injection engine for live debugger.

This module handles the injection of breakpoint calls into Python functions
using bytecode injection for Python 3.10-3.11.
"""

import inspect
import logging
import sys
import threading
import time
from dataclasses import dataclass
from types import CodeType, FunctionType
from typing import Any, Dict, Optional, Set

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED
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

# Global name for the breakpoint handler function injected into function globals
_HANDLER_NAME = "_breakpoint_handler"
# Global name for the locals() builtin injected into function globals
_LOCALS_NAME = "_breakpoint_locals"

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
    Bytecode injection engine for line-level debugging on Python 3.10-3.11.

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

        if not IS_BYTECODE_INSTALLED:
            logger.warning(
                "bytecode library not available. "
                "Debugger will not function on Python 3.10-3.11. "
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
        """Check if bytecode injection is supported (Python 3.10-3.11)."""
        return (3, 10) <= sys.version_info < (3, 12) and IS_BYTECODE_INSTALLED

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
                max_depth=capture_config.max_object_depth if capture_config else DEFAULT_MAX_OBJECT_DEPTH,
                max_collection_size=(
                    capture_config.max_collection_width if capture_config else DEFAULT_MAX_COLLECTION_WIDTH
                ),
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
            if sys.version_info >= (3, 10):
                return self._create_breakpoint_instructions_py310(function_key, line_number)
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
    def _create_breakpoint_instructions_py310(function_key: str, line_number: int) -> list:
        """
        Create Python 3.10-specific breakpoint instructions.

        Generates bytecode that calls:
        _breakpoint_handler(function_key, line_number, locals())

        Python 3.10 calling convention:
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
