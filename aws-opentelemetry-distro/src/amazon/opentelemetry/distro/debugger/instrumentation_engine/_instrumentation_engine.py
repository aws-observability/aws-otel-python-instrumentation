# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Abstract base class for instrumentation engines.

This module provides a common interface for different instrumentation mechanisms:
- SysMonitoringEngine (Python 3.12+)
- BytecodeInjectionEngine (Python 3.9-3.11)
"""

from abc import ABC, abstractmethod
from types import CodeType, FunctionType
from typing import Any, Callable, Dict, Optional, Set


class InstrumentationEngine(ABC):
    """Abstract base class for instrumentation engines."""

    @abstractmethod
    def initialize(self, hit_count_callback: Optional[Callable[[str], bool]] = None) -> None:
        """
        One-time initialization of the engine.

        Args:
            hit_count_callback: Optional callback to be called when a breakpoint is hit.
                               The callback receives the breakpoint_key (function_key:line_number)
                               and returns True if the hit should be captured, False if rate-limited.
        """

    @abstractmethod
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

        All breakpoints for the function are enabled together as a single operation.

        Args:
            code: Code object of the function
            func: Function object (needed for bytecode injection)
            line_numbers: Set of line numbers to enable breakpoints on
            function_key: Function key (module.function) for constructing breakpoint_key
            line_location_hashes: Optional mapping of line_number -> location_hash for span events
            line_capture_configs: Optional mapping of line_number -> CaptureConfig for capture filtering
        """

    def enable_function_entry(  # pylint: disable=too-many-arguments
        self,
        code: CodeType,
        func: FunctionType,
        function_key: str,
        module_name: str,
        qualified_name: str,
        capture_config: Optional[Any] = None,
        location_hash: Optional[str] = None,
        instrumentation_type: Optional[str] = None,
    ) -> bool:
        """
        Enable function-entry/exit instrumentation for a code object.

        Hooks the interpreter's bytecode dispatch directly so the instrumentation
        fires on every invocation regardless of which Python-level reference
        (module attribute, decorator capture, framework registry, stale closure)
        the caller holds. This is what makes PROBE survive Django's URL resolver,
        Flask's view_functions, FastAPI's route table, etc.

        Default implementation returns False (engine does not support entry hooks);
        subclasses override when supported (currently SysMonitoringEngine only).

        Args:
            code: Code object of the function (the hook target — survives stale refs)
            func: Function object (used for trace context / signature inspection)
            function_key: Function key (module.qualname) for snapshot routing
            module_name: Module name for snapshot metadata
            qualified_name: Qualified function name for snapshot metadata
            capture_config: CaptureConfig controlling argument/return/stack capture
            location_hash: LocationHash to attach to the emitted snapshot
            instrumentation_type: "PROBE" or "BREAKPOINT" — surfaces on the snapshot

        Returns:
            True if the engine accepted the hook, False if not supported.
        """
        return False

    def disable_function_entry(self, code: CodeType) -> None:
        """
        Tear down function-entry instrumentation for a code object.

        Default implementation is a no-op for engines that don't implement
        ``enable_function_entry``.
        """

    def suppress_function_entry_for_thread(self) -> None:
        """
        Suppress function-entry callbacks on the current thread.

        Called by ``FunctionWrapper`` before invoking the original function from
        the wrapper path so the engine's entry handlers (which would otherwise
        produce a duplicate snapshot) skip while the wrapper is in flight.

        Default implementation is a no-op.
        """

    def release_function_entry_suppression(self) -> None:
        """Counterpart to ``suppress_function_entry_for_thread``."""

    @abstractmethod
    def disable_breakpoints_for_function(self, code: CodeType, func: FunctionType) -> None:
        """
        Disable ALL breakpoints for a function and restore it to original state.

        This is an all-or-nothing operation that:
        - Removes ALL active breakpoints for the function
        - Restores the function to its original (uninstrumented) form
        - Cleans up any engine-specific state

        Args:
            code: Code object of the function
            func: Function object (needed for restoration)
        """

    @staticmethod
    @abstractmethod
    def supports_runtime() -> bool:
        """
        Check if this engine supports the current Python runtime.

        Returns:
            True if engine can run on current Python version
        """

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up engine resources."""
