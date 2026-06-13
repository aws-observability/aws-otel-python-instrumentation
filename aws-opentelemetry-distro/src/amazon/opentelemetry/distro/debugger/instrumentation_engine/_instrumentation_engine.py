# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Abstract base class for instrumentation engines (sys.monitoring or bytecode)."""

from abc import ABC, abstractmethod
from types import CodeType, FunctionType
from typing import Any, Callable, Dict, Optional, Set


class InstrumentationEngine(ABC):
    """Abstract base for instrumentation engines."""

    def __init__(self) -> None:
        self._supports_function_entry: bool = False

    @abstractmethod
    def initialize(self, hit_count_callback: Optional[Callable[[str], bool]] = None) -> None:
        """One-time engine initialization."""

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
        """Enable all line-level breakpoints for a function atomically."""

    def supports_function_entry(self) -> bool:
        """True if this engine implements ``enable_function_entry``."""
        return getattr(self, "_supports_function_entry", False)

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
        """Enable function-entry/exit instrumentation. Default no-op returns False."""
        _ = self
        return False

    def disable_function_entry(self, code: CodeType, func: Optional[FunctionType] = None) -> None:
        """Tear down function-entry instrumentation. Default no-op."""
        _ = self
        _ = code
        _ = func

    @abstractmethod
    def disable_breakpoints_for_function(self, code: CodeType, func: FunctionType) -> None:
        """Disable all breakpoints for a function and restore original state."""

    @staticmethod
    @abstractmethod
    def supports_runtime() -> bool:
        """True if this engine supports the current Python runtime."""

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up engine resources."""
