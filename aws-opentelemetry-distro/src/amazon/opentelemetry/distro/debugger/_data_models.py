# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data models for refactored debugger functionality.

This module contains the core data structures for atomic breakpoint management:
- BreakpointConfiguration: Parsed from API responses
- BreakpointState: Runtime state tracking (hit counts, timestamps)
- FunctionBreakpointSet: Groups all breakpoints for one function
- CaptureConfig: Configuration for data capture
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import CodeType
from typing import Any, Callable, Dict, List, Optional, Set

from dateutil.parser import isoparse

from amazon.opentelemetry.distro.debugger._capture_rate_limiter import CaptureRateLimiter

logger = logging.getLogger(__name__)

# Default capture configuration limits
DEFAULT_MAX_HITS = 100
DEFAULT_MAX_STRING_LENGTH = 255
DEFAULT_MAX_COLLECTION_WIDTH = 20
DEFAULT_MAX_COLLECTION_DEPTH = 3
DEFAULT_MAX_STACK_FRAMES = 20
DEFAULT_MAX_STACK_TRACE_SIZE = 200
DEFAULT_MAX_OBJECT_DEPTH = 3
DEFAULT_MAX_FIELDS_PER_OBJECT = 20
DEFAULT_RETURN_ATTRIBUTE_NAME = "aws.di.return_value"

# Validation ranges
MIN_MAX_HITS, MAX_MAX_HITS = 1, 1000
MIN_MAX_STRING_LENGTH, MAX_MAX_STRING_LENGTH = 1, 255
MIN_MAX_COLLECTION_WIDTH, MAX_MAX_COLLECTION_WIDTH = 1, 20
MIN_MAX_COLLECTION_DEPTH, MAX_MAX_COLLECTION_DEPTH = 1, 5
MIN_MAX_STACK_FRAMES, MAX_MAX_STACK_FRAMES = 1, 20
MIN_MAX_STACK_TRACE_SIZE, MAX_MAX_STACK_TRACE_SIZE = 1, 1000
MIN_MAX_OBJECT_DEPTH, MAX_MAX_OBJECT_DEPTH = 1, 5
MIN_MAX_FIELDS_PER_OBJECT, MAX_MAX_FIELDS_PER_OBJECT = 1, 20


@dataclass
class CaptureConfig:
    """
    Configuration for parameter and return value capture.

    Attributes:
        capture_return: Whether to capture the return value
        capture_stack_trace: Whether to capture the stack trace
        capture_arguments: Arguments to capture (None=don't capture, []=all, [names]=named only)
        capture_locals: Locals to capture (None=don't capture, []=all, [names]=named only)
        arg_mappings: Maps parameter names to custom attribute names
        return_attribute_name: Custom attribute name for the return value
        max_string_length: Maximum length for captured string values (truncated beyond this)
        max_collection_width: Maximum number of elements captured per collection
        max_collection_depth: Maximum nesting depth for captured collections
        max_stack_frames: Maximum number of stack frames to capture
        max_stack_trace_size: Maximum total size of the captured stack trace
        max_object_depth: Maximum nesting depth for captured objects
        max_fields_per_object: Maximum number of fields captured per object
    """

    capture_return: bool = False
    capture_stack_trace: bool = False
    # None = field absent from API (do not capture)
    # [] = field present as empty list (capture all)
    # ["a", "b"] = capture only the named items
    capture_arguments: Optional[List[str]] = None
    capture_locals: Optional[List[str]] = None
    arg_mappings: Optional[Dict[str, str]] = field(default_factory=dict)
    return_attribute_name: str = DEFAULT_RETURN_ATTRIBUTE_NAME
    max_string_length: int = DEFAULT_MAX_STRING_LENGTH
    max_collection_width: int = DEFAULT_MAX_COLLECTION_WIDTH
    max_collection_depth: int = DEFAULT_MAX_COLLECTION_DEPTH
    max_stack_frames: int = DEFAULT_MAX_STACK_FRAMES
    max_stack_trace_size: int = DEFAULT_MAX_STACK_TRACE_SIZE
    max_object_depth: int = DEFAULT_MAX_OBJECT_DEPTH
    max_fields_per_object: int = DEFAULT_MAX_FIELDS_PER_OBJECT

    def __post_init__(self):
        """
        Validate and clamp configuration values to allowed ranges.
        """
        if self.arg_mappings is None:
            self.arg_mappings = {}

        self.max_string_length = self._clamp(
            self.max_string_length,
            MIN_MAX_STRING_LENGTH,
            MAX_MAX_STRING_LENGTH,
            DEFAULT_MAX_STRING_LENGTH,
            "max_string_length",
        )
        self.max_collection_width = self._clamp(
            self.max_collection_width,
            MIN_MAX_COLLECTION_WIDTH,
            MAX_MAX_COLLECTION_WIDTH,
            DEFAULT_MAX_COLLECTION_WIDTH,
            "max_collection_width",
        )
        self.max_collection_depth = self._clamp(
            self.max_collection_depth,
            MIN_MAX_COLLECTION_DEPTH,
            MAX_MAX_COLLECTION_DEPTH,
            DEFAULT_MAX_COLLECTION_DEPTH,
            "max_collection_depth",
        )
        self.max_stack_frames = self._clamp(
            self.max_stack_frames,
            MIN_MAX_STACK_FRAMES,
            MAX_MAX_STACK_FRAMES,
            DEFAULT_MAX_STACK_FRAMES,
            "max_stack_frames",
        )
        self.max_stack_trace_size = self._clamp(
            self.max_stack_trace_size,
            MIN_MAX_STACK_TRACE_SIZE,
            MAX_MAX_STACK_TRACE_SIZE,
            DEFAULT_MAX_STACK_TRACE_SIZE,
            "max_stack_trace_size",
        )
        self.max_object_depth = self._clamp(
            self.max_object_depth,
            MIN_MAX_OBJECT_DEPTH,
            MAX_MAX_OBJECT_DEPTH,
            DEFAULT_MAX_OBJECT_DEPTH,
            "max_object_depth",
        )
        self.max_fields_per_object = self._clamp(
            self.max_fields_per_object,
            MIN_MAX_FIELDS_PER_OBJECT,
            MAX_MAX_FIELDS_PER_OBJECT,
            DEFAULT_MAX_FIELDS_PER_OBJECT,
            "max_fields_per_object",
        )

        if not isinstance(self.return_attribute_name, str) or not self.return_attribute_name.strip():
            logger.warning("Invalid return_attribute_name '%s', using default", self.return_attribute_name)
            self.return_attribute_name = DEFAULT_RETURN_ATTRIBUTE_NAME

    @staticmethod
    def _clamp(value: int, min_val: int, max_val: int, default: int, name: str) -> int:
        """Clamp value to valid range, use default if invalid type."""
        if not isinstance(value, int):
            logger.warning("Invalid %s=%s (not an int), using %s", name, value, default)
            return default
        if value < min_val:
            logger.warning("%s=%s below minimum %s, clamping to %s", name, value, min_val, min_val)
            return min_val
        if value > max_val:
            logger.warning("%s=%s above maximum %s, clamping to %s", name, value, max_val, max_val)
            return max_val
        return value


@dataclass
class BreakpointConfiguration:
    """
    Complete breakpoint configuration parsed from API response.

    Supports both temporary (BREAKPOINT) and permanent (PROBE) instrumentations:
    - BREAKPOINT: Temporary instrumentation with expiration and hit limits
    - PROBE: Permanent instrumentation that persists until explicitly deleted

    Location format: python:func:<module>:<function_name>:<line_number>
    - line_number is OPTIONAL: 0 or missing = function-level only, >0 = line-level
    - For PROBE: line_number is always 0 (function-level only)
    - Function wrapper is always applied if any breakpoints exist
    - Line breakpoints (line_number > 0) create span events within the function span

    Attributes:
        module: Module name (e.g., "myapp.services")
        function_name: Function name (e.g., "process_order" or "MyClass.method")
        line_number: Line number (0 = function-level only, >0 = line-level)
        capture_config: Configuration for data capture
        config_id: Unique identifier from API
        instrumentation_type: Type of instrumentation ("PROBE" or "BREAKPOINT")
        instrumentation_name: Optional name for the instrumentation (defaults to "" if absent)
        expires_at: Optional expiration timestamp (ignored for PROBE)
        max_hits: Maximum hits before breakpoint is disabled (ignored for PROBE)
        attribute_filters: List of attribute filter objects
    """

    module: str
    function_name: str
    line_number: int
    capture_config: CaptureConfig
    config_id: str
    instrumentation_type: str = "BREAKPOINT"  # "PROBE" or "BREAKPOINT"
    instrumentation_name: Optional[str] = None
    expires_at: Optional[datetime] = None
    max_hits: int = DEFAULT_MAX_HITS
    attribute_filters: List[Dict[str, Any]] = field(default_factory=list)
    created_at: Optional[datetime] = None

    @property
    def function_key(self) -> str:
        """Unique key for the function: module.function_name"""
        return f"{self.module}.{self.function_name}"

    @property
    def breakpoint_key(self) -> str:
        """Unique key for this specific breakpoint: module.function:line"""
        return f"{self.function_key}:{self.line_number}"

    @property
    def is_valid(self) -> bool:
        """Check if this is a valid breakpoint (line_number >= 0)"""
        return self.line_number >= 0

    @property
    def is_line_breakpoint(self) -> bool:
        """Check if this is a line-level breakpoint (line_number > 0)"""
        return self.line_number > 0

    @property
    def is_permanent(self) -> bool:
        """Check if this is a permanent instrumentation (PROBE)"""
        return self.instrumentation_type == "PROBE"

    @property
    def is_temporary(self) -> bool:
        """Check if this is a temporary instrumentation (BREAKPOINT)"""
        return self.instrumentation_type == "BREAKPOINT"

    @staticmethod
    def _parse_utc_datetime(value: Any, field_name: str) -> Optional[datetime]:
        try:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value, tz=timezone.utc)
            if isinstance(value, str):
                parsed = isoparse(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            return None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Invalid %s format %s: %s", field_name, value, exc)
            return None

    # TODO: Should refactor and simplify this method in the future and get rid of the disable.
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-return-statements
    @classmethod
    def from_api_config(cls, breakpoint_config: Dict[str, Any]) -> Optional["BreakpointConfiguration"]:
        """
        Parse API response into BreakpointConfiguration.

        Supports both the new union-based API format and legacy flat format:

        New API format (union types):
        {
            "InstrumentationType": "BREAKPOINT",
            "SignalType": "SNAPSHOT",
            "Location": {
                "CodeLocation": {
                    "Language": "Python",
                    "CodeUnit": "demo_app",
                    "MethodName": "calculate_total",
                    "FilePath": "demo_app.py",
                    "LineNumber": 1
                }
            },
            "CaptureConfiguration": {
                "CodeCapture": {
                    "CaptureArguments": ["items"],
                    "CaptureReturn": true,
                    "CaptureLimits": {"MaxStringLength": 255}
                }
            },
            "LocationHash": "abc123...",
            "ExpiresAt": "2026-03-10T19:34:00Z",
            "ARN": "arn:aws:application-signals:..."
        }

        Legacy flat format (backward compatible):
        {
            "Location": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func", "LineNumber": 10},
            "CaptureConfiguration": {"CaptureReturn": true, "CaptureLimits": {...}},
            "LocationHash": "config-123"
        }

        Args:
            breakpoint_config: Dictionary from API response for a single latest_config

        Returns:
            BreakpointConfiguration instance, or None if parsing fails
        """

        # Helper function for safe integer parsing
        def safe_int(value, default):
            try:
                if isinstance(value, bool):
                    return default
                return int(value) if value is not None else default
            except (ValueError, TypeError):
                return default

        # Helper function for safe boolean parsing
        def safe_bool(value, default):
            try:
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ("true", "1", "yes")
                return bool(value) if value is not None else default
            except (ValueError, TypeError):
                return default

        try:
            # Handle new union Location format: {CodeLocation: {...}} or legacy flat format
            raw_location = breakpoint_config.get("Location", {})
            if not isinstance(raw_location, dict):
                logger.warning("Invalid Location type: %s", type(raw_location))
                return None

            # New API format wraps code location in a CodeLocation key (union type)
            if "CodeLocation" in raw_location:
                location = raw_location["CodeLocation"]
                if not isinstance(location, dict):
                    logger.warning("Invalid CodeLocation type: %s", type(location))
                    return None
            else:
                # Legacy flat format for backward compatibility
                location = raw_location

            # Check language
            language = location.get("Language", "")
            if language.lower() != "python":
                return None

            # Extract location fields
            module = location.get("CodeUnit", "")
            class_name = location.get("ClassName", "")
            method_name = location.get("MethodName", "")

            # Build function name (ClassName.MethodName or just MethodName)
            if class_name:
                function = f"{class_name}.{method_name}"
            else:
                function = method_name

            # Validate required fields
            if not module or not function:
                logger.warning(
                    "Invalid location in API config: module='%s', function='%s'. Skipping this breakpoint.",
                    module,
                    function,
                )
                return None

            # Parse instrumentation type (defaults to BREAKPOINT for backward compatibility)
            instrumentation_type = str(breakpoint_config.get("InstrumentationType", "BREAKPOINT")).upper()
            if instrumentation_type not in ("PROBE", "BREAKPOINT"):
                logger.warning(
                    "Invalid InstrumentationType '%s' for %s.%s. "
                    "Must be 'PROBE' or 'BREAKPOINT'. Defaulting to 'BREAKPOINT'.",
                    instrumentation_type,
                    module,
                    function,
                )
                instrumentation_type = "BREAKPOINT"

            instrumentation_name = breakpoint_config.get("InstrumentationName") or ""

            # Parse line number safely - 0 or missing means function-level only, >0 means line-level
            # For PROBE: Always force line_number to 0 (function-level only)
            line_number = safe_int(location.get("LineNumber"), 0)
            if instrumentation_type == "PROBE" and line_number > 0:
                logger.debug(
                    "PROBE instrumentation for %s.%s has line_number %d. Forcing to 0 (function-level only).",
                    module,
                    function,
                    line_number,
                )
                line_number = 0

            if not isinstance(line_number, int) or line_number < 0:
                logger.warning(
                    "Invalid line_number %s for %s.%s. Must be >= 0. Skipping this breakpoint.",
                    line_number,
                    module,
                    function,
                )
                return None

            # Parse capture config with safe defaults
            # New API format wraps code capture in a CodeCapture key (union type)
            raw_capture_config = breakpoint_config.get("CaptureConfiguration", {})
            if not isinstance(raw_capture_config, dict):
                logger.warning("Invalid CaptureConfiguration type: %s, using defaults", type(raw_capture_config))
                raw_capture_config = {}

            if "CodeCapture" in raw_capture_config:
                config_data = raw_capture_config["CodeCapture"]
                if not isinstance(config_data, dict):
                    logger.warning("Invalid CodeCapture type: %s, using defaults", type(config_data))
                    config_data = {}
            else:
                # Legacy flat format for backward compatibility
                config_data = raw_capture_config

            capture_limits = config_data.get("CaptureLimits", {})
            if not isinstance(capture_limits, dict):
                capture_limits = {}

            # Parse capture arguments and locals
            # Distinguish missing (None = do not capture) from present-but-empty ([] = capture all).
            capture_arguments = None
            if "CaptureArguments" in config_data:
                val = config_data["CaptureArguments"]
                capture_arguments = val if isinstance(val, list) else []

            capture_locals = None
            if "CaptureLocals" in config_data:
                val = config_data["CaptureLocals"]
                capture_locals = val if isinstance(val, list) else []

            # Create CaptureConfig (it handles its own validation)
            logger.debug(
                "Creating CaptureConfig with limits: MaxStringLength=%s, MaxObjectDepth=%s, MaxCollectionDepth=%s",
                capture_limits.get("MaxStringLength"),
                capture_limits.get("MaxObjectDepth"),
                capture_limits.get("MaxCollectionDepth"),
            )
            capture_config = CaptureConfig(
                capture_return=safe_bool(config_data.get("CaptureReturn"), False),
                capture_stack_trace=safe_bool(config_data.get("CaptureStackTrace"), False),
                capture_arguments=capture_arguments,
                capture_locals=capture_locals,
                arg_mappings=(
                    config_data.get("arg_mappings") if isinstance(config_data.get("arg_mappings"), dict) else {}
                ),
                return_attribute_name=str(config_data.get("return_attribute_name", DEFAULT_RETURN_ATTRIBUTE_NAME)),
                max_string_length=safe_int(capture_limits.get("MaxStringLength"), DEFAULT_MAX_STRING_LENGTH),
                max_collection_width=safe_int(capture_limits.get("MaxCollectionWidth"), DEFAULT_MAX_COLLECTION_WIDTH),
                max_collection_depth=safe_int(capture_limits.get("MaxCollectionDepth"), DEFAULT_MAX_COLLECTION_DEPTH),
                max_stack_frames=safe_int(capture_limits.get("MaxStackFrames"), DEFAULT_MAX_STACK_FRAMES),
                max_stack_trace_size=safe_int(capture_limits.get("MaxStackTraceSize"), DEFAULT_MAX_STACK_TRACE_SIZE),
                max_object_depth=safe_int(capture_limits.get("MaxObjectDepth"), DEFAULT_MAX_OBJECT_DEPTH),
                max_fields_per_object=safe_int(capture_limits.get("MaxFieldsPerObject"), DEFAULT_MAX_FIELDS_PER_OBJECT),
            )

            # Parse metadata safely
            config_id = str(breakpoint_config.get("LocationHash", ""))

            # Parse expiry timestamp safely (Unix timestamp or ISO 8601 string)
            # For PROBE: Ignore expires_at (permanent instrumentation)
            expires_at = None
            if instrumentation_type == "BREAKPOINT":
                expires_at_value = breakpoint_config.get("ExpiresAt")
                if expires_at_value:
                    expires_at = BreakpointConfiguration._parse_utc_datetime(expires_at_value, "ExpiresAt")
            elif breakpoint_config.get("ExpiresAt"):
                logger.debug("Ignoring ExpiresAt for PROBE instrumentation %s.%s", module, function)

            # Parse CreatedAt (optional)
            created_at = None
            created_at_value = breakpoint_config.get("CreatedAt")
            if created_at_value is not None:
                created_at = BreakpointConfiguration._parse_utc_datetime(created_at_value, "CreatedAt")

            # Parse and clamp max_hits
            # For PROBE: Ignore max_hits (permanent instrumentation)
            if instrumentation_type == "BREAKPOINT":
                max_hits = CaptureConfig._clamp(
                    safe_int(capture_limits.get("MaxHits"), DEFAULT_MAX_HITS),
                    MIN_MAX_HITS,
                    MAX_MAX_HITS,
                    DEFAULT_MAX_HITS,
                    "max_hits",
                )
            else:
                max_hits = DEFAULT_MAX_HITS  # Ignored for PROBE, but set default for consistency
                if capture_limits.get("MaxHits"):
                    logger.debug("Ignoring MaxHits for PROBE instrumentation %s.%s", module, function)
            logger.debug(
                "Parsed config: max_hits=%s, max_string_length=%s, max_object_depth=%s, max_collection_depth=%s",
                max_hits,
                capture_config.max_string_length,
                capture_config.max_object_depth,
                capture_config.max_collection_depth,
            )

            # Parse attribute filters safely
            attribute_filters = breakpoint_config.get("AttributeFilters", [])
            if not isinstance(attribute_filters, list):
                logger.warning("Invalid AttributeFilters type, using empty list")
                attribute_filters = []

            return cls(
                module=module,
                function_name=function,
                line_number=line_number,
                capture_config=capture_config,
                config_id=config_id,
                instrumentation_type=instrumentation_type,
                instrumentation_name=instrumentation_name,
                expires_at=expires_at,
                max_hits=max_hits,
                attribute_filters=attribute_filters,
                created_at=created_at,
            )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            # Catch-all for any unexpected errors
            logger.error("Unexpected error parsing API config: %s", exc, exc_info=True)
            return None


@dataclass
class BreakpointState:
    """
    Runtime state for a breakpoint, preserved across configuration updates.

    Attributes:
        breakpoint_key: Unique identifier (function_key:line_number)
        location_hash: LocationHash (config_id) for status reporting
        instrumentation_type: Type of instrumentation ("PROBE" or "BREAKPOINT")
        hit_count: Number of times this breakpoint was hit
        is_disabled: Whether breakpoint is disabled due to hit limit
        hit_in_last_period: Whether breakpoint was hit since last status report (reset after reporting)
    """

    breakpoint_key: str
    location_hash: str = ""
    instrumentation_type: str = "BREAKPOINT"
    hit_count: int = 0
    is_disabled: bool = False
    hit_in_last_period: bool = False
    rate_limiter: CaptureRateLimiter = field(default_factory=CaptureRateLimiter)


@dataclass
class FunctionBreakpointSet:
    """
    All breakpoints for one function, managed atomically.

    This represents the target state for a function's instrumentation.
    All breakpoints in this set are applied/removed together.

    Attributes:
        function_key: Unique identifier (module.function_name)
        module: Module name
        function_name: Function name (may include class: "Class.method")
        breakpoints: Dict mapping line_number to BreakpointConfiguration
        original_function: Reference to original uninstrumented function
        wrapped_function: Reference to wrapped function (creates span)
        code_object: Code object for line breakpoint monitoring
        is_instrumented: Whether instrumentation has been applied
        states: Dict mapping breakpoint_key to BreakpointState
    """

    function_key: str
    module: str
    function_name: str
    breakpoints: Dict[int, BreakpointConfiguration] = field(default_factory=dict)

    # Instrumentation state (populated after application)
    original_function: Optional[Callable] = None
    wrapped_function: Optional[Callable] = None
    code_object: Optional[CodeType] = None
    is_instrumented: bool = False

    # Breakpoint states (preserved across updates)
    states: Dict[str, BreakpointState] = field(default_factory=dict)

    @property
    def line_numbers(self) -> Set[int]:
        """All line numbers > 0 (line-level breakpoints only, excludes function-level)"""
        return {line for line in self.breakpoints if line > 0}

    @property
    def needs_wrapper(self) -> bool:
        """Check if function wrapper is needed (always True if any breakpoints exist)"""
        return len(self.breakpoints) > 0

    @property
    def capture_config(self) -> Optional[CaptureConfig]:
        """Get capture config (use first breakpoint's config)"""
        if self.breakpoints:
            return next(iter(self.breakpoints.values())).capture_config
        return None
