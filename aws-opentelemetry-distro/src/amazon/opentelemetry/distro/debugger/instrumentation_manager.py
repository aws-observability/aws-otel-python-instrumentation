# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=too-many-lines

"""
Instrumentation Manager - Central coordinator for atomic breakpoint management.

This module provides the InstrumentationManager class that handles:
- Grouping breakpoint configurations by function
- Determining configuration changes
- Managing atomic updates with state preservation
- Thread-safe operations with comprehensive error handling
"""

import importlib.util
import logging
import sys
from threading import RLock
from typing import Any, Dict, List, Optional, Set

from amazon.opentelemetry.distro.debugger._data_models import (
    BreakpointConfiguration,
    BreakpointState,
    FunctionBreakpointSet,
)
from amazon.opentelemetry.distro.debugger._function_wrapper import FunctionWrapper, set_snapshot_emitter
from amazon.opentelemetry.distro.debugger._snapshot_otlp_emitter import SnapshotOtlpEmitter
from amazon.opentelemetry.distro.debugger._status_reporter import ConfigurationStatus, ErrorCause
from amazon.opentelemetry.distro.debugger.instrumentation_engine._instrumentation_engine import InstrumentationEngine
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv._incubating.attributes.deployment_attributes import DEPLOYMENT_ENVIRONMENT_NAME
from opentelemetry.semconv.resource import ResourceAttributes

logger = logging.getLogger(__name__)


class InstrumentationManager:
    """
    Central coordinator for atomic breakpoint management.

    Key responsibilities:
    - Group breakpoints by function for atomic operations
    - Determine what changed between configurations
    - Preserve state for unchanged breakpoints
    - Never raise exceptions (graceful degradation)
    """

    def __init__(self, tracer_provider=None, service: str = "", environment: str = ""):
        """
        Initialize the instrumentation manager.

        Args:
            tracer_provider: OpenTelemetry TracerProvider (kept for backward compat, no longer used for spans)
            service: Service name for status reporting
            environment: Environment name for status reporting
        """
        # Active instrumentation (function_key -> FunctionBreakpointSet)
        self._active_functions: Dict[str, FunctionBreakpointSet] = {}

        # Preserved states (breakpoint_key -> BreakpointState)
        self._preserved_states: Dict[str, BreakpointState] = {}

        # Initialize components
        self._wrapper = FunctionWrapper()
        self._engine = self._select_engine()

        # Initialize OTLP snapshot emitter (replaces file-based SnapshotFileWriter)
        # Build a Resource with service name and environment for the LoggerProvider
        resource = self._build_resource(service, environment)
        self._snapshot_emitter = SnapshotOtlpEmitter(resource=resource)
        # Eagerly initialize from this (well-behaved user) thread. Lazy init from a
        # sys.monitoring PY_RETURN callback can hit
        # ``RuntimeError("cannot schedule new futures after interpreter shutdown")``
        # because ``BatchLogRecordProcessor`` spawns a daemon worker on construction.
        try:
            self._snapshot_emitter.initialize()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("Snapshot emitter eager initialization deferred", exc_info=True)
        set_snapshot_emitter(self._snapshot_emitter)

        # Status reporter (will be set later by debugger.py)
        self._status_reporter = None

        # Failed configuration tracking: location_hash -> error_cause
        # Prevents retrying the same broken config on every poll cycle.
        # Entries are cleared when the config is removed from the incoming list.
        self._failed_configs: Dict[str, str] = {}

        # Thread safety
        self._lock = RLock()

        logger.debug("InstrumentationManager initialized")

    @staticmethod
    def _build_resource(service, environment):
        """Build a Resource with service name and environment for the OTLP LoggerProvider."""
        try:
            attrs = {}
            if service:
                attrs[ResourceAttributes.SERVICE_NAME] = service
            if environment:
                attrs[DEPLOYMENT_ENVIRONMENT_NAME] = environment
                attrs[ResourceAttributes.DEPLOYMENT_ENVIRONMENT] = environment
            return Resource.create(attrs) if attrs else None
        except Exception:  # pylint: disable=broad-exception-caught
            return None

    def _select_engine(self) -> Optional[InstrumentationEngine]:  # pragma: no cover  # pylint: disable=no-self-use
        """
        Select and initialize the appropriate instrumentation engine based on Python version.

        CRITICAL: Never raises exceptions.

        Returns:
            InstrumentationEngine instance or None if no engine available

        Not unit-tested: it imports and initializes the real, version-specific line-breakpoint
        engine (registering sys.monitoring or rewriting bytecode). Exercised on every real DI
        startup and validated end-to-end by the DI contract tests.
        """
        try:
            # Python 3.12+ - Use SysMonitoringEngine
            if sys.version_info >= (3, 12):
                try:
                    # pylint: disable=import-outside-toplevel
                    from amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine import (
                        SysMonitoringEngine,
                    )

                    engine = SysMonitoringEngine()
                    engine.initialize(hit_count_callback=self.increment_hit_count)
                    logger.debug(
                        "Selected SysMonitoringEngine for Python %d.%d",
                        sys.version_info.major,
                        sys.version_info.minor,
                    )
                    return engine
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.error("Failed to initialize SysMonitoringEngine: %s", exception, exc_info=True)

            # Python 3.9-3.11 - Use BytecodeInjectionEngine
            elif sys.version_info >= (3, 9):
                try:
                    # pylint: disable=import-outside-toplevel
                    from amazon.opentelemetry.distro.debugger.instrumentation_engine._bytecode_injection_engine import (
                        BytecodeInjectionEngine,
                    )

                    engine = BytecodeInjectionEngine()
                    engine.initialize(hit_count_callback=self.increment_hit_count)
                    logger.debug(
                        "Selected BytecodeInjectionEngine for Python %d.%d",
                        sys.version_info.major,
                        sys.version_info.minor,
                    )
                    return engine
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.error("Failed to initialize BytecodeInjectionEngine: %s", exception, exc_info=True)

            # Python < 3.9 - Not supported
            else:
                logger.warning(
                    "Python %d.%d is not supported for line breakpoints. "
                    "Supported versions: 3.9-3.12+. Function wrapping will still work, but line breakpoints will not.",
                    sys.version_info.major,
                    sys.version_info.minor,
                )

            return None

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Unexpected error selecting engine: %s", exception, exc_info=True)
            return None

    def get_status(self) -> Dict:
        """
        Get comprehensive status of the manager.

        Returns:
            Dict with status information
        """
        try:
            with self._lock:
                return {
                    "active_functions": len(self._active_functions),
                    "total_breakpoints": sum(len(bp_set.breakpoints) for bp_set in self._active_functions.values()),
                    "preserved_states": len(self._preserved_states),
                    "functions": {
                        func_key: {
                            "line_numbers": list(bp_set.line_numbers),
                            "is_instrumented": bp_set.is_instrumented,
                            "breakpoint_count": len(bp_set.states),
                            "total_hits": sum(  # TODO: Do we need total hits for a function? Think more on the value
                                state.hit_count for state in bp_set.states.values()
                            ),
                        }
                        for func_key, bp_set in self._active_functions.items()
                    },
                }
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error getting status: %s", exception, exc_info=True)
            return {
                "error": str(exception),
                "active_functions": 0,
                "total_breakpoints": 0,
                "preserved_states": 0,
                "functions": {},
            }

    @staticmethod
    def _group_by_function(
        configs: List[BreakpointConfiguration],
    ) -> Dict[str, FunctionBreakpointSet]:
        """
        Group breakpoint configurations by function.

        Multiple configs for the same function are merged into one set.
        The API ensures no duplicates (only one PROBE per function, one BREAKPOINT per line).
        PROBE (line=0) and BREAKPOINT (line>0) can coexist for the same function.

        Invalid configs are skipped with logging.

        CRITICAL: Never raises exceptions.

        Args:
            configs: List of breakpoint configurations

        Returns:
            Dict mapping function_key to FunctionBreakpointSet
        """
        grouped = {}
        skipped_count = 0

        try:
            for config in configs:
                try:
                    if not config:
                        skipped_count += 1
                        continue

                    func_key = config.function_key

                    if func_key not in grouped:
                        grouped[func_key] = FunctionBreakpointSet(
                            function_key=func_key,
                            module=config.module,
                            function_name=config.function_name,
                            breakpoints={},
                        )

                    # Add breakpoint to set (API ensures no duplicates at same line)
                    # PROBE will be at line=0, BREAKPOINT at line>0
                    grouped[func_key].breakpoints[config.line_number] = config

                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.error("Failed to group config: %s", exception, exc_info=True)
                    skipped_count += 1
                    continue

            if skipped_count > 0:
                logger.warning("Skipped %d invalid configurations", skipped_count)

            logger.debug("Grouped %d configs into %d functions", len(configs), len(grouped))
            return grouped

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Unexpected error in grouping: %s", exception, exc_info=True)
            return {}

    @staticmethod
    def _has_changed(  # pylint: disable=too-many-return-statements
        old: FunctionBreakpointSet, new: FunctionBreakpointSet
    ) -> bool:
        """
        Check if breakpoint set has changed by comparing config identity.

        Uses config_id (locationHash) and created_at to detect recreated configs.
        This is consistent with Java and JavaScript SDK implementations.

        CRITICAL: Never raises exceptions.

        Args:
            old: Current breakpoint set
            new: New breakpoint set

        Returns:
            True if breakpoints have changed, False otherwise
        """
        try:
            # Compare breakpoint line numbers — if different lines, definitely changed
            if set(old.breakpoints.keys()) != set(new.breakpoints.keys()):
                return True

            # Compare config identity for each line
            for line_num in old.breakpoints.keys():
                try:
                    if line_num in new.breakpoints:
                        old_bp = old.breakpoints[line_num]
                        new_bp = new.breakpoints[line_num]
                        # Check locationHash
                        if old_bp.config_id != new_bp.config_id:
                            return True
                        # Check created_at — if both non-None and differ, config was recreated
                        if (
                            old_bp.created_at is not None
                            and new_bp.created_at is not None
                            and old_bp.created_at != new_bp.created_at
                        ):
                            return True
                        # If old has no created_at but new does (upgrade), treat as changed
                        if old_bp.created_at is None and new_bp.created_at is not None:
                            return True
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.warning("Error comparing line %d: %s", line_num, exception)
                    return True

            return False

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error comparing breakpoint sets: %s", exception, exc_info=True)
            return True

    @staticmethod
    def _get_unchanged_breakpoints(old: FunctionBreakpointSet, new: FunctionBreakpointSet) -> Set[str]:
        """
        Get breakpoint keys that are unchanged between old and new.

        A breakpoint is unchanged if:
        - Same line number exists in both
        - Same config_id (locationHash)
        - Same created_at timestamp

        CRITICAL: Never raises exceptions.

        Args:
            old: Current breakpoint set
            new: New breakpoint set

        Returns:
            Set of breakpoint_keys to preserve state for
        """
        try:
            unchanged = set()

            try:
                common_lines = set(old.breakpoints.keys()) & set(new.breakpoints.keys())
            except Exception as exception:  # pylint: disable=broad-exception-caught
                logger.error("Error getting common lines: %s", exception)
                return set()

            for line_num in common_lines:
                try:
                    old_bp = old.breakpoints.get(line_num)
                    new_bp = new.breakpoints.get(line_num)

                    if old_bp and new_bp:
                        # Config is unchanged only if both config_id and created_at match
                        config_id_matches = old_bp.config_id == new_bp.config_id
                        created_at_matches = old_bp.created_at == new_bp.created_at
                        if config_id_matches and created_at_matches:
                            bp_key = f"{old.function_key}:{line_num}"
                            unchanged.add(bp_key)
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.warning("Error comparing line %d: %s", line_num, exception)
                    continue

            logger.debug("Found %d unchanged breakpoints", len(unchanged))
            return unchanged

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error getting unchanged breakpoints: %s", exception, exc_info=True)
            return set()

    def _instrument_function_level(self, bp_set: FunctionBreakpointSet):
        """Arm function-level (line=0) instrumentation via the engine.

        Returns (target_func, target_code) on success, (None, None) on refusal
        or when no function-level breakpoint is configured.
        """
        if not (self._engine and 0 in bp_set.breakpoints):
            return None, None

        try:
            discovered = FunctionWrapper._discover_function(bp_set.module, bp_set.function_name)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.debug("Function discovery failed for %s: %s", bp_set.function_key, exception)
            return None, None

        target_func = discovered.method if hasattr(discovered, "method") else discovered
        target_code = getattr(target_func, "__code__", None)
        if target_func is None or target_code is None:
            return None, None

        fn_bp = bp_set.breakpoints[0]
        try:
            accepted = bool(
                self._engine.enable_function_level_instrumentation(
                    code=target_code,
                    func=target_func,
                    function_key=bp_set.function_key,
                    module_name=bp_set.module,
                    qualified_name=bp_set.function_name,
                    capture_config=fn_bp.capture_config,
                    location_hash=fn_bp.config_id,
                    instrumentation_type=fn_bp.instrumentation_type,
                )
            )
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Engine refused function-level hook for %s: %s",
                bp_set.function_key,
                exception,
                exc_info=True,
            )
            return None, None

        return (target_func, target_code) if accepted else (None, None)

    def _apply_function(  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        self, bp_set: FunctionBreakpointSet
    ):
        """
        Apply function wrapper (no line breakpoints yet - Phase 5).

        CRITICAL: Can raise exceptions (caught by caller for rollback).

        Args:
            bp_set: Function breakpoint set to apply

        Raises:
            Exception: If instrumentation fails (triggers rollback)
        """
        try:
            if not bp_set.needs_wrapper:
                logger.debug("No wrapper needed for %s", bp_set.function_key)
                return

            # Early module existence check — catch missing modules before the deep call chain
            # to produce a clean warning instead of a full ModuleNotFoundError traceback.
            # Skip for modules already loaded (includes __main__, imported libs, etc.)
            # since the wrapper resolves modules via sys.modules directly.
            module_name = bp_set.module
            if module_name not in sys.modules:
                try:
                    spec = importlib.util.find_spec(module_name)
                except (ValueError, ModuleNotFoundError):
                    spec = None
                if spec is None:
                    raise ModuleNotFoundError(
                        f"Module '{module_name}' not found. "
                        f"Breakpoint for {bp_set.function_key} will not be applied. "
                        f"Verify the CodeUnit value matches the Python module name."
                    )

            # Only set location_hash for span if there's a method-level breakpoint (line 0)
            # Line-level breakpoints should only have location_hash in span events, not in span
            location_hash = None
            if 0 in bp_set.breakpoints:
                location_hash = bp_set.breakpoints[0].config_id

            # Function-level (line=0): engine only, no setattr wrapper. The
            # engine fires PY_START / PY_RETURN / PY_UNWIND natively, which
            # captures exception flow correctly without a Python-level wrapper.
            target_func, target_code = self._instrument_function_level(bp_set)
            if target_func is not None and target_code is not None:
                bp_set.original_function = target_func
                bp_set.wrapped_function = target_func
                bp_set.code_object = target_code
                bp_set.is_instrumented = True
                original_callable = target_func
            else:
                # Line-only breakpoints (or engine refused): use the setattr
                # wrapper so line BPs land span-events on a known parent span.
                original, wrapped = self._wrapper.instrument_function(
                    module_name=bp_set.module,
                    function_name=bp_set.function_name,
                    capture_config=bp_set.capture_config,
                    location_hash=location_hash,
                    manager=self,
                )
                original_callable = original.__func__ if isinstance(original, (staticmethod, classmethod)) else original
                bp_set.original_function = original
                bp_set.wrapped_function = wrapped
                bp_set.code_object = getattr(original_callable, "__code__", None)
                bp_set.is_instrumented = True

            # Enable line breakpoints if engine is available and there are line breakpoints
            if self._engine and bp_set.line_numbers and bp_set.code_object:
                try:
                    # Build location hash and capture config mappings for line-level breakpoints
                    # Only for line-level breakpoints (line_num > 0), not method-level (line_num == 0)
                    line_location_hashes = {}
                    line_capture_configs = {}
                    for line_num, bp_config in bp_set.breakpoints.items():
                        if line_num > 0:  # Only line-level breakpoints need hash in events
                            line_location_hashes[line_num] = bp_config.config_id
                            line_capture_configs[line_num] = bp_config.capture_config

                    self._engine.enable_breakpoints_for_function(
                        code=bp_set.code_object,
                        func=original_callable,
                        line_numbers=bp_set.line_numbers,
                        function_key=bp_set.function_key,
                        line_location_hashes=line_location_hashes,
                        line_capture_configs=line_capture_configs,
                    )
                    logger.debug("Enabled %d line breakpoints for %s", len(bp_set.line_numbers), bp_set.function_key)
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to enable line breakpoints for %s: %s. "
                        "Function wrapping will continue, but line breakpoints will not work.",
                        bp_set.function_key,
                        exception,
                        exc_info=True,
                    )
                    # Don't re-raise - function wrapping still works without line breakpoints
            elif not self._engine and bp_set.line_numbers:
                logger.debug(
                    "No engine available for line breakpoints in %s. "
                    "Function wrapping will work, but line breakpoints will not.",
                    bp_set.function_key,
                )

            # Restore/create states for each breakpoint (including function-level line 0)
            for line_num in bp_set.breakpoints.keys():
                bp_key = f"{bp_set.function_key}:{line_num}"
                if bp_key in self._preserved_states:
                    # Restore preserved state (unchanged breakpoint)
                    bp_set.states[bp_key] = self._preserved_states.pop(bp_key)
                    bp_set.states[bp_key].instrumentation_type = bp_set.breakpoints[line_num].instrumentation_type
                    logger.debug("Restored state for %s: %d hits", bp_key, bp_set.states[bp_key].hit_count)
                else:
                    # Create new state (new breakpoint)
                    bp_set.states[bp_key] = BreakpointState(
                        breakpoint_key=bp_key,
                        location_hash=bp_set.breakpoints[line_num].config_id,
                        instrumentation_type=bp_set.breakpoints[line_num].instrumentation_type,
                    )
                    logger.debug("Created new state for %s", bp_key)

            # Store in active functions
            self._active_functions[bp_set.function_key] = bp_set

            logger.debug("Applied function wrapper: %s", bp_set.function_key)

        except ModuleNotFoundError as exception:
            # Graceful handling — log a clean warning, no traceback
            logger.warning("%s", exception)
            raise
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Failed to apply function %s: %s", bp_set.function_key, exception, exc_info=True)

            # Attempt rollback before re-raising
            self._rollback(bp_set)
            raise

    def _remove_function(self, func_key: str, preserve_state_for_bp_keys: Optional[Set[str]] = None):
        """
        Remove function wrapper and restore original.

        CRITICAL: Never raises exceptions.

        Args:
            func_key: Function key to remove
            preserve_state_for_bp_keys: Set of breakpoint_keys to preserve state for.
                                       None = don't preserve any state.
                                       Set = preserve only these specific breakpoints.
        """
        try:
            if func_key not in self._active_functions:
                logger.debug("Function %s not in active functions", func_key)
                return

            bp_set = self._active_functions[func_key]

            # Preserve state for specified breakpoints
            if preserve_state_for_bp_keys is not None:
                for bp_key, state in bp_set.states.items():
                    if bp_key in preserve_state_for_bp_keys:
                        self._preserved_states[bp_key] = state
                        logger.debug(
                            "Preserved state for %s: %d hits, disabled=%s",
                            bp_key,
                            state.hit_count,
                            state.is_disabled,
                        )

            # Disable line breakpoints if engine is available
            if self._engine and bp_set.code_object and bp_set.original_function:
                try:
                    original = bp_set.original_function
                    original_callable = (
                        original.__func__ if isinstance(original, (staticmethod, classmethod)) else original
                    )
                    self._engine.disable_breakpoints_for_function(code=bp_set.code_object, func=original_callable)
                    logger.debug("Disabled line breakpoints for %s", func_key)
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to disable line breakpoints for %s: %s. Continuing with function restoration.",
                        func_key,
                        exception,
                        exc_info=True,
                    )
                    # Don't re-raise - continue with function restoration

            # Disable function-level (line=0) engine hook if it was armed.
            # No-op when only line BPs were configured; engine clears state by code_id.
            if self._engine and bp_set.code_object and bp_set.original_function:
                try:
                    original = bp_set.original_function
                    original_callable = (
                        original.__func__ if isinstance(original, (staticmethod, classmethod)) else original
                    )
                    self._engine.disable_function_level_instrumentation(code=bp_set.code_object, func=original_callable)
                except Exception as exception:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to disable function-level hook for %s: %s",
                        func_key,
                        exception,
                        exc_info=True,
                    )

            # Restore original function
            if bp_set.is_instrumented:
                success = self._wrapper.restore_function(
                    module_name=bp_set.module,
                    function_name=bp_set.function_name,
                    original_func=bp_set.original_function,
                )
                if not success:
                    logger.warning("Failed to restore function %s", func_key)

            # Remove from active
            del self._active_functions[func_key]

            logger.debug("Removed function wrapper: %s", func_key)

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Failed to remove function %s: %s", func_key, exception, exc_info=True)
            # Don't re-raise - this is cleanup code

    def _rollback(self, bp_set: FunctionBreakpointSet):
        """
        Rollback partial instrumentation on failure.

        CRITICAL: Never raises exceptions.

        Args:
            bp_set: Function breakpoint set to rollback
        """
        try:
            if bp_set.is_instrumented and bp_set.original_function:
                success = self._wrapper.restore_function(
                    module_name=bp_set.module,
                    function_name=bp_set.function_name,
                    original_func=bp_set.original_function,
                )
                if success:
                    logger.debug("Rollback successful for %s", bp_set.function_key)
                else:
                    logger.warning("Rollback failed for %s", bp_set.function_key)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Rollback error for %s: %s", bp_set.function_key, exception, exc_info=True)

    def increment_hit_count(self, breakpoint_key: str) -> bool:
        """
        Increment hit count for a breakpoint and check for hit limit and rate limit.

        This method is called by the instrumentation engine when a breakpoint is hit.
        It increments the hit count, disables the breakpoint if it reaches its limit,
        and applies per-instrumentation rate limiting.

        Returns:
            True if capture should proceed (allowed), False if capture should be
            skipped (rate-limited or disabled). Callers should skip snapshot emission
            when this returns False.

        Args:
            breakpoint_key: Breakpoint key (function_key:line_number)
        """
        logger.debug("increment_hit_count called for %s, status_reporter=%s", breakpoint_key, self._status_reporter)
        try:
            with self._lock:
                func_key = breakpoint_key.rsplit(":", 1)[0]
                bp_set = self._active_functions.get(func_key)
                if bp_set is None or breakpoint_key not in bp_set.states:
                    logger.warning("Breakpoint %s not found in active functions", breakpoint_key)
                    return False
                state = bp_set.states[breakpoint_key]

                # Don't increment if already disabled
                if state.is_disabled:
                    logger.debug("Breakpoint %s is disabled, ignoring hit", breakpoint_key)
                    return False

                # Find the configuration for this breakpoint
                line_number = int(breakpoint_key.rsplit(":", 1)[-1])
                config = bp_set.breakpoints.get(line_number)

                # Check rate limit FIRST — transient throttle, skip without
                # counting toward max_hits so a burst can't exhaust the budget
                # of attempts before any snapshots are actually captured.
                if not state.rate_limiter.try_acquire():
                    logger.debug("Breakpoint %s rate-limited (count %d)", breakpoint_key, state.hit_count)
                    return False

                # Capture is going to proceed — count it.
                state.hit_count += 1
                state.hit_in_last_period = True

                # Report ACTIVE immediately on first successful hit
                if state.hit_count == 1:
                    self._report_immediate(state.location_hash, state.instrumentation_type, ConfigurationStatus.ACTIVE)

                # Check maxHits disable condition (BREAKPOINT only, not PROBE).
                # After hit_count reaches max_hits, the next call disables.
                if config and not config.is_permanent and state.hit_count > config.max_hits:
                    # Disable the breakpoint
                    state.is_disabled = True
                    logger.debug(
                        "Disabled breakpoint %s after %d hits (limit: %d)",
                        breakpoint_key,
                        state.hit_count,
                        config.max_hits,
                    )
                    # Report DISABLED immediately
                    self._report_immediate(
                        state.location_hash, state.instrumentation_type, ConfigurationStatus.DISABLED
                    )
                    return False

                logger.debug("Breakpoint %s hit count: %d", breakpoint_key, state.hit_count)
                return True

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error incrementing hit count for %s: %s", breakpoint_key, exception, exc_info=True)
            return False

    def _cleanup_orphaned_states(self):
        """
        Remove preserved states for breakpoints that are no longer active.

        A state is orphaned if its function is not in active_functions or
        the specific breakpoint is not in the function's current breakpoints.

        """
        try:
            with self._lock:
                # Get all currently active breakpoint keys
                active_bp_keys = set()
                for bp_set in self._active_functions.values():
                    try:
                        if bp_set.states:
                            active_bp_keys.update(bp_set.states.keys())
                    except Exception as exception:  # pylint: disable=broad-exception-caught
                        logger.warning("Error getting states from %s: %s", bp_set.function_key, exception)
                        continue

                # Find orphaned states
                orphaned = set(self._preserved_states.keys()) - active_bp_keys

                # Remove orphaned states
                for bp_key in orphaned:
                    del self._preserved_states[bp_key]
                    logger.debug("Cleaned up orphaned state: %s", bp_key)

                if orphaned:
                    logger.debug("Cleaned up %d orphaned states", len(orphaned))

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error cleaning up orphaned states: %s", exception, exc_info=True)

    def apply_configuration(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        self, configs: List[BreakpointConfiguration]
    ) -> Dict[str, Any]:
        """
        Apply new breakpoint configuration atomically with error isolation.

        This is the main public API for configuration updates. It orchestrates:
        1. Grouping configurations by function
        2. Determining what changed
        3. Preserving state for unchanged breakpoints
        4. Applying changes with error isolation (one function failure doesn't stop others)
        5. Cleaning up orphaned states

        CRITICAL: Never raises exceptions. Returns detailed results about successes and failures.

        Args:
            configs: List of breakpoint configurations to apply

        Returns:
            Dict with detailed results:
            {
                'success': bool,  # Overall success (true if no failures)
                'applied': int,   # Number of functions successfully updated
                'failed': int,    # Number of functions that failed
                'removed': int,   # Number of functions removed
                'unchanged': int, # Number of functions unchanged
                'details': {
                    'succeeded': [function_keys],
                    'failed': [{'function_key': str, 'error': str}],
                    'removed': [function_keys],
                    'unchanged': [function_keys]
                }
            }
        """
        try:
            with self._lock:
                # Track results
                succeeded = []
                failed = []
                removed = []
                unchanged = []

                # Step 1: Group new configurations by function
                new_grouped = self._group_by_function(configs)
                logger.debug("Applying configuration: %d functions in new config", len(new_grouped))

                # Step 2: Identify functions to remove (in active but not in new config)
                functions_to_remove = set(self._active_functions.keys()) - set(new_grouped.keys())

                # Collect all incoming config_ids (location_hashes) for stale tracking
                incoming_config_ids = set()
                for bp_set in new_grouped.values():
                    for bp_config in bp_set.breakpoints.values():
                        if bp_config.config_id:
                            incoming_config_ids.add(bp_config.config_id)

                # Clean up _failed_configs entries for configs no longer in the incoming list
                stale_failed = [cid for cid in self._failed_configs if cid not in incoming_config_ids]
                for cid in stale_failed:
                    del self._failed_configs[cid]

                # Step 3: Process each function in new configuration
                for func_key, new_bp_set in new_grouped.items():
                    try:
                        # Check if ALL configs in this function are already known to be failed.
                        # If so, skip silently to avoid log spam on every poll cycle.
                        bp_config_ids = [bp.config_id for bp in new_bp_set.breakpoints.values() if bp.config_id]
                        if bp_config_ids and all(cid in self._failed_configs for cid in bp_config_ids):
                            # All configs already failed — skip without logging
                            continue

                        # Check if function exists in active functions
                        if func_key in self._active_functions:
                            old_bp_set = self._active_functions[func_key]

                            # Check if configuration changed
                            if not self._has_changed(old_bp_set, new_bp_set):
                                # No change - skip this function
                                unchanged.append(func_key)
                                logger.debug("Function %s unchanged, skipping", func_key)
                                continue

                            # Configuration changed — clear failed tracking for these configs
                            # since the user may have fixed the issue
                            for cid in bp_config_ids:
                                self._failed_configs.pop(cid, None)

                            # Configuration changed - update with state preservation
                            logger.debug("Updating function %s", func_key)

                            # Get unchanged breakpoints for state preservation
                            unchanged_bp_keys = self._get_unchanged_breakpoints(old_bp_set, new_bp_set)

                            # Remove old instrumentation (preserving state for unchanged breakpoints)
                            self._remove_function(func_key, preserve_state_for_bp_keys=unchanged_bp_keys)

                            # Apply new instrumentation
                            self._apply_function(new_bp_set)
                            succeeded.append(func_key)
                            logger.debug("Successfully updated function %s", func_key)

                            # Report READY immediately for each config in the updated function
                            for bp in new_bp_set.breakpoints.values():
                                if bp.config_id:
                                    self._report_immediate(
                                        bp.config_id, bp.instrumentation_type, ConfigurationStatus.READY
                                    )

                        else:
                            # New function - apply instrumentation
                            logger.debug("Adding new function %s", func_key)
                            self._apply_function(new_bp_set)
                            succeeded.append(func_key)
                            logger.debug("Successfully added function %s", func_key)

                            # Report READY immediately for each config in the new function
                            for bp in new_bp_set.breakpoints.values():
                                if bp.config_id:
                                    self._report_immediate(
                                        bp.config_id, bp.instrumentation_type, ConfigurationStatus.READY
                                    )

                    except Exception as exception:  # pylint: disable=broad-exception-caught
                        # Error isolation: one function failure doesn't stop others
                        error_cause = self._determine_error_cause(exception)
                        failed.append(
                            {
                                "function_key": func_key,
                                "error": str(exception),
                                "error_cause": error_cause,
                                "config_ids": bp_config_ids,
                            }
                        )

                        # Track failed configs to avoid retrying on next poll
                        for cid in bp_config_ids:
                            self._failed_configs[cid] = error_cause

                        # Report ERROR immediately for each config
                        for bp in new_bp_set.breakpoints.values():
                            if bp.config_id:
                                self._report_immediate(
                                    bp.config_id, bp.instrumentation_type, ConfigurationStatus.ERROR, error_cause
                                )

                        # Continue processing other functions
                        continue

                # Step 4: Remove obsolete functions (not in new config)
                for func_key in functions_to_remove:
                    try:
                        logger.debug("Removing obsolete function %s", func_key)
                        # Don't preserve state when removing completely
                        self._remove_function(func_key, preserve_state_for_bp_keys=None)
                        removed.append(func_key)
                        logger.debug("Successfully removed function %s", func_key)
                    except Exception as exception:  # pylint: disable=broad-exception-caught
                        # Log but don't fail the whole operation
                        logger.error("Failed to remove function %s: %s", func_key, exception, exc_info=True)
                        # Still count as removed since it's no longer in active config
                        removed.append(func_key)

                # Step 5: Cleanup orphaned states
                self._cleanup_orphaned_states()

                # Build result
                result = {
                    "success": len(failed) == 0,
                    "applied": len(succeeded),
                    "failed": len(failed),
                    "removed": len(removed),
                    "unchanged": len(unchanged),
                    "details": {"succeeded": succeeded, "failed": failed, "removed": removed, "unchanged": unchanged},
                }

                logger.debug(
                    "Configuration application complete: %d succeeded, %d failed, %d removed, %d unchanged",
                    len(succeeded),
                    len(failed),
                    len(removed),
                    len(unchanged),
                )

                return result

        except Exception as exception:  # pylint: disable=broad-exception-caught
            # Catastrophic error - return error result
            error_msg = f"Catastrophic error applying configuration: {exception}"
            logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "applied": 0,
                "failed": 0,
                "removed": 0,
                "unchanged": 0,
                "error": error_msg,
                "details": {"succeeded": [], "failed": [], "removed": [], "unchanged": []},
            }

    def _build_location(self, module: str, function: str, line: int) -> Dict:  # pylint: disable=no-self-use
        """Build location dict for status reporting."""
        return {"Language": "Python", "Type": "func", "Module": module, "Function": function, "Line": line}

    def _determine_error_cause(self, error: Exception) -> ErrorCause:  # pylint: disable=no-self-use
        """Determine error cause from exception."""
        error_str = str(error).lower()
        if "not found" in error_str or "no module" in error_str:
            if "file" in error_str:
                return ErrorCause.FILE_NOT_FOUND
            return ErrorCause.METHOD_NOT_FOUND
        return ErrorCause.RUNTIME_ERROR

    def report_initial_status(self):
        """Trigger immediate status report for new configurations."""
        if self._status_reporter:
            self._status_reporter.report_now()

    def _report_immediate(
        self, location_hash: str, instrumentation_type: str, status: ConfigurationStatus, error_cause: ErrorCause = None
    ):
        """Report a status change immediately if the status reporter is available."""
        if self._status_reporter:
            self._status_reporter.report_status_immediately(location_hash, instrumentation_type, status, error_cause)


# Global manager singleton
_global_manager_instance: Optional[InstrumentationManager] = None


def get_global_manager() -> Optional[InstrumentationManager]:
    """Get the global InstrumentationManager instance.

    Returns:
        The global manager instance, or None if not initialized
    """
    return _global_manager_instance


def initialize_global_manager(tracer_provider=None, service: str = "", environment: str = "") -> InstrumentationManager:
    """Initialize the global InstrumentationManager instance.

    This should be called once at debugger startup.

    Args:
        tracer_provider: Optional OpenTelemetry TracerProvider
        service: Service name for status reporting
        environment: Environment name for status reporting

    Returns:
        The initialized manager instance
    """
    global _global_manager_instance  # pylint: disable=global-statement

    if _global_manager_instance is not None:
        logger.warning("Global InstrumentationManager already initialized")
        return _global_manager_instance

    _global_manager_instance = InstrumentationManager(tracer_provider, service, environment)
    logger.debug("Global InstrumentationManager initialized")
    return _global_manager_instance
