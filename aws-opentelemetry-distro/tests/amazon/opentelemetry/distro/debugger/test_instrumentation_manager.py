# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for InstrumentationManager.

These tests construct the manager with its heavy collaborators (FunctionWrapper,
SnapshotOtlpEmitter, the engine) patched out so the coordination logic can be
exercised deterministically without real instrumentation, threads, or network.
"""

import sys
import types
import unittest
from types import SimpleNamespace
from unittest import mock

from amazon.opentelemetry.distro.debugger import instrumentation_manager as im_module
from amazon.opentelemetry.distro.debugger._data_models import (
    BreakpointConfiguration,
    BreakpointState,
    FunctionBreakpointSet,
)
from amazon.opentelemetry.distro.debugger._status_reporter import ConfigurationStatus, ErrorCause
from amazon.opentelemetry.distro.debugger.instrumentation_manager import (
    InstrumentationManager,
    get_global_manager,
    initialize_global_manager,
)


def _make_manager():
    """Build an InstrumentationManager with heavy collaborators patched out."""
    with mock.patch.object(im_module, "FunctionWrapper"), mock.patch.object(
        im_module, "SnapshotOtlpEmitter"
    ), mock.patch.object(im_module, "set_snapshot_emitter"), mock.patch.object(
        InstrumentationManager, "_select_engine", return_value=mock.MagicMock()
    ):
        manager = InstrumentationManager(service="svc", environment="prod")
    return manager


def _api_config(
    method_name="process_order",
    line_number=42,
    location_hash="config-123",
    instrumentation_type="BREAKPOINT",
    code_unit="myapp.services",
):
    """Build a valid API config item for BreakpointConfiguration.from_api_config."""
    item = {
        "InstrumentationType": instrumentation_type,
        "Location": {
            "CodeLocation": {
                "Language": "python",
                "CodeUnit": code_unit,
                "MethodName": method_name,
                "LineNumber": line_number,
            }
        },
        "CaptureConfiguration": {
            "CodeCapture": {
                "CaptureReturn": False,
                "CaptureStackTrace": True,
                "CaptureArguments": [],
                "CaptureLocals": [],
            }
        },
        "LocationHash": location_hash,
    }
    return item


def _make_config(**kwargs):
    """Build a real BreakpointConfiguration from a valid API item."""
    config = BreakpointConfiguration.from_api_config(_api_config(**kwargs))
    assert config is not None, "test fixture produced an invalid config"
    return config


class TestGetStatus(unittest.TestCase):
    """Tests for InstrumentationManager.get_status."""

    def test_empty_manager_returns_zeros(self):
        manager = _make_manager()
        status = manager.get_status()
        self.assertEqual(status["active_functions"], 0)
        self.assertEqual(status["total_breakpoints"], 0)
        self.assertEqual(status["preserved_states"], 0)
        self.assertEqual(status["functions"], {})

    def test_with_active_function_returns_counts(self):
        manager = _make_manager()
        config = _make_config()
        bp_set = FunctionBreakpointSet(
            function_key=config.function_key,
            module=config.module,
            function_name=config.function_name,
            breakpoints={config.line_number: config},
        )
        bp_set.is_instrumented = True
        state = BreakpointState(breakpoint_key=f"{config.function_key}:{config.line_number}")
        state.hit_count = 3
        bp_set.states[state.breakpoint_key] = state
        manager._active_functions[config.function_key] = bp_set

        status = manager.get_status()

        self.assertEqual(status["active_functions"], 1)
        self.assertEqual(status["total_breakpoints"], 1)
        function_info = status["functions"][config.function_key]
        self.assertEqual(function_info["breakpoint_count"], 1)
        self.assertEqual(function_info["total_hits"], 3)
        self.assertTrue(function_info["is_instrumented"])
        self.assertEqual(function_info["line_numbers"], [42])

    def test_get_status_handles_internal_error(self):
        manager = _make_manager()
        # A non-dict in active_functions triggers the broad except branch.
        manager._active_functions = mock.MagicMock()
        manager._active_functions.values.side_effect = RuntimeError("boom")
        status = manager.get_status()
        self.assertIn("error", status)
        self.assertEqual(status["active_functions"], 0)


class TestBuildResource(unittest.TestCase):
    """Tests for the static _build_resource helper."""

    def test_returns_resource_when_attrs_present(self):
        resource = InstrumentationManager._build_resource("svc", "prod")
        self.assertIsNotNone(resource)
        self.assertEqual(resource.attributes.get("service.name"), "svc")
        self.assertEqual(resource.attributes.get("deployment.environment.name"), "prod")
        self.assertEqual(resource.attributes.get("deployment.environment"), "prod")

    def test_returns_resource_with_only_service(self):
        resource = InstrumentationManager._build_resource("svc", "")
        self.assertIsNotNone(resource)
        self.assertEqual(resource.attributes.get("service.name"), "svc")

    def test_returns_none_when_both_empty(self):
        self.assertIsNone(InstrumentationManager._build_resource("", ""))


class TestGroupByFunction(unittest.TestCase):
    """Tests for the static _group_by_function helper."""

    def test_groups_single_config(self):
        config = _make_config()
        grouped = InstrumentationManager._group_by_function([config])
        self.assertEqual(len(grouped), 1)
        self.assertIn(config.function_key, grouped)
        self.assertIn(config.line_number, grouped[config.function_key].breakpoints)

    def test_merges_configs_for_same_function(self):
        # Same function (PROBE line 0 + BREAKPOINT line 42) merges into one set.
        probe = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        line_bp = _make_config(method_name="handler", line_number=42, location_hash="bp-1")
        grouped = InstrumentationManager._group_by_function([probe, line_bp])
        self.assertEqual(len(grouped), 1)
        bp_set = grouped[probe.function_key]
        self.assertIn(0, bp_set.breakpoints)
        self.assertIn(42, bp_set.breakpoints)

    def test_separates_distinct_functions(self):
        first = _make_config(method_name="alpha", location_hash="a")
        second = _make_config(method_name="beta", location_hash="b")
        grouped = InstrumentationManager._group_by_function([first, second])
        self.assertEqual(len(grouped), 2)

    def test_skips_none_configs(self):
        config = _make_config()
        grouped = InstrumentationManager._group_by_function([None, config])
        self.assertEqual(len(grouped), 1)


class TestHasChanged(unittest.TestCase):
    """Tests for the static _has_changed helper."""

    def _bp_set(self, config):
        return FunctionBreakpointSet(
            function_key=config.function_key,
            module=config.module,
            function_name=config.function_name,
            breakpoints={config.line_number: config},
        )

    def test_same_configs_returns_false(self):
        old_config = _make_config(location_hash="same")
        new_config = _make_config(location_hash="same")
        self.assertFalse(self._has_changed(old_config, new_config))

    def test_different_config_id_returns_true(self):
        old_config = _make_config(location_hash="old")
        new_config = _make_config(location_hash="new")
        self.assertTrue(self._has_changed(old_config, new_config))

    def test_different_line_numbers_returns_true(self):
        old_config = _make_config(line_number=10, location_hash="x")
        new_config = _make_config(line_number=20, location_hash="x")
        self.assertTrue(self._has_changed(old_config, new_config))

    def _has_changed(self, old_config, new_config):
        return InstrumentationManager._has_changed(self._bp_set(old_config), self._bp_set(new_config))


class TestGetUnchangedBreakpoints(unittest.TestCase):
    """Tests for the static _get_unchanged_breakpoints helper."""

    def _bp_set(self, config):
        return FunctionBreakpointSet(
            function_key=config.function_key,
            module=config.module,
            function_name=config.function_name,
            breakpoints={config.line_number: config},
        )

    def test_identical_config_is_unchanged(self):
        old_config = _make_config(location_hash="same")
        new_config = _make_config(location_hash="same")
        unchanged = InstrumentationManager._get_unchanged_breakpoints(
            self._bp_set(old_config), self._bp_set(new_config)
        )
        self.assertEqual(unchanged, {f"{old_config.function_key}:{old_config.line_number}"})

    def test_changed_config_id_not_in_unchanged(self):
        old_config = _make_config(location_hash="old")
        new_config = _make_config(location_hash="new")
        unchanged = InstrumentationManager._get_unchanged_breakpoints(
            self._bp_set(old_config), self._bp_set(new_config)
        )
        self.assertEqual(unchanged, set())

    def test_no_common_lines_returns_empty(self):
        old_config = _make_config(line_number=10, location_hash="x")
        new_config = _make_config(line_number=20, location_hash="x")
        unchanged = InstrumentationManager._get_unchanged_breakpoints(
            self._bp_set(old_config), self._bp_set(new_config)
        )
        self.assertEqual(unchanged, set())


class TestDetermineErrorCause(unittest.TestCase):
    """Tests for _determine_error_cause."""

    def setUp(self):
        self.manager = _make_manager()

    def test_not_found_with_file_maps_to_file_not_found(self):
        cause = self.manager._determine_error_cause(Exception("source file not found"))
        self.assertEqual(cause, ErrorCause.FILE_NOT_FOUND)

    def test_no_module_with_file_maps_to_file_not_found(self):
        cause = self.manager._determine_error_cause(Exception("No module and missing file"))
        self.assertEqual(cause, ErrorCause.FILE_NOT_FOUND)

    def test_not_found_without_file_maps_to_method_not_found(self):
        cause = self.manager._determine_error_cause(Exception("method not found"))
        self.assertEqual(cause, ErrorCause.METHOD_NOT_FOUND)

    def test_no_module_without_file_maps_to_method_not_found(self):
        cause = self.manager._determine_error_cause(Exception("No module named foo"))
        self.assertEqual(cause, ErrorCause.METHOD_NOT_FOUND)

    def test_other_error_maps_to_runtime_error(self):
        cause = self.manager._determine_error_cause(Exception("something exploded"))
        self.assertEqual(cause, ErrorCause.RUNTIME_ERROR)


class TestBuildLocation(unittest.TestCase):
    """Tests for _build_location."""

    def test_builds_location_dict(self):
        manager = _make_manager()
        location = manager._build_location("myapp.services", "process_order", 42)
        self.assertEqual(
            location,
            {
                "Language": "Python",
                "Type": "func",
                "Module": "myapp.services",
                "Function": "process_order",
                "Line": 42,
            },
        )


class TestIncrementHitCount(unittest.TestCase):
    """Tests for increment_hit_count."""

    def _manager_with_breakpoint(self, max_hits=5, is_permanent=False, acquire=True):
        manager = _make_manager()
        function_key = "myapp.services.process_order"
        line_number = 42
        breakpoint_key = f"{function_key}:{line_number}"

        rate_limiter = SimpleNamespace(try_acquire=mock.MagicMock(return_value=acquire))
        state = SimpleNamespace(
            hit_count=0,
            is_disabled=False,
            hit_in_last_period=False,
            rate_limiter=rate_limiter,
            location_hash="config-123",
            instrumentation_type="BREAKPOINT",
        )
        config = SimpleNamespace(is_permanent=is_permanent, max_hits=max_hits)
        bp_set = SimpleNamespace(states={breakpoint_key: state}, breakpoints={line_number: config})
        manager._active_functions[function_key] = bp_set
        return manager, breakpoint_key, state

    def test_not_found_returns_false(self):
        manager = _make_manager()
        self.assertFalse(manager.increment_hit_count("missing.func:1"))

    def test_disabled_returns_false(self):
        manager, breakpoint_key, state = self._manager_with_breakpoint()
        state.is_disabled = True
        self.assertFalse(manager.increment_hit_count(breakpoint_key))

    def test_first_hit_reports_active_and_returns_true(self):
        manager, breakpoint_key, state = self._manager_with_breakpoint()
        reporter = mock.MagicMock()
        manager._status_reporter = reporter

        result = manager.increment_hit_count(breakpoint_key)

        self.assertTrue(result)
        self.assertEqual(state.hit_count, 1)
        self.assertTrue(state.hit_in_last_period)
        reporter.report_status_immediately.assert_called_once_with(
            "config-123", "BREAKPOINT", ConfigurationStatus.ACTIVE, None
        )

    def test_exceeds_max_hits_disables_and_returns_false(self):
        manager, breakpoint_key, state = self._manager_with_breakpoint(max_hits=2)
        state.hit_count = 2  # next hit makes it 3, which exceeds max_hits=2
        reporter = mock.MagicMock()
        manager._status_reporter = reporter

        result = manager.increment_hit_count(breakpoint_key)

        self.assertFalse(result)
        self.assertTrue(state.is_disabled)
        reporter.report_status_immediately.assert_called_once_with(
            "config-123", "BREAKPOINT", ConfigurationStatus.DISABLED, None
        )

    def test_permanent_probe_does_not_disable_on_max_hits(self):
        manager, breakpoint_key, state = self._manager_with_breakpoint(max_hits=1, is_permanent=True)
        state.hit_count = 5  # well beyond max_hits, but PROBE is permanent
        result = manager.increment_hit_count(breakpoint_key)
        self.assertTrue(result)
        self.assertFalse(state.is_disabled)

    def test_rate_limited_returns_false_without_disabling(self):
        manager, breakpoint_key, state = self._manager_with_breakpoint(acquire=False)
        result = manager.increment_hit_count(breakpoint_key)
        self.assertFalse(result)
        self.assertFalse(state.is_disabled)

    def test_normal_hit_returns_true(self):
        manager, breakpoint_key, state = self._manager_with_breakpoint()
        state.hit_count = 1  # not first hit, below max
        result = manager.increment_hit_count(breakpoint_key)
        self.assertTrue(result)
        self.assertEqual(state.hit_count, 2)


class TestCleanupOrphanedStates(unittest.TestCase):
    """Tests for _cleanup_orphaned_states."""

    def test_removes_orphaned_state(self):
        manager = _make_manager()
        manager._preserved_states["orphan.func:1"] = BreakpointState(breakpoint_key="orphan.func:1")
        manager._cleanup_orphaned_states()
        self.assertEqual(manager._preserved_states, {})

    def test_keeps_state_with_active_breakpoint(self):
        manager = _make_manager()
        function_key = "live.func"
        breakpoint_key = f"{function_key}:0"
        state = BreakpointState(breakpoint_key=breakpoint_key)
        bp_set = SimpleNamespace(states={breakpoint_key: state}, function_key=function_key)
        manager._active_functions[function_key] = bp_set
        manager._preserved_states[breakpoint_key] = state

        manager._cleanup_orphaned_states()

        self.assertIn(breakpoint_key, manager._preserved_states)


class TestReportStatus(unittest.TestCase):
    """Tests for report_initial_status and _report_immediate."""

    def test_report_initial_status_noop_without_reporter(self):
        manager = _make_manager()
        manager._status_reporter = None
        # Should not raise.
        manager.report_initial_status()

    def test_report_initial_status_calls_reporter(self):
        manager = _make_manager()
        reporter = mock.MagicMock()
        manager._status_reporter = reporter
        manager.report_initial_status()
        reporter.report_now.assert_called_once()

    def test_report_immediate_noop_without_reporter(self):
        manager = _make_manager()
        manager._status_reporter = None
        # Should not raise.
        manager._report_immediate("hash", "BREAKPOINT", ConfigurationStatus.READY)

    def test_report_immediate_calls_reporter(self):
        manager = _make_manager()
        reporter = mock.MagicMock()
        manager._status_reporter = reporter
        manager._report_immediate("hash", "BREAKPOINT", ConfigurationStatus.READY, ErrorCause.RUNTIME_ERROR)
        reporter.report_status_immediately.assert_called_once_with(
            "hash", "BREAKPOINT", ConfigurationStatus.READY, ErrorCause.RUNTIME_ERROR
        )


class TestApplyConfiguration(unittest.TestCase):
    """Tests for apply_configuration, the main public API."""

    def setUp(self):
        self.manager = _make_manager()
        # Make _apply_function / _remove_function track active state without real
        # instrumentation. apply records the bp_set into _active_functions, remove pops it.
        self.applied_sets = []

        def fake_apply(bp_set):
            self.applied_sets.append(bp_set)
            self.manager._active_functions[bp_set.function_key] = bp_set

        def fake_remove(func_key, preserve_state_for_bp_keys=None):
            self.manager._active_functions.pop(func_key, None)

        self._apply_patch = mock.patch.object(self.manager, "_apply_function", side_effect=fake_apply)
        self._remove_patch = mock.patch.object(self.manager, "_remove_function", side_effect=fake_remove)
        self._apply_patch.start()
        self._remove_patch.start()

    def tearDown(self):
        self._apply_patch.stop()
        self._remove_patch.stop()

    def test_new_function_applied(self):
        config = _make_config()
        result = self.manager.apply_configuration([config])
        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertTrue(result["success"])
        self.assertIn(config.function_key, result["details"]["succeeded"])

    def test_reapplying_identical_config_unchanged(self):
        config = _make_config(location_hash="stable")
        self.manager.apply_configuration([config])
        # Re-apply an identical config; the existing function is unchanged.
        same_config = _make_config(location_hash="stable")
        result = self.manager.apply_configuration([same_config])
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["applied"], 0)
        self.assertIn(config.function_key, result["details"]["unchanged"])

    def test_removing_function_after_empty_config(self):
        config = _make_config()
        self.manager.apply_configuration([config])
        result = self.manager.apply_configuration([])
        self.assertGreaterEqual(result["removed"], 1)
        self.assertIn(config.function_key, result["details"]["removed"])

    def test_error_isolation_one_failure_others_succeed(self):
        good_config = _make_config(method_name="good", location_hash="good-1")
        bad_config = _make_config(method_name="bad", location_hash="bad-1")

        def selective_apply(bp_set):
            if bp_set.function_name == "bad":
                raise RuntimeError("module not found: missing file")
            self.manager._active_functions[bp_set.function_key] = bp_set

        self._apply_patch.stop()
        with mock.patch.object(self.manager, "_apply_function", side_effect=selective_apply):
            result = self.manager.apply_configuration([good_config, bad_config])
        self._apply_patch.start()

        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertFalse(result["success"])
        self.assertIn(good_config.function_key, result["details"]["succeeded"])
        failed_entry = result["details"]["failed"][0]
        self.assertEqual(failed_entry["function_key"], bad_config.function_key)
        self.assertEqual(failed_entry["error_cause"], ErrorCause.FILE_NOT_FOUND)
        # Failed config is tracked to avoid retrying.
        self.assertIn("bad-1", self.manager._failed_configs)

    def test_changed_config_updates_function(self):
        config = _make_config(location_hash="v1")
        self.manager.apply_configuration([config])
        changed = _make_config(location_hash="v2")
        result = self.manager.apply_configuration([changed])
        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["unchanged"], 0)

    def test_already_failed_config_skipped_silently(self):
        config = _make_config(location_hash="known-bad")
        self.manager._failed_configs["known-bad"] = ErrorCause.RUNTIME_ERROR
        result = self.manager.apply_configuration([config])
        # Skipped silently: not applied, not failed, not unchanged.
        self.assertEqual(result["applied"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["unchanged"], 0)

    def test_reports_ready_for_new_function(self):
        reporter = mock.MagicMock()
        self.manager._status_reporter = reporter
        config = _make_config(location_hash="ready-1")
        self.manager.apply_configuration([config])
        reporter.report_status_immediately.assert_any_call("ready-1", "BREAKPOINT", ConfigurationStatus.READY, None)


def _real_target_function(value):
    """A real function used as the 'original' for instrument/restore tests."""
    return value * 2


def _bp_set_for(config):
    """Build a FunctionBreakpointSet whose module is already importable.

    Uses this test module's own name as the module so _apply_function's
    sys.modules existence check passes without a real find_spec lookup.
    """
    return FunctionBreakpointSet(
        function_key=config.function_key,
        module=config.module,
        function_name=config.function_name,
        breakpoints={config.line_number: config},
    )


class TestApplyFunction(unittest.TestCase):
    """Tests for _apply_function with a mocked wrapper and engine."""

    def _make_set(self, configs, module=__name__):
        first = configs[0]
        bp_set = FunctionBreakpointSet(
            function_key=f"{module}.{first.function_name}",
            module=module,
            function_name=first.function_name,
            breakpoints={config.line_number: config for config in configs},
        )
        return bp_set

    def test_no_wrapper_needed_returns_early(self):
        manager = _make_manager()
        empty_set = FunctionBreakpointSet(function_key="m.f", module="m", function_name="f", breakpoints={})
        # needs_wrapper is False with no breakpoints; should return without touching wrapper.
        manager._apply_function(empty_set)
        manager._wrapper.instrument_function.assert_not_called()

    def test_method_level_wrap_creates_state(self):
        manager = _make_manager()
        manager._engine = None  # no line breakpoints needed for method-level
        manager._wrapper.instrument_function.return_value = (_real_target_function, mock.MagicMock())

        probe = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = self._make_set([probe])

        manager._apply_function(bp_set)

        self.assertTrue(bp_set.is_instrumented)
        self.assertIs(bp_set.original_function, _real_target_function)
        self.assertIn(bp_set.function_key, manager._active_functions)
        # A fresh state was created for the method-level breakpoint (line 0).
        state_key = f"{bp_set.function_key}:0"
        self.assertIn(state_key, bp_set.states)
        self.assertEqual(bp_set.states[state_key].location_hash, "probe-1")

    def test_engine_decline_falls_back_to_setattr_wrapper(self):
        """Contract: when the bytecode engine returns False from
        enable_function_level_instrumentation (the case for any code object
        in _DECLINE_BYTECODE_REWRITE_MASK — generators, coroutines, async
        generators), _apply_function MUST fall through to
        FunctionWrapper.instrument_function so the wrapper's coroutine
        path can still wire up async PROBEs.

        This locks down the wrapper-fallback half of the contract that
        TestBytecodeInjectionEngineFunctionLevel only verifies on the
        engine half (asserting the engine returns False)."""
        manager = _make_manager()
        # Engine declines (as it would for any async/generator code).
        manager._engine.enable_function_level_instrumentation.return_value = False
        manager._wrapper.instrument_function.return_value = (_real_target_function, mock.MagicMock())

        probe = _make_config(
            instrumentation_type="PROBE", method_name=_real_target_function.__name__, location_hash="probe-async"
        )
        bp_set = self._make_set([probe])

        # FunctionWrapper._discover_function is called by _instrument_function_level
        # to find the live callable in the module — patch it to return our real
        # function so we can exercise the engine-declines branch deterministically.
        with mock.patch.object(im_module.FunctionWrapper, "_discover_function", return_value=_real_target_function):
            manager._apply_function(bp_set)

        # Engine was asked first.
        manager._engine.enable_function_level_instrumentation.assert_called_once()
        # Wrapper fallback fired; bp_set is instrumented via setattr path.
        manager._wrapper.instrument_function.assert_called_once()
        self.assertTrue(bp_set.is_instrumented)
        self.assertIs(bp_set.original_function, _real_target_function)

    def test_line_level_enables_engine_breakpoints(self):
        manager = _make_manager()
        manager._wrapper.instrument_function.return_value = (_real_target_function, mock.MagicMock())

        line_bp = _make_config(method_name="handler", line_number=42, location_hash="bp-1")
        bp_set = self._make_set([line_bp])

        manager._apply_function(bp_set)

        manager._engine.enable_breakpoints_for_function.assert_called_once()
        _, kwargs = manager._engine.enable_breakpoints_for_function.call_args
        self.assertEqual(kwargs["line_numbers"], {42})
        self.assertEqual(kwargs["line_location_hashes"], {42: "bp-1"})

    def test_engine_failure_does_not_break_wrapping(self):
        manager = _make_manager()
        manager._wrapper.instrument_function.return_value = (_real_target_function, mock.MagicMock())
        manager._engine.enable_breakpoints_for_function.side_effect = RuntimeError("engine boom")

        line_bp = _make_config(method_name="handler", line_number=42, location_hash="bp-1")
        bp_set = self._make_set([line_bp])

        # Engine failure is swallowed; wrapping still succeeds.
        manager._apply_function(bp_set)
        self.assertTrue(bp_set.is_instrumented)
        self.assertIn(bp_set.function_key, manager._active_functions)

    def test_restores_preserved_state(self):
        manager = _make_manager()
        manager._engine = None
        manager._wrapper.instrument_function.return_value = (_real_target_function, mock.MagicMock())

        probe = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = self._make_set([probe])
        state_key = f"{bp_set.function_key}:0"

        preserved = BreakpointState(breakpoint_key=state_key, location_hash="probe-1")
        preserved.hit_count = 7
        manager._preserved_states[state_key] = preserved

        manager._apply_function(bp_set)

        # The preserved state (with its 7 hits) was restored, not recreated.
        self.assertEqual(bp_set.states[state_key].hit_count, 7)
        self.assertNotIn(state_key, manager._preserved_states)

    def test_missing_module_raises_module_not_found(self):
        manager = _make_manager()
        line_bp = _make_config(method_name="handler", line_number=42, location_hash="bp-1")
        bp_set = self._make_set([line_bp], module="this_module_does_not_exist_xyz")

        with self.assertRaises(ModuleNotFoundError):
            manager._apply_function(bp_set)
        # Wrapper should never be called for a missing module.
        manager._wrapper.instrument_function.assert_not_called()

    def test_wrapper_failure_triggers_rollback_and_reraises(self):
        manager = _make_manager()
        manager._engine = None
        manager._wrapper.instrument_function.side_effect = RuntimeError("wrap failed")

        probe = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = self._make_set([probe])

        with self.assertRaises(RuntimeError):
            manager._apply_function(bp_set)


class TestApplyFunctionDescriptors(unittest.TestCase):
    def _make_set(self, configs, module=__name__):
        first = configs[0]
        bp_set = FunctionBreakpointSet(
            function_key=f"{module}.{first.function_name}",
            module=module,
            function_name=first.function_name,
            breakpoints={config.line_number: config for config in configs},
        )
        return bp_set

    def test_staticmethod_descriptor_resolves_code_object_and_enables_engine(self):
        manager = _make_manager()
        descriptor = staticmethod(_real_target_function)
        manager._wrapper.instrument_function.return_value = (descriptor, mock.MagicMock())

        line_bp = _make_config(method_name="C.handler", line_number=42, location_hash="bp-1")
        bp_set = self._make_set([line_bp])

        manager._apply_function(bp_set)

        self.assertIs(bp_set.code_object, _real_target_function.__code__)
        manager._engine.enable_breakpoints_for_function.assert_called_once()
        _, kwargs = manager._engine.enable_breakpoints_for_function.call_args
        self.assertIs(kwargs["code"], _real_target_function.__code__)
        self.assertIs(kwargs["func"], _real_target_function)

    def test_classmethod_descriptor_resolves_code_object_and_enables_engine(self):
        manager = _make_manager()
        descriptor = classmethod(_real_target_function)
        manager._wrapper.instrument_function.return_value = (descriptor, mock.MagicMock())

        line_bp = _make_config(method_name="C.handler", line_number=99, location_hash="bp-cm")
        bp_set = self._make_set([line_bp])

        manager._apply_function(bp_set)

        self.assertIs(bp_set.code_object, _real_target_function.__code__)
        manager._engine.enable_breakpoints_for_function.assert_called_once()
        _, kwargs = manager._engine.enable_breakpoints_for_function.call_args
        self.assertIs(kwargs["func"], _real_target_function)

    def test_staticmethod_original_function_stored_as_descriptor_for_restore(self):
        manager = _make_manager()
        descriptor = staticmethod(_real_target_function)
        manager._wrapper.instrument_function.return_value = (descriptor, mock.MagicMock())

        line_bp = _make_config(method_name="C.handler", line_number=42, location_hash="bp-1")
        bp_set = self._make_set([line_bp])

        manager._apply_function(bp_set)

        self.assertIs(bp_set.original_function, descriptor)


class TestRemoveFunction(unittest.TestCase):
    """Tests for _remove_function with a mocked wrapper and engine."""

    def _active_set(self, manager, preserve_keys_state=True):
        config = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = _bp_set_for(config)
        bp_set.is_instrumented = True
        bp_set.original_function = _real_target_function
        bp_set.code_object = _real_target_function.__code__
        state_key = f"{bp_set.function_key}:0"
        bp_set.states[state_key] = BreakpointState(breakpoint_key=state_key, location_hash="probe-1")
        manager._active_functions[bp_set.function_key] = bp_set
        return bp_set, state_key

    def test_remove_not_active_is_noop(self):
        manager = _make_manager()
        manager._remove_function("not.there")
        manager._wrapper.restore_function.assert_not_called()

    def test_remove_restores_and_deletes(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = True
        bp_set, _ = self._active_set(manager)

        manager._remove_function(bp_set.function_key)

        manager._engine.disable_breakpoints_for_function.assert_called_once()
        manager._wrapper.restore_function.assert_called_once()
        self.assertNotIn(bp_set.function_key, manager._active_functions)

    def test_remove_preserves_specified_state(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = True
        bp_set, state_key = self._active_set(manager)

        manager._remove_function(bp_set.function_key, preserve_state_for_bp_keys={state_key})

        self.assertIn(state_key, manager._preserved_states)
        self.assertNotIn(bp_set.function_key, manager._active_functions)

    def test_remove_handles_restore_failure(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = False  # restore reports failure
        bp_set, _ = self._active_set(manager)

        # Should still complete and remove from active functions.
        manager._remove_function(bp_set.function_key)
        self.assertNotIn(bp_set.function_key, manager._active_functions)

    def test_remove_unwraps_staticmethod_descriptor_for_engine_disable(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = True
        config = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = _bp_set_for(config)
        bp_set.is_instrumented = True
        bp_set.original_function = staticmethod(_real_target_function)
        bp_set.code_object = _real_target_function.__code__
        manager._active_functions[bp_set.function_key] = bp_set

        manager._remove_function(bp_set.function_key)

        manager._engine.disable_breakpoints_for_function.assert_called_once()
        _, kwargs = manager._engine.disable_breakpoints_for_function.call_args
        self.assertIs(kwargs["func"], _real_target_function)

    def test_remove_handles_engine_disable_failure(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = True
        manager._engine.disable_breakpoints_for_function.side_effect = RuntimeError("disable boom")
        bp_set, _ = self._active_set(manager)

        # Engine failure is swallowed; function is still restored and removed.
        manager._remove_function(bp_set.function_key)
        manager._wrapper.restore_function.assert_called_once()
        self.assertNotIn(bp_set.function_key, manager._active_functions)


class TestRollback(unittest.TestCase):
    """Tests for _rollback with a mocked wrapper."""

    def _instrumented_set(self):
        config = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = _bp_set_for(config)
        bp_set.is_instrumented = True
        bp_set.original_function = _real_target_function
        return bp_set

    def test_rollback_restores_function(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = True
        bp_set = self._instrumented_set()
        manager._rollback(bp_set)
        manager._wrapper.restore_function.assert_called_once()

    def test_rollback_handles_restore_failure(self):
        manager = _make_manager()
        manager._wrapper.restore_function.return_value = False
        bp_set = self._instrumented_set()
        # Should not raise even when restore reports failure.
        manager._rollback(bp_set)

    def test_rollback_swallows_exceptions(self):
        manager = _make_manager()
        manager._wrapper.restore_function.side_effect = RuntimeError("boom")
        bp_set = self._instrumented_set()
        # Rollback must never raise.
        manager._rollback(bp_set)

    def test_rollback_noop_when_not_instrumented(self):
        manager = _make_manager()
        config = _make_config(instrumentation_type="PROBE", method_name="handler", location_hash="probe-1")
        bp_set = _bp_set_for(config)
        bp_set.is_instrumented = False
        manager._rollback(bp_set)
        manager._wrapper.restore_function.assert_not_called()


class TestGlobalManager(unittest.TestCase):
    """Tests for the global manager accessors."""

    def setUp(self):
        self._original = im_module._global_manager_instance
        im_module._global_manager_instance = None

    def tearDown(self):
        im_module._global_manager_instance = self._original

    def test_get_global_manager_none_initially(self):
        self.assertIsNone(get_global_manager())

    def test_initialize_global_manager_creates_instance(self):
        with mock.patch.object(im_module, "FunctionWrapper"), mock.patch.object(
            im_module, "SnapshotOtlpEmitter"
        ), mock.patch.object(im_module, "set_snapshot_emitter"), mock.patch.object(
            InstrumentationManager, "_select_engine", return_value=mock.MagicMock()
        ):
            manager = initialize_global_manager(service="svc", environment="prod")
        self.assertIsNotNone(manager)
        self.assertIs(get_global_manager(), manager)

    def test_initialize_global_manager_idempotent(self):
        with mock.patch.object(im_module, "FunctionWrapper"), mock.patch.object(
            im_module, "SnapshotOtlpEmitter"
        ), mock.patch.object(im_module, "set_snapshot_emitter"), mock.patch.object(
            InstrumentationManager, "_select_engine", return_value=mock.MagicMock()
        ):
            first = initialize_global_manager()
            second = initialize_global_manager()
        self.assertIs(first, second)


class TestInheritedMethodEndToEnd(unittest.TestCase):
    def setUp(self):
        self._module_name = "_test_im_inherited_e2e_module"
        sys.modules.pop(self._module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self._module_name, None))

        with mock.patch.object(im_module, "SnapshotOtlpEmitter"), mock.patch.object(
            im_module, "set_snapshot_emitter"
        ), mock.patch.object(InstrumentationManager, "_select_engine", return_value=mock.MagicMock()):
            self.manager = InstrumentationManager(service="svc", environment="prod")

        self.reporter = mock.MagicMock()
        self.manager._status_reporter = self.reporter

    def test_apply_inherited_method_reports_method_not_found_error(self):
        class Base:
            def handle(self, x):
                return x + 1

        class Child(Base):
            pass

        module = types.ModuleType(self._module_name)
        module.Base = Base
        module.Child = Child
        sys.modules[self._module_name] = module

        config = BreakpointConfiguration.from_api_config(
            {
                "InstrumentationType": "BREAKPOINT",
                "Location": {
                    "CodeLocation": {
                        "Language": "python",
                        "CodeUnit": self._module_name,
                        "ClassName": "Child",
                        "MethodName": "handle",
                        "LineNumber": 0,
                    }
                },
                "CaptureConfiguration": {"CodeCapture": {}},
                "LocationHash": "loc-inherited-1",
            }
        )
        self.assertIsNotNone(config)

        result = self.manager.apply_configuration([config])

        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["applied"], 0)
        self.assertFalse(result["success"])

        failed_entry = result["details"]["failed"][0]
        self.assertEqual(failed_entry["error_cause"], ErrorCause.METHOD_NOT_FOUND)
        self.assertIn("inherited from", failed_entry["error"])

        self.assertEqual(self.manager._failed_configs.get("loc-inherited-1"), ErrorCause.METHOD_NOT_FOUND)

        self.reporter.report_status_immediately.assert_called_with(
            "loc-inherited-1",
            "BREAKPOINT",
            ConfigurationStatus.ERROR,
            ErrorCause.METHOD_NOT_FOUND,
        )

        self.assertNotIn("handle", Child.__dict__)


if __name__ == "__main__":
    unittest.main()
