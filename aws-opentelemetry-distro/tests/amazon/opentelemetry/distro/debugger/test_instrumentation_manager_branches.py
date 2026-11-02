# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Branch-coverage tests for InstrumentationManager error/edge paths.

These complement test_instrumentation_manager.py by driving the defensive
``except`` branches and edge cases (malformed bp_sets, raising collaborators,
catastrophic errors) that the happy-path tests do not reach. Heavy collaborators
(FunctionWrapper, SnapshotOtlpEmitter, the engine) are patched out so no real
instrumentation, threads, or network occur.
"""

import unittest
from types import SimpleNamespace
from unittest import mock

from amazon.opentelemetry.distro.debugger import instrumentation_manager as im_module
from amazon.opentelemetry.distro.debugger._data_models import (
    BreakpointConfiguration,
    BreakpointState,
    CaptureConfig,
    FunctionBreakpointSet,
)
from amazon.opentelemetry.distro.debugger.instrumentation_manager import InstrumentationManager


def _make_manager():
    """Build an InstrumentationManager with heavy collaborators patched out."""
    with mock.patch.object(im_module, "FunctionWrapper"), mock.patch.object(
        im_module, "SnapshotOtlpEmitter"
    ), mock.patch.object(im_module, "set_snapshot_emitter"), mock.patch.object(
        InstrumentationManager, "_select_engine", return_value=mock.MagicMock()
    ):
        manager = InstrumentationManager(service="svc", environment="prod")
    return manager


def _config(method_name="process_order", line_number=42, location_hash="config-123", instrumentation_type="BREAKPOINT"):
    return BreakpointConfiguration(
        module="myapp.services",
        function_name=method_name,
        line_number=line_number,
        capture_config=CaptureConfig(),
        config_id=location_hash,
        instrumentation_type=instrumentation_type,
    )


def _bp_set(config):
    return FunctionBreakpointSet(
        function_key=config.function_key,
        module=config.module,
        function_name=config.function_name,
        breakpoints={config.line_number: config},
    )


class TestBuildResourceException(unittest.TestCase):
    """Covers _build_resource's broad except branch (lines 94-95)."""

    def test_resource_import_failure_returns_none(self):
        # Force the Resource import to raise -> the except branch returns None.
        with mock.patch.dict("sys.modules", {"opentelemetry.sdk.resources": None}):
            result = InstrumentationManager._build_resource("svc", "prod")
        self.assertIsNone(result)

    def test_resource_create_failure_returns_none(self):
        with mock.patch("opentelemetry.sdk.resources.Resource.create", side_effect=RuntimeError("boom")):
            result = InstrumentationManager._build_resource("svc", "prod")
        self.assertIsNone(result)


class TestGroupByFunctionErrorBranches(unittest.TestCase):
    """Covers _group_by_function inner/outer except branches (lines 244-247, 255-257)."""

    def test_config_raising_on_attribute_access_is_skipped(self):
        bad_config = mock.MagicMock()
        # Accessing .function_key raises -> inner except increments skipped_count.
        type(bad_config).function_key = mock.PropertyMock(side_effect=RuntimeError("bad attr"))
        good_config = _config()
        grouped = InstrumentationManager._group_by_function([bad_config, good_config])
        # Only the good config survives.
        self.assertEqual(len(grouped), 1)
        self.assertIn(good_config.function_key, grouped)

    def test_non_iterable_configs_returns_empty(self):
        # Iterating a non-iterable raises in the outer try -> returns {}.
        grouped = InstrumentationManager._group_by_function(object())
        self.assertEqual(grouped, {})


class TestHasChangedErrorBranches(unittest.TestCase):
    """Covers _has_changed inner/outer except branches (lines 302-304, 308-310)."""

    def test_inner_comparison_error_returns_changed(self):
        old_config = _config(location_hash="x")
        old_set = _bp_set(old_config)
        new_set = _bp_set(_config(location_hash="x"))
        # Make config_id access raise during the per-line comparison.
        type(old_set.breakpoints[42]).config_id = mock.PropertyMock(side_effect=RuntimeError("boom"))
        try:
            self.assertTrue(InstrumentationManager._has_changed(old_set, new_set))
        finally:
            # Restore so other tests using this dataclass type are unaffected.
            del type(old_set.breakpoints[42]).config_id

    def test_outer_error_returns_changed(self):
        old_set = mock.MagicMock()
        # Accessing .breakpoints.keys() raises in the outer try.
        type(old_set).breakpoints = mock.PropertyMock(side_effect=RuntimeError("kaboom"))
        new_set = _bp_set(_config())
        self.assertTrue(InstrumentationManager._has_changed(old_set, new_set))


class TestGetUnchangedBreakpointsErrorBranches(unittest.TestCase):
    """Covers _get_unchanged_breakpoints except branches (lines 336-338, 352-354, 359-361)."""

    def test_common_lines_error_returns_empty(self):
        old_set = mock.MagicMock()
        # old.breakpoints.keys() raises in the inner try around common_lines.
        old_set.breakpoints.keys.side_effect = RuntimeError("boom")
        new_set = _bp_set(_config())
        self.assertEqual(InstrumentationManager._get_unchanged_breakpoints(old_set, new_set), set())

    def test_per_line_error_is_skipped(self):
        old_config = _config(location_hash="x")
        old_set = _bp_set(old_config)
        new_set = _bp_set(_config(location_hash="x"))
        # Force config_id access to raise during the per-line comparison.
        type(old_set.breakpoints[42]).config_id = mock.PropertyMock(side_effect=RuntimeError("boom"))
        try:
            # The single common line raises and is skipped -> empty set.
            self.assertEqual(InstrumentationManager._get_unchanged_breakpoints(old_set, new_set), set())
        finally:
            del type(old_set.breakpoints[42]).config_id

    def test_outer_error_returns_empty(self):
        old_config = _config(location_hash="x")
        old_set = _bp_set(old_config)
        new_set = _bp_set(_config(location_hash="x"))
        # The common-lines computation succeeds; force the trailing logger.debug
        # (the only statement between the inner try blocks and the outer except)
        # to raise so the outer except branch (359-361) returns an empty set.
        with mock.patch.object(im_module.logger, "debug", side_effect=RuntimeError("log boom")):
            self.assertEqual(InstrumentationManager._get_unchanged_breakpoints(old_set, new_set), set())


class TestApplyFunctionFindSpecValueError(unittest.TestCase):
    """Covers the find_spec (ValueError, ModuleNotFoundError) -> spec None branch (lines 388-389)."""

    def test_find_spec_value_error_raises_module_not_found(self):
        manager = _make_manager()
        config = _config(method_name="handler", line_number=0, location_hash="h", instrumentation_type="PROBE")
        bp_set = FunctionBreakpointSet(
            function_key="not_loaded_module_xyz.handler",
            module="not_loaded_module_xyz",
            function_name="handler",
            breakpoints={0: config},
        )
        # find_spec raises ValueError -> spec=None -> ModuleNotFoundError.
        with mock.patch.object(im_module.importlib.util, "find_spec", side_effect=ValueError("relative import")):
            with self.assertRaises(ModuleNotFoundError):
                manager._apply_function(bp_set)


class TestApplyFunctionNoEngineWithLineNumbers(unittest.TestCase):
    """Covers the 'no engine but line breakpoints exist' debug branch (line 449)."""

    def test_no_engine_with_line_breakpoints_logs_and_creates_state(self):
        manager = _make_manager()
        manager._engine = None  # no engine

        def real_func(value):
            return value

        manager._wrapper.instrument_function.return_value = (real_func, mock.MagicMock())

        line_config = _config(method_name="handler", line_number=42, location_hash="bp-1")
        bp_set = FunctionBreakpointSet(
            function_key=f"{__name__}.handler",
            module=__name__,
            function_name="handler",
            breakpoints={42: line_config},
        )

        manager._apply_function(bp_set)

        # The function is still wrapped and instrumented even with no engine.
        self.assertTrue(bp_set.is_instrumented)
        self.assertIn(bp_set.function_key, manager._active_functions)


class TestRemoveFunctionOuterException(unittest.TestCase):
    """Covers _remove_function's outer except branch (lines 550-551)."""

    def test_remove_swallows_unexpected_error(self):
        manager = _make_manager()
        bad_set = mock.MagicMock()
        # Accessing .states raises during the preserve-state loop.
        type(bad_set).states = mock.PropertyMock(side_effect=RuntimeError("boom"))
        manager._active_functions["bad.func"] = bad_set
        # Must not raise — cleanup code swallows errors.
        manager._remove_function("bad.func", preserve_state_for_bp_keys={"bad.func:0"})


class TestIncrementHitCountOuterException(unittest.TestCase):
    """Covers increment_hit_count's outer except branch (lines 652-654)."""

    def test_lock_acquisition_error_returns_false(self):
        manager = _make_manager()
        # Replace the lock with one whose context-manager entry raises.
        broken_lock = mock.MagicMock()
        broken_lock.__enter__.side_effect = RuntimeError("lock boom")
        manager._lock = broken_lock
        self.assertFalse(manager.increment_hit_count("any.func:0"))


class TestCleanupOrphanedStatesBranches(unittest.TestCase):
    """Covers _cleanup_orphaned_states inner/outer except branches (lines 672-674, 687-688)."""

    def test_states_access_error_on_one_set_is_skipped(self):
        manager = _make_manager()
        good_key = "good.func"
        good_state = BreakpointState(breakpoint_key=f"{good_key}:0")
        good_set = SimpleNamespace(states={f"{good_key}:0": good_state}, function_key=good_key)

        bad_set = mock.MagicMock()
        bad_set.function_key = "bad.func"
        type(bad_set).states = mock.PropertyMock(side_effect=RuntimeError("states boom"))

        manager._active_functions = {good_key: good_set, "bad.func": bad_set}
        # An orphan that IS in the good set's keys should be retained; a true orphan removed.
        manager._preserved_states[f"{good_key}:0"] = good_state
        manager._preserved_states["orphan.func:9"] = BreakpointState(breakpoint_key="orphan.func:9")

        manager._cleanup_orphaned_states()

        # Good (active) state retained; the unrelated orphan removed; no crash from bad_set.
        self.assertIn(f"{good_key}:0", manager._preserved_states)
        self.assertNotIn("orphan.func:9", manager._preserved_states)

    def test_outer_error_is_swallowed(self):
        manager = _make_manager()
        # Make _active_functions.values() raise to hit the outer except.
        broken = mock.MagicMock()
        broken.values.side_effect = RuntimeError("boom")
        manager._active_functions = broken
        # Must not raise.
        manager._cleanup_orphaned_states()


class TestApplyConfigurationRemoveFailureAndCatastrophic(unittest.TestCase):
    """Covers remove-obsolete-function failure (846-850) and catastrophic error (875-879)."""

    def test_remove_obsolete_failure_still_counts_as_removed(self):
        manager = _make_manager()
        # Seed an active function that will become obsolete.
        existing = _config(method_name="old", location_hash="old-1")
        manager._active_functions[existing.function_key] = _bp_set(existing)

        # _remove_function raises for the obsolete function; apply_configuration must
        # still count it as removed and not fail the whole operation.
        with mock.patch.object(manager, "_remove_function", side_effect=RuntimeError("remove boom")):
            result = manager.apply_configuration([])

        self.assertIn(existing.function_key, result["details"]["removed"])
        self.assertGreaterEqual(result["removed"], 1)

    def test_catastrophic_error_returns_error_result(self):
        manager = _make_manager()
        # Make the very first step (_group_by_function) raise inside the locked block.
        with mock.patch.object(manager, "_group_by_function", side_effect=RuntimeError("catastrophe")):
            result = manager.apply_configuration([_config()])
        self.assertFalse(result["success"])
        self.assertIn("error", result)
        self.assertEqual(result["applied"], 0)
        self.assertEqual(result["details"], {"succeeded": [], "failed": [], "removed": [], "unchanged": []})


if __name__ == "__main__":
    unittest.main()
