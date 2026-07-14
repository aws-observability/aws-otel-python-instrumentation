# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for failed configuration tracking and graceful module-not-found handling."""

import unittest
from unittest.mock import patch

from amazon.opentelemetry.distro.debugger._data_models import (
    BreakpointConfiguration,
    CaptureConfig,
    FunctionBreakpointSet,
)
from amazon.opentelemetry.distro.debugger._status_reporter import ErrorCause
from amazon.opentelemetry.distro.debugger.instrumentation_manager import InstrumentationManager


def _make_config(module="server", function_name="process_order", line_number=0, config_id="hash001"):
    """Helper to create a BreakpointConfiguration for testing."""
    return BreakpointConfiguration(
        config_id=config_id,
        module=module,
        function_name=function_name,
        line_number=line_number,
        instrumentation_type="BREAKPOINT",
        capture_config=CaptureConfig(),
        max_hits=100,
    )


class TestFailedConfigTracking(unittest.TestCase):
    """Test that failed configs are tracked and not retried on subsequent polls."""

    def setUp(self):
        """Create a manager with mocked internals."""
        with patch.object(InstrumentationManager, "_select_engine", return_value=None):
            with patch.object(InstrumentationManager, "_build_resource", return_value=None):
                with patch("amazon.opentelemetry.distro.debugger.instrumentation_manager.SnapshotOtlpEmitter"):
                    with patch("amazon.opentelemetry.distro.debugger.instrumentation_manager.set_snapshot_emitter"):
                        self.manager = InstrumentationManager()

    def test_module_not_found_tracked_in_failed_configs(self):
        """When a module doesn't exist, its config_id should be added to _failed_configs."""
        config = _make_config(module="nonexistent_module_xyz", config_id="hash_bad")

        result = self.manager.apply_configuration([config])

        self.assertEqual(result["failed"], 1)
        self.assertIn("hash_bad", self.manager._failed_configs)

    def test_failed_config_not_retried_on_next_poll(self):
        """A config that previously failed should be silently skipped on the next poll."""
        config = _make_config(module="nonexistent_module_xyz", config_id="hash_bad")

        # First call — triggers the failure and logs it
        result1 = self.manager.apply_configuration([config])
        self.assertEqual(result1["failed"], 1)

        # Second call — same config, should be skipped silently (not retried)
        result2 = self.manager.apply_configuration([config])
        # Should NOT appear in failed again (skipped entirely)
        self.assertEqual(result2["failed"], 0)
        self.assertEqual(result2["applied"], 0)

    def test_failed_config_cleared_when_removed(self):
        """When a failed config is no longer in the incoming list, it should be cleared."""
        config_bad = _make_config(module="nonexistent_module_xyz", config_id="hash_bad")

        # First call — fail the config
        self.manager.apply_configuration([config_bad])
        self.assertIn("hash_bad", self.manager._failed_configs)

        # Second call — empty config list (config was deleted)
        self.manager.apply_configuration([])
        self.assertNotIn("hash_bad", self.manager._failed_configs)

    def test_failed_config_retried_when_config_changes(self):
        """If the config at a function changes, the failed entry should be cleared and retried."""
        config_bad = _make_config(module="nonexistent_module_xyz", config_id="hash_bad")

        # Fail the config
        self.manager.apply_configuration([config_bad])
        self.assertIn("hash_bad", self.manager._failed_configs)

        # New config with a different config_id at the same function (simulates user fix)
        config_new = _make_config(module="nonexistent_module_xyz", config_id="hash_new")
        self.manager.apply_configuration([config_new])

        # hash_bad should be cleared (not in incoming list)
        self.assertNotIn("hash_bad", self.manager._failed_configs)
        # hash_new should be attempted (and fail, since module still doesn't exist)
        self.assertIn("hash_new", self.manager._failed_configs)

    def test_error_cause_is_file_not_found_for_missing_module(self):
        """ModuleNotFoundError should produce FILE_NOT_FOUND or METHOD_NOT_FOUND error cause."""
        config = _make_config(module="nonexistent_module_xyz", config_id="hash_bad")

        result = self.manager.apply_configuration([config])

        failure = result["details"]["failed"][0]
        self.assertIn(failure["error_cause"], [ErrorCause.FILE_NOT_FOUND, ErrorCause.METHOD_NOT_FOUND])

    def test_good_config_not_affected_by_failed_sibling(self):
        """A valid config should still be applied even if another config fails."""
        config_bad = _make_config(module="nonexistent_module_xyz", config_id="hash_bad")
        # Use a real module that exists (like 'json' from stdlib)
        config_good = _make_config(module="json", function_name="dumps", config_id="hash_good")

        result = self.manager.apply_configuration([config_bad, config_good])

        # One should fail, one should succeed (or at least be attempted)
        self.assertEqual(result["failed"], 1)
        # hash_bad tracked, hash_good not
        self.assertIn("hash_bad", self.manager._failed_configs)
        self.assertNotIn("hash_good", self.manager._failed_configs)


class TestEarlyModuleCheck(unittest.TestCase):
    """Test that the early module existence check produces clean errors."""

    def setUp(self):
        with patch.object(InstrumentationManager, "_select_engine", return_value=None):
            with patch.object(InstrumentationManager, "_build_resource", return_value=None):
                with patch("amazon.opentelemetry.distro.debugger.instrumentation_manager.SnapshotOtlpEmitter"):
                    with patch("amazon.opentelemetry.distro.debugger.instrumentation_manager.set_snapshot_emitter"):
                        self.manager = InstrumentationManager()

    def test_early_check_raises_module_not_found(self):
        """_apply_function should raise ModuleNotFoundError for missing module."""
        bp_set = FunctionBreakpointSet(
            function_key="nonexistent_module.SomeClass.some_method",
            module="nonexistent_module",
            function_name="SomeClass.some_method",
            breakpoints={
                0: _make_config(module="nonexistent_module", function_name="SomeClass.some_method"),
            },
        )

        with self.assertRaises(ModuleNotFoundError) as ctx:
            self.manager._apply_function(bp_set)

        # Should contain a helpful message, not just "No module named..."
        self.assertIn("nonexistent_module", str(ctx.exception))
        self.assertIn("CodeUnit", str(ctx.exception))

    def test_valid_module_passes_check(self):
        """_apply_function should not raise ModuleNotFoundError for a valid module."""
        bp_set = FunctionBreakpointSet(
            function_key="json.dumps",
            module="json",
            function_name="dumps",
            breakpoints={
                0: _make_config(module="json", function_name="dumps", config_id="hash_json"),
            },
        )

        # Should not raise ModuleNotFoundError (may raise other errors from wrapping)
        try:
            self.manager._apply_function(bp_set)
        except ModuleNotFoundError:
            self.fail("_apply_function raised ModuleNotFoundError for valid module 'json'")
        except Exception:
            # Other errors (from wrapping) are expected — we only care about module check
            pass


if __name__ == "__main__":
    unittest.main()
