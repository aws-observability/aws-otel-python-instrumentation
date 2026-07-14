# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for config recreate detection via created_at comparison.

Tests _has_changed and _get_unchanged_breakpoints static methods which use
config_id (locationHash) and created_at to detect recreated configs.
"""

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

# Mock heavy dependencies that are not needed for these unit tests, to avoid the
# import chain pulling in SnapshotOtlpEmitter and the OTel SDK exporters.
#
# pytest imports every test module during collection (before running any test), and
# Python caches imports in sys.modules. If we left these mocks (or a copy of
# instrumentation_manager that was imported while they were mocked) in sys.modules, then
# unrelated test modules collected afterwards would pick up MagicMocks for e.g.
# StatusReporter/ErrorCause and fail. To keep the mocking strictly local to this module,
# we snapshot sys.modules for every name we may touch, install the mocks just for the
# imports below, then restore the snapshot so other test modules re-import the real code.
_MOCKED_MODULES = (
    "amazon.opentelemetry.distro.debugger._snapshot_otlp_emitter",
    "amazon.opentelemetry.distro.debugger._function_wrapper",
    "amazon.opentelemetry.distro.debugger._status_reporter",
    "amazon.opentelemetry.distro.debugger.instrumentation_engine._instrumentation_engine",
)
# Also snapshot the modules we import below: importing them while deps are mocked binds
# the mocks into their namespaces, so they must be re-imported fresh afterwards too.
_RESTORE_MODULES = _MOCKED_MODULES + (
    "amazon.opentelemetry.distro.debugger._data_models",
    "amazon.opentelemetry.distro.debugger.instrumentation_manager",
)
_ORIGINAL_MODULES = {name: sys.modules.get(name) for name in _RESTORE_MODULES}
for _name in _MOCKED_MODULES:
    sys.modules[_name] = MagicMock()

try:
    from amazon.opentelemetry.distro.debugger._data_models import (  # noqa: E402
        BreakpointConfiguration,
        CaptureConfig,
        FunctionBreakpointSet,
    )
    from amazon.opentelemetry.distro.debugger.instrumentation_manager import InstrumentationManager  # noqa: E402
finally:
    # Restore the real modules (or evict the entry if there was none before) so the mocks
    # do not leak into other test modules in the same pytest session.
    for _name, _original in _ORIGINAL_MODULES.items():
        if _original is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _original


def _make_config(
    module="myapp.service",
    function="process",
    line=42,
    config_id="hash1",
    created_at=None,
    instrumentation_type="BREAKPOINT",
):
    """Helper to create a BreakpointConfiguration for testing."""
    return BreakpointConfiguration(
        module=module,
        function_name=function,
        line_number=line,
        capture_config=CaptureConfig(),
        config_id=config_id,
        instrumentation_type=instrumentation_type,
        created_at=created_at,
    )


def _make_bp_set(configs):
    """Helper to create a FunctionBreakpointSet from a list of configs."""
    if not configs:
        return None
    first = configs[0]
    bp_set = FunctionBreakpointSet(
        function_key=first.function_key,
        module=first.module,
        function_name=first.function_name,
        breakpoints={},
    )
    for c in configs:
        bp_set.breakpoints[c.line_number] = c
    return bp_set


class TestConfigRecreateDetection:
    """Tests for _has_changed and _get_unchanged_breakpoints with created_at."""

    def test_same_location_different_created_at_detected_as_changed(self):
        """Delete+recreate at same location with new created_at should be detected."""
        t1 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2025, 1, 1, 0, 1, 0, tzinfo=timezone.utc)

        old_set = _make_bp_set([_make_config(config_id="hash1", created_at=t1)])
        new_set = _make_bp_set([_make_config(config_id="hash1", created_at=t2)])

        assert InstrumentationManager._has_changed(old_set, new_set) is True
        assert InstrumentationManager._get_unchanged_breakpoints(old_set, new_set) == set()

    def test_same_location_same_created_at_detected_as_unchanged(self):
        """Same config_id and created_at should be unchanged."""
        t1 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        old_set = _make_bp_set([_make_config(config_id="hash1", created_at=t1)])
        new_set = _make_bp_set([_make_config(config_id="hash1", created_at=t1)])

        assert InstrumentationManager._has_changed(old_set, new_set) is False
        unchanged = InstrumentationManager._get_unchanged_breakpoints(old_set, new_set)
        assert len(unchanged) == 1

    def test_null_created_at_falls_back_to_config_id(self):
        """When created_at is None on both, fall back to config_id comparison."""
        # Same config_id, both None created_at -> unchanged
        old_set = _make_bp_set([_make_config(config_id="hash1", created_at=None)])
        new_set = _make_bp_set([_make_config(config_id="hash1", created_at=None)])
        assert InstrumentationManager._has_changed(old_set, new_set) is False

        # Different config_id, both None created_at -> changed
        old_set2 = _make_bp_set([_make_config(config_id="hash1", created_at=None)])
        new_set2 = _make_bp_set([_make_config(config_id="hash2", created_at=None)])
        assert InstrumentationManager._has_changed(old_set2, new_set2) is True

    def test_upgrade_scenario_null_to_non_null_created_at(self):
        """Old config without created_at, new config with created_at -> changed."""
        t1 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        old_set = _make_bp_set([_make_config(config_id="hash1", created_at=None)])
        new_set = _make_bp_set([_make_config(config_id="hash1", created_at=t1)])

        assert InstrumentationManager._has_changed(old_set, new_set) is True

    def test_different_line_numbers_detected_as_changed(self):
        """Different line numbers should always be detected as changed."""
        t1 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        old_set = _make_bp_set([_make_config(line=42, config_id="hash1", created_at=t1)])
        new_set = _make_bp_set([_make_config(line=43, config_id="hash1", created_at=t1)])

        assert InstrumentationManager._has_changed(old_set, new_set) is True
