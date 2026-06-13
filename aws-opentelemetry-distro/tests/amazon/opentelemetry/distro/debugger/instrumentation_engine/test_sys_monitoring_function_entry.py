# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for SysMonitoringEngine.enable_function_entry (PY_START / PY_RETURN).

Covers the property that distinguishes the engine path from the setattr-based
wrapper: function-entry instrumentation fires regardless of which Python-level
reference invokes the function, so framework patterns that capture the original
function object at import time (Django URLPattern.callback, Flask
view_functions, decorators, ``from x import y``) don't bypass it.
"""

import sys
import unittest
from unittest import mock

import pytest

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig

if sys.version_info >= (3, 12):
    # pylint: disable=import-error,wrong-import-position
    from amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine import (
        SysMonitoringEngine,
    )
else:  # pragma: no cover
    SysMonitoringEngine = None  # type: ignore[assignment]


# Module-level functions used as instrumentation targets so they have stable
# code objects and can be referenced by stale closures created at module load.
def _module_target(value):
    """Plain target — used to validate stale-ref invocation."""
    return value * 2


def _module_target_with_kwargs(a, b=10):
    """Target accepting kwargs — used to validate argument capture."""
    return a + b


# Stale reference captured at module-import time. Mimics how Django's URL
# resolver captures ``views.aws_sdk_call`` into ``URLPattern.callback`` — once
# the SDK monkey-patches ``views.aws_sdk_call``, this stale reference is the
# only thing the dispatch path can find, and ``setattr`` no longer helps.
_STALE_REF = _module_target


@pytest.mark.skipif(sys.version_info < (3, 12), reason="sys.monitoring requires Python 3.12+")
class TestEnableFunctionEntry(unittest.TestCase):
    """Validate enable_function_entry on real ``sys.monitoring`` infrastructure."""

    def setUp(self):
        # Clean any leftover registration from another test run.
        try:
            if sys.monitoring.get_tool(sys.monitoring.DEBUGGER_ID) is not None:
                sys.monitoring.free_tool_id(sys.monitoring.DEBUGGER_ID)
        except Exception:
            pass
        self.engine = SysMonitoringEngine()
        self.engine.initialize(hit_count_callback=lambda key: True)

        # Mock out the snapshot emitter so we can count emit calls without
        # running through the real OTLP exporter.
        self._emit_patcher = mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter",
            create=False,
        )
        # The engine imports get_snapshot_emitter inside _handle_function_entry,
        # so the patch needs to land on the source module instead.
        self._emit_patcher = mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter",
            return_value=mock.MagicMock(),
        )
        self._mock_get_emitter = self._emit_patcher.start()
        self._mock_emitter = self._mock_get_emitter.return_value

    def tearDown(self):
        self._emit_patcher.stop()
        self.engine.cleanup()

    # ------------------------------------------------------------------
    # Stale reference is the whole point of the engine path.
    # ------------------------------------------------------------------
    def test_fires_when_invoked_via_stale_module_reference(self):
        """A reference captured BEFORE instrumentation still triggers PY_START."""
        captured = []
        local_ref = _module_target  # capture before enable

        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=CaptureConfig(capture_arguments=[], capture_return=True),
            location_hash="hash-stale",
            instrumentation_type="PROBE",
        )

        # Patch the global ``_module_target`` to something different — the
        # caller below uses ``local_ref`` (stale) so should still hit our hook.
        original = sys.modules[__name__]._module_target
        try:
            sys.modules[__name__]._module_target = lambda v: "PATCHED"
            result = local_ref(7)
            self.assertEqual(result, 14)  # original code path ran
        finally:
            sys.modules[__name__]._module_target = original

        # _emit_snapshot should have been called exactly once.
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)
        snapshot = self._mock_emitter.emit_snapshot.call_args[0][0]
        self.assertEqual(snapshot.location_hash, "hash-stale")
        self.assertEqual(snapshot.instrumentation_type, "PROBE")
        self.assertEqual(snapshot.instrumentation.location.method_name, "_module_target")

    def test_module_attribute_with_different_code_does_not_fire(self):
        """PY_START is per-code-object — replacing the attr with a different func is invisible."""
        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=None,
            location_hash="hash",
            instrumentation_type="PROBE",
        )
        replacement = lambda v: "REPLACED"  # different code object
        sys.modules[__name__]._module_target = replacement
        try:
            self.assertEqual(replacement(99), "REPLACED")
        finally:
            sys.modules[__name__]._module_target = _module_target

        # The replacement's different __code__ was never armed.
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 0)

    # ------------------------------------------------------------------
    # Argument capture matches CaptureConfig semantics.
    # ------------------------------------------------------------------
    def test_capture_arguments_empty_list_captures_all(self):
        self.engine.enable_function_entry(
            code=_module_target_with_kwargs.__code__,
            func=_module_target_with_kwargs,
            function_key=f"{__name__}._module_target_with_kwargs",
            module_name=__name__,
            qualified_name="_module_target_with_kwargs",
            capture_config=CaptureConfig(capture_arguments=[], capture_return=False),
            location_hash="h",
            instrumentation_type="PROBE",
        )
        _module_target_with_kwargs(3, b=4)

        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)
        snapshot = self._mock_emitter.emit_snapshot.call_args[0][0]
        args = snapshot.captures.entry.arguments
        self.assertIn("a", args)
        self.assertIn("b", args)

    def test_capture_arguments_filters_to_named_subset(self):
        self.engine.enable_function_entry(
            code=_module_target_with_kwargs.__code__,
            func=_module_target_with_kwargs,
            function_key=f"{__name__}._module_target_with_kwargs",
            module_name=__name__,
            qualified_name="_module_target_with_kwargs",
            capture_config=CaptureConfig(capture_arguments=["a"], capture_return=False),
            location_hash="h",
            instrumentation_type="PROBE",
        )
        _module_target_with_kwargs(11, b=22)

        snapshot = self._mock_emitter.emit_snapshot.call_args[0][0]
        args = snapshot.captures.entry.arguments
        self.assertIn("a", args)
        self.assertNotIn("b", args)

    # ------------------------------------------------------------------
    # Reentrancy guard prevents double-emission via the wrapper path.
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Disable is a clean teardown.
    # ------------------------------------------------------------------
    def test_disable_function_entry_stops_firing(self):
        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=None,
            location_hash="h",
            instrumentation_type="PROBE",
        )
        _module_target(1)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)

        self.engine.disable_function_entry(_module_target.__code__)
        _module_target(2)
        # Still 1 — disable cleared the registry, the handler returns DISABLE.
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)

    # ------------------------------------------------------------------
    # Hit-count callback denial path.
    # ------------------------------------------------------------------
    def test_hit_count_denial_skips_emission(self):
        # Reinitialize with a callback that always rate-limits.
        self.engine.cleanup()
        self.engine = SysMonitoringEngine()
        self.engine.initialize(hit_count_callback=lambda key: False)

        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=None,
            location_hash="h",
            instrumentation_type="PROBE",
        )
        _module_target(1)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 0)

    # ------------------------------------------------------------------
    # Negative path — engine not initialized.
    # ------------------------------------------------------------------
    def test_enable_when_not_initialized_returns_false(self):
        self.engine.cleanup()
        self.engine = SysMonitoringEngine()  # never initialize()d
        ok = self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key="x",
            module_name="x",
            qualified_name="x",
        )
        self.assertFalse(ok)


