# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for BytecodeInjectionEngine.enable_function_entry (Python 3.9-3.11).

Mirrors test_sys_monitoring_function_entry.py — the two engines must be
behaviorally identical for the framework-bypass class of bug.

Covers the property that distinguishes the engine path from the setattr-based
wrapper: function-entry instrumentation fires regardless of which Python-level
reference invokes the function. Bytecode is rewritten via
``func.__code__ = new_code``, so framework registries holding the function
reference (Django ``URLPattern.callback``, Flask ``view_functions[endpoint]``,
``functools.wraps`` decorator closures) execute the new bytecode on next call.
"""

import functools
import sys
import unittest
from functools import partial

import pytest

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig

if (3, 9) <= sys.version_info < (3, 12):
    # pylint: disable=import-error,wrong-import-position
    from amazon.opentelemetry.distro.debugger.instrumentation_engine._bytecode_injection_engine import (
        BytecodeInjectionEngine,
    )
else:  # pragma: no cover
    BytecodeInjectionEngine = None  # type: ignore[assignment]


# Module-level functions used as instrumentation targets so they have stable
# code objects and can be referenced by stale closures created at module load.
def _module_target(value):
    """Plain target — used to validate stale-ref invocation."""
    return value * 2


def _module_target_with_kwargs(a, b=10):
    """Target accepting kwargs — used to validate argument capture."""
    return a + b


def _module_generator(n):
    """Generator function — must be skipped by enable_function_entry."""
    for i in range(n):
        yield i * 2


def _module_target_that_raises(value):
    """Target that always raises — used to validate exception-path coverage."""
    raise ValueError(f"intentional: {value}")


def _module_recursive_factorial(n):
    """Self-recursive target — used to validate frame-local state isolation."""
    if n <= 1:
        return 1
    return n * _module_recursive_factorial(n - 1)


# A typical Django-style decorator: produces a wrapper function whose __code__
# is NOT the user view's __code__. Used to verify _undecorated traversal.
def _login_required(view_func):
    @functools.wraps(view_func)
    def wrapper(request, *args, **kwargs):
        return view_func(request, *args, **kwargs)
    return wrapper


@_login_required
def _decorated_view(request):
    return f"original-view:{request}"


# Stale reference captured at module-import time. Mimics how Django's URL
# resolver captures ``views.aws_sdk_call`` into ``URLPattern.callback`` —
# once a wrapper or bytecode rewrite happens, this stale reference is still
# the one the dispatch path uses.
_STALE_REF = _module_target


@pytest.mark.skipif(
    not ((3, 9) <= sys.version_info < (3, 12)),
    reason="BytecodeInjectionEngine only runs on Python 3.9-3.11",
)
class TestEnableFunctionEntry(unittest.TestCase):
    """Validate enable_function_entry on real bytecode injection."""

    def setUp(self):
        # Mock the snapshot emitter so we count emit calls without touching OTLP
        from amazon.opentelemetry.distro.debugger import _function_wrapper as fw

        self._mock_emitter = unittest.mock.MagicMock()
        self._prev_emitter = fw.get_snapshot_emitter()
        fw.set_snapshot_emitter(self._mock_emitter)

        self.engine = BytecodeInjectionEngine()
        self.engine.initialize(hit_count_callback=lambda key: True)

    def tearDown(self):
        # Restore the previous emitter
        from amazon.opentelemetry.distro.debugger import _function_wrapper as fw
        fw.set_snapshot_emitter(self._prev_emitter)

        # Disable every active function-entry hook BEFORE cleanup() (which
        # clears the tracking dict). Each disable restores func.__code__ to
        # the original, so the next test starts from a clean module-level
        # function — otherwise the previous test's bytecode rewrite leaks
        # and emit counts compound across tests.
        try:
            for entry in list(self.engine._function_entries.values()):
                func = entry.get("func")
                original = entry.get("original_code")
                if func is not None and original is not None:
                    try:
                        func.__code__ = original
                    except Exception:
                        pass
            self.engine._function_entries.clear()
            self.engine.cleanup()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Stale reference is the whole point of the engine path.
    # ------------------------------------------------------------------
    def test_fires_when_invoked_via_stale_module_reference(self):
        """A reference captured BEFORE instrumentation still triggers the hook.

        This is the property that closes the framework-bypass class of bug.
        Django ``URLPattern.callback`` captures the function at urls.py import
        time; bytecode rewrite via ``func.__code__ = new_code`` is observed
        because Python re-reads ``__code__`` at every call dispatch.
        """
        local_ref = _module_target  # capture before enable

        ok = self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=CaptureConfig(capture_arguments=[], capture_return=True),
            location_hash="hash-stale",
            instrumentation_type="PROBE",
        )
        self.assertTrue(ok)

        result = local_ref(7)
        self.assertEqual(result, 14, "function still returns the correct value")
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)

        snapshot = self._mock_emitter.emit_snapshot.call_args[0][0]
        self.assertEqual(snapshot.location_hash, "hash-stale")
        self.assertEqual(snapshot.instrumentation_type, "PROBE")
        self.assertEqual(snapshot.instrumentation.location.method_name, "_module_target")

    def test_multiple_invocations_each_fire(self):
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
        for v in (1, 2, 3, 4, 5):
            self.assertEqual(_module_target(v), v * 2)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 5)

    # ------------------------------------------------------------------
    # Argument and return-value capture follow CaptureConfig semantics.
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

    def test_capture_return_value(self):
        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=CaptureConfig(capture_arguments=None, capture_return=True),
            location_hash="h",
            instrumentation_type="PROBE",
        )
        _module_target(21)
        snapshot = self._mock_emitter.emit_snapshot.call_args[0][0]
        self.assertIsNotNone(snapshot.captures.return_context)

    # ------------------------------------------------------------------
    # Decorator unwrapping ("@login_required problem") — the BFS lives in
    # the shared _undecorate module; engine-level smoke tests confirm the
    # engine integrates with it correctly. Detailed coverage for the BFS
    # itself lives in test_undecorate.py.
    # ------------------------------------------------------------------
    def test_engine_instruments_inner_view_under_functools_wraps(self):
        """Enabling on a decorated view should rewrite the INNER function's bytecode."""
        original_inner_code = _decorated_view.__wrapped__.__code__
        ok = self.engine.enable_function_entry(
            code=_decorated_view.__code__,
            func=_decorated_view,
            function_key=f"{__name__}._decorated_view",
            module_name=__name__,
            qualified_name="_decorated_view",
            capture_config=None,
            location_hash="h-dec",
            instrumentation_type="PROBE",
        )
        self.assertTrue(ok)
        # The inner function's __code__ has been replaced (rewrite happened
        # there, not on the wrapper).
        self.assertIsNot(_decorated_view.__wrapped__.__code__, original_inner_code)

    # ------------------------------------------------------------------
    # Generators / async-generators must be skipped.
    # ------------------------------------------------------------------
    def test_generator_skipped(self):
        ok = self.engine.enable_function_entry(
            code=_module_generator.__code__,
            func=_module_generator,
            function_key=f"{__name__}._module_generator",
            module_name=__name__,
            qualified_name="_module_generator",
        )
        # Returns False — instrumenting YIELD_VALUE corrupts .send()
        self.assertFalse(ok)
        # Generator still works normally
        self.assertEqual(list(_module_generator(3)), [0, 2, 4])

    # ------------------------------------------------------------------
    # Reentrancy guard — wrapper path skips the engine handler.
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Disable restores original bytecode.
    # ------------------------------------------------------------------
    def test_disable_function_entry_restores_original(self):
        original_code = _module_target.__code__
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
        # __code__ has been replaced
        self.assertIsNot(_module_target.__code__, original_code)
        _module_target(1)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)

        self.engine.disable_function_entry(_module_target.__code__, func=_module_target)
        # __code__ restored to original (same identity)
        self.assertIs(_module_target.__code__, original_code)
        # Subsequent calls don't emit
        _module_target(2)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)

    def test_disable_with_only_code_falls_back_to_scan(self):
        """If caller passes only ``code``, engine scans entries to find the match."""
        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
        )
        # Caller passes the (rewritten) code object only — engine should still find it
        self.engine.disable_function_entry(_module_target.__code__)
        self.assertEqual(len(self.engine._function_entries), 0)

    # ------------------------------------------------------------------
    # Hit-count denial path.
    # ------------------------------------------------------------------
    def test_hit_count_denial_skips_emission(self):
        # Reinitialize with a callback that always rate-limits
        self.engine.cleanup()
        self.engine = BytecodeInjectionEngine()
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
    # Exception-path coverage (the real bug fix).
    # ------------------------------------------------------------------
    def test_exception_in_user_code_emits_snapshot_and_reraises(self):
        """Exception escaping the function still fires a snapshot and propagates.

        Without the try/except wrapper around the injected body, an exception
        would skip the exit handler entirely and the snapshot for the failure
        path (the one the user cares about most) would be dropped silently.
        """
        self.engine.enable_function_entry(
            code=_module_target_that_raises.__code__,
            func=_module_target_that_raises,
            function_key=f"{__name__}._module_target_that_raises",
            module_name=__name__,
            qualified_name="_module_target_that_raises",
            capture_config=CaptureConfig(capture_arguments=[], capture_return=False),
            location_hash="hash-exc",
            instrumentation_type="PROBE",
        )

        # The original ValueError must propagate unchanged.
        with self.assertRaises(ValueError) as ctx:
            _module_target_that_raises(7)
        self.assertEqual(str(ctx.exception), "intentional: 7")

        # AND a snapshot was emitted for the exception path.
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 1)
        snapshot = self._mock_emitter.emit_snapshot.call_args[0][0]
        self.assertEqual(snapshot.location_hash, "hash-exc")
        self.assertEqual(snapshot.instrumentation_type, "PROBE")

    def test_repeated_exceptions_emit_one_snapshot_each(self):
        """5 raises => 5 snapshots and no cross-call state to drift.

        Each invocation owns its own frame slots (start_ns + entry_context),
        so repeated exceptions can't accumulate state — that property used to
        require a per-thread LIFO stack scan; now it falls out of CPython's
        per-call frame allocation.
        """
        self.engine.enable_function_entry(
            code=_module_target_that_raises.__code__,
            func=_module_target_that_raises,
            function_key=f"{__name__}._module_target_that_raises",
            module_name=__name__,
            qualified_name="_module_target_that_raises",
            capture_config=None,
            location_hash="h",
            instrumentation_type="PROBE",
        )
        for v in range(5):
            with self.assertRaises(ValueError):
                _module_target_that_raises(v)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 5)

    def test_normal_then_exception_then_normal_attributes_correctly(self):
        """Mixed normal + exception calls don't cross-contaminate frames."""
        self.engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key=f"{__name__}._module_target",
            module_name=__name__,
            qualified_name="_module_target",
            capture_config=None,
            location_hash="h-ok",
            instrumentation_type="PROBE",
        )
        self.engine.enable_function_entry(
            code=_module_target_that_raises.__code__,
            func=_module_target_that_raises,
            function_key=f"{__name__}._module_target_that_raises",
            module_name=__name__,
            qualified_name="_module_target_that_raises",
            capture_config=None,
            location_hash="h-raise",
            instrumentation_type="PROBE",
        )
        # 1 normal, 1 raise, 1 normal — interleaved through different functions.
        self.assertEqual(_module_target(3), 6)
        with self.assertRaises(ValueError):
            _module_target_that_raises(99)
        self.assertEqual(_module_target(4), 8)

        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 3)
        hashes = [
            call.args[0].location_hash for call in self._mock_emitter.emit_snapshot.call_args_list
        ]
        self.assertEqual(hashes, ["h-ok", "h-raise", "h-ok"])

    def test_recursive_function_emits_snapshot_per_call(self):
        """factorial(5) recurses 5x; each invocation must emit independently.

        Frame-local state (start_ns + entry_context per CPython frame)
        guarantees recursion correctness without any cross-call coordination.
        """
        self.engine.enable_function_entry(
            code=_module_recursive_factorial.__code__,
            func=_module_recursive_factorial,
            function_key=f"{__name__}._module_recursive_factorial",
            module_name=__name__,
            qualified_name="_module_recursive_factorial",
            capture_config=None,
            location_hash="h-rec",
            instrumentation_type="PROBE",
        )
        self.assertEqual(_module_recursive_factorial(5), 120)
        # 5 invocations: factorial(5) -> factorial(4) -> ... -> factorial(1)
        self.assertEqual(self._mock_emitter.emit_snapshot.call_count, 5)

    # ------------------------------------------------------------------
    # Negative path — engine not initialized.
    # ------------------------------------------------------------------
    def test_enable_when_not_initialized_returns_false(self):
        engine = BytecodeInjectionEngine()  # never initialize()d
        ok = engine.enable_function_entry(
            code=_module_target.__code__,
            func=_module_target,
            function_key="x",
            module_name="x",
            qualified_name="x",
        )
        self.assertFalse(ok)


# Import unittest.mock at module scope so tearDown can use it
import unittest.mock  # noqa: E402


