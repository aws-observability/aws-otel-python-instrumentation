# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the core (non-Flask) surface of _function_wrapper.py.

These tests cover the reflection helpers, snapshot building, emission, and the
sync/async wrapper bodies exercised end-to-end via ``instrument_function``.

The Flask view_functions patching is covered separately in
test_function_wrapper_flask.py and is intentionally not duplicated here.

No real network, threads, fork, or sleep are used. The global snapshot emitter
is mocked, and a lightweight in-process module is registered/cleaned up around
each test that needs module-level discovery.
"""

import asyncio
import sys
import threading
import types
import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger._data_models import (
    BreakpointConfiguration,
    BreakpointState,
    CaptureConfig,
    FunctionBreakpointSet,
)
from amazon.opentelemetry.distro.debugger._function_wrapper import (
    FunctionWrapper,
    MethodInfo,
    MethodType,
    get_snapshot_emitter,
    set_snapshot_emitter,
)
from amazon.opentelemetry.distro.debugger._snapshot_models import CapturedContext, Snapshot, TraceContext


# ---------------------------------------------------------------------------
# Sample classes for method-discovery / method-type detection tests.
# ---------------------------------------------------------------------------
class _SampleBase:
    def inherited_method(self):
        return "inherited"


class _SampleClass(_SampleBase):
    class_attr = 123

    def instance_method(self, value):
        return value

    @staticmethod
    def static_method(value):
        return value

    @classmethod
    def class_method(cls, value):
        return value


class _Service:
    """Module-level class whose method qualname is exactly 'ClassName.method'."""

    def handle(self, value):
        return value + 1


# ---------------------------------------------------------------------------
# Helpers for building a fake manager whose state drives the wrapper paths.
# ---------------------------------------------------------------------------
def _make_function_bp_set(func_key, module, function_name, *, has_line0=True, disabled=False):
    """Build a real FunctionBreakpointSet with an optional function-level (line 0) breakpoint."""
    bp_set = FunctionBreakpointSet(function_key=func_key, module=module, function_name=function_name)
    if has_line0:
        config = BreakpointConfiguration(
            module=module,
            function_name=function_name,
            line_number=0,
            capture_config=CaptureConfig(),
            config_id="loc-hash-1",
            instrumentation_type="PROBE",
        )
        bp_set.breakpoints[0] = config
        state = BreakpointState(
            breakpoint_key=f"{func_key}:0",
            location_hash="loc-hash-1",
            instrumentation_type="PROBE",
            is_disabled=disabled,
        )
        bp_set.states[f"{func_key}:0"] = state
    return bp_set


class _FakeManager:
    """Minimal stand-in for InstrumentationManager used by the wrapper bodies.

    Provides the attributes the wrapper reads under lock (``_lock``,
    ``_active_functions``) and a controllable ``increment_hit_count``.
    """

    def __init__(self, bp_sets, increment_result=True):
        self._lock = threading.Lock()
        self._active_functions = bp_sets
        self.increment_result = increment_result
        self.increment_calls = []

    def increment_hit_count(self, breakpoint_key):
        self.increment_calls.append(breakpoint_key)
        return self.increment_result


def _register_module(name, **attrs):
    """Create and register a real module object so discovery/replace works."""
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class _SnapshotEmitterFixture(unittest.TestCase):
    """Base class that mocks the global snapshot emitter for every test."""

    def setUp(self):
        self._prev_emitter = get_snapshot_emitter()
        self.emitter = mock.MagicMock()
        set_snapshot_emitter(self.emitter)
        self.addCleanup(lambda: set_snapshot_emitter(self._prev_emitter))


# ===========================================================================
# Reflection helpers
# ===========================================================================
class TestDiscoverFunction(_SnapshotEmitterFixture):
    def setUp(self):
        super().setUp()
        self.module_name = "_test_fw_discover_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_discover_module_function_resolves_callable(self):
        def target():
            return "ok"

        _register_module(self.module_name, target=target)
        discovered = FunctionWrapper._discover_function(self.module_name, "target")
        self.assertIs(discovered, target)

    def test_discover_function_not_found_raises_attribute_error(self):
        _register_module(self.module_name)
        with self.assertRaises(AttributeError):
            FunctionWrapper._discover_function(self.module_name, "missing")

    def test_discover_function_non_callable_raises_attribute_error(self):
        _register_module(self.module_name, not_a_func=42)
        with self.assertRaises(AttributeError):
            FunctionWrapper._discover_function(self.module_name, "not_a_func")

    def test_discover_missing_module_raises_import_error(self):
        with self.assertRaises(ImportError):
            FunctionWrapper._discover_function("_definitely_not_a_real_module_xyz", "f")

    def test_discover_class_method_returns_method_info(self):
        _register_module(self.module_name, _SampleClass=_SampleClass)
        discovered = FunctionWrapper._discover_function(self.module_name, "_SampleClass.instance_method")
        self.assertIsInstance(discovered, MethodInfo)
        self.assertEqual(discovered.method_name, "instance_method")
        self.assertEqual(discovered.method_type, MethodType.INSTANCE)
        self.assertEqual(discovered.class_name, "_SampleClass")
        self.assertEqual(discovered.full_name, f"{self.module_name}._SampleClass.instance_method")

    def test_discover_class_method_missing_method_raises(self):
        _register_module(self.module_name, _SampleClass=_SampleClass)
        with self.assertRaises(AttributeError):
            FunctionWrapper._discover_function(self.module_name, "_SampleClass.no_such_method")

    def test_discover_class_path_non_class_raises(self):
        _register_module(self.module_name, _SampleClass=_SampleClass)
        # instance_method is not a class, so navigating through it should fail.
        with self.assertRaises(AttributeError):
            FunctionWrapper._discover_function(self.module_name, "_SampleClass.instance_method.deeper")


class TestFindDefiningClassAndMethodType(unittest.TestCase):
    def test_find_defining_class_for_own_method(self):
        defining = FunctionWrapper._find_defining_class(_SampleClass, "instance_method")
        self.assertIs(defining, _SampleClass)

    def test_find_defining_class_for_inherited_method(self):
        defining = FunctionWrapper._find_defining_class(_SampleClass, "inherited_method")
        self.assertIs(defining, _SampleBase)

    def test_find_defining_class_fallback_when_absent(self):
        # A name that exists on the class but not in any __dict__ entry (e.g. a synthesized
        # attribute) falls back to the starting class.
        defining = FunctionWrapper._find_defining_class(_SampleClass, "does_not_exist")
        self.assertIs(defining, _SampleClass)

    def test_detect_instance_method(self):
        self.assertEqual(FunctionWrapper._detect_method_type(_SampleClass, "instance_method"), MethodType.INSTANCE)

    def test_detect_static_method(self):
        self.assertEqual(FunctionWrapper._detect_method_type(_SampleClass, "static_method"), MethodType.STATIC)

    def test_detect_class_method(self):
        self.assertEqual(FunctionWrapper._detect_method_type(_SampleClass, "class_method"), MethodType.CLASS)

    def test_detect_inherited_method_walks_mro(self):
        # inherited_method lives on _SampleBase; detection should still resolve it as INSTANCE.
        self.assertEqual(FunctionWrapper._detect_method_type(_SampleClass, "inherited_method"), MethodType.INSTANCE)

    def test_detect_missing_method_defaults_to_instance(self):
        self.assertEqual(FunctionWrapper._detect_method_type(_SampleClass, "totally_absent"), MethodType.INSTANCE)


class TestResolveModule(unittest.TestCase):
    def test_resolve_imports_normal_module(self):
        module = FunctionWrapper._resolve_module("json")
        self.assertEqual(module.__name__, "json")

    def test_resolve_matches_main_by_spec_name(self):
        fake_main = types.ModuleType("__main__")
        fake_main.__spec__ = types.SimpleNamespace(name="my_app_entry")
        with mock.patch.dict(sys.modules, {"__main__": fake_main}):
            resolved = FunctionWrapper._resolve_module("my_app_entry")
        self.assertIs(resolved, fake_main)

    def test_resolve_matches_main_by_file_stem(self):
        fake_main = types.ModuleType("__main__")
        fake_main.__spec__ = None
        fake_main.__file__ = "/some/path/demo_app.py"
        with mock.patch.dict(sys.modules, {"__main__": fake_main}):
            resolved = FunctionWrapper._resolve_module("demo_app")
        self.assertIs(resolved, fake_main)

    def test_resolve_main_no_match_falls_through_to_import(self):
        fake_main = types.ModuleType("__main__")
        fake_main.__spec__ = types.SimpleNamespace(name="something_else")
        fake_main.__file__ = "/some/path/other.py"
        with mock.patch.dict(sys.modules, {"__main__": fake_main}):
            resolved = FunctionWrapper._resolve_module("json")
        self.assertEqual(resolved.__name__, "json")


class TestGetQualifiedName(unittest.TestCase):
    def test_qualified_name_uses_qualname(self):
        self.assertEqual(
            FunctionWrapper._get_qualified_name(_SampleClass.instance_method),
            "_SampleClass.instance_method",
        )

    def test_qualified_name_for_plain_function(self):
        def plain():
            return None

        # Nested functions use a dotted __qualname__; the helper returns it verbatim.
        self.assertEqual(FunctionWrapper._get_qualified_name(plain), plain.__qualname__)
        self.assertTrue(plain.__qualname__.endswith("<locals>.plain"))

    def test_qualified_name_falls_back_to_name(self):
        # An object without __qualname__ falls back to __name__.
        fake = types.SimpleNamespace(__name__="bare")
        self.assertEqual(FunctionWrapper._get_qualified_name(fake), "bare")


class TestGetTraceContext(unittest.TestCase):
    def test_valid_span_returns_trace_context(self):
        span = mock.MagicMock()
        span_ctx = mock.MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0x0123456789ABCDEF0123456789ABCDEF
        span_ctx.span_id = 0x0123456789ABCDEF
        span.get_span_context.return_value = span_ctx
        with mock.patch("opentelemetry.trace.get_current_span", return_value=span):
            ctx = FunctionWrapper._get_trace_context()
        self.assertIsInstance(ctx, TraceContext)
        self.assertEqual(ctx.trace_id, "0123456789abcdef0123456789abcdef")
        self.assertEqual(ctx.span_id, "0123456789abcdef")

    def test_invalid_span_returns_none(self):
        span = mock.MagicMock()
        span_ctx = mock.MagicMock()
        span_ctx.is_valid = False
        span.get_span_context.return_value = span_ctx
        with mock.patch("opentelemetry.trace.get_current_span", return_value=span):
            self.assertIsNone(FunctionWrapper._get_trace_context())

    def test_exception_returns_none(self):
        with mock.patch("opentelemetry.trace.get_current_span", side_effect=RuntimeError("boom")):
            self.assertIsNone(FunctionWrapper._get_trace_context())


# ===========================================================================
# Context capture + snapshot building
# ===========================================================================
class TestCaptureEntryContext(unittest.TestCase):
    def setUp(self):
        self.wrapper = FunctionWrapper()

    def test_capture_all_arguments(self):
        def func(alpha, beta):
            return alpha + beta

        config = CaptureConfig(capture_arguments=[])  # [] => capture all
        ctx = self.wrapper._capture_entry_context(func, (1, 2), {}, config)
        self.assertIsInstance(ctx, CapturedContext)
        self.assertIn("alpha", ctx.arguments)
        self.assertIn("beta", ctx.arguments)
        self.assertEqual(ctx.arguments["alpha"].value, "1")

    def test_capture_named_subset_only(self):
        def func(alpha, beta):
            return alpha + beta

        config = CaptureConfig(capture_arguments=["beta"])
        ctx = self.wrapper._capture_entry_context(func, (1, 2), {}, config)
        self.assertNotIn("alpha", ctx.arguments)
        self.assertIn("beta", ctx.arguments)

    def test_capture_returns_none_when_no_matching_args(self):
        def func(alpha):
            return alpha

        config = CaptureConfig(capture_arguments=["not_a_param"])
        self.assertIsNone(self.wrapper._capture_entry_context(func, (1,), {}, config))

    def test_capture_entry_context_swallows_binding_error(self):
        def func(alpha):
            return alpha

        config = CaptureConfig(capture_arguments=[])
        # Too many positional args => sig.bind raises, caught internally.
        self.assertIsNone(self.wrapper._capture_entry_context(func, (1, 2, 3), {}, config))

    def test_entry_capture_respects_capture_config_max_string_length(self):
        def func(payload):
            return payload

        long_value = "x" * 200
        config = CaptureConfig(capture_arguments=[], max_string_length=10)
        ctx = self.wrapper._capture_entry_context(func, (long_value,), {}, config)
        captured = ctx.arguments["payload"]
        self.assertEqual(captured.value, "x" * 10)
        self.assertTrue(captured.truncated)
        self.assertEqual(captured.size, 200)

    def test_entry_capture_respects_capture_config_max_fields_per_object(self):
        class Bag:
            def __init__(self):
                for idx in range(40):
                    setattr(self, f"f{idx}", idx)

        def func(bag):
            return bag

        config = CaptureConfig(capture_arguments=[], max_fields_per_object=5)
        ctx = self.wrapper._capture_entry_context(func, (Bag(),), {}, config)
        captured = ctx.arguments["bag"]
        self.assertEqual(len(captured.fields), 5)
        self.assertEqual(captured.not_captured_reason, "fieldCount")
        self.assertEqual(captured.size, 40)


class TestCaptureReturnContext(unittest.TestCase):
    def setUp(self):
        self.wrapper = FunctionWrapper()

    def test_capture_return_value(self):
        config = CaptureConfig(capture_return=True)
        ctx = self.wrapper._capture_return_context("result", None, config, None)
        self.assertIsNotNone(ctx.return_value)
        self.assertEqual(ctx.return_value.value, "result")
        self.assertIsNone(ctx.throwable)

    def test_capture_return_none_value_no_return_field(self):
        config = CaptureConfig(capture_return=True)
        ctx = self.wrapper._capture_return_context(None, None, config, None)
        self.assertIsNone(ctx.return_value)

    def test_capture_thrown_exception_populates_throwable(self):
        config = CaptureConfig(capture_return=False)
        try:
            raise ValueError("kaboom")
        except ValueError as exc:
            ctx = self.wrapper._capture_return_context(None, exc, config, None)
        self.assertIsNotNone(ctx.throwable)
        self.assertEqual(ctx.throwable.type, "ValueError")
        self.assertEqual(ctx.throwable.message, "kaboom")

    def test_capture_thrown_with_caller_stack(self):
        config = CaptureConfig(capture_return=False)
        import traceback as tb_mod

        caller_stack = tb_mod.extract_stack()
        try:
            raise RuntimeError("with stack")
        except RuntimeError as exc:
            ctx = self.wrapper._capture_return_context(None, exc, config, caller_stack)
        self.assertEqual(ctx.throwable.type, "RuntimeError")

    def test_return_capture_respects_capture_config_max_string_length(self):
        long_value = "y" * 200
        config = CaptureConfig(capture_return=True, max_string_length=10)
        ctx = self.wrapper._capture_return_context(long_value, None, config, None)
        self.assertEqual(ctx.return_value.value, "y" * 10)
        self.assertTrue(ctx.return_value.truncated)
        self.assertEqual(ctx.return_value.size, 200)

    def test_return_capture_respects_capture_config_max_collection_width(self):
        big_list = list(range(50))
        config = CaptureConfig(capture_return=True, max_collection_width=4)
        ctx = self.wrapper._capture_return_context(big_list, None, config, None)
        self.assertEqual(len(ctx.return_value.elements), 4)
        self.assertTrue(ctx.return_value.truncated)
        self.assertEqual(ctx.return_value.size, 50)


class TestBuildSnapshot(unittest.TestCase):
    def setUp(self):
        self.wrapper = FunctionWrapper()

    def _build(self, qualified_name="process_order", **overrides):
        def original_func():
            return None

        kwargs = dict(
            module_name="myapp.services",
            qualified_name=qualified_name,
            original_func=original_func,
            location_hash="loc-1",
            duration_ns=2_000_000,
            entry_context=None,
            return_context=None,
            capture_config=CaptureConfig(),
            instrumentation_type="PROBE",
        )
        kwargs.update(overrides)
        return self.wrapper._build_snapshot(**kwargs)

    def test_basic_snapshot_fields(self):
        snapshot = self._build()
        self.assertIsInstance(snapshot, Snapshot)
        self.assertEqual(snapshot.location_hash, "loc-1")
        self.assertEqual(snapshot.instrumentation_type, "PROBE")
        # 2_000_000 ns => 2.0 ms
        self.assertEqual(snapshot.duration, 2.0)
        self.assertEqual(snapshot.instrumentation.location.method_name, "process_order")
        self.assertEqual(snapshot.instrumentation.location.code_unit, "myapp.services")
        # No class component => class_name_fq == module_name
        self.assertEqual(snapshot.instrumentation.location.class_name, "myapp.services")
        self.assertEqual(snapshot.instrumentation.location.line_number, 0)
        self.assertEqual(snapshot.thread.id, threading.get_ident())

    def test_qualified_name_with_class_sets_class_name(self):
        snapshot = self._build(qualified_name="MyClass.do_work")
        self.assertEqual(snapshot.instrumentation.location.method_name, "do_work")
        self.assertEqual(snapshot.instrumentation.location.class_name, "myapp.services.MyClass")

    def test_duration_none_when_zero(self):
        snapshot = self._build(duration_ns=0)
        self.assertIsNone(snapshot.duration)

    def test_service_and_environment_from_env(self):
        env = {
            "OTEL_SERVICE_NAME": "",
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=cart, deployment.environment.name=prod",
        }
        with mock.patch.dict("os.environ", env, clear=False):
            snapshot = self._build()
        self.assertEqual(snapshot.service, "cart")
        self.assertEqual(snapshot.environment, "prod")

    def test_environment_legacy_key_fallback(self):
        env = {
            "OTEL_SERVICE_NAME": "svc",
            "OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=staging",
        }
        with mock.patch.dict("os.environ", env, clear=False):
            snapshot = self._build()
        self.assertEqual(snapshot.service, "svc")
        self.assertEqual(snapshot.environment, "staging")

    def test_stack_trace_captured_when_enabled(self):
        snapshot = self._build(capture_config=CaptureConfig(capture_stack_trace=True))
        self.assertIsNotNone(snapshot.stack)


class TestEmitSnapshot(unittest.TestCase):
    def setUp(self):
        self._prev = get_snapshot_emitter()
        self.addCleanup(lambda: set_snapshot_emitter(self._prev))

    def _snapshot(self):
        return Snapshot(timestamp=1)

    def test_emit_calls_emitter(self):
        emitter = mock.MagicMock()
        set_snapshot_emitter(emitter)
        snap = self._snapshot()
        FunctionWrapper._emit_snapshot(snap)
        emitter.emit_snapshot.assert_called_once_with(snap)

    def test_emit_noop_when_emitter_none(self):
        set_snapshot_emitter(None)
        # Should not raise.
        FunctionWrapper._emit_snapshot(self._snapshot())

    def test_emit_swallows_emitter_exception(self):
        emitter = mock.MagicMock()
        emitter.emit_snapshot.side_effect = RuntimeError("send failed")
        set_snapshot_emitter(emitter)
        # Should not raise.
        FunctionWrapper._emit_snapshot(self._snapshot())


# ===========================================================================
# Sync wrapper bodies (end-to-end via instrument_function)
# ===========================================================================
class TestSyncWrapper(_SnapshotEmitterFixture):
    def setUp(self):
        super().setUp()
        self.module_name = "_test_fw_sync_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))
        self.wrapper = FunctionWrapper()

    def _instrument(self, func, *, increment_result=True, has_line0=True, disabled=False, capture_config=None):
        func_name = func.__name__
        module = _register_module(self.module_name, **{func_name: func})
        # The manager registers breakpoint sets under config.function_key == module.function_name
        # (the CONFIGURED target name), and the wrapper now keys its lookup the same way,
        # so register under module.func_name — not the runtime __qualname__.
        func_key = f"{self.module_name}.{func_name}"
        bp_set = _make_function_bp_set(func_key, self.module_name, func_name, has_line0=has_line0, disabled=disabled)
        manager = _FakeManager({func_key: bp_set}, increment_result=increment_result)
        capture_config = capture_config if capture_config is not None else CaptureConfig(capture_return=True)
        original, instrumented = self.wrapper.instrument_function(
            self.module_name, func_name, capture_config=capture_config, location_hash="loc-hash-1", manager=manager
        )
        return module, func_key, manager, original, instrumented

    def test_wrapped_returns_original_value_and_emits_snapshot(self):
        def add(left, right):
            return left + right

        module, func_key, manager, original, _ = self._instrument(add)
        result = module.add(3, 4)
        self.assertEqual(result, 7)
        self.assertIs(original, add)
        self.emitter.emit_snapshot.assert_called_once()
        self.assertEqual(manager.increment_calls, [f"{func_key}:0"])

    def test_partial_target_fires_via_configured_name_key(self):
        """Regression: a functools.partial has no __qualname__/__name__, so the
        wrapper's old runtime-name key ("<module>.<anonymous>") missed the breakpoint set
        the manager registered under "<module>.<function_name>", and the partial silently
        never fired. The wrapper must key off the CONFIGURED function_name instead."""
        import functools

        def _base(prefix, value):
            return prefix + str(value)

        # Module-level name bound to a partial — the case that regressed.
        add_hello = functools.partial(_base, "hello:")
        # Sanity: a partial really has neither __qualname__ nor __name__.
        self.assertFalse(hasattr(add_hello, "__qualname__"))
        self.assertFalse(hasattr(add_hello, "__name__"))

        func_name = "add_hello"
        module = _register_module(self.module_name, **{func_name: add_hello})
        # Manager registers under the CONFIGURED key (module.function_name), as production does.
        func_key = f"{self.module_name}.{func_name}"
        bp_set = _make_function_bp_set(func_key, self.module_name, func_name)
        manager = _FakeManager({func_key: bp_set})

        original, _instrumented = self.wrapper.instrument_function(
            self.module_name,
            func_name,
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )

        result = module.add_hello(9)
        self.assertEqual(result, "hello:9")  # behavior preserved
        self.emitter.emit_snapshot.assert_called_once()  # THE FIX: it fires (was 0 before)
        self.assertEqual(manager.increment_calls, [f"{func_key}:0"])

    def test_wrapped_reraises_user_exception_and_still_emits(self):
        def boom():
            raise ValueError("user error")

        module, _, _, _, _ = self._instrument(boom)
        with self.assertRaises(ValueError) as caught:
            module.boom()
        self.assertEqual(str(caught.exception), "user error")
        # SAFETY: snapshot still attempted in the finally block.
        self.emitter.emit_snapshot.assert_called_once()

    def test_rate_limited_skips_capture_but_calls_original(self):
        calls = []

        def tracked():
            calls.append(1)
            return "done"

        module, _, manager, _, _ = self._instrument(tracked, increment_result=False)
        result = module.tracked()
        self.assertEqual(result, "done")
        self.assertEqual(calls, [1])
        # increment_hit_count returned False => no snapshot emitted.
        self.emitter.emit_snapshot.assert_not_called()

    def test_all_breakpoints_disabled_calls_original_without_manager_path(self):
        # When the only state is disabled and there is no permanent line-0 breakpoint,
        # the wrapper returns the original result immediately (no hit-count increment).
        def func():
            return "value"

        func_name = func.__name__
        module = _register_module(self.module_name, **{func_name: func})
        func_key = f"{self.module_name}.{func_name}"
        # Build a set with a disabled state but NO line-0 breakpoint registered.
        bp_set = FunctionBreakpointSet(function_key=func_key, module=self.module_name, function_name=func_name)
        bp_set.states[f"{func_key}:5"] = BreakpointState(breakpoint_key=f"{func_key}:5", is_disabled=True)
        manager = _FakeManager({func_key: bp_set})
        _, instrumented = self.wrapper.instrument_function(
            self.module_name, func_name, capture_config=CaptureConfig(), location_hash="h", manager=manager
        )
        self.assertEqual(module.func(), "value")
        self.assertEqual(manager.increment_calls, [])
        self.emitter.emit_snapshot.assert_not_called()

    def test_no_function_level_bp_calls_original_no_snapshot(self):
        # has_line0=False => no function-level breakpoint => original called, no snapshot.
        def func():
            return 99

        module, _, manager, _, _ = self._instrument(func, has_line0=False)
        self.assertEqual(module.func(), 99)
        self.assertEqual(manager.increment_calls, [])
        self.emitter.emit_snapshot.assert_not_called()

    def test_wrapper_without_manager_calls_original_no_snapshot(self):
        def func():
            return 5

        func_name = func.__name__
        module = _register_module(self.module_name, **{func_name: func})
        _, instrumented = self.wrapper.instrument_function(
            self.module_name, func_name, capture_config=CaptureConfig(), location_hash="h", manager=None
        )
        # No manager => no function-level breakpoint detection => original behavior, no snapshot.
        self.assertEqual(module.func(), 5)
        self.emitter.emit_snapshot.assert_not_called()

    def test_capture_arguments_none_skips_entry_capture(self):
        def add(a, b):
            return a + b

        # capture_arguments defaults to None => entry context not captured, but snapshot still emitted.
        module, _, _, _, _ = self._instrument(add, capture_config=CaptureConfig(capture_return=True))
        self.assertEqual(module.add(2, 5), 7)
        self.emitter.emit_snapshot.assert_called_once()
        emitted = self.emitter.emit_snapshot.call_args[0][0]
        self.assertIsNone(emitted.captures.entry)

    def test_wrapper_preserves_function_name(self):
        def well_named():
            return None

        _, _, _, _, instrumented = self._instrument(well_named)
        self.assertEqual(instrumented.__name__, "well_named")

    def test_exception_captures_return_context_throwable(self):
        # capture_return=True + thrown exception exercises the sync return-context branch.
        def boom():
            raise RuntimeError("sync detail")

        module, _, _, _, _ = self._instrument(boom, capture_config=CaptureConfig(capture_return=True))
        with self.assertRaises(RuntimeError):
            module.boom()
        emitted = self.emitter.emit_snapshot.call_args[0][0]
        self.assertIsNotNone(emitted.captures.return_context)
        self.assertEqual(emitted.captures.return_context.throwable.type, "RuntimeError")

    def test_entry_capture_failure_does_not_block_snapshot(self):
        def add(a, b):
            return a + b

        # Force entry-context capture to raise; the wrapper logs and still emits a snapshot.
        with mock.patch.object(FunctionWrapper, "_capture_entry_context", side_effect=RuntimeError("capture boom")):
            module, _, _, _, _ = self._instrument(add, capture_config=CaptureConfig(capture_arguments=[]))
            self.assertEqual(module.add(1, 2), 3)
        self.emitter.emit_snapshot.assert_called_once()


# ===========================================================================
# Async wrapper bodies (end-to-end via instrument_function)
# ===========================================================================
class TestAsyncWrapper(_SnapshotEmitterFixture):
    def setUp(self):
        super().setUp()
        self.module_name = "_test_fw_async_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))
        self.wrapper = FunctionWrapper()

    def _instrument(self, func, *, increment_result=True, has_line0=True, disabled=False, capture_config=None):
        func_name = func.__name__
        module = _register_module(self.module_name, **{func_name: func})
        # The wrapper keys breakpoint sets by the function's __qualname__.
        func_key = f"{self.module_name}.{func_name}"
        bp_set = _make_function_bp_set(func_key, self.module_name, func_name, has_line0=has_line0, disabled=disabled)
        manager = _FakeManager({func_key: bp_set}, increment_result=increment_result)
        capture_config = capture_config if capture_config is not None else CaptureConfig(capture_return=True)
        original, instrumented = self.wrapper.instrument_function(
            self.module_name, func_name, capture_config=capture_config, location_hash="loc-hash-1", manager=manager
        )
        return module, func_key, manager, original, instrumented

    def test_async_wrapped_returns_value_and_emits(self):
        async def fetch(value):
            return value * 2

        module, func_key, manager, _, _ = self._instrument(fetch)
        result = asyncio.run(module.fetch(21))
        self.assertEqual(result, 42)
        self.emitter.emit_snapshot.assert_called_once()
        self.assertEqual(manager.increment_calls, [f"{func_key}:0"])

    def test_async_wrapped_reraises_and_still_emits(self):
        async def boom():
            raise KeyError("async boom")

        module, _, _, _, _ = self._instrument(boom)
        with self.assertRaises(KeyError):
            asyncio.run(module.boom())
        self.emitter.emit_snapshot.assert_called_once()

    def test_async_rate_limited_skips_capture(self):
        calls = []

        async def tracked():
            calls.append(1)
            return "ok"

        module, _, _, _, _ = self._instrument(tracked, increment_result=False)
        self.assertEqual(asyncio.run(module.tracked()), "ok")
        self.assertEqual(calls, [1])
        self.emitter.emit_snapshot.assert_not_called()

    def test_async_all_disabled_calls_original(self):
        async def func():
            return "done"

        func_name = func.__name__
        module = _register_module(self.module_name, **{func_name: func})
        func_key = f"{self.module_name}.{func_name}"
        bp_set = FunctionBreakpointSet(function_key=func_key, module=self.module_name, function_name=func_name)
        bp_set.states[f"{func_key}:5"] = BreakpointState(breakpoint_key=f"{func_key}:5", is_disabled=True)
        manager = _FakeManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name, func_name, capture_config=CaptureConfig(), location_hash="h", manager=manager
        )
        self.assertEqual(asyncio.run(module.func()), "done")
        self.emitter.emit_snapshot.assert_not_called()

    def test_async_wrapper_is_coroutine_function(self):
        async def func():
            return None

        _, _, _, _, instrumented = self._instrument(func)
        self.assertTrue(asyncio.iscoroutinefunction(instrumented))

    def test_async_exception_captures_return_context_with_stack(self):
        # capture_return=True + a thrown exception drives the return-context capture branch.
        async def boom():
            raise ValueError("async detail")

        module, _, _, _, _ = self._instrument(boom, capture_config=CaptureConfig(capture_return=True))
        with self.assertRaises(ValueError):
            asyncio.run(module.boom())
        emitted = self.emitter.emit_snapshot.call_args[0][0]
        self.assertIsNotNone(emitted.captures.return_context)
        self.assertEqual(emitted.captures.return_context.throwable.type, "ValueError")


# ===========================================================================
# instrument_function error handling + restore_function
# ===========================================================================
class TestInstrumentFunctionErrors(_SnapshotEmitterFixture):
    def setUp(self):
        super().setUp()
        self.wrapper = FunctionWrapper()

    def test_import_error_propagates(self):
        with self.assertRaises(ImportError):
            self.wrapper.instrument_function("_no_such_module_abc", "f")

    def test_attribute_error_propagates(self):
        name = "_test_fw_err_module"
        sys.modules.pop(name, None)
        self.addCleanup(lambda: sys.modules.pop(name, None))
        _register_module(name)
        with self.assertRaises(AttributeError):
            self.wrapper.instrument_function(name, "missing_func")

    def test_unexpected_error_wrapped_in_runtime_error(self):
        name = "_test_fw_err_module2"
        sys.modules.pop(name, None)
        self.addCleanup(lambda: sys.modules.pop(name, None))

        def target():
            return None

        _register_module(name, target=target)
        # Force _create_wrapper to raise an unexpected (non-Import/Attribute) error.
        with mock.patch.object(self.wrapper, "_create_wrapper", side_effect=ValueError("boom")):
            with self.assertRaises(RuntimeError):
                self.wrapper.instrument_function(name, "target")


class TestRestoreFunction(unittest.TestCase):
    def setUp(self):
        self.module_name = "_test_fw_restore_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_restore_module_function(self):
        def original():
            return "orig"

        def wrapped():
            return "wrapped"

        module = _register_module(self.module_name, original=original)
        module.original = wrapped  # simulate instrumentation
        result = FunctionWrapper.restore_function(self.module_name, "original", original)
        self.assertTrue(result)
        self.assertIs(module.original, original)

    def test_restore_class_method(self):
        class Holder:
            def method(self):
                return "wrapped"

        def original_method(self):
            return "orig"

        _register_module(self.module_name, Holder=Holder)
        result = FunctionWrapper.restore_function(self.module_name, "Holder.method", original_method)
        self.assertTrue(result)
        self.assertIs(sys.modules[self.module_name].Holder.method, original_method)

    def test_restore_returns_false_on_missing_module(self):
        result = FunctionWrapper.restore_function("_no_such_module_for_restore", "f", lambda: None)
        self.assertFalse(result)

    def test_instrument_then_restore_roundtrip(self):
        prev = get_snapshot_emitter()
        set_snapshot_emitter(mock.MagicMock())
        self.addCleanup(lambda: set_snapshot_emitter(prev))

        def compute():
            return 1

        module = _register_module(self.module_name, compute=compute)
        wrapper = FunctionWrapper()
        original, instrumented = wrapper.instrument_function(
            self.module_name, "compute", capture_config=CaptureConfig(), location_hash="h", manager=None
        )
        self.assertIs(module.compute, instrumented)
        self.assertTrue(FunctionWrapper.restore_function(self.module_name, "compute", original))
        self.assertIs(module.compute, compute)


class TestClassMethodInstrumentation(_SnapshotEmitterFixture):
    """End-to-end class-method instrumentation (MethodInfo path + class replace/restore)."""

    def setUp(self):
        super().setUp()
        self.module_name = "_test_fw_classmethod_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))
        self.wrapper = FunctionWrapper()

    def test_instrument_class_method_emits_and_restores(self):
        # _Service is module-level so its method qualname is exactly 'Service.handle'.
        # Use a dedicated module attribute name matching that qualname's class component.
        Service = type("Service", (_Service,), {"handle": _Service.handle})
        _register_module(self.module_name, Service=Service)
        # The wrapper keys by the bound method's __qualname__.
        func_key = f"{self.module_name}.Service.handle"
        bp_set = _make_function_bp_set(func_key, self.module_name, "Service.handle")
        manager = _FakeManager({func_key: bp_set})

        original, instrumented = self.wrapper.instrument_function(
            self.module_name,
            "Service.handle",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        self.assertIs(Service.handle, instrumented)

        result = Service().handle(10)
        self.assertEqual(result, 11)
        self.emitter.emit_snapshot.assert_called_once()
        self.assertEqual(manager.increment_calls, [f"{func_key}:0"])

        # Restore puts the original method back on the class.
        self.assertTrue(FunctionWrapper.restore_function(self.module_name, "Service.handle", original))
        self.assertIs(Service.handle, original)

    def test_replace_class_method_missing_method_raises(self):
        class Empty:
            pass

        _register_module(self.module_name, Empty=Empty)
        with self.assertRaises(AttributeError):
            FunctionWrapper._replace_function_in_module(self.module_name, "Empty.absent", lambda self: None)

    def test_instrument_inherited_method_raises_attribute_error(self):
        class Base:
            def handle(self, x):
                return x + 1

        class Child(Base):
            pass

        _register_module(self.module_name, Base=Base, Child=Child)

        with self.assertRaises(AttributeError) as caught:
            self.wrapper.instrument_function(
                self.module_name,
                "Child.handle",
                capture_config=CaptureConfig(),
                location_hash="loc-hash-inherit",
                manager=None,
            )

        msg = str(caught.exception)
        self.assertIn("not found", msg)
        self.assertIn("inherited from", msg)
        self.assertIn("Child", msg)
        self.assertIn("Base", msg)
        self.assertNotIn("handle", Child.__dict__)

    def test_instrument_inherited_classmethod_raises_attribute_error(self):
        class Base:
            @classmethod
            def make(cls, value):
                return f"{cls.__name__}-{value}"

        class Child(Base):
            pass

        _register_module(self.module_name, Base=Base, Child=Child)

        with self.assertRaises(AttributeError) as caught:
            self.wrapper.instrument_function(
                self.module_name,
                "Child.make",
                capture_config=CaptureConfig(),
                location_hash="loc-hash-inherit-cm",
                manager=None,
            )

        self.assertIn("not found", str(caught.exception))
        self.assertIn("inherited from", str(caught.exception))
        self.assertNotIn("make", Child.__dict__)

    def test_instrument_diamond_inheritance_inherited_leaf_raises_attribute_error(self):
        class A:
            def method(self, value):
                return value * 10

        class B(A):
            pass

        class C(A):
            pass

        class D(B, C):
            pass

        _register_module(self.module_name, A=A, B=B, C=C, D=D)

        with self.assertRaises(AttributeError) as caught:
            self.wrapper.instrument_function(
                self.module_name,
                "D.method",
                capture_config=CaptureConfig(),
                location_hash="loc-hash-diamond",
                manager=None,
            )

        self.assertIn("not found", str(caught.exception))
        self.assertIn("inherited from", str(caught.exception))
        self.assertNotIn("method", D.__dict__)


if __name__ == "__main__":
    unittest.main()
