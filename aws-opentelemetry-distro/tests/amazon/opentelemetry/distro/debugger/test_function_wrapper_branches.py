# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Branch-coverage tests for _function_wrapper.py error/edge paths.

These drive the defensive ``except`` branches inside the sync/async wrappers
and the discovery/replace/capture helpers that the happy-path tests in
test_function_wrapper.py do not reach (lock-read failures, increment failures,
extract_stack failures, snapshot-build failures in the finally block, the
functools.update_wrapper fallback, and the Flask-patch error branches).

No real network, threads, fork, or sleep are used. The global snapshot emitter
is mocked and in-process modules are registered/cleaned up per test.
"""

import asyncio
import functools
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
    get_snapshot_emitter,
    set_snapshot_emitter,
)


def _register_module(name, **attrs):
    """Create and register a real module object so discovery/replace works."""
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


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


class _RaisingLockManager:
    """Manager whose ``_lock`` context-manager entry raises (drives the lock-read except)."""

    def __init__(self):
        self._lock = mock.MagicMock()
        self._lock.__enter__.side_effect = RuntimeError("lock boom")
        self._active_functions = {}
        self.increment_calls = []

    def increment_hit_count(self, breakpoint_key):  # pragma: no cover - not reached in these tests
        self.increment_calls.append(breakpoint_key)
        return True


class _IncrementRaisesManager:
    """Manager that resolves a line-0 breakpoint but raises in increment_hit_count."""

    def __init__(self, bp_sets):
        self._lock = threading.Lock()
        self._active_functions = bp_sets

    def increment_hit_count(self, breakpoint_key):
        raise RuntimeError("increment boom")


class _FakeIncrementOkManager:
    """Manager that resolves a line-0 breakpoint and allows capture (increment returns True)."""

    def __init__(self, bp_sets):
        self._lock = threading.Lock()
        self._active_functions = bp_sets
        self.increment_calls = []

    def increment_hit_count(self, breakpoint_key):
        self.increment_calls.append(breakpoint_key)
        return True


class _SnapshotEmitterFixture(unittest.TestCase):
    """Base class that mocks the global snapshot emitter for every test."""

    def setUp(self):
        self._prev_emitter = get_snapshot_emitter()
        self.emitter = mock.MagicMock()
        set_snapshot_emitter(self.emitter)
        self.addCleanup(lambda: set_snapshot_emitter(self._prev_emitter))
        self.wrapper = FunctionWrapper()


class TestDiscoverClassMethodNonCallable(unittest.TestCase):
    """Covers _discover_class_method's 'not callable' branch (line 314)."""

    def setUp(self):
        self.module_name = "_test_fwb_noncallable_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_non_callable_class_attribute_raises(self):
        class Holder:
            attr = 123  # a non-callable class attribute

        _register_module(self.module_name, Holder=Holder)
        with self.assertRaises(AttributeError):
            FunctionWrapper._discover_function(self.module_name, "Holder.attr")


class TestReplaceFunctionInModuleErrors(unittest.TestCase):
    """Covers _replace_function_in_module missing-function (724) and ImportError (740-741)."""

    def setUp(self):
        self.module_name = "_test_fwb_replace_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_missing_module_level_function_raises_attribute_error(self):
        _register_module(self.module_name)  # empty module, no 'ghost' attribute
        with self.assertRaises(AttributeError):
            FunctionWrapper._replace_function_in_module(self.module_name, "ghost", lambda: None)

    def test_import_error_propagates(self):
        # Resolving a non-existent module raises ImportError from the replace path.
        with self.assertRaises(ImportError):
            FunctionWrapper._replace_function_in_module("_no_such_module_for_replace_xyz", "f", lambda: None)


class TestSyncWrapperErrorBranches(_SnapshotEmitterFixture):
    """Covers the sync wrapper defensive except branches (456-457, 469-470, 497-498, 523-524)."""

    def setUp(self):
        super().setUp()
        self.module_name = "_test_fwb_sync_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_lock_read_failure_is_swallowed(self):
        # Manager whose _lock.__enter__ raises -> the wrapper swallows it (lines 456-457),
        # leaving no function-level breakpoint, so original runs and no snapshot is emitted.
        def add(left, right):
            return left + right

        module = _register_module(self.module_name, add=add)
        manager = _RaisingLockManager()
        self.wrapper.instrument_function(
            self.module_name,
            "add",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="h",
            manager=manager,
        )
        self.assertEqual(module.add(2, 3), 5)
        self.emitter.emit_snapshot.assert_not_called()

    def test_increment_failure_is_swallowed_and_snapshot_emitted(self):
        # increment_hit_count raises -> swallowed (469-470); capture_allowed stays True,
        # so a snapshot is still emitted (SAFETY: instrumentation errors never crash the app).
        def compute():
            return 7

        func_key = f"{self.module_name}.{compute.__name__}"
        module = _register_module(self.module_name, compute=compute)
        bp_set = _make_function_bp_set(func_key, self.module_name, "compute")
        manager = _IncrementRaisesManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name,
            "compute",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        self.assertEqual(module.compute(), 7)
        self.emitter.emit_snapshot.assert_called_once()

    def test_extract_stack_failure_is_swallowed_on_exception(self):
        # When the user function raises AND traceback.extract_stack itself raises,
        # the inner except (497-498) is hit; the original exception still propagates.
        def boom():
            raise ValueError("user error")

        func_key = f"{self.module_name}.{boom.__name__}"
        module = _register_module(self.module_name, boom=boom)
        bp_set = _make_function_bp_set(func_key, self.module_name, "boom")
        manager = _FakeIncrementOkManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name,
            "boom",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        with mock.patch("traceback.extract_stack", side_effect=RuntimeError("no stack")):
            with self.assertRaises(ValueError):
                module.boom()

    def test_snapshot_build_failure_in_finally_is_swallowed(self):
        # _build_snapshot raises inside the finally block -> caught (523-524); the
        # original return value is still produced.
        def compute():
            return 99

        func_key = f"{self.module_name}.{compute.__name__}"
        module = _register_module(self.module_name, compute=compute)
        bp_set = _make_function_bp_set(func_key, self.module_name, "compute")
        manager = _FakeIncrementOkManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name,
            "compute",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        with mock.patch.object(FunctionWrapper, "_build_snapshot", side_effect=RuntimeError("build boom")):
            self.assertEqual(module.compute(), 99)
        self.emitter.emit_snapshot.assert_not_called()


class TestWrapperUpdateWrapperFallback(_SnapshotEmitterFixture):
    """Covers the functools.update_wrapper fallback in both wrappers (531-533, 645-647)."""

    def test_sync_update_wrapper_failure_sets_name_fallback(self):
        def original():
            return None

        with mock.patch.object(functools, "update_wrapper", side_effect=RuntimeError("wraps boom")):
            wrapped = self.wrapper._create_sync_wrapper(original, CaptureConfig(), "mod", "h", None)
        # Fallback assigned __name__ and __wrapped__ instead of raising.
        self.assertEqual(wrapped.__name__, "original")
        self.assertIs(wrapped.__wrapped__, original)

    def test_async_update_wrapper_failure_sets_name_fallback(self):
        async def original():
            return None

        with mock.patch.object(functools, "update_wrapper", side_effect=RuntimeError("wraps boom")):
            wrapped = self.wrapper._create_async_wrapper(original, CaptureConfig(), "mod", "h", None)
        self.assertEqual(wrapped.__name__, "original")
        self.assertIs(wrapped.__wrapped__, original)


class TestAsyncWrapperErrorBranches(_SnapshotEmitterFixture):
    """Covers the async wrapper defensive except branches (575-576, 586-587, 597-600, 612-613, 637-638)."""

    def setUp(self):
        super().setUp()
        self.module_name = "_test_fwb_async_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_lock_read_failure_is_swallowed(self):
        async def fetch(value):
            return value * 2

        module = _register_module(self.module_name, fetch=fetch)
        manager = _RaisingLockManager()
        self.wrapper.instrument_function(
            self.module_name,
            "fetch",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="h",
            manager=manager,
        )
        self.assertEqual(asyncio.run(module.fetch(5)), 10)
        self.emitter.emit_snapshot.assert_not_called()

    def test_increment_failure_is_swallowed_and_snapshot_emitted(self):
        async def fetch():
            return "ok"

        func_key = f"{self.module_name}.{fetch.__name__}"
        module = _register_module(self.module_name, fetch=fetch)
        bp_set = _make_function_bp_set(func_key, self.module_name, "fetch")
        manager = _IncrementRaisesManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name,
            "fetch",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        self.assertEqual(asyncio.run(module.fetch()), "ok")
        self.emitter.emit_snapshot.assert_called_once()

    def test_entry_capture_failure_does_not_block_snapshot(self):
        async def fetch(a, b):
            return a + b

        func_key = f"{self.module_name}.{fetch.__name__}"
        module = _register_module(self.module_name, fetch=fetch)
        bp_set = _make_function_bp_set(func_key, self.module_name, "fetch")
        manager = _FakeIncrementOkManager({func_key: bp_set})
        # capture_arguments=[] => the wrapper attempts entry capture; force it to raise.
        with mock.patch.object(FunctionWrapper, "_capture_entry_context", side_effect=RuntimeError("capture boom")):
            self.wrapper.instrument_function(
                self.module_name,
                "fetch",
                capture_config=CaptureConfig(capture_arguments=[]),
                location_hash="loc-hash-1",
                manager=manager,
            )
            self.assertEqual(asyncio.run(module.fetch(1, 2)), 3)
        self.emitter.emit_snapshot.assert_called_once()

    def test_extract_stack_failure_is_swallowed_on_exception(self):
        async def boom():
            raise KeyError("async error")

        func_key = f"{self.module_name}.{boom.__name__}"
        module = _register_module(self.module_name, boom=boom)
        bp_set = _make_function_bp_set(func_key, self.module_name, "boom")
        manager = _FakeIncrementOkManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name,
            "boom",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        with mock.patch("traceback.extract_stack", side_effect=RuntimeError("no stack")):
            with self.assertRaises(KeyError):
                asyncio.run(module.boom())

    def test_snapshot_build_failure_in_finally_is_swallowed(self):
        async def fetch():
            return 42

        func_key = f"{self.module_name}.{fetch.__name__}"
        module = _register_module(self.module_name, fetch=fetch)
        bp_set = _make_function_bp_set(func_key, self.module_name, "fetch")
        manager = _FakeIncrementOkManager({func_key: bp_set})
        self.wrapper.instrument_function(
            self.module_name,
            "fetch",
            capture_config=CaptureConfig(capture_return=True),
            location_hash="loc-hash-1",
            manager=manager,
        )
        with mock.patch.object(FunctionWrapper, "_build_snapshot", side_effect=RuntimeError("build boom")):
            self.assertEqual(asyncio.run(module.fetch()), 42)
        self.emitter.emit_snapshot.assert_not_called()


class TestCaptureReturnContextError(unittest.TestCase):
    """Covers _capture_return_context's broad except branch (lines 924-926)."""

    def setUp(self):
        self.wrapper = FunctionWrapper()

    def test_serialize_failure_returns_none(self):
        config = CaptureConfig(capture_return=True)
        with mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.SnapshotSerializer.serialize",
            side_effect=RuntimeError("serialize boom"),
        ):
            result = self.wrapper._capture_return_context("a-value", None, config, None)
        self.assertIsNone(result)


class TestPatchFlaskViewFunctionsErrors(unittest.TestCase):
    """Covers Flask-patch error branches (800-805) and the no-view_functions branch (812-813).

    Flask is not required: a fake Flask class is injected via sys.modules so the
    isinstance() check matches without the real dependency.
    """

    def setUp(self):
        self.module_name = "_test_fwb_flask_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    def test_no_view_functions_dict_logs_and_returns(self):
        # _patch_single_flask_app with an app whose view_functions is missing/None (812-813).
        fake_app = types.SimpleNamespace(view_functions=None)

        def original():
            return None

        def replacement():
            return None

        # Should not raise.
        FunctionWrapper._patch_single_flask_app(fake_app, "app", original, replacement, "original")

    def test_outer_exception_is_swallowed(self):
        # dir(module) raising drives the outer except (804-805).
        class WeirdModule:
            def __dir__(self):
                raise RuntimeError("dir boom")

        fake_flask = types.ModuleType("flask")

        class _Flask:
            pass

        fake_flask.Flask = _Flask
        with mock.patch.dict(sys.modules, {"flask": fake_flask}):
            # Must not raise even though dir() blows up.
            FunctionWrapper._patch_flask_view_functions(WeirdModule(), lambda: None, lambda: None)

    def test_per_attribute_exception_is_swallowed(self):
        # getattr on one attribute raises -> per-attr except (800-802); patching continues.
        fake_flask = types.ModuleType("flask")

        class _Flask:
            pass

        fake_flask.Flask = _Flask

        class ModuleWithBadAttr:
            good = 1

            def __dir__(self):
                return ["explosive", "good"]

            def __getattribute__(self, name):
                if name == "explosive":
                    raise RuntimeError("attr boom")
                return object.__getattribute__(self, name)

        with mock.patch.dict(sys.modules, {"flask": fake_flask}):
            # Must not raise; the explosive attribute is skipped.
            FunctionWrapper._patch_flask_view_functions(ModuleWithBadAttr(), lambda: None, lambda: None)


class TestPatchDjangoUrlPatternsErrors(unittest.TestCase):
    """Covers Django-patch error branches in _patch_django_url_patterns,
    _patch_single_resolver, and _maybe_patch_pattern.

    Django is not required: a fake django.urls module is injected via
    sys.modules with stub URLPattern / URLResolver classes the isinstance()
    checks match against, plus a stub get_resolver.
    """

    def setUp(self):
        self.module_name = "_test_fwb_django_module"
        sys.modules.pop(self.module_name, None)
        self.addCleanup(lambda: sys.modules.pop(self.module_name, None))

    @staticmethod
    def _fake_django_urls(get_resolver=None, url_pattern_cls=None, url_resolver_cls=None):
        """Build a fake django.urls module with stub classes / get_resolver."""

        class _StubURLPattern:
            pass

        class _StubURLResolver:
            pass

        fake = types.ModuleType("django.urls")
        fake.URLPattern = url_pattern_cls or _StubURLPattern
        fake.URLResolver = url_resolver_cls or _StubURLResolver
        fake.get_resolver = get_resolver or (lambda _conf=None: None)
        return fake

    def test_no_django_installed_returns(self):
        # ImportError on `from django.urls import ...` -> early return; no raise.
        with mock.patch.dict(sys.modules, {"django.urls": None}):
            FunctionWrapper._patch_django_url_patterns(types.ModuleType(self.module_name), lambda: None, lambda: None)

    def test_get_resolver_failure_swallowed(self):
        # get_resolver(None) raises (e.g. ImproperlyConfigured) -> log debug,
        # continue with the module-scan fallback. No raise.
        def boom(_conf=None):
            raise RuntimeError("ImproperlyConfigured")

        fake = self._fake_django_urls(get_resolver=boom)
        with mock.patch.dict(sys.modules, {"django.urls": fake}):
            FunctionWrapper._patch_django_url_patterns(types.ModuleType(self.module_name), lambda: None, lambda: None)

    def test_module_dir_failure_swallowed(self):
        # dir(module) raising drives the outer except. No raise.
        class WeirdModule:
            def __dir__(self):
                raise RuntimeError("dir boom")

        fake = self._fake_django_urls()
        with mock.patch.dict(sys.modules, {"django.urls": fake}):
            FunctionWrapper._patch_django_url_patterns(WeirdModule(), lambda: None, lambda: None)

    def test_per_attribute_getattr_failure_swallowed(self):
        # getattr on one attribute raises -> per-attr except continues the loop.
        class ModuleWithBadAttr:
            good = 1

            def __dir__(self):
                return ["explosive", "good"]

            def __getattribute__(self, name):
                if name == "explosive":
                    raise RuntimeError("attr boom")
                return object.__getattribute__(self, name)

        fake = self._fake_django_urls()
        with mock.patch.dict(sys.modules, {"django.urls": fake}):
            FunctionWrapper._patch_django_url_patterns(ModuleWithBadAttr(), lambda: None, lambda: None)

    def test_resolver_url_patterns_raises_swallowed(self):
        # _patch_single_resolver: url_patterns @cached_property raises ->
        # log debug + return. No raise.
        class _URLResolverStub:
            @property
            def url_patterns(self):
                raise RuntimeError("ImproperlyConfigured")

        fake = self._fake_django_urls(url_resolver_cls=_URLResolverStub)
        broken_resolver = _URLResolverStub()
        mod = types.ModuleType(self.module_name)
        setattr(mod, "broken", broken_resolver)
        with mock.patch.dict(sys.modules, {"django.urls": fake}):
            FunctionWrapper._patch_django_url_patterns(mod, lambda: None, lambda: None)

    def test_pattern_callback_read_failure_swallowed(self):
        # _maybe_patch_pattern: getattr(pattern, "callback") raises -> log + return.
        class _URLPatternStub:
            @property
            def callback(self):
                raise RuntimeError("callback boom")

            @callback.setter
            def callback(self, _value):
                pass

        fake = self._fake_django_urls(url_pattern_cls=_URLPatternStub)
        bad_pattern = _URLPatternStub()
        mod = types.ModuleType(self.module_name)
        setattr(mod, "urlpatterns", [bad_pattern])
        with mock.patch.dict(sys.modules, {"django.urls": fake}):
            # Should not raise even though pattern.callback access raises.
            FunctionWrapper._patch_django_url_patterns(mod, lambda: None, lambda: None)

    def test_pattern_callback_write_failure_swallowed(self):
        # _maybe_patch_pattern: identity match found, but pattern.callback
        # setter raises -> log + return without raising.
        class _URLPatternStub:
            def __init__(self, cb):
                self._cb = cb

            @property
            def callback(self):
                return self._cb

            @callback.setter
            def callback(self, _value):
                raise RuntimeError("setter boom")

        original = lambda: None  # noqa: E731
        original.__name__ = "original"
        wrapper = lambda: None  # noqa: E731

        fake = self._fake_django_urls(url_pattern_cls=_URLPatternStub)
        pattern = _URLPatternStub(original)
        mod = types.ModuleType(self.module_name)
        setattr(mod, "urlpatterns", [pattern])
        with mock.patch.dict(sys.modules, {"django.urls": fake}):
            FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)
        # Original callback unchanged because setter raised.
        self.assertIs(pattern.callback, original)


if __name__ == "__main__":
    unittest.main()
