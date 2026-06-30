# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests that the function-level instrumentation engine fires for Django view
handlers without requiring URLPattern.callback patching.

Django's path() / re_path() / include() store a direct reference to the view
function in the URL resolver tree (URLPattern.callback) at import time. The
setattr-wrapper approach required us to walk that tree and rewrite every
callback because replacing the module-level name does not update Django's
internal references. The bytecode-injection / sys.monitoring engines mutate
the function's __code__ in place, so any reference Django is holding (or any
other framework) sees the rewritten code on the next call — no URLPattern
patch needed.
"""

import sys
import types
import unittest

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger._function_wrapper import set_snapshot_emitter


def _make_module(name, **attrs):
    """Create a fake module with given attributes."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _remove_module(name):
    sys.modules.pop(name, None)


def _make_engine():
    """Pick the engine the running Python supports.

    3.12+ → SysMonitoringEngine. 3.10-3.11 → BytecodeInjectionEngine.
    """
    if sys.version_info >= (3, 12):
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine import (
            SysMonitoringEngine,
        )

        engine = SysMonitoringEngine()
    else:
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.debugger.instrumentation_engine._bytecode_injection_engine import (
            BytecodeInjectionEngine,
        )

        engine = BytecodeInjectionEngine()
    engine.initialize()
    return engine


def _configure_django_if_needed():
    """Make Django importable in test contexts. Returns True if Django is
    available + minimally configured; False otherwise (caller should skip)."""
    try:
        import django  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import
    except ImportError:
        return False
    try:
        from django.conf import settings  # pylint: disable=import-outside-toplevel

        if not settings.configured:
            settings.configure(
                DEBUG=False,
                ROOT_URLCONF="_test_django_root_urlconf_does_not_exist",
                DATABASES={},
                INSTALLED_APPS=[],
                ALLOWED_HOSTS=["*"],
            )
        return True
    except Exception:  # pylint: disable=broad-exception-caught
        return False


class TestDjangoUrlPatternsPatching(unittest.TestCase):
    """Exercises the engine against Django's URLPattern.callback to prove no
    URL-resolver patching is required: mutating __code__ in place flows through
    any reference Django holds in its resolver tree."""

    def setUp(self):
        self.module_name = "_test_django_module"
        _remove_module(self.module_name)
        self.snapshots = []

        class _FakeEmitter:
            def emit_snapshot(_self, snap):  # noqa: N805
                self.snapshots.append(snap)

        set_snapshot_emitter(_FakeEmitter())
        self.engine = _make_engine()

    def tearDown(self):
        try:
            self.engine.cleanup()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        _remove_module(self.module_name)

    # ------------------------------------------------------------------
    # URLPattern.callback identity match
    # ------------------------------------------------------------------

    def test_patch_django_url_patterns_identity_match(self):
        """When URLPattern.callback is the same identity as the armed function,
        the engine's __code__ mutation flows through — no URLPattern patch needed."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def my_view(request):
            return "original"

        urlpattern = path("my-view/", my_view)
        _make_module(self.module_name, urlpatterns=[urlpattern], my_view=my_view)

        ok = self.engine.enable_function_level_instrumentation(
            code=my_view.__code__,
            func=my_view,
            function_key=f"{self.module_name}.my_view",
            module_name=self.module_name,
            qualified_name="my_view",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # Calling through URLPattern.callback (Django's internal reference) still
        # fires the engine — same function object, mutated __code__.
        result = urlpattern.callback(None)
        self.assertEqual(result, "original")
        self.assertEqual(len(self.snapshots), 1)

    # ------------------------------------------------------------------
    # OTel-wrapped view (different identity, same underlying call)
    # ------------------------------------------------------------------

    def test_patch_django_url_patterns_name_fallback(self):
        """When Django holds an OTel-wrapped view (different identity from the
        underlying function), arming the underlying function still produces a
        snapshot when the wrapper invokes it — the wrapper calls the same
        function object whose __code__ we mutated."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def get_order(request):
            return "original"

        # Simulate OTel's auto-instrumentation wrapping the view.
        def otel_wrapped(request):
            return get_order(request)

        otel_wrapped.__name__ = "get_order"
        otel_wrapped.__module__ = self.module_name

        urlpattern = path("orders/", otel_wrapped)
        _make_module(self.module_name, urlpatterns=[urlpattern], get_order=get_order)

        ok = self.engine.enable_function_level_instrumentation(
            code=get_order.__code__,
            func=get_order,
            function_key=f"{self.module_name}.get_order",
            module_name=self.module_name,
            qualified_name="get_order",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # OTel wrapper still calls the underlying get_order, whose __code__ is
        # now mutated. Snapshot fires.
        result = urlpattern.callback(None)
        self.assertEqual(result, "original")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_django_no_django_installed(self):
        """When Django is not installed, the engine arms unaffected — the engine
        path has no Django dependency at all."""

        def some_func():
            return "ok"

        _make_module(self.module_name, some_func=some_func)

        ok = self.engine.enable_function_level_instrumentation(
            code=some_func.__code__,
            func=some_func,
            function_key=f"{self.module_name}.some_func",
            module_name=self.module_name,
            qualified_name="some_func",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        self.assertEqual(some_func(), "ok")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_django_no_urlpatterns_in_module(self):
        """When the module has no urlpatterns, the engine still arms the function
        correctly — the engine doesn't scan for URL patterns."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")

        def some_func():
            return "no urls here"

        _make_module(self.module_name, some_func=some_func, x=42, y="hello")

        ok = self.engine.enable_function_level_instrumentation(
            code=some_func.__code__,
            func=some_func,
            function_key=f"{self.module_name}.some_func",
            module_name=self.module_name,
            qualified_name="some_func",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        self.assertEqual(some_func(), "no urls here")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_django_function_not_in_url_patterns(self):
        """Arming function A leaves a different view B in the resolver tree
        un-instrumented — no cross-talk."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def helper_func(request):
            return "helper"

        def other_view(request):
            return "other"

        urlpattern = path("other/", other_view)
        _make_module(self.module_name, urlpatterns=[urlpattern], helper_func=helper_func)

        ok = self.engine.enable_function_level_instrumentation(
            code=helper_func.__code__,
            func=helper_func,
            function_key=f"{self.module_name}.helper_func",
            module_name=self.module_name,
            qualified_name="helper_func",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # other_view should NOT be instrumented.
        self.assertIs(urlpattern.callback, other_view)
        self.assertEqual(urlpattern.callback(None), "other")
        self.assertEqual(len(self.snapshots), 0)

    # ------------------------------------------------------------------
    # include()-nested resolvers
    # ------------------------------------------------------------------

    def test_patch_django_nested_resolvers(self):
        """include()-nested URLs hold the same function object, so the engine's
        in-place __code__ mutation fires there too — no recursive resolver-tree
        descent required."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import include, path  # pylint: disable=import-outside-toplevel

        def deep_view(request):
            return "deep"

        inner_pattern = path("deep/", deep_view)

        # Build a nested module with its own urlpatterns, then include() it.
        inner_module_name = self.module_name + "_inner"
        _make_module(inner_module_name, urlpatterns=[inner_pattern])
        outer_pattern = path("api/", include(inner_module_name))
        try:
            _make_module(self.module_name, urlpatterns=[outer_pattern])

            ok = self.engine.enable_function_level_instrumentation(
                code=deep_view.__code__,
                func=deep_view,
                function_key=f"{self.module_name}.deep_view",
                module_name=self.module_name,
                qualified_name="deep_view",
                capture_config=CaptureConfig(capture_locals=[], capture_return=True),
            )
            self.assertTrue(ok)

            self.assertIs(inner_pattern.callback, deep_view)
            self.assertEqual(inner_pattern.callback(None), "deep")
            self.assertEqual(len(self.snapshots), 1)
        finally:
            _remove_module(inner_module_name)

    # ------------------------------------------------------------------
    # Cross-module urlpatterns (the common Django layout: urls module separate
    # from the views module)
    # ------------------------------------------------------------------

    def test_patch_django_get_resolver_path(self):
        """When the views module has no urlpatterns (the common Django layout —
        urlpatterns lives in a dedicated urls module), the engine still fires:
        the urls module's URLPattern.callback references the same function object
        whose __code__ the engine mutated."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def cross_module_view(request):
            return "original"

        urlpattern = path("cross/", cross_module_view)

        # views module — has no urlpatterns
        views_module_name = self.module_name + "_views"
        _make_module(views_module_name, cross_module_view=cross_module_view)
        # urls module — holds the URLPattern in a separate module
        urls_module_name = self.module_name + "_urls"
        _make_module(urls_module_name, urlpatterns=[urlpattern])
        try:
            ok = self.engine.enable_function_level_instrumentation(
                code=cross_module_view.__code__,
                func=cross_module_view,
                function_key=f"{views_module_name}.cross_module_view",
                module_name=views_module_name,
                qualified_name="cross_module_view",
                capture_config=CaptureConfig(capture_locals=[], capture_return=True),
            )
            self.assertTrue(ok)

            self.assertIs(urlpattern.callback, cross_module_view)
            self.assertEqual(urlpattern.callback(None), "original")
            self.assertEqual(len(self.snapshots), 1)
        finally:
            _remove_module(views_module_name)
            _remove_module(urls_module_name)

    # ------------------------------------------------------------------
    # Decorator-wrapped view (functools.wraps)
    # ------------------------------------------------------------------

    def test_patch_django_decorator_wrapped(self):
        """A view wrapped by a functools.wraps-preserving decorator (the
        @login_required / @csrf_exempt pattern): the URLPattern holds the
        decorator wrapper, but the wrapper calls the underlying view whose
        __code__ the engine mutated — so the snapshot still fires."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        import functools  # pylint: disable=import-outside-toplevel

        from django.urls import path  # pylint: disable=import-outside-toplevel

        def my_view(request):
            return "view"

        my_view.__module__ = self.module_name

        @functools.wraps(my_view)
        def decorator_wrapper(request):
            return my_view(request)

        urlpattern = path("decorated/", decorator_wrapper)
        _make_module(self.module_name, urlpatterns=[urlpattern], my_view=my_view)

        ok = self.engine.enable_function_level_instrumentation(
            code=my_view.__code__,
            func=my_view,
            function_key=f"{self.module_name}.my_view",
            module_name=self.module_name,
            qualified_name="my_view",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # The decorator wrapper invokes the underlying my_view, whose __code__
        # is now mutated. Snapshot fires.
        result = urlpattern.callback(None)
        self.assertEqual(result, "view")
        self.assertEqual(len(self.snapshots), 1)

    # ------------------------------------------------------------------
    # Class-based views — arming the method fires via dispatch
    # ------------------------------------------------------------------

    def test_patch_django_cbv_unaffected(self):
        """The closure returned by View.as_view() (URLPattern.callback) is never
        touched by the engine. Probing MyView.get arms the method's __code__;
        invoking the method (as dispatch's getattr(self, method) would) fires the
        snapshot, while the as_view closure stays intact."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel
        from django.views import View  # pylint: disable=import-outside-toplevel

        class MyView(View):
            def get(self, request):
                return "get"

        cbv_callable = MyView.as_view()  # closure: __name__ == 'view'

        urlpattern = path("cbv/", cbv_callable)
        _make_module(self.module_name, urlpatterns=[urlpattern])

        # User puts a probe on MyView.get — arm the unbound method's code.
        ok = self.engine.enable_function_level_instrumentation(
            code=MyView.get.__code__,
            func=MyView.get,
            function_key=f"{self.module_name}.MyView.get",
            module_name=self.module_name,
            qualified_name="MyView.get",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # The as_view closure is NOT replaced — the engine doesn't touch it.
        self.assertIs(urlpattern.callback, cbv_callable)
        # Invoking MyView.get fires the snapshot (CBV instrumentation flows
        # through dispatch's getattr(self, method)).
        self.assertEqual(MyView().get(None), "get")
        self.assertEqual(len(self.snapshots), 1)

    # ------------------------------------------------------------------
    # Integration: arm a view, fire via URLPattern.callback
    # ------------------------------------------------------------------

    def test_replace_function_patches_django_url_patterns(self):
        """Engine arming a view: invoking via Django's URLPattern.callback
        produces a snapshot. (No setattr/resolver-tree patching required — the
        __code__ mutation is in-place.)"""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def my_route(request):
            return "hello"

        my_route.__module__ = self.module_name

        urlpattern = path("my-route/", my_route)
        _make_module(self.module_name, my_route=my_route, urlpatterns=[urlpattern])

        ok = self.engine.enable_function_level_instrumentation(
            code=my_route.__code__,
            func=my_route,
            function_key=f"{self.module_name}.my_route",
            module_name=self.module_name,
            qualified_name="my_route",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # Django's stored reference is the SAME function object — engine mutated
        # its __code__ — so calling through URLPattern.callback fires.
        self.assertEqual(urlpattern.callback(None), "hello")
        self.assertEqual(len(self.snapshots), 1)

    # ------------------------------------------------------------------
    # Integration: disarm restores the original __code__
    # ------------------------------------------------------------------

    def test_restore_function_restores_django_url_patterns(self):
        """Engine disarm restores original __code__: invoking via Django's
        URLPattern.callback after disable produces NO snapshot."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def my_route(request):
            return "original"

        my_route.__module__ = self.module_name

        urlpattern = path("my-route/", my_route)
        _make_module(self.module_name, my_route=my_route, urlpatterns=[urlpattern])

        original_code = my_route.__code__

        self.engine.enable_function_level_instrumentation(
            code=my_route.__code__,
            func=my_route,
            function_key=f"{self.module_name}.my_route",
            module_name=self.module_name,
            qualified_name="my_route",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )

        # Sanity: armed call fires.
        urlpattern.callback(None)
        self.assertEqual(len(self.snapshots), 1)
        self.snapshots.clear()

        self.engine.disable_function_level_instrumentation(code=original_code, func=my_route)

        # __code__ restored; calling via URLPattern.callback emits no new snapshot.
        self.assertIs(my_route.__code__, original_code)
        self.assertEqual(urlpattern.callback(None), "original")
        self.assertEqual(len(self.snapshots), 0)


if __name__ == "__main__":
    unittest.main()
