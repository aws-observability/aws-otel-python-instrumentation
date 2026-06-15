# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for Django URLPattern.callback patching in _function_wrapper.py.

Validates that _replace_function_in_module and restore_function correctly
patch Django's URL resolver tree (URLPattern.callback) when instrumenting
view functions registered via path() / re_path() / include().
"""

import sys
import types
import unittest
from unittest.mock import patch

from amazon.opentelemetry.distro.debugger._function_wrapper import FunctionWrapper


def _make_module(name, **attrs):
    """Create a fake module with given attributes."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _remove_module(name):
    sys.modules.pop(name, None)


def _configure_django_if_needed():
    """Make Django importable in test contexts. Returns True if Django is
    available + minimally configured; False otherwise (caller should skip).
    Tests that exercise URLPattern/URLResolver only need Django importable;
    tests that hit get_resolver(None) need ROOT_URLCONF set."""
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
    """Tests for _patch_django_url_patterns and its integration."""

    def setUp(self):
        self.module_name = "_test_django_module"
        _remove_module(self.module_name)

    def tearDown(self):
        _remove_module(self.module_name)

    # ------------------------------------------------------------------
    # _patch_django_url_patterns: identity match
    # ------------------------------------------------------------------

    def test_patch_django_url_patterns_identity_match(self):
        """When URLPattern.callback is original_func, identity-based patching works."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        original = lambda request: "original"  # noqa: E731
        wrapper = lambda request: "wrapper"  # noqa: E731
        original.__name__ = "my_view"

        urlpattern = path("my-view/", original)
        mod = _make_module(self.module_name, urlpatterns=[urlpattern])

        FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

        self.assertIs(urlpattern.callback, wrapper)

    # ------------------------------------------------------------------
    # _patch_django_url_patterns: name+module fallback
    # ------------------------------------------------------------------

    def test_patch_django_url_patterns_name_fallback(self):
        """When identity doesn't match (e.g. OTel wrapping or @login_required),
        name+module-based fallback works."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        original = lambda request: "original"  # noqa: E731
        otel_wrapped = lambda request: "otel_wrapped"  # noqa: E731
        di_wrapper = lambda request: "di_wrapper"  # noqa: E731
        original.__name__ = "get_order"
        original.__module__ = self.module_name
        otel_wrapped.__name__ = "get_order"
        otel_wrapped.__module__ = self.module_name

        urlpattern = path("orders/", otel_wrapped)
        mod = _make_module(self.module_name, urlpatterns=[urlpattern])

        FunctionWrapper._patch_django_url_patterns(mod, original, di_wrapper)

        self.assertIs(urlpattern.callback, di_wrapper)

    # ------------------------------------------------------------------
    # _patch_django_url_patterns: no-op edge cases
    # ------------------------------------------------------------------

    def test_patch_django_no_django_installed(self):
        """When Django is not installed, patching is a no-op."""
        original = lambda request: None  # noqa: E731
        wrapper = lambda request: None  # noqa: E731
        mod = _make_module(self.module_name, some_func=original)

        with patch.dict(sys.modules, {"django.urls": None}):
            # Should not raise
            FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

    def test_patch_django_no_urlpatterns_in_module(self):
        """When module has no urlpatterns / URLPattern instances, patching is a no-op."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")

        original = lambda request: None  # noqa: E731
        wrapper = lambda request: None  # noqa: E731
        mod = _make_module(self.module_name, some_func=original, x=42, y="hello")

        # Should not raise
        FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

    def test_patch_django_function_not_in_url_patterns(self):
        """When function is not a registered view, urlpatterns is unchanged."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        original = lambda request: None  # noqa: E731
        wrapper = lambda request: None  # noqa: E731
        original.__name__ = "helper_func"
        original.__module__ = self.module_name

        other_view = lambda request: "other"  # noqa: E731
        other_view.__name__ = "other_view"
        other_view.__module__ = self.module_name

        urlpattern = path("other/", other_view)
        mod = _make_module(self.module_name, urlpatterns=[urlpattern])

        FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

        # other_view should be unchanged
        self.assertIs(urlpattern.callback, other_view)

    # ------------------------------------------------------------------
    # Django-specific: include()-nested resolvers
    # ------------------------------------------------------------------

    def test_patch_django_nested_resolvers(self):
        """include()-nested URLs are reached via recursive descent."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import include, path  # pylint: disable=import-outside-toplevel

        original = lambda request: "original"  # noqa: E731
        wrapper = lambda request: "wrapper"  # noqa: E731
        original.__name__ = "deep_view"

        inner_pattern = path("deep/", original)

        # Build a nested module with its own urlpatterns, then include() it.
        inner_module_name = self.module_name + "_inner"
        _make_module(inner_module_name, urlpatterns=[inner_pattern])
        outer_pattern = path("api/", include(inner_module_name))
        try:
            mod = _make_module(self.module_name, urlpatterns=[outer_pattern])

            FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

            self.assertIs(inner_pattern.callback, wrapper)
        finally:
            _remove_module(inner_module_name)

    # ------------------------------------------------------------------
    # Django-specific: get_resolver(None) discovery for cross-module urlpatterns
    # ------------------------------------------------------------------

    def test_patch_django_get_resolver_path(self):
        """When the views module has no urlpatterns (the common Django layout —
        urlpatterns lives in a dedicated urls module), the patcher reaches the
        active routes via get_resolver(None)."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import URLResolver, path  # pylint: disable=import-outside-toplevel

        original = lambda request: "original"  # noqa: E731
        wrapper = lambda request: "wrapper"  # noqa: E731
        original.__name__ = "cross_module_view"

        urlpattern = path("cross/", original)

        # views module — has no urlpatterns
        views_module_name = self.module_name + "_views"
        views_mod = _make_module(views_module_name, cross_module_view=original)

        # Build a fake URLResolver that returns our urlpattern. Mock get_resolver
        # to return it.
        fake_resolver = unittest.mock.MagicMock(spec=URLResolver)
        fake_resolver.url_patterns = [urlpattern]
        try:
            with patch(
                "amazon.opentelemetry.distro.debugger._function_wrapper.FunctionWrapper._patch_django_url_patterns",
                wraps=FunctionWrapper._patch_django_url_patterns,
            ):
                with patch("django.urls.get_resolver", return_value=fake_resolver):
                    FunctionWrapper._patch_django_url_patterns(views_mod, original, wrapper)

            self.assertIs(urlpattern.callback, wrapper)
        finally:
            _remove_module(views_module_name)

    # ------------------------------------------------------------------
    # Django-specific: decorator-wrapped view (functools.wraps)
    # ------------------------------------------------------------------

    def test_patch_django_decorator_wrapped(self):
        """A view wrapped by a functools.wraps-preserving decorator (the
        @login_required / @csrf_exempt pattern): identity match misses, but
        name+module fallback succeeds."""
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

        # decorator_wrapper inherits __name__ + __module__ from my_view.
        wrapper = lambda request: "di_wrapper"  # noqa: E731

        urlpattern = path("decorated/", decorator_wrapper)
        mod = _make_module(self.module_name, urlpatterns=[urlpattern])

        FunctionWrapper._patch_django_url_patterns(mod, my_view, wrapper)

        self.assertIs(urlpattern.callback, wrapper)

    # ------------------------------------------------------------------
    # Django-specific: class-based views — patcher does NOT touch the
    # as_view() closure
    # ------------------------------------------------------------------

    def test_patch_django_cbv_unaffected(self):
        """The closure returned by View.as_view() has __name__ == 'view', not
        the user's view-name, so neither identity nor name match. CBV
        instrumentation flows through dispatch's getattr(self, method)
        elsewhere; the patcher must not mutate the closure."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel
        from django.views import View  # pylint: disable=import-outside-toplevel

        class MyView(View):
            def get(self, request):
                return "get"

        cbv_callable = MyView.as_view()  # closure: __name__ == 'view'

        # User puts a probe on MyView.get — original_func is the unbound method.
        original = MyView.get
        wrapper = lambda self, request: "wrapped"  # noqa: E731

        urlpattern = path("cbv/", cbv_callable)
        mod = _make_module(self.module_name, urlpatterns=[urlpattern])

        FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

        # The as_view closure is NOT replaced — closure's __name__ is 'view'.
        self.assertIs(urlpattern.callback, cbv_callable)

    # ------------------------------------------------------------------
    # Django-specific: error-branch swallowing
    # ------------------------------------------------------------------

    def test_patch_django_resolver_traversal_error_swallowed(self):
        """When a URLResolver.url_patterns access raises (e.g. ImproperlyConfigured),
        the patcher logs and continues without raising."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import URLResolver  # pylint: disable=import-outside-toplevel

        original = lambda request: None  # noqa: E731
        wrapper = lambda request: None  # noqa: E731
        original.__name__ = "some_view"

        broken_resolver = unittest.mock.MagicMock(spec=URLResolver)
        type(broken_resolver).url_patterns = unittest.mock.PropertyMock(
            side_effect=RuntimeError("ImproperlyConfigured")
        )
        mod = _make_module(self.module_name, broken_resolver=broken_resolver)

        # Should not raise.
        FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

    def test_patch_django_get_resolver_unconfigured_swallowed(self):
        """When get_resolver(None) raises (e.g. ImproperlyConfigured before
        ROOT_URLCONF is set), the patcher continues with the module-scan fallback."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        original = lambda request: "original"  # noqa: E731
        wrapper = lambda request: "wrapper"  # noqa: E731
        original.__name__ = "fallback_view"

        urlpattern = path("fb/", original)
        mod = _make_module(self.module_name, urlpatterns=[urlpattern])

        with patch("django.urls.get_resolver", side_effect=RuntimeError("ImproperlyConfigured")):
            FunctionWrapper._patch_django_url_patterns(mod, original, wrapper)

        # Module-scan fallback succeeded.
        self.assertIs(urlpattern.callback, wrapper)

    # ------------------------------------------------------------------
    # Integration: _replace_function_in_module with Django patching
    # ------------------------------------------------------------------

    def test_replace_function_patches_django_url_patterns(self):
        """_replace_function_in_module patches Django URLPattern.callback."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def my_route(request):
            return "hello"

        my_route.__module__ = self.module_name

        urlpattern = path("my-route/", my_route)
        mod = _make_module(self.module_name, my_route=my_route, urlpatterns=[urlpattern])

        wrapper = lambda request: "wrapped"  # noqa: E731

        with patch.object(FunctionWrapper, "_resolve_module", return_value=mod):
            FunctionWrapper._replace_function_in_module(self.module_name, "my_route", wrapper)

        # Module attribute replaced
        self.assertIs(mod.my_route, wrapper)
        # Django URLPattern.callback also patched
        self.assertIs(urlpattern.callback, wrapper)

    # ------------------------------------------------------------------
    # Integration: restore_function with Django patching
    # ------------------------------------------------------------------

    def test_restore_function_restores_django_url_patterns(self):
        """restore_function restores Django URLPattern.callback to original."""
        if not _configure_django_if_needed():
            self.skipTest("Django not installed")
        from django.urls import path  # pylint: disable=import-outside-toplevel

        def original_route(request):
            return "original"

        def wrapped_route(request):
            return "wrapped"

        original_route.__module__ = self.module_name
        wrapped_route.__module__ = self.module_name

        # Simulate: DI already wrapped and patched URLPattern.
        urlpattern = path("my-route/", wrapped_route)
        mod = _make_module(self.module_name, my_route=wrapped_route, urlpatterns=[urlpattern])

        with patch.object(FunctionWrapper, "_resolve_module", return_value=mod):
            result = FunctionWrapper.restore_function(self.module_name, "my_route", original_route)

        self.assertTrue(result)
        self.assertIs(mod.my_route, original_route)
        # URLPattern.callback restored
        self.assertIs(urlpattern.callback, original_route)

    # ------------------------------------------------------------------
    # _patch_framework_references (dispatcher delegates to Django patcher)
    # ------------------------------------------------------------------

    def test_patch_framework_references_calls_django_patching(self):
        """_patch_framework_references delegates to _patch_django_url_patterns."""
        original = lambda request: None  # noqa: E731
        wrapper = lambda request: None  # noqa: E731
        mod = _make_module(self.module_name)

        with patch.object(FunctionWrapper, "_patch_django_url_patterns") as mock_django:
            FunctionWrapper._patch_framework_references(mod, original, wrapper)
            mock_django.assert_called_once_with(mod, original, wrapper)

    def test_patch_framework_references_swallows_django_exceptions(self):
        """A failure in Django patching never raises and never blocks Flask patching."""
        original = lambda request: None  # noqa: E731
        wrapper = lambda request: None  # noqa: E731
        mod = _make_module(self.module_name)

        with patch.object(FunctionWrapper, "_patch_django_url_patterns", side_effect=RuntimeError("boom")):
            with patch.object(FunctionWrapper, "_patch_flask_view_functions") as mock_flask:
                # Should not raise.
                FunctionWrapper._patch_framework_references(mod, original, wrapper)
                # Flask patcher was still called even though Django raised.
                mock_flask.assert_called_once_with(mod, original, wrapper)


if __name__ == "__main__":
    unittest.main()
