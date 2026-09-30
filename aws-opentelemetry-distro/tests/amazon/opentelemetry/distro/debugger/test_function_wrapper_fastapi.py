# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for FastAPI route-table patching in _function_wrapper.py.

Validates that _replace_function_in_module and restore_function correctly
patch a FastAPI app's route table (APIRoute.endpoint and APIRoute.dependant.call)
when instrumenting route handlers, mirroring the Flask view_functions patching.
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


def _route_for(app, path):
    """Return the APIRoute registered on `app` for `path`."""
    for route in app.router.routes:
        if getattr(route, "path", None) == path and getattr(route, "endpoint", None) is not None:
            return route
    raise AssertionError(f"No route found for path {path}")


class TestFastAPIRoutesPatching(unittest.TestCase):
    """Tests for _patch_fastapi_routes and its integration."""

    def setUp(self):
        self.module_name = "_test_fastapi_module"
        _remove_module(self.module_name)

    def tearDown(self):
        _remove_module(self.module_name)

    # ------------------------------------------------------------------
    # _patch_fastapi_routes
    # ------------------------------------------------------------------

    def test_patch_fastapi_routes_identity_match(self):
        """When route.endpoint is original_func, identity-based patching rebinds endpoint + dependant.call."""
        try:
            from fastapi import FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/orders")
        def get_orders():
            return "orders"

        route = _route_for(app, "/orders")
        original = route.endpoint  # the function object FastAPI captured
        wrapper = lambda: "wrapper"  # noqa: E731

        mod = _make_module(self.module_name, app=app, get_orders=original)

        FunctionWrapper._patch_fastapi_routes(mod, original, wrapper)

        self.assertIs(route.endpoint, wrapper)
        self.assertIs(route.dependant.call, wrapper)

    def test_patch_fastapi_routes_name_fallback(self):
        """When identity doesn't match (e.g. external wrapping), name+module fallback rebinds the route."""
        try:
            from fastapi import FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/orders")
        def get_orders():
            return "orders"

        route = _route_for(app, "/orders")
        original = route.endpoint

        # Simulate something having replaced the stored handler with a different-identity
        # callable that preserves __name__/__module__ (as functools.wraps does).
        def other_same_name():
            return "other"

        other_same_name.__name__ = original.__name__
        other_same_name.__module__ = original.__module__
        route.endpoint = other_same_name
        route.dependant.call = other_same_name

        di_wrapper = lambda: "di_wrapper"  # noqa: E731

        mod = _make_module(self.module_name, app=app, get_orders=original)

        FunctionWrapper._patch_fastapi_routes(mod, original, di_wrapper)

        self.assertIs(route.endpoint, di_wrapper)
        self.assertIs(route.dependant.call, di_wrapper)

    def test_patch_fastapi_no_fastapi_installed(self):
        """When FastAPI is not installed, patching is a no-op."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name, some_func=original)

        with patch.dict(sys.modules, {"fastapi": None}):
            # Should not raise
            FunctionWrapper._patch_fastapi_routes(mod, original, wrapper)

    def test_patch_fastapi_no_app_in_module(self):
        """When module has no FastAPI app instances, patching is a no-op."""
        try:
            from fastapi import FastAPI  # noqa: F401
        except ImportError:
            self.skipTest("FastAPI not installed")

        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name, some_func=original, x=42, y="hello")

        # Should not raise
        FunctionWrapper._patch_fastapi_routes(mod, original, wrapper)

    def test_patch_fastapi_function_not_a_route(self):
        """When the function is not a route handler, the route table is unchanged."""
        try:
            from fastapi import FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/other")
        def other_view():
            return "other"

        route = _route_for(app, "/other")
        other_endpoint = route.endpoint
        other_call = route.dependant.call

        original = lambda: None  # noqa: E731
        original.__name__ = "helper_func"
        wrapper = lambda: None  # noqa: E731

        mod = _make_module(self.module_name, app=app, helper_func=original)

        FunctionWrapper._patch_fastapi_routes(mod, original, wrapper)

        # The unrelated route should be untouched.
        self.assertIs(route.endpoint, other_endpoint)
        self.assertIs(route.dependant.call, other_call)

    # ------------------------------------------------------------------
    # Depends() sub-dependency patching (regression: sub-dependency
    # breakpoints silently never fired because only the top-level
    # dependant.call was rebound, never dependant.dependencies[*].call)
    # ------------------------------------------------------------------

    def test_patch_fastapi_routes_sub_dependency(self):
        """A function used as a Depends() sub-dependency is rebound on dependant.dependencies[*].call.

        The per-request solver invokes the sub-dependency via
        route.dependant.dependencies[i].call, not the top-level endpoint, so
        rebinding only endpoint/dependant.call would leave it pointing at the
        original (the breakpoint would silently never fire).
        """
        try:
            from fastapi import Depends, FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        def square_dependency(value: int = 7) -> int:
            return value * value

        app = FastAPI()

        @app.get("/dep")
        def decorators_dependency(dep: int = Depends(square_dependency)):
            return {"result": dep}

        route = _route_for(app, "/dep")
        sub_calls = [d.call for d in route.dependant.dependencies]
        # Precondition: the solver currently points at the ORIGINAL sub-dependency.
        self.assertIn(square_dependency, sub_calls)

        wrapper = lambda value=7: value * value  # noqa: E731
        wrapper.__name__ = square_dependency.__name__
        wrapper.__module__ = square_dependency.__module__

        mod = _make_module(self.module_name, app=app, square_dependency=square_dependency)

        FunctionWrapper._patch_fastapi_routes(mod, square_dependency, wrapper)

        rebound_calls = [d.call for d in route.dependant.dependencies]
        self.assertIn(wrapper, rebound_calls)
        self.assertNotIn(square_dependency, rebound_calls)
        # The route's own endpoint (a different function) must not be clobbered.
        self.assertIsNot(route.endpoint, wrapper)

    def test_patch_fastapi_routes_only_target_sub_dependency_rebound(self):
        """With two Depends() on a route, only the targeted sub-dependency is rebound."""
        try:
            from fastapi import Depends, FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        def square_dependency(value: int = 7) -> int:
            return value * value

        def inner_dep() -> int:
            return 1

        app = FastAPI()

        @app.get("/nested")
        def nested_dependency(dep: int = Depends(square_dependency), x: int = Depends(inner_dep)):
            return {"result": dep + x}

        route = _route_for(app, "/nested")
        wrapper = lambda value=7: value * value  # noqa: E731
        wrapper.__name__ = square_dependency.__name__
        wrapper.__module__ = square_dependency.__module__

        mod = _make_module(self.module_name, app=app, square_dependency=square_dependency)

        FunctionWrapper._patch_fastapi_routes(mod, square_dependency, wrapper)

        rebound_calls = [d.call for d in route.dependant.dependencies]
        self.assertIn(wrapper, rebound_calls)  # target rebound
        self.assertIn(inner_dep, rebound_calls)  # the other dep left intact
        self.assertNotIn(square_dependency, rebound_calls)

    # ------------------------------------------------------------------
    # Integration: _replace_function_in_module with FastAPI patching
    # ------------------------------------------------------------------

    def test_replace_function_patches_fastapi_routes(self):
        """_replace_function_in_module patches FastAPI route references."""
        try:
            from fastapi import FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/hello")
        def my_route():
            return "hello"

        route = _route_for(app, "/hello")
        original = route.endpoint

        mod = _make_module(self.module_name, app=app, my_route=original)
        wrapper = lambda: "wrapped"  # noqa: E731

        with patch.object(FunctionWrapper, "_resolve_module", return_value=mod):
            FunctionWrapper._replace_function_in_module(self.module_name, "my_route", wrapper)

        # Module attribute replaced
        self.assertIs(mod.my_route, wrapper)
        # FastAPI route table also patched
        self.assertIs(route.endpoint, wrapper)
        self.assertIs(route.dependant.call, wrapper)

    # ------------------------------------------------------------------
    # Integration: restore_function with FastAPI patching
    # ------------------------------------------------------------------

    def test_restore_function_restores_fastapi_routes(self):
        """restore_function restores FastAPI route references back to the original."""
        try:
            from fastapi import FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        def original_route():
            return "original"

        def wrapped_route():
            return "wrapped"

        app = FastAPI()

        @app.get("/hello")
        def my_route():
            return "hello"

        route = _route_for(app, "/hello")
        # Simulate: DI already wrapped the function and patched the route table.
        route.endpoint = wrapped_route
        route.dependant.call = wrapped_route

        mod = _make_module(self.module_name, app=app, my_route=wrapped_route)

        with patch.object(FunctionWrapper, "_resolve_module", return_value=mod):
            result = FunctionWrapper.restore_function(self.module_name, "my_route", original_route)

        self.assertTrue(result)
        self.assertIs(mod.my_route, original_route)
        # FastAPI route table restored
        self.assertIs(route.endpoint, original_route)
        self.assertIs(route.dependant.call, original_route)

    # ------------------------------------------------------------------
    # _patch_framework_references (dispatcher)
    # ------------------------------------------------------------------

    def test_patch_framework_references_calls_fastapi_patching(self):
        """_patch_framework_references delegates to _patch_fastapi_routes."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name)

        with patch.object(FunctionWrapper, "_patch_fastapi_routes") as mock_fastapi:
            FunctionWrapper._patch_framework_references(mod, original, wrapper)
            mock_fastapi.assert_called_once_with(mod, original, wrapper)

    def test_patch_framework_references_swallows_fastapi_exceptions(self):
        """_patch_framework_references never raises even if FastAPI patching fails."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name)

        with patch.object(FunctionWrapper, "_patch_fastapi_routes", side_effect=RuntimeError("boom")):
            # Should not raise
            FunctionWrapper._patch_framework_references(mod, original, wrapper)


if __name__ == "__main__":
    unittest.main()
