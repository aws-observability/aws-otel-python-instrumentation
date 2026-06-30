# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests that the function-level instrumentation engine fires for FastAPI route
handlers without requiring route-table patching.

FastAPI's @app.get()/@app.post() decorators store a direct reference to the
route function in the app's route table (APIRoute.endpoint and
APIRoute.dependant.call) at import time, and Depends() sub-dependencies in
APIRoute.dependant.dependencies[*].call. The setattr-wrapper approach required
us to walk that route table and rewrite every reference because replacing the
module-level name does not update FastAPI's internal references. The
bytecode-injection / sys.monitoring engines mutate the function's __code__ in
place, so any reference FastAPI is holding (or any other framework) sees the
rewritten code on the next call — no route-table patch needed.
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


def _route_for(app, path):
    """Return the APIRoute registered on `app` for `path`."""
    for route in app.router.routes:
        if getattr(route, "path", None) == path and getattr(route, "endpoint", None) is not None:
            return route
    raise AssertionError(f"No route found for path {path}")


class TestFastAPIRoutesPatching(unittest.TestCase):
    """Exercises the engine against a FastAPI app's route table to prove no
    route-table patching is required: mutating __code__ in place flows through
    any reference FastAPI holds (endpoint, dependant.call, sub-dependencies)."""

    def setUp(self):
        self.module_name = "_test_fastapi_module"
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
    # APIRoute.endpoint / dependant.call identity match
    # ------------------------------------------------------------------

    def test_patch_fastapi_routes_identity_match(self):
        """When route.endpoint is the armed function, the engine's __code__
        mutation flows through endpoint + dependant.call — no route-table patch."""
        try:
            from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/orders")
        def get_orders():
            return "orders"

        route = _route_for(app, "/orders")
        original = route.endpoint  # the function object FastAPI captured

        _make_module(self.module_name, app=app, get_orders=original)

        ok = self.engine.enable_function_level_instrumentation(
            code=original.__code__,
            func=original,
            function_key=f"{self.module_name}.get_orders",
            module_name=self.module_name,
            qualified_name="get_orders",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # FastAPI's stored references are the SAME function object — engine
        # mutated its __code__ — so calling through either fires.
        self.assertIs(route.endpoint, original)
        self.assertIs(route.dependant.call, original)
        self.assertEqual(route.endpoint(), "orders")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_fastapi_routes_name_fallback(self):
        """When something has replaced the stored handler with a different-identity
        callable that calls the underlying function (as functools.wraps does),
        arming the underlying function still fires when the wrapper invokes it."""
        try:
            from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/orders")
        def get_orders():
            return "orders"

        route = _route_for(app, "/orders")
        original = route.endpoint

        # Simulate something having replaced the stored handler with a
        # different-identity wrapper that calls the underlying function.
        def other_same_name():
            return original()

        other_same_name.__name__ = original.__name__
        other_same_name.__module__ = original.__module__
        route.endpoint = other_same_name
        route.dependant.call = other_same_name

        _make_module(self.module_name, app=app, get_orders=original)

        ok = self.engine.enable_function_level_instrumentation(
            code=original.__code__,
            func=original,
            function_key=f"{self.module_name}.get_orders",
            module_name=self.module_name,
            qualified_name="get_orders",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # The wrapper still calls the underlying get_orders, whose __code__ is
        # now mutated. Snapshot fires.
        self.assertEqual(route.endpoint(), "orders")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_fastapi_no_fastapi_installed(self):
        """When FastAPI is not installed, the engine arms unaffected — the engine
        path has no FastAPI dependency at all."""

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

    def test_patch_fastapi_no_app_in_module(self):
        """When module has no FastAPI app instance, the engine still arms the
        function correctly — the engine doesn't scan for FastAPI apps."""
        try:
            from fastapi import FastAPI  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            self.skipTest("FastAPI not installed")

        def some_func():
            return "no fastapi here"

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
        self.assertEqual(some_func(), "no fastapi here")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_fastapi_function_not_a_route(self):
        """Arming a function that is not a route handler leaves the route table
        un-instrumented — no cross-talk."""
        try:
            from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/other")
        def other_view():
            return "other"

        route = _route_for(app, "/other")
        other_endpoint = route.endpoint
        other_call = route.dependant.call

        def helper_func():
            return "helper"

        _make_module(self.module_name, app=app, helper_func=helper_func)

        ok = self.engine.enable_function_level_instrumentation(
            code=helper_func.__code__,
            func=helper_func,
            function_key=f"{self.module_name}.helper_func",
            module_name=self.module_name,
            qualified_name="helper_func",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # The unrelated route should be untouched and fire no snapshot.
        self.assertIs(route.endpoint, other_endpoint)
        self.assertIs(route.dependant.call, other_call)
        self.assertEqual(route.endpoint(), "other")
        self.assertEqual(len(self.snapshots), 0)

    # ------------------------------------------------------------------
    # Depends() sub-dependency: solver invokes dependant.dependencies[*].call
    # ------------------------------------------------------------------

    def test_patch_fastapi_routes_sub_dependency(self):
        """A function used as a Depends() sub-dependency is held in
        route.dependant.dependencies[i].call. The per-request solver invokes it
        there, not via the top-level endpoint — and because the engine mutated
        the same function object's __code__ in place, the snapshot fires."""
        try:
            from fastapi import Depends, FastAPI  # pylint: disable=import-outside-toplevel
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
        # Precondition: the solver points at the sub-dependency function object.
        self.assertIn(square_dependency, sub_calls)

        _make_module(self.module_name, app=app, square_dependency=square_dependency)

        ok = self.engine.enable_function_level_instrumentation(
            code=square_dependency.__code__,
            func=square_dependency,
            function_key=f"{self.module_name}.square_dependency",
            module_name=self.module_name,
            qualified_name="square_dependency",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # The solver still holds the SAME function object — engine mutated its
        # __code__ — so invoking the sub-dependency fires.
        sub_dep = next(d.call for d in route.dependant.dependencies if d.call is square_dependency)
        self.assertEqual(sub_dep(), 49)
        self.assertEqual(len(self.snapshots), 1)
        # The route's own endpoint (a different function) must not be clobbered.
        self.assertIsNot(route.endpoint, square_dependency)

    def test_patch_fastapi_routes_only_target_sub_dependency_rebound(self):
        """With two Depends() on a route, arming one sub-dependency instruments
        only that one — invoking the other fires no snapshot."""
        try:
            from fastapi import Depends, FastAPI  # pylint: disable=import-outside-toplevel
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

        _make_module(self.module_name, app=app, square_dependency=square_dependency)

        ok = self.engine.enable_function_level_instrumentation(
            code=square_dependency.__code__,
            func=square_dependency,
            function_key=f"{self.module_name}.square_dependency",
            module_name=self.module_name,
            qualified_name="square_dependency",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        calls = [d.call for d in route.dependant.dependencies]
        self.assertIn(square_dependency, calls)  # target instrumented
        self.assertIn(inner_dep, calls)  # the other dep left intact

        # Invoking the target fires; invoking the untouched dep does not.
        target = next(d.call for d in route.dependant.dependencies if d.call is square_dependency)
        self.assertEqual(target(), 49)
        self.assertEqual(len(self.snapshots), 1)
        self.snapshots.clear()

        other = next(d.call for d in route.dependant.dependencies if d.call is inner_dep)
        self.assertEqual(other(), 1)
        self.assertEqual(len(self.snapshots), 0)

    # ------------------------------------------------------------------
    # Integration: arm a route handler, fire via the route table
    # ------------------------------------------------------------------

    def test_replace_function_patches_fastapi_routes(self):
        """Engine arming a route handler: invoking via FastAPI's route table
        produces a snapshot. (No setattr/route-table patching required — the
        __code__ mutation is in-place.)"""
        try:
            from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/hello")
        def my_route():
            return "hello"

        route = _route_for(app, "/hello")
        original = route.endpoint

        _make_module(self.module_name, app=app, my_route=original)

        ok = self.engine.enable_function_level_instrumentation(
            code=original.__code__,
            func=original,
            function_key=f"{self.module_name}.my_route",
            module_name=self.module_name,
            qualified_name="my_route",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # FastAPI's stored references are the SAME function object — engine
        # mutated its __code__ — so calling through the route table fires.
        self.assertIs(route.endpoint, original)
        self.assertIs(route.dependant.call, original)
        self.assertEqual(route.endpoint(), "hello")
        self.assertEqual(len(self.snapshots), 1)

    # ------------------------------------------------------------------
    # Integration: disarm restores the original __code__
    # ------------------------------------------------------------------

    def test_restore_function_restores_fastapi_routes(self):
        """Engine disarm restores original __code__: invoking via FastAPI's
        route table after disable produces NO snapshot."""
        try:
            from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/hello")
        def my_route():
            return "original"

        route = _route_for(app, "/hello")
        original = route.endpoint
        original_code = original.__code__

        _make_module(self.module_name, app=app, my_route=original)

        self.engine.enable_function_level_instrumentation(
            code=original.__code__,
            func=original,
            function_key=f"{self.module_name}.my_route",
            module_name=self.module_name,
            qualified_name="my_route",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )

        # Sanity: armed call fires.
        route.endpoint()
        self.assertEqual(len(self.snapshots), 1)
        self.snapshots.clear()

        self.engine.disable_function_level_instrumentation(code=original_code, func=original)

        # __code__ restored; calling via the route table emits no new snapshot.
        self.assertIs(original.__code__, original_code)
        self.assertEqual(route.endpoint(), "original")
        self.assertEqual(len(self.snapshots), 0)


if __name__ == "__main__":
    unittest.main()
