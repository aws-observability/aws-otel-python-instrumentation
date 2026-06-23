# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests that the function-level instrumentation engine fires for Starlette route
handlers without requiring route.app rebuilding.

A pure-Starlette app stores ``route.app = request_response(endpoint)`` at import
time, capturing the original handler in a closure; the per-request path invokes
``route.app``, never ``route.endpoint``. The setattr-wrapper approach required a
patcher to rebuild ``route.app`` from the wrapper because a module-level setattr
(or an endpoint-only rebind) never reached the live handler. The
bytecode-injection / sys.monitoring engines mutate the handler's ``__code__`` in
place, so the closure Starlette is holding sees the rewritten code on the next
call — no route.app rebuild needed.
"""

import asyncio
import sys
import types
import unittest

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger._function_wrapper import set_snapshot_emitter


def _make_module(name, **attrs):
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
    for route in app.router.routes:
        if getattr(route, "path", None) == path:
            return route
    raise AssertionError(f"No route found for path {path}")


async def _drive_route(route, path):
    """Drive a Starlette route's ASGI app end-to-end (single GET request)."""
    scope = {"type": "http", "method": "GET", "headers": [], "path": path}

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_msg):
        return None

    await route.app(scope, receive, send)


class TestStarletteRoutesPatching(unittest.TestCase):
    """Exercises the engine against a pure-Starlette route.app closure to prove no
    route.app rebuild is required: mutating __code__ in place flows through the
    closure Starlette captured at import time."""

    def setUp(self):
        self.module_name = "_test_starlette_module"
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

    def _arm(self, func, name):
        ok = self.engine.enable_function_level_instrumentation(
            code=func.__code__,
            func=func,
            function_key=f"{self.module_name}.{name}",
            module_name=self.module_name,
            qualified_name=name,
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

    def test_patch_starlette_route_rebuilds_app_and_fires(self):
        """A pure-Starlette route handler is armed: driving the route's ASGI app
        (route.app, the closure over the original handler) fires the engine —
        the closure holds the same function object whose __code__ was mutated."""
        try:
            from starlette.applications import Starlette  # pylint: disable=import-outside-toplevel
            from starlette.responses import JSONResponse  # pylint: disable=import-outside-toplevel
            from starlette.routing import Route  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Starlette not installed")

        hits = {"n": 0}

        async def handler(request):
            hits["n"] += 1
            return JSONResponse({"v": 1})

        handler.__module__ = self.module_name

        app = Starlette(routes=[Route("/h", handler)])
        route = _route_for(app, "/h")

        # Precondition: route.app is a request_response closure over the ORIGINAL handler.
        self.assertIs(route.endpoint, handler)

        self._arm(handler, "handler")

        # Drive the route's ASGI app end-to-end: the closure invokes the same
        # handler object, whose __code__ is now mutated. The handler runs (proving
        # route.app reaches it) and the engine fires a snapshot for it.
        # Assert on the handler's own invocation count rather than the shared
        # snapshot list, which on the global sys.monitoring (3.12) path can also
        # collect snapshots for unrelated code armed by other tests in-process.
        asyncio.run(_drive_route(route, "/h"))
        self.assertEqual(hits["n"], 1, "handler should run exactly once via route.app")
        self.assertGreaterEqual(len(self.snapshots), 1)

    def test_patch_starlette_name_module_fallback(self):
        """When something replaced the endpoint with a different-identity callable
        that calls the underlying handler (as functools.wraps does), arming the
        underlying handler still fires when the wrapper invokes it."""
        try:
            from starlette.applications import Starlette  # pylint: disable=import-outside-toplevel
            from starlette.responses import JSONResponse  # pylint: disable=import-outside-toplevel
            from starlette.routing import Route  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Starlette not installed")

        hits = {"n": 0}

        async def handler(request):
            hits["n"] += 1
            return JSONResponse({})

        handler.__module__ = self.module_name

        # An external wrapper (different identity) that still calls the underlying handler.
        async def otel_wrapped(request):
            return await handler(request)

        otel_wrapped.__name__ = handler.__name__
        otel_wrapped.__module__ = handler.__module__

        app = Starlette(routes=[Route("/h", otel_wrapped)])
        route = _route_for(app, "/h")

        self._arm(handler, "handler")

        # The wrapper invokes the underlying handler, whose __code__ is mutated.
        # Assert on the handler's own invocation count (leak-proof on the global
        # sys.monitoring path); the engine fires a snapshot for it.
        asyncio.run(_drive_route(route, "/h"))
        self.assertEqual(hits["n"], 1, "underlying handler should run exactly once via the wrapper")
        self.assertGreaterEqual(len(self.snapshots), 1)

    def test_patch_starlette_skips_fastapi_app(self):
        """FastAPI is a Starlette subclass with its own route table; arming a
        FastAPI route handler fires through FastAPI's stored references and the
        engine leaves the Starlette closure path untouched (no double-fire)."""
        try:
            from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/orders")
        def get_orders():
            return "orders"

        route = _route_for(app, "/orders")
        original_endpoint = route.endpoint

        self._arm(original_endpoint, "get_orders")

        # FastAPI holds the same function object on the route table — engine
        # mutated its __code__ — so a single invocation fires exactly once.
        self.assertIs(route.endpoint, original_endpoint)
        self.assertEqual(route.endpoint(), "orders")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_starlette_no_starlette_app_is_noop(self):
        """The engine has no Starlette dependency: arming a plain function works
        whether or not a Starlette app is present in the module."""

        def some_func():
            return "ok"

        _make_module(self.module_name, x=1, y="z", some_func=some_func)

        self._arm(some_func, "some_func")
        self.assertEqual(some_func(), "ok")
        self.assertEqual(len(self.snapshots), 1)


if __name__ == "__main__":
    unittest.main()
