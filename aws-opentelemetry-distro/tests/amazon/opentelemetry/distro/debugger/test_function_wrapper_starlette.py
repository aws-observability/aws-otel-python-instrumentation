# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for Starlette route patching in _function_wrapper.py.

A pure-Starlette app stores ``route.app = request_response(endpoint)`` at import time,
capturing the original handler in a closure; the per-request path invokes ``route.app``,
never ``route.endpoint``. So a module-level setattr (or an endpoint-only rebind) never
reaches the live handler and a function-level breakpoint silently never fires. The fix
rebuilds ``route.app`` from the wrapper. FastAPI subclasses Starlette and is handled by
its own patcher, so the Starlette patcher must SKIP FastAPI apps (no double-patch).
"""

import sys
import types
import unittest

from amazon.opentelemetry.distro.debugger._function_wrapper import FunctionWrapper


def _make_module(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _remove_module(name):
    sys.modules.pop(name, None)


def _route_for(app, path):
    for route in app.router.routes:
        if getattr(route, "path", None) == path:
            return route
    raise AssertionError(f"No route found for path {path}")


class TestStarletteRoutesPatching(unittest.TestCase):
    def setUp(self):
        self.module_name = "_test_starlette_module"
        _remove_module(self.module_name)

    def tearDown(self):
        _remove_module(self.module_name)

    def test_patch_starlette_route_rebuilds_app_and_fires(self):
        """A pure-Starlette route endpoint is rebound: route.app is rebuilt from the wrapper,
        and invoking the route's ASGI app actually calls the wrapper."""
        try:
            from starlette.applications import Starlette
            from starlette.responses import JSONResponse
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not installed")

        async def handler(request):
            return JSONResponse({"v": 1})

        app = Starlette(routes=[Route("/h", handler)])
        route = _route_for(app, "/h")

        # Precondition: route.app is a request_response closure over the ORIGINAL handler,
        # and route.endpoint is the original.
        self.assertIs(route.endpoint, handler)
        original_app = route.app

        hits = {"n": 0}

        async def wrapper(request):
            hits["n"] += 1
            return JSONResponse({"v": "wrapped"})

        wrapper.__name__ = handler.__name__
        wrapper.__module__ = handler.__module__

        mod = _make_module(self.module_name, app=app, handler=handler)
        FunctionWrapper._patch_starlette_routes(mod, handler, wrapper)

        # endpoint rebound AND route.app rebuilt (a new closure, not the original).
        self.assertIs(route.endpoint, wrapper)
        self.assertIsNot(route.app, original_app)

        # Drive the route's ASGI app end-to-end and confirm the wrapper actually runs.
        import asyncio

        async def _drive():
            scope = {"type": "http", "method": "GET", "headers": [], "path": "/h"}
            sent = []

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(msg):
                sent.append(msg)

            await route.app(scope, receive, send)
            return sent

        asyncio.run(_drive())
        self.assertEqual(hits["n"], 1, "wrapper should have been invoked via route.app")

    def test_patch_starlette_name_module_fallback(self):
        """When identity doesn't match (e.g. external wrapping) the name+module fallback rebinds."""
        try:
            from starlette.applications import Starlette
            from starlette.responses import JSONResponse
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not installed")

        async def handler(request):
            return JSONResponse({})

        app = Starlette(routes=[Route("/h", handler)])
        route = _route_for(app, "/h")

        # Simulate a different-identity callable that preserves __name__/__module__.
        async def other_same_name(request):
            return JSONResponse({})

        other_same_name.__name__ = handler.__name__
        other_same_name.__module__ = handler.__module__
        route.endpoint = other_same_name

        async def di_wrapper(request):
            return JSONResponse({})

        di_wrapper.__name__ = handler.__name__
        di_wrapper.__module__ = handler.__module__

        mod = _make_module(self.module_name, app=app, handler=handler)
        FunctionWrapper._patch_starlette_routes(mod, handler, di_wrapper)
        self.assertIs(route.endpoint, di_wrapper)

    def test_patch_starlette_skips_fastapi_app(self):
        """FastAPI is a Starlette subclass handled by _patch_fastapi_routes; the Starlette
        patcher must NOT touch a FastAPI app (no double-patch)."""
        try:
            from fastapi import FastAPI
        except ImportError:
            self.skipTest("FastAPI not installed")

        app = FastAPI()

        @app.get("/orders")
        def get_orders():
            return "orders"

        route = _route_for(app, "/orders")
        original_endpoint = route.endpoint
        original_app = route.app

        wrapper = lambda: "wrapper"  # noqa: E731
        wrapper.__name__ = original_endpoint.__name__
        wrapper.__module__ = original_endpoint.__module__

        mod = _make_module(self.module_name, app=app, get_orders=original_endpoint)
        FunctionWrapper._patch_starlette_routes(mod, original_endpoint, wrapper)

        # Untouched by the Starlette patcher (FastAPI's own patcher handles it).
        self.assertIs(route.endpoint, original_endpoint)
        self.assertIs(route.app, original_app)

    def test_patch_starlette_route_with_middleware_is_skipped(self):
        """A route carrying per-route middleware is left untouched (rebuilding route.app
        would drop the middleware) — a safe no-op rather than a silent middleware loss."""
        try:
            from starlette.applications import Starlette
            from starlette.middleware import Middleware
            from starlette.responses import JSONResponse
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not installed")

        class _NoopMW:
            def __init__(self, app):
                self.app = app

            async def __call__(self, scope, receive, send):
                await self.app(scope, receive, send)

        async def handler(request):
            return JSONResponse({})

        route = Route("/h", handler, middleware=[Middleware(_NoopMW)])
        app = Starlette(routes=[route])
        app_before = route.app

        # Pin that this route IS the matched target (endpoint identity holds), so the test
        # exercises "matched-but-middleware → safe skip" rather than passing because the
        # route simply didn't match.
        self.assertIs(route.endpoint, handler)
        # And the middleware-wrapped app is genuinely NOT the plain request_response closure
        # over the endpoint — i.e. the skip is driven by the structural detection, not by a
        # name check that could drift.
        self.assertFalse(FunctionWrapper._starlette_app_wraps_endpoint(route.app, handler))

        async def wrapper(request):
            return JSONResponse({})

        wrapper.__name__ = handler.__name__
        wrapper.__module__ = handler.__module__

        mod = _make_module(self.module_name, app=app, handler=handler)
        FunctionWrapper._patch_starlette_routes(mod, handler, wrapper)
        # route.app NOT rebuilt (middleware preserved) and endpoint left intact.
        self.assertIs(route.app, app_before)
        self.assertIs(route.endpoint, handler)

    def test_patch_starlette_no_starlette_app_is_noop(self):
        """A module with no Starlette app instance is a safe no-op."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name, x=1, y="z")
        # Should not raise.
        FunctionWrapper._patch_starlette_routes(mod, original, wrapper)

    def test_starlette_app_wraps_endpoint_detection(self):
        """The structural plain-app detection recognizes async and sync endpoints, and
        rejects middleware-wrapped apps — independent of any closure function name."""
        try:
            from starlette.middleware import Middleware
            from starlette.responses import JSONResponse
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not installed")

        async def async_handler(request):
            return JSONResponse({})

        def sync_handler(request):
            return JSONResponse({})

        # Async endpoint: captured directly in the request_response closure -> True.
        async_route = Route("/a", async_handler)
        self.assertTrue(FunctionWrapper._starlette_app_wraps_endpoint(async_route.app, async_handler))

        # Sync endpoint: wrapped in functools.partial(run_in_threadpool, endpoint) -> True.
        sync_route = Route("/s", sync_handler)
        self.assertTrue(FunctionWrapper._starlette_app_wraps_endpoint(sync_route.app, sync_handler))

        # Wrong endpoint must not match the closure.
        self.assertFalse(FunctionWrapper._starlette_app_wraps_endpoint(async_route.app, sync_handler))

        # Middleware-wrapped app (no __closure__) -> False.
        class _NoopMW:
            def __init__(self, app):
                self.app = app

            async def __call__(self, scope, receive, send):
                await self.app(scope, receive, send)

        mw_route = Route("/m", async_handler, middleware=[Middleware(_NoopMW)])
        self.assertFalse(FunctionWrapper._starlette_app_wraps_endpoint(mw_route.app, async_handler))

        # Non-closure values are safe (return False, never raise).
        self.assertFalse(FunctionWrapper._starlette_app_wraps_endpoint(None, async_handler))
        self.assertFalse(FunctionWrapper._starlette_app_wraps_endpoint(object(), async_handler))


if __name__ == "__main__":
    unittest.main()
