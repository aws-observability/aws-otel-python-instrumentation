# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for aiohttp route-handler patching in _function_wrapper.py.

aiohttp stores the route handler on each route (``route.handler`` / the resource's
``_handler``) at registration time; the per-request path invokes that stored handler,
not the module-level name. A module-level setattr doesn't reach it, so a function-level
breakpoint on an aiohttp handler silently never fires. The fix rebinds the stored handler.
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


class TestAiohttpRoutesPatching(unittest.TestCase):
    def setUp(self):
        self.module_name = "_test_aiohttp_module"
        _remove_module(self.module_name)

    def tearDown(self):
        _remove_module(self.module_name)

    def _route_handlers(self, app):
        return [getattr(r, "handler", None) for r in app.router.routes()]

    def test_patch_aiohttp_route_handler_identity(self):
        """A handler registered via add_get is rebound to the wrapper by identity."""
        try:
            from aiohttp import web
        except ImportError:
            self.skipTest("aiohttp not installed")

        async def handler(request):
            return web.json_response({"v": 1})

        app = web.Application()
        app.router.add_get("/h", handler)

        # Precondition: the route stores the original handler.
        self.assertIn(handler, self._route_handlers(app))

        async def wrapper(request):
            return web.json_response({"v": "wrapped"})

        wrapper.__name__ = handler.__name__
        wrapper.__module__ = handler.__module__

        mod = _make_module(self.module_name, app=app, handler=handler)
        FunctionWrapper._patch_aiohttp_routes(mod, handler, wrapper)

        handlers = self._route_handlers(app)
        self.assertIn(wrapper, handlers)
        self.assertNotIn(handler, handlers)

    def test_patch_aiohttp_name_module_fallback(self):
        """When identity doesn't match, name+module fallback rebinds the handler."""
        try:
            from aiohttp import web
        except ImportError:
            self.skipTest("aiohttp not installed")

        async def handler(request):
            return web.json_response({})

        app = web.Application()
        app.router.add_get("/h", handler)

        # Force a different-identity callable with the same name+module onto the route.
        async def other_same_name(request):
            return web.json_response({})

        other_same_name.__name__ = handler.__name__
        other_same_name.__module__ = handler.__module__
        route = next(iter(app.router.routes()))
        route._handler = other_same_name  # pylint: disable=protected-access

        async def di_wrapper(request):
            return web.json_response({})

        di_wrapper.__name__ = handler.__name__
        di_wrapper.__module__ = handler.__module__

        mod = _make_module(self.module_name, app=app, handler=handler)
        FunctionWrapper._patch_aiohttp_routes(mod, handler, di_wrapper)
        self.assertIn(di_wrapper, self._route_handlers(app))

    def test_patch_aiohttp_unrelated_handler_untouched(self):
        """A non-matching handler on another route is not rebound."""
        try:
            from aiohttp import web
        except ImportError:
            self.skipTest("aiohttp not installed")

        async def handler(request):
            return web.json_response({})

        async def other(request):
            return web.json_response({})

        app = web.Application()
        app.router.add_get("/h", handler)
        app.router.add_get("/o", other)

        wrapper = lambda request: None  # noqa: E731
        wrapper.__name__ = handler.__name__
        wrapper.__module__ = handler.__module__

        mod = _make_module(self.module_name, app=app, handler=handler, other=other)
        FunctionWrapper._patch_aiohttp_routes(mod, handler, wrapper)

        handlers = self._route_handlers(app)
        self.assertIn(other, handlers)  # untouched
        self.assertIn(wrapper, handlers)

    def test_patch_aiohttp_no_app_is_noop(self):
        """A module with no aiohttp Application is a safe no-op."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name, x=1)
        FunctionWrapper._patch_aiohttp_routes(mod, original, wrapper)  # should not raise


if __name__ == "__main__":
    unittest.main()
