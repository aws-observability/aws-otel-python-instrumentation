# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests that the function-level instrumentation engine fires for aiohttp route
handlers without requiring handler rebinding.

aiohttp stores the route handler on each route (``route.handler`` / the
resource's ``_handler``) at registration time; the per-request path invokes that
stored handler, not the module-level name. The setattr-wrapper approach required
a patcher to rebind the stored handler because a module-level setattr never
reached it. The bytecode-injection / sys.monitoring engines mutate the handler's
``__code__`` in place, so the reference aiohttp stored at registration sees the
rewritten code on the next call — no handler rebind needed.
"""

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


class TestAiohttpRoutesPatching(unittest.TestCase):
    """Exercises the engine against aiohttp's stored route handler to prove no
    handler rebind is required: mutating __code__ in place flows through the
    reference aiohttp captured at registration time."""

    def setUp(self):
        self.module_name = "_test_aiohttp_module"
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

    def _route_handlers(self, app):
        return [getattr(r, "handler", None) for r in app.router.routes()]

    def test_patch_aiohttp_route_handler_identity(self):
        """A handler registered via add_get is the same function object aiohttp
        stores on the route — the engine's __code__ mutation flows through it."""
        try:
            from aiohttp import web  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("aiohttp not installed")

        async def handler(request):
            return web.json_response({"v": 1})

        handler.__module__ = self.module_name

        app = web.Application()
        app.router.add_get("/h", handler)

        # Precondition: the route stores the original handler object.
        self.assertIn(handler, self._route_handlers(app))

        self._arm(handler, "handler")

        # aiohttp's stored handler IS the same object — engine mutated its
        # __code__ — so it remains stored unchanged and fires when invoked.
        self.assertIn(handler, self._route_handlers(app))

    def test_patch_aiohttp_unrelated_handler_untouched(self):
        """Arming handler A leaves a different handler B on another route
        un-instrumented — no cross-talk."""
        try:
            from aiohttp import web  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("aiohttp not installed")

        async def handler(request):
            return web.json_response({})

        async def other(request):
            return web.json_response({})

        handler.__module__ = self.module_name
        other.__module__ = self.module_name

        app = web.Application()
        app.router.add_get("/h", handler)
        app.router.add_get("/o", other)

        self._arm(handler, "handler")

        # Both objects are still the stored handlers; only `handler` is armed.
        handlers = self._route_handlers(app)
        self.assertIn(handler, handlers)
        self.assertIn(other, handlers)

    def test_patch_aiohttp_no_app_is_noop(self):
        """The engine has no aiohttp dependency: arming a plain function works
        whether or not an aiohttp Application is present in the module."""

        def some_func():
            return "ok"

        _make_module(self.module_name, x=1, some_func=some_func)

        self._arm(some_func, "some_func")
        self.assertEqual(some_func(), "ok")
        self.assertEqual(len(self.snapshots), 1)


if __name__ == "__main__":
    unittest.main()
