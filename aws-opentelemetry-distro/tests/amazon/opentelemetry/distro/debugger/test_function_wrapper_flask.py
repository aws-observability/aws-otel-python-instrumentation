# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests that the function-level instrumentation engine fires for Flask route
handlers without requiring view_functions patching.

Flask's @app.route() decorator stores a direct reference to the route
function in app.view_functions at import time. The setattr-wrapper approach
required us to patch that dict because replacing the module-level name does
not update Flask's internal references. The bytecode-injection / sys.monitoring
engines mutate the function's __code__ in place, so any reference Flask is
holding (or any other framework) sees the rewritten code on the next call —
no view_functions patch needed.
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


class TestFlaskViewFunctionsPatching(unittest.TestCase):
    """Exercises the engine against Flask app.view_functions to prove no
    framework-references patching is required: mutating __code__ in place
    flows through any reference Flask holds in its internal dict."""

    def setUp(self):
        self.module_name = "_test_flask_module"
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
    # _patch_flask_view_functions
    # ------------------------------------------------------------------

    def test_patch_flask_view_functions_identity_match(self):
        """When view_func is the same identity as the armed function, the engine's
        __code__ mutation flows through — no view_functions patch needed."""
        try:
            from flask import Flask  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Flask not installed")

        def my_view():
            return "original"

        flask_app = Flask(__name__)
        flask_app.view_functions["my_view"] = my_view

        mod = _make_module(self.module_name, app=flask_app, my_view=my_view)
        del mod  # silence linter; module exists in sys.modules

        ok = self.engine.enable_function_level_instrumentation(
            code=my_view.__code__,
            func=my_view,
            function_key=f"{self.module_name}.my_view",
            module_name=self.module_name,
            qualified_name="my_view",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # Calling through view_functions (Flask's internal reference) still
        # fires the engine — same function object, mutated __code__.
        result = flask_app.view_functions["my_view"]()
        self.assertEqual(result, "original")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_flask_view_functions_name_fallback(self):
        """When Flask holds an OTel-wrapped view (different identity from the
        underlying function), arming the underlying function still produces a
        snapshot when the wrapper invokes it — because the wrapper calls the
        same function object whose __code__ we mutated."""
        try:
            from flask import Flask  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Flask not installed")

        def get_order():
            return "original"

        # Simulate OTel's auto-instrumentation wrapping the view.
        def otel_wrapped():
            return get_order()

        otel_wrapped.__name__ = "get_order"

        flask_app = Flask(__name__)
        flask_app.view_functions["get_order"] = otel_wrapped

        mod = _make_module(self.module_name, app=flask_app, get_order=get_order)
        del mod

        ok = self.engine.enable_function_level_instrumentation(
            code=get_order.__code__,
            func=get_order,
            function_key=f"{self.module_name}.get_order",
            module_name=self.module_name,
            qualified_name="get_order",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # OTel wrapper still calls the underlying get_order, whose __code__
        # is now mutated. Snapshot fires.
        result = flask_app.view_functions["get_order"]()
        self.assertEqual(result, "original")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_flask_no_flask_installed(self):
        """When Flask is not installed, the engine arms unaffected — engine
        path has no Flask dependency at all."""

        def some_func():
            return "ok"

        mod = _make_module(self.module_name, some_func=some_func)
        del mod

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

    def test_patch_flask_no_flask_app_in_module(self):
        """When module has no Flask app instance, the engine still arms the
        function correctly — engine doesn't scan for Flask apps."""
        try:
            from flask import Flask  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            self.skipTest("Flask not installed")

        def some_func():
            return "no flask here"

        mod = _make_module(self.module_name, some_func=some_func, x=42, y="hello")
        del mod

        ok = self.engine.enable_function_level_instrumentation(
            code=some_func.__code__,
            func=some_func,
            function_key=f"{self.module_name}.some_func",
            module_name=self.module_name,
            qualified_name="some_func",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        self.assertEqual(some_func(), "no flask here")
        self.assertEqual(len(self.snapshots), 1)

    def test_patch_flask_function_not_in_view_functions(self):
        """Arming function A leaves a different function B in view_functions
        un-instrumented — no cross-talk."""
        try:
            from flask import Flask  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Flask not installed")

        def helper_func():
            return "helper"

        def other_view():
            return "other"

        flask_app = Flask(__name__)
        flask_app.view_functions["other_view"] = other_view

        mod = _make_module(self.module_name, app=flask_app, helper_func=helper_func)
        del mod

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
        self.assertIs(flask_app.view_functions["other_view"], other_view)
        self.assertEqual(flask_app.view_functions["other_view"](), "other")
        self.assertEqual(len(self.snapshots), 0)

    # ------------------------------------------------------------------
    # Integration: enable_function_level_instrumentation flows through Flask
    # ------------------------------------------------------------------

    def test_replace_function_patches_flask_view_functions(self):
        """Engine arming a route handler: invoking via Flask's view_functions
        produces a snapshot. (No setattr/view_functions patching required —
        the __code__ mutation is in-place.)"""
        try:
            from flask import Flask  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Flask not installed")

        def my_route():
            return "hello"

        flask_app = Flask(__name__)
        flask_app.view_functions["my_route"] = my_route

        mod = _make_module(self.module_name, app=flask_app, my_route=my_route)
        del mod

        ok = self.engine.enable_function_level_instrumentation(
            code=my_route.__code__,
            func=my_route,
            function_key=f"{self.module_name}.my_route",
            module_name=self.module_name,
            qualified_name="my_route",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)

        # Flask's stored reference is the SAME function object — engine
        # mutated its __code__ — so calling through view_functions fires.
        self.assertEqual(flask_app.view_functions["my_route"](), "hello")
        self.assertEqual(len(self.snapshots), 1)

    # ------------------------------------------------------------------
    # Integration: disable_function_level_instrumentation
    # ------------------------------------------------------------------

    def test_restore_function_restores_flask_view_functions(self):
        """Engine disarm restores original __code__: invoking via Flask's
        view_functions after disable produces NO snapshot."""
        try:
            from flask import Flask  # pylint: disable=import-outside-toplevel
        except ImportError:
            self.skipTest("Flask not installed")

        def my_route():
            return "original"

        flask_app = Flask(__name__)
        flask_app.view_functions["my_route"] = my_route

        mod = _make_module(self.module_name, app=flask_app, my_route=my_route)
        del mod

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
        flask_app.view_functions["my_route"]()
        self.assertEqual(len(self.snapshots), 1)
        self.snapshots.clear()

        self.engine.disable_function_level_instrumentation(code=original_code, func=my_route)

        # __code__ restored; calling via view_functions emits no new snapshot.
        self.assertIs(my_route.__code__, original_code)
        self.assertEqual(flask_app.view_functions["my_route"](), "original")
        self.assertEqual(len(self.snapshots), 0)


if __name__ == "__main__":
    unittest.main()
