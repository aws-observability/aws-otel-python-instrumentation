# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for Flask view_functions patching in _function_wrapper.py.

Validates that _replace_function_in_module and restore_function correctly
patch Flask's internal view_functions dict when instrumenting route handlers.
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


class TestFlaskViewFunctionsPatching(unittest.TestCase):
    """Tests for _patch_flask_view_functions and its integration."""

    def setUp(self):
        self.module_name = "_test_flask_module"
        _remove_module(self.module_name)

    def tearDown(self):
        _remove_module(self.module_name)

    # ------------------------------------------------------------------
    # _patch_flask_view_functions
    # ------------------------------------------------------------------

    @patch("amazon.opentelemetry.distro.debugger._function_wrapper.FunctionWrapper._resolve_module")
    def test_patch_flask_view_functions_identity_match(self, mock_resolve):
        """When view_func is original_func, identity-based patching works."""
        try:
            from flask import Flask
        except ImportError:
            self.skipTest("Flask not installed")

        original = lambda: "original"  # noqa: E731
        wrapper = lambda: "wrapper"  # noqa: E731
        original.__name__ = "my_view"

        flask_app = Flask(__name__)
        flask_app.view_functions["my_view"] = original

        mod = _make_module(self.module_name, app=flask_app, my_view=original)
        mock_resolve.return_value = mod

        FunctionWrapper._patch_flask_view_functions(mod, original, wrapper)

        self.assertIs(flask_app.view_functions["my_view"], wrapper)

    @patch("amazon.opentelemetry.distro.debugger._function_wrapper.FunctionWrapper._resolve_module")
    def test_patch_flask_view_functions_name_fallback(self, mock_resolve):
        """When identity doesn't match (e.g. OTel wrapping), name-based fallback works."""
        try:
            from flask import Flask
        except ImportError:
            self.skipTest("Flask not installed")

        original = lambda: "original"  # noqa: E731
        otel_wrapped = lambda: "otel_wrapped"  # noqa: E731
        di_wrapper = lambda: "di_wrapper"  # noqa: E731
        original.__name__ = "get_order"
        otel_wrapped.__name__ = "get_order"

        flask_app = Flask(__name__)
        # Flask stores the OTel-wrapped version (different identity from original)
        flask_app.view_functions["get_order"] = otel_wrapped

        mod = _make_module(self.module_name, app=flask_app, get_order=original)
        mock_resolve.return_value = mod

        FunctionWrapper._patch_flask_view_functions(mod, original, di_wrapper)

        self.assertIs(flask_app.view_functions["get_order"], di_wrapper)

    def test_patch_flask_no_flask_installed(self):
        """When Flask is not installed, patching is a no-op."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name, some_func=original)

        with patch.dict(sys.modules, {"flask": None}):
            # Should not raise
            FunctionWrapper._patch_flask_view_functions(mod, original, wrapper)

    def test_patch_flask_no_flask_app_in_module(self):
        """When module has no Flask app instances, patching is a no-op."""
        try:
            from flask import Flask  # noqa: F401
        except ImportError:
            self.skipTest("Flask not installed")

        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name, some_func=original, x=42, y="hello")

        # Should not raise
        FunctionWrapper._patch_flask_view_functions(mod, original, wrapper)

    def test_patch_flask_function_not_in_view_functions(self):
        """When function is not a route handler, view_functions is unchanged."""
        try:
            from flask import Flask
        except ImportError:
            self.skipTest("Flask not installed")

        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        original.__name__ = "helper_func"

        other_view = lambda: "other"  # noqa: E731
        other_view.__name__ = "other_view"

        flask_app = Flask(__name__)
        flask_app.view_functions["other_view"] = other_view

        mod = _make_module(self.module_name, app=flask_app, helper_func=original)

        FunctionWrapper._patch_flask_view_functions(mod, original, wrapper)

        # other_view should be unchanged
        self.assertIs(flask_app.view_functions["other_view"], other_view)

    # ------------------------------------------------------------------
    # Integration: _replace_function_in_module with Flask patching
    # ------------------------------------------------------------------

    def test_replace_function_patches_flask_view_functions(self):
        """_replace_function_in_module patches Flask view_functions."""
        try:
            from flask import Flask
        except ImportError:
            self.skipTest("Flask not installed")

        def my_route():
            return "hello"

        flask_app = Flask(__name__)
        flask_app.view_functions["my_route"] = my_route

        mod = _make_module(self.module_name, app=flask_app, my_route=my_route)

        wrapper = lambda: "wrapped"  # noqa: E731

        with patch.object(FunctionWrapper, "_resolve_module", return_value=mod):
            FunctionWrapper._replace_function_in_module(self.module_name, "my_route", wrapper)

        # Module attribute replaced
        self.assertIs(mod.my_route, wrapper)
        # Flask view_functions also patched
        self.assertIs(flask_app.view_functions["my_route"], wrapper)

    # ------------------------------------------------------------------
    # Integration: restore_function with Flask patching
    # ------------------------------------------------------------------

    def test_restore_function_restores_flask_view_functions(self):
        """restore_function restores Flask view_functions back to original."""
        try:
            from flask import Flask
        except ImportError:
            self.skipTest("Flask not installed")

        def original_route():
            return "original"

        def wrapped_route():
            return "wrapped"

        flask_app = Flask(__name__)
        # Simulate: DI already wrapped the function and patched view_functions
        flask_app.view_functions["my_route"] = wrapped_route

        mod = _make_module(self.module_name, app=flask_app, my_route=wrapped_route)

        with patch.object(FunctionWrapper, "_resolve_module", return_value=mod):
            result = FunctionWrapper.restore_function(self.module_name, "my_route", original_route)

        self.assertTrue(result)
        self.assertIs(mod.my_route, original_route)
        # Flask view_functions restored
        self.assertIs(flask_app.view_functions["my_route"], original_route)

    # ------------------------------------------------------------------
    # _patch_framework_references (dispatcher)
    # ------------------------------------------------------------------

    def test_patch_framework_references_calls_flask_patching(self):
        """_patch_framework_references delegates to _patch_flask_view_functions."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name)

        with patch.object(FunctionWrapper, "_patch_flask_view_functions") as mock_flask:
            FunctionWrapper._patch_framework_references(mod, original, wrapper)
            mock_flask.assert_called_once_with(mod, original, wrapper)

    def test_patch_framework_references_swallows_exceptions(self):
        """_patch_framework_references never raises."""
        original = lambda: None  # noqa: E731
        wrapper = lambda: None  # noqa: E731
        mod = _make_module(self.module_name)

        with patch.object(FunctionWrapper, "_patch_flask_view_functions", side_effect=RuntimeError("boom")):
            # Should not raise
            FunctionWrapper._patch_framework_references(mod, original, wrapper)


if __name__ == "__main__":
    unittest.main()
