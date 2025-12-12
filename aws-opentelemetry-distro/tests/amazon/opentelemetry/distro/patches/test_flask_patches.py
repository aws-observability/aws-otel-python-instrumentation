# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import flask
from werkzeug.test import Client
from werkzeug.wrappers import Response

from amazon.opentelemetry.distro.patches._flask_patches import _apply_flask_instrumentation_patches
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.test.test_base import TestBase

# Store truly original Flask methods at module level before any patches
_ORIGINAL_FLASK_ADD_URL_RULE = flask.Flask.add_url_rule
_ORIGINAL_FLASK_DISPATCH_REQUEST = flask.Flask.dispatch_request

# Store original FlaskInstrumentor methods before any patches
_ORIGINAL_FLASK_INSTRUMENTOR_INSTRUMENT = FlaskInstrumentor._instrument
_ORIGINAL_FLASK_INSTRUMENTOR_UNINSTRUMENT = FlaskInstrumentor._uninstrument


class TestFlaskPatchesRealApp(TestBase):
    """Test Flask patches using a real Flask application."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

        # Always start with clean Flask methods
        flask.Flask.add_url_rule = _ORIGINAL_FLASK_ADD_URL_RULE
        flask.Flask.dispatch_request = _ORIGINAL_FLASK_DISPATCH_REQUEST

        # Always start with clean FlaskInstrumentor methods
        FlaskInstrumentor._instrument = _ORIGINAL_FLASK_INSTRUMENTOR_INSTRUMENT
        FlaskInstrumentor._uninstrument = _ORIGINAL_FLASK_INSTRUMENTOR_UNINSTRUMENT

        # Create real Flask app
        self.app = flask.Flask(__name__)

        # Add test routes
        @self.app.route("/hello")
        def hello():
            return "Hello!"

        @self.app.route("/error")
        def error_endpoint():
            raise ValueError("Test error")

        @self.app.route("/simple")
        def simple():
            return "OK"

        # Create test client
        self.client = Client(self.app, Response)

    def tearDown(self):
        """Clean up after tests."""
        super().tearDown()

        # Always restore original Flask methods to avoid contamination between tests
        flask.Flask.add_url_rule = _ORIGINAL_FLASK_ADD_URL_RULE
        flask.Flask.dispatch_request = _ORIGINAL_FLASK_DISPATCH_REQUEST

        # Always restore original FlaskInstrumentor methods to avoid contamination between tests
        FlaskInstrumentor._instrument = _ORIGINAL_FLASK_INSTRUMENTOR_INSTRUMENT
        FlaskInstrumentor._uninstrument = _ORIGINAL_FLASK_INSTRUMENTOR_UNINSTRUMENT

        # Clear any stored class attributes from patches
        for attr_name in list(vars(FlaskInstrumentor).keys()):
            if attr_name.startswith("_original_flask_"):
                delattr(FlaskInstrumentor, attr_name)

        # CRITICAL: Clear instance attributes from the singleton FlaskInstrumentor instance
        # FlaskInstrumentor is a singleton, so we need to clean up the instance attributes
        instrumentor_instance = FlaskInstrumentor()
        instance_attrs_to_remove = []
        for attr_name in dir(instrumentor_instance):
            if attr_name.startswith("_original_flask_"):
                instance_attrs_to_remove.append(attr_name)

        for attr_name in instance_attrs_to_remove:
            if hasattr(instrumentor_instance, attr_name):
                delattr(instrumentor_instance, attr_name)

        # Clean up instrumentor - use global uninstrument
        try:
            FlaskInstrumentor().uninstrument()
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    def test_flask_patches_with_real_app(self):
        """Test Flask patches with real Flask app covering various scenarios."""
        # Store original Flask methods - use the module level constants
        original_add_url_rule = _ORIGINAL_FLASK_ADD_URL_RULE
        original_dispatch_request = _ORIGINAL_FLASK_DISPATCH_REQUEST

        # Apply patches FIRST
        _apply_flask_instrumentation_patches()

        # Create instrumentor and manually call _instrument to trigger Flask method wrapping
        instrumentor = FlaskInstrumentor()
        instrumentor._instrument()

        # Check if Flask methods were wrapped by patches
        current_add_url_rule = flask.Flask.add_url_rule
        current_dispatch_request = flask.Flask.dispatch_request

        # Test that Flask methods are actually wrapped - this is the core functionality
        self.assertNotEqual(current_add_url_rule, original_add_url_rule, "Flask.add_url_rule should be wrapped")
        self.assertNotEqual(
            current_dispatch_request, original_dispatch_request, "Flask.dispatch_request should be wrapped"
        )

        # Test a request to trigger the patches
        instrumentor.instrument_app(self.app)
        resp = self.client.get("/hello")
        self.assertEqual(200, resp.status_code)

        # Test uninstrumentation - this should restore original Flask methods
        instrumentor._uninstrument()

        # Check if Flask methods were restored
        restored_add_url_rule = flask.Flask.add_url_rule
        restored_dispatch_request = flask.Flask.dispatch_request

        # Methods should be restored to original after uninstrument
        self.assertEqual(restored_add_url_rule, original_add_url_rule, "Flask.add_url_rule should be restored")
        self.assertEqual(
            restored_dispatch_request, original_dispatch_request, "Flask.dispatch_request should be restored"
        )

    def test_flask_patches_import_error_handling(self):
        """Test Flask patches with import errors."""
        # Test that patches handle import errors gracefully by mocking sys.modules
        import sys

        original_modules = sys.modules.copy()

        try:
            # Remove flask from sys.modules to simulate import error
            if "flask" in sys.modules:
                del sys.modules["flask"]

            # Should not raise exception even with missing flask
            _apply_flask_instrumentation_patches()

        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_flask_patches_view_function_decoration(self):
        """Test Flask patches view function decoration edge cases."""
        # Create instrumentor and apply patches
        instrumentor = FlaskInstrumentor()
        instrumentor.instrument_app(self.app)
        _apply_flask_instrumentation_patches()
        instrumentor._instrument()

        # Test adding routes with None view_func (edge case)
        try:
            self.app.add_url_rule("/test_none", "test_none", None)
        except Exception:
            pass  # Expected to handle gracefully

        # Test adding routes with non-callable view_func
        try:
            self.app.add_url_rule("/test_string", "test_string", "not_callable")
        except Exception:
            pass  # Expected to handle gracefully

        # Test route with lambda (should be decorated)
        def lambda_func():
            return "lambda response"

        self.app.add_url_rule("/test_lambda", "test_lambda", lambda_func)

        # Clean up - don't call uninstrument_app to avoid Flask instrumentation issues

    def test_flask_patches_dispatch_request_coverage(self):
        """Test Flask patches dispatch_request method coverage."""
        # Create a special app with deferred view function binding
        test_app = flask.Flask(__name__)

        # Add route after creating app but before applying patches
        @test_app.route("/deferred")
        def deferred_view():
            return "deferred"

        # Create instrumentor and apply patches
        instrumentor = FlaskInstrumentor()
        _apply_flask_instrumentation_patches()
        instrumentor._instrument()
        instrumentor.instrument_app(test_app)

        # Create test client and make request to trigger dispatch_request
        client = Client(test_app, Response)
        resp = client.get("/deferred")
        self.assertEqual(200, resp.status_code)

    def test_flask_patches_uninstrument_error_handling(self):
        """Test Flask patches uninstrument error handling."""
        # Create instrumentor and apply patches
        instrumentor = FlaskInstrumentor()
        _apply_flask_instrumentation_patches()
        instrumentor._instrument()

        # Manually break the stored references to trigger error handling
        if hasattr(instrumentor, "_original_flask_add_url_rule"):
            # Set invalid values to trigger exceptions during restoration
            instrumentor._original_flask_add_url_rule = None
            instrumentor._original_flask_dispatch_request = None

        # This should trigger error handling in patched_uninstrument
        try:
            instrumentor._uninstrument()
        except Exception:
            pass  # Expected to handle gracefully

    def test_flask_patches_code_correlation_import_error(self):
        """Test Flask patches when code_correlation import fails."""
        # Mock import error for code_correlation module
        import sys

        original_modules = sys.modules.copy()

        try:
            # Remove code_correlation module to simulate import error
            modules_to_remove = [
                "amazon.opentelemetry.distro.code_correlation",
                "amazon.opentelemetry.distro.code_correlation.record_code_attributes",
            ]
            for module in modules_to_remove:
                if module in sys.modules:
                    del sys.modules[module]

            # Create instrumentor and apply patches - should handle import error gracefully
            instrumentor = FlaskInstrumentor()
            _apply_flask_instrumentation_patches()

            # Try to trigger the patched methods
            instrumentor._instrument()

        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)
