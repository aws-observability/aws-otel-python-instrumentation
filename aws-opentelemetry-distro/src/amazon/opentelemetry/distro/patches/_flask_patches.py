# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.

from logging import getLogger

_logger = getLogger(__name__)


def _apply_flask_instrumentation_patches() -> None:
    """Flask instrumentation patches

    Applies patches to provide code attributes support for Flask instrumentation.
    This patches the Flask instrumentation to automatically add code attributes
    to spans by decorating view functions with record_code_attributes.
    """
    _apply_flask_code_attributes_patch()


def _apply_flask_code_attributes_patch() -> None:  # pylint: disable=too-many-statements
    """Flask instrumentation patch for code attributes

    This patch modifies the Flask instrumentation to automatically apply
    the current_span_code_attributes decorator to all view functions when
    the Flask app is instrumented.

    The patch:
    1. Imports current_span_code_attributes decorator from AWS distro utils
    2. Hooks Flask's add_url_rule method during _instrument by patching Flask class
    3. Hooks Flask's dispatch_request method to handle deferred view function binding
    4. Automatically decorates view functions as they are registered or at request time
    5. Adds code.function.name, code.file.path, and code.line.number to spans
    6. Provides cleanup during _uninstrument
    """
    try:
        # Import Flask instrumentation classes and AWS decorator
        import flask  # pylint: disable=import-outside-toplevel

        from amazon.opentelemetry.distro.code_correlation import (  # pylint: disable=import-outside-toplevel
            record_code_attributes,
        )
        from opentelemetry.instrumentation.flask import FlaskInstrumentor  # pylint: disable=import-outside-toplevel

        # Store the original _instrument and _uninstrument methods
        original_instrument = FlaskInstrumentor._instrument
        original_uninstrument = FlaskInstrumentor._uninstrument

        # Store reference to original Flask methods
        original_flask_add_url_rule = flask.Flask.add_url_rule
        original_flask_dispatch_request = flask.Flask.dispatch_request

        def _decorate_view_func(view_func, endpoint=None):
            """Helper function to decorate a view function with code attributes."""
            try:
                if view_func and callable(view_func):
                    # Check if function is already decorated (avoid double decoration)
                    if not hasattr(view_func, "_current_span_code_attributes_decorated"):
                        # Apply decorator
                        decorated_view_func = record_code_attributes(view_func)
                        # Mark as decorated to avoid double decoration
                        decorated_view_func._current_span_code_attributes_decorated = True
                        decorated_view_func._original_view_func = view_func
                        return decorated_view_func
                return view_func
            except Exception as exc:  # pylint: disable=broad-exception-caught
                _logger.warning("Failed to apply code attributes decorator to view function %s: %s", endpoint, exc)
                return view_func

        def _wrapped_add_url_rule(self, rule, endpoint=None, view_func=None, **options):
            """Wrapped Flask.add_url_rule method with code attributes decoration."""
            # Apply decorator to view function if available
            if view_func:
                view_func = _decorate_view_func(view_func, endpoint)

            return original_flask_add_url_rule(self, rule, endpoint, view_func, **options)

        def _wrapped_dispatch_request(self):
            """Wrapped Flask.dispatch_request method to handle deferred view function binding."""
            try:
                # Get the current request context
                from flask import request  # pylint: disable=import-outside-toplevel

                # Check if there's an endpoint for this request
                endpoint = request.endpoint
                if endpoint and endpoint in self.view_functions:
                    view_func = self.view_functions[endpoint]

                    # Check if the view function needs decoration
                    if view_func and callable(view_func):
                        if not hasattr(view_func, "_current_span_code_attributes_decorated"):
                            # Decorate the view function and replace it in view_functions
                            decorated_view_func = _decorate_view_func(view_func, endpoint)
                            if decorated_view_func != view_func:
                                self.view_functions[endpoint] = decorated_view_func
                                _logger.debug(
                                    "Applied code attributes decorator to deferred view function for endpoint: %s",
                                    endpoint,
                                )

            except Exception as exc:  # pylint: disable=broad-exception-caught
                _logger.warning("Failed to process deferred view function decoration: %s", exc)

            # Call the original dispatch_request method
            return original_flask_dispatch_request(self)

        def patched_instrument(self, **kwargs):
            """Patched _instrument method with Flask method wrapping"""
            # Store original methods if not already stored
            if not hasattr(self, "_original_flask_add_url_rule"):
                self._original_flask_add_url_rule = flask.Flask.add_url_rule
                self._original_flask_dispatch_request = flask.Flask.dispatch_request

                # Wrap Flask methods with code attributes decoration
                flask.Flask.add_url_rule = _wrapped_add_url_rule
                flask.Flask.dispatch_request = _wrapped_dispatch_request

            # Call the original _instrument method
            original_instrument(self, **kwargs)

        def patched_uninstrument(self, **kwargs):
            """Patched _uninstrument method with Flask method restoration"""
            # Call the original _uninstrument method first
            original_uninstrument(self, **kwargs)

            # Restore original Flask methods if they exist
            if hasattr(self, "_original_flask_add_url_rule"):
                try:
                    flask.Flask.add_url_rule = self._original_flask_add_url_rule
                    flask.Flask.dispatch_request = self._original_flask_dispatch_request
                    delattr(self, "_original_flask_add_url_rule")
                    delattr(self, "_original_flask_dispatch_request")
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    _logger.warning("Failed to restore original Flask methods: %s", exc)

        # Apply the patches to FlaskInstrumentor
        FlaskInstrumentor._instrument = patched_instrument
        FlaskInstrumentor._uninstrument = patched_uninstrument

        _logger.debug("Flask instrumentation code attributes patch applied successfully")

    except Exception as exc:  # pylint: disable=broad-exception-caught
        _logger.warning("Failed to apply Flask code attributes patch: %s", exc)
