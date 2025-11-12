# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.

from logging import getLogger

_logger = getLogger(__name__)


def _apply_fastapi_instrumentation_patches() -> None:
    """FastAPI instrumentation patches

    Applies patches to provide code attributes support for FastAPI instrumentation.
    This patches the FastAPI instrumentation to automatically add code attributes
    to spans by decorating view functions with record_code_attributes.
    """
    _apply_fastapi_code_attributes_patch()


def _apply_fastapi_code_attributes_patch() -> None:
    """FastAPI instrumentation patch for code attributes

    This patch modifies the FastAPI instrumentation to automatically apply
    the current_span_code_attributes decorator to all endpoint functions when
    the FastAPI app is instrumented.

    The patch:
    1. Imports current_span_code_attributes decorator from AWS distro utils
    2. Hooks FastAPI's APIRouter.add_api_route method during instrumentation
    3. Automatically decorates endpoint functions as they are registered
    4. Adds code.function.name, code.file.path, and code.line.number to spans
    5. Provides cleanup during uninstrumentation
    """
    try:
        # Import FastAPI instrumentation classes and AWS decorator
        from fastapi import routing  # pylint: disable=import-outside-toplevel

        from amazon.opentelemetry.distro.code_correlation import (  # pylint: disable=import-outside-toplevel
            record_code_attributes,
        )
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor  # pylint: disable=import-outside-toplevel

        # Store the original _instrument and _uninstrument methods
        original_instrument = FastAPIInstrumentor._instrument
        original_uninstrument = FastAPIInstrumentor._uninstrument

        def _wrapped_add_api_route(original_add_api_route_method):
            """Wrapper for APIRouter.add_api_route method."""

            def wrapper(self, *args, **kwargs):
                # Apply current_span_code_attributes decorator to endpoint function
                try:
                    # Get endpoint function from args or kwargs
                    endpoint = None
                    if len(args) >= 2:
                        endpoint = args[1]
                    else:
                        endpoint = kwargs.get("endpoint")

                    if endpoint and callable(endpoint):
                        # Check if function is already decorated (avoid double decoration)
                        if not hasattr(endpoint, "_current_span_code_attributes_decorated"):
                            # Apply decorator
                            decorated_endpoint = record_code_attributes(endpoint)
                            # Mark as decorated to avoid double decoration
                            decorated_endpoint._current_span_code_attributes_decorated = True
                            decorated_endpoint._original_endpoint = endpoint

                            # Replace endpoint in args or kwargs
                            if len(args) >= 2:
                                args = list(args)
                                args[1] = decorated_endpoint
                                args = tuple(args)
                            elif "endpoint" in kwargs:
                                kwargs["endpoint"] = decorated_endpoint

                except Exception as exc:  # pylint: disable=broad-exception-caught
                    _logger.warning("Failed to apply code attributes decorator to endpoint: %s", exc)

                return original_add_api_route_method(self, *args, **kwargs)

            return wrapper

        def patched_instrument(self, **kwargs):
            """Patched _instrument method with APIRouter.add_api_route wrapping"""
            # Store original add_api_route method if not already stored
            if not hasattr(self, "_original_apirouter"):
                self._original_apirouter = routing.APIRouter.add_api_route

            # Wrap APIRouter.add_api_route with code attributes decoration
            routing.APIRouter.add_api_route = _wrapped_add_api_route(self._original_apirouter)

            # Call the original _instrument method
            original_instrument(self, **kwargs)

        def patched_uninstrument(self, **kwargs):
            """Patched _uninstrument method with APIRouter.add_api_route restoration"""
            # Call the original _uninstrument method first
            original_uninstrument(self, **kwargs)

            # Restore original APIRouter.add_api_route method if it exists
            if hasattr(self, "_original_apirouter"):
                try:
                    routing.APIRouter.add_api_route = self._original_apirouter
                    delattr(self, "_original_apirouter")
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    _logger.warning("Failed to restore original APIRouter.add_api_route method: %s", exc)

        # Apply the patches to FastAPIInstrumentor
        FastAPIInstrumentor._instrument = patched_instrument
        FastAPIInstrumentor._uninstrument = patched_uninstrument

        _logger.debug("FastAPI instrumentation code attributes patch applied successfully")

    except Exception as exc:  # pylint: disable=broad-exception-caught
        _logger.warning("Failed to apply FastAPI code attributes patch: %s", exc)
