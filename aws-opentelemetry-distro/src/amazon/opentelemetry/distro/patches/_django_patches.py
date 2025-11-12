# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.

from logging import getLogger

_logger = getLogger(__name__)


def _apply_django_instrumentation_patches() -> None:
    """Django instrumentation patches

    Applies patches to provide code attributes support for Django instrumentation.
    This patches the Django instrumentation to automatically add code attributes
    to spans by modifying the process_view method of the Django middleware.
    Also patches Django's path/re_path functions for URL pattern instrumentation.
    """
    _apply_django_code_attributes_patch()


def _apply_django_code_attributes_patch() -> None:  # pylint: disable=too-many-statements
    """Django instrumentation patch for code attributes

    This patch modifies the Django middleware's process_view method to automatically add
    code attributes to the current span when a view function is about to be executed.

    The patch includes:
    1. Support for class-based views by extracting the actual HTTP method handler
    2. Automatic addition of code.function.name, code.file.path, and code.line.number
    3. Graceful error handling and cleanup during uninstrument
    """
    try:
        # Import Django instrumentation classes and AWS code correlation function
        from amazon.opentelemetry.distro.code_correlation import (  # pylint: disable=import-outside-toplevel
            add_code_attributes_to_span,
        )
        from opentelemetry.instrumentation.django import DjangoInstrumentor  # pylint: disable=import-outside-toplevel

        # Store the original _instrument and _uninstrument methods
        original_instrument = DjangoInstrumentor._instrument
        original_uninstrument = DjangoInstrumentor._uninstrument

        # Store reference to original Django middleware process_view method
        original_process_view = None

        def _patch_django_middleware():
            """Patch Django middleware's process_view method to add code attributes."""
            try:
                # Import Django middleware class
                # pylint: disable=import-outside-toplevel
                from opentelemetry.instrumentation.django.middleware.otel_middleware import _DjangoMiddleware

                nonlocal original_process_view
                if original_process_view is None:
                    original_process_view = _DjangoMiddleware.process_view

                def patched_process_view(
                    self, request, view_func, *args, **kwargs
                ):  # pylint: disable=too-many-locals,too-many-nested-blocks,too-many-branches
                    """Patched process_view method to add code attributes to the span."""
                    # First call the original process_view method
                    # pylint: disable=assignment-from-none
                    result = original_process_view(self, request, view_func, *args, **kwargs)

                    # Add code attributes if we have a span and view function
                    try:
                        if (
                            self._environ_activation_key in request.META.keys()
                            and self._environ_span_key in request.META.keys()
                        ):
                            span = request.META[self._environ_span_key]
                            if span and view_func and span.is_recording():
                                # Determine the target function/method to analyze
                                target = view_func

                                # If it's a class-based view, get the corresponding HTTP method handler
                                view_class = getattr(view_func, "view_class", None)
                                if view_class:
                                    method_name = request.method.lower()
                                    handler = getattr(view_class, method_name, None) or view_class
                                    target = handler

                                # Call the existing add_code_attributes_to_span function
                                add_code_attributes_to_span(span, target)
                                _logger.debug(
                                    "Added code attributes to span for Django view: %s",
                                    getattr(target, "__name__", str(target)),
                                )
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        # Don't let code attributes addition break the request processing
                        _logger.warning("Failed to add code attributes to Django span: %s", exc)

                    return result

                # Apply the patch
                _DjangoMiddleware.process_view = patched_process_view
                _logger.debug("Django middleware process_view patched successfully for code attributes")

            except Exception as exc:  # pylint: disable=broad-exception-caught
                _logger.warning("Failed to patch Django middleware process_view: %s", exc)

        def _unpatch_django_middleware():
            """Restore original Django middleware process_view method."""
            try:
                # pylint: disable=import-outside-toplevel
                from opentelemetry.instrumentation.django.middleware.otel_middleware import _DjangoMiddleware

                if original_process_view is not None:
                    _DjangoMiddleware.process_view = original_process_view
                    _logger.debug("Django middleware process_view restored successfully")

            except Exception as exc:  # pylint: disable=broad-exception-caught
                _logger.warning("Failed to restore Django middleware process_view: %s", exc)

        def patched_instrument(self, **kwargs):
            """Patched _instrument method with Django middleware patching"""
            # Apply Django middleware patches
            _patch_django_middleware()

            # Call the original _instrument method
            original_instrument(self, **kwargs)  # pylint: disable=assignment-from-none

        def patched_uninstrument(self, **kwargs):
            """Patched _uninstrument method with Django middleware patch restoration"""
            # Call the original _uninstrument method first
            original_uninstrument(self, **kwargs)  # pylint: disable=assignment-from-none

            # Restore original Django middleware
            _unpatch_django_middleware()

        # Apply the patches to DjangoInstrumentor
        DjangoInstrumentor._instrument = patched_instrument
        DjangoInstrumentor._uninstrument = patched_uninstrument

        _logger.debug("Django instrumentation code attributes patch applied successfully")

    except Exception as exc:  # pylint: disable=broad-exception-caught
        _logger.warning("Failed to apply Django code attributes patch: %s", exc)
