# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from logging import Logger, getLogger
from typing import Collection

from amazon.opentelemetry.distro._utils import is_agent_observability_enabled

_logger: Logger = getLogger(__name__)


def _apply_starlette_instrumentation_patches() -> None:
    """Apply patches to the Starlette instrumentation.

    This applies both version compatibility patches and code attributes support.
    """
    _apply_starlette_version_patches()
    _apply_starlette_code_attributes_patch()


# Upstream fix available in OpenTelemetry 1.34.0/0.55b0 (2025-06-04)
# Reference: https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3456
# TODO: Remove this patch after upgrading to version 1.34.0 or later
def _apply_starlette_version_patches() -> None:
    """Apply version compatibility patches to the Starlette instrumentation.

    This patch modifies the instrumentation_dependencies method in the starlette
    instrumentation to loose an upper version constraint for auto-instrumentation
    """
    try:
        # pylint: disable=import-outside-toplevel
        from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
        from opentelemetry.instrumentation.starlette import StarletteInstrumentor

        # Patch starlette dependencies version check
        # Loose the upper check from ("starlette >= 0.13, <0.15",)
        def patched_instrumentation_dependencies(self) -> Collection[str]:
            return ("starlette >= 0.13",)

        # Apply the patch
        StarletteInstrumentor.instrumentation_dependencies = patched_instrumentation_dependencies

        # pylint: disable=line-too-long
        # Patch to exclude http receive/send ASGI event spans from Bedrock AgentCore,
        # this Middleware instrumentation is injected internally by Starlette Instrumentor, see:
        # https://github.com/open-telemetry/opentelemetry-python-contrib/blob/51da0a766e5d3cbc746189e10c9573163198cfcd/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py#L573
        #
        # Issue for tracking a feature to customize this setting within Starlette:
        # https://github.com/open-telemetry/opentelemetry-python-contrib/issues/3725
        if is_agent_observability_enabled():
            original_init = OpenTelemetryMiddleware.__init__

            def patched_init(self, app, **kwargs):
                original_init(self, app, **kwargs)
                if hasattr(self, "exclude_receive_span"):
                    self.exclude_receive_span = True
                if hasattr(self, "exclude_send_span"):
                    self.exclude_send_span = True

            OpenTelemetryMiddleware.__init__ = patched_init

        _logger.debug("Successfully patched Starlette instrumentation_dependencies method")
    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to apply Starlette instrumentation patches: %s", exc)


def _apply_starlette_code_attributes_patch() -> None:
    """Starlette instrumentation patch for code attributes

    This patch modifies Starlette Route class to automatically apply
    the record_code_attributes decorator to endpoint functions when
    routes are created.

    The patch:
    1. Imports record_code_attributes decorator from AWS distro code correlation
    2. Hooks Starlette's Route.__init__ method during instrumentation
    3. Automatically decorates endpoint functions as routes are created
    4. Adds code.function.name, code.file.path, and code.line.number to spans
    5. Provides cleanup during uninstrumentation
    """
    try:
        # Import Starlette routing classes and AWS decorator
        from starlette.routing import Route  # pylint: disable=import-outside-toplevel

        from amazon.opentelemetry.distro.code_correlation import (  # pylint: disable=import-outside-toplevel
            record_code_attributes,
        )
        from opentelemetry.instrumentation.starlette import (  # pylint: disable=import-outside-toplevel
            StarletteInstrumentor,
        )

        # Store the original _instrument and _uninstrument methods
        original_instrument = StarletteInstrumentor._instrument
        original_uninstrument = StarletteInstrumentor._uninstrument

        # Store reference to original Route.__init__
        original_route_init = Route.__init__

        def _decorate_endpoint(endpoint):
            """Helper function to decorate an endpoint function with code attributes."""
            try:
                if endpoint and callable(endpoint):
                    # Check if function is already decorated (avoid double decoration)
                    if not hasattr(endpoint, "_current_span_code_attributes_decorated"):
                        # Apply decorator
                        decorated_endpoint = record_code_attributes(endpoint)
                        # Mark as decorated to avoid double decoration
                        decorated_endpoint._current_span_code_attributes_decorated = True
                        decorated_endpoint._original_endpoint = endpoint
                        return decorated_endpoint
                return endpoint
            except Exception as exc:  # pylint: disable=broad-exception-caught
                _logger.warning("Failed to apply code attributes decorator to endpoint: %s", exc)
                return endpoint

        def _wrapped_route_init(self, path, endpoint=None, **kwargs):
            """Wrapped Route.__init__ method with code attributes decoration."""
            # Decorate endpoint if provided
            if endpoint:
                endpoint = _decorate_endpoint(endpoint)

            # Call the original Route.__init__ with decorated endpoint
            return original_route_init(self, path, endpoint=endpoint, **kwargs)

        def patched_instrument(self, **kwargs):
            """Patched _instrument method with Route.__init__ wrapping"""
            # Store original Route.__init__ method if not already stored
            if not hasattr(self, "_original_route_init"):
                self._original_route_init = Route.__init__

                # Wrap Route.__init__ with code attributes decoration
                Route.__init__ = _wrapped_route_init

            # Call the original _instrument method
            original_instrument(self, **kwargs)

        def patched_uninstrument(self, **kwargs):
            """Patched _uninstrument method with Route.__init__ restoration"""
            # Call the original _uninstrument method first
            original_uninstrument(self, **kwargs)

            # Restore original Route.__init__ method if it exists
            if hasattr(self, "_original_route_init"):
                try:
                    Route.__init__ = self._original_route_init
                    delattr(self, "_original_route_init")
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    _logger.warning("Failed to restore original Route.__init__ method: %s", exc)

        # Apply the patches to StarletteInstrumentor
        StarletteInstrumentor._instrument = patched_instrument
        StarletteInstrumentor._uninstrument = patched_uninstrument

        _logger.debug("Starlette instrumentation code attributes patch applied successfully")

    except Exception as exc:  # pylint: disable=broad-exception-caught
        _logger.warning("Failed to apply Starlette code attributes patch: %s", exc)
