# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from logging import Logger, getLogger
from typing import Collection

from amazon.opentelemetry.distro._utils import is_agent_observability_enabled

_logger: Logger = getLogger(__name__)


# Upstream fix available in OpenTelemetry 1.34.0/0.55b0 (2025-06-04)
# Reference: https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3456
# TODO: Remove this patch after upgrading to version 1.34.0 or later
def _apply_starlette_instrumentation_patches() -> None:
    """Apply patches to the Starlette instrumentation.

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

        # Patch to exclude http receive/send ASGI event spans from Bedrock AgentCore,
        # this Middleware instrumentation is injected internally by Starlette Instrumentor, see:
        # https://github.com/open-telemetry/opentelemetry-python-contrib/blob/51da0a766e5d3cbc746189e10c9573163198cfcd/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py#L573
        #
        # Issue for tracking a feature to customize this setting within Starlette: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/3725
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
