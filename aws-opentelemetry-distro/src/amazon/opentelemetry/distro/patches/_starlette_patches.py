# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from logging import Logger, getLogger

from amazon.opentelemetry.distro._utils import is_agent_observability_enabled

_logger: Logger = getLogger(__name__)


def _apply_starlette_instrumentation_patches() -> None:
    """Apply ASGI middleware patches for Bedrock AgentCore.

    Patches OpenTelemetryMiddleware to exclude http receive/send ASGI event spans.
    """
    try:
        # pylint: disable=import-outside-toplevel
        from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

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

        _logger.debug("Successfully patched Starlette ASGI middleware")
    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to apply Starlette instrumentation patches: %s", exc)
