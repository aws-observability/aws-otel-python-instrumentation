# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from logging import Logger, getLogger
from typing import Collection

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
        from opentelemetry.instrumentation.starlette import StarletteInstrumentor

        # Patch starlette dependencies version check
        # Loose the upper check from ("starlette >= 0.13, <0.15",)
        def patched_instrumentation_dependencies(self) -> Collection[str]:
            return ("starlette >= 0.13",)

        # Apply the patch
        StarletteInstrumentor.instrumentation_dependencies = patched_instrumentation_dependencies

        _logger.debug("Successfully patched Starlette instrumentation_dependencies method")
    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to apply Starlette instrumentation patches: %s", exc)
