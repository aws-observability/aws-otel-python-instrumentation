# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from logging import Logger, getLogger

from amazon.opentelemetry.distro._utils import is_installed

_logger: Logger = getLogger(__name__)


def apply_instrumentation_patches() -> None:  # pylint: disable=too-many-branches
    """Apply patches to upstream instrumentation libraries.

    This method is invoked to apply changes to upstream instrumentation libraries, typically when changes to upstream
    are required on a timeline that cannot wait for upstream release. Generally speaking, patches should be short-term
    local solutions that are comparable to long-term upstream solutions.

    Where possible, automated testing should be run to catch upstream changes resulting in broken patches
    """
    # Redact AWS SigV4 pre-signed URL credentials from captured HTTP URLs. Applied unconditionally
    # because it hardens URL sanitization for every HTTP client instrumentation and has no external
    # dependency of its own.
    # pylint: disable=import-outside-toplevel
    from amazon.opentelemetry.distro.patches._url_query_redaction_patches import _apply_url_query_redaction_patches

    _apply_url_query_redaction_patches()

    if is_installed("botocore ~= 1.0"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._botocore_patches import _apply_botocore_instrumentation_patches

        _apply_botocore_instrumentation_patches()

    if is_installed("starlette"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._starlette_patches import _apply_starlette_instrumentation_patches

        # Patch to exclude http receive/send ASGI event spans from Bedrock AgentCore
        _apply_starlette_instrumentation_patches()
