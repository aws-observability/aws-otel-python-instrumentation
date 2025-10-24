# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from logging import Logger, getLogger

from amazon.opentelemetry.distro._utils import is_installed
from amazon.opentelemetry.distro.patches._resource_detector_patches import _apply_resource_detector_patches

_logger: Logger = getLogger(__name__)


def apply_instrumentation_patches() -> None:  # pylint: disable=too-many-branches
    """Apply patches to upstream instrumentation libraries.

    This method is invoked to apply changes to upstream instrumentation libraries, typically when changes to upstream
    are required on a timeline that cannot wait for upstream release. Generally speaking, patches should be short-term
    local solutions that are comparable to long-term upstream solutions.

    Where possible, automated testing should be run to catch upstream changes resulting in broken patches
    """
    if is_installed("botocore ~= 1.0"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._botocore_patches import _apply_botocore_instrumentation_patches

        _apply_botocore_instrumentation_patches()

    if is_installed("starlette"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._starlette_patches import _apply_starlette_instrumentation_patches

        # Starlette auto-instrumentation v0.54b includes a strict dependency version check
        # This restriction was removed in v1.34.0/0.55b0. Applying temporary patch for Bedrock AgentCore launch
        # TODO: Remove patch after syncing with upstream v1.34.0 or later
        _apply_starlette_instrumentation_patches()

    if is_installed("flask"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._flask_patches import _apply_flask_instrumentation_patches

        _apply_flask_instrumentation_patches()

    if is_installed("fastapi"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._fastapi_patches import _apply_fastapi_instrumentation_patches

        _apply_fastapi_instrumentation_patches()

    if is_installed("django"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._django_patches import _apply_django_instrumentation_patches

        _apply_django_instrumentation_patches()

    if is_installed("celery"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._celery_patches import _apply_celery_instrumentation_patches

        _apply_celery_instrumentation_patches()

    if is_installed("pika"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._pika_patches import _apply_pika_instrumentation_patches

        _apply_pika_instrumentation_patches()

    if is_installed("aio-pika"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._aio_pika_patches import _apply_aio_pika_instrumentation_patches

        _apply_aio_pika_instrumentation_patches()

    # No need to check if library is installed as this patches opentelemetry.sdk,
    # which must be installed for the distro to work at all.
    _apply_resource_detector_patches()
