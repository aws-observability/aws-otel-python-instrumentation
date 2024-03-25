# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import sys
from logging import Logger, getLogger

import pkg_resources

from amazon.opentelemetry.distro.patches._resource_detector_patches import _apply_resource_detector_patches

_logger: Logger = getLogger(__name__)


def apply_instrumentation_patches() -> None:
    """Apply patches to upstream instrumentation libraries.

    This method is invoked to apply changes to upstream instrumentation libraries, typically when changes to upstream
    are required on a timeline that cannot wait for upstream release. Generally speaking, patches should be short-term
    local solutions that are comparable to long-term upstream solutions.

    Where possible, automated testing should be run to catch upstream changes resulting in broken patches
    """

    if _is_installed("botocore ~= 1.0"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches is safe to apply (e.g. the instrumented library is installed).
        from amazon.opentelemetry.distro.patches._botocore_patches import _apply_botocore_instrumentation_patches

        _apply_botocore_instrumentation_patches()

    # No need to check if library is installed as this patches opentelemetry.sdk,
    # which must be installed for the distro to work at all.
    _apply_resource_detector_patches()


def _is_installed(req: str) -> bool:
    if req in sys.modules:
        return True

    try:
        pkg_resources.get_distribution(req)
    except Exception as exc:  # pylint: disable=broad-except
        _logger.debug("Skipping instrumentation patch: package %s, exception: %s", req, exc)
        return False
    return True
