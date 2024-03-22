# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import sys
from logging import Logger, getLogger
from typing import Dict, List, Optional

import pkg_resources

from amazon.opentelemetry.distro._resource_detector_patches import _apply_resource_detector_patches

_logger: Logger = getLogger(__name__)

patch_libraries: Dict[str, List[str]] = {
    "botocore": ["botocore ~= 1.0"],
}


def apply_instrumentation_patches() -> None:
    """Apply patches to upstream instrumentation libraries.

    This method is invoked to apply changes to upstream instrumentation libraries, typically when changes to upstream
    are required on a timeline that cannot wait for upstream release. Generally speaking, patches should be short-term
    local solutions that are comparable to long-term upstream solutions.

    Where possible, automated testing should be run to catch upstream changes resulting in broken patches
    """

    if _check_patches("botocore"):
        # pylint: disable=import-outside-toplevel
        # Delay import to only occur if patches are detected from the system.
        from amazon.opentelemetry.distro._botocore_patches import _apply_botocore_instrumentation_patches

        _apply_botocore_instrumentation_patches()

    _apply_resource_detector_patches()


def _check_patches(patch_name) -> bool:
    if patch_name not in patch_libraries:
        return False
    for patch_lib in patch_libraries[patch_name]:
        if not _is_installed(patch_lib):
            return False
    return True


def _is_installed(req: str) -> bool:
    if req in sys.modules:
        return True

    try:
        pkg_resources.get_distribution(req)
    except pkg_resources.DistributionNotFound:
        return False
    except pkg_resources.VersionConflict as exc:
        required_version: Optional[str] = pkg_resources.parse_version(exc.req.specs[0][1]) if exc.req.specs else None
        _logger.dehug(
            "instrumentation for package %s version %s is available but version %s is installed. Skipping.",
            exc.req.name,
            required_version,
            exc.dist,  # pylint: disable=no-member
        )
        return False
    return True
