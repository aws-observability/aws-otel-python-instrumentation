# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import sys
from logging import Logger, getLogger

import pkg_resources

_logger: Logger = getLogger(__name__)

XRAY_OTLP_ENDPOINT_PATTERN = r"https://xray\.([a-z0-9-]+)\.amazonaws\.com/v1/traces$"


def is_xray_otlp_endpoint(otlp_endpoint: str = None) -> bool:
    """Is the given endpoint the XRay OTLP endpoint?"""

    if not otlp_endpoint:
        return False

    return bool(re.match(XRAY_OTLP_ENDPOINT_PATTERN, otlp_endpoint.lower()))


def is_installed(req: str) -> bool:
    """Is the given required package installed?"""

    if req in sys.modules and sys.modules[req] is not None:
        return True

    try:
        pkg_resources.get_distribution(req)
    except Exception as exc:  # pylint: disable=broad-except
        _logger.debug("Skipping instrumentation patch: package %s, exception: %s", req, exc)
        return False
    return True
