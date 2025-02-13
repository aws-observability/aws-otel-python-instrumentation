# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import sys
from logging import Logger, getLogger

import pkg_resources

_logger: Logger = getLogger(__name__)


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
