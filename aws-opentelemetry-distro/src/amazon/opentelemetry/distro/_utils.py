# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from importlib.metadata import PackageNotFoundError, version
from logging import Logger, getLogger

from packaging.requirements import Requirement

_logger: Logger = getLogger(__name__)

AGENT_OBSERVABILITY_ENABLED = "AGENT_OBSERVABILITY_ENABLED"


def is_installed(req: str) -> bool:
    """Is the given required package installed?"""

    if req in sys.modules and sys.modules[req] is not None:
        return True

    return _is_installed(req)


def _is_installed(req):
    req = Requirement(req)

    try:
        dist_version = version(req.name)
    except PackageNotFoundError:
        return False

    if not req.specifier.filter(dist_version):
        _logger.warning(
            "instrumentation for package %s is available but version %s is installed. Skipping.",
            req,
            dist_version,
        )
        return False
    return True


def is_agent_observability_enabled() -> bool:
    """Is the Agentic AI monitoring flag set to true?"""
    return os.environ.get(AGENT_OBSERVABILITY_ENABLED, "false").lower() == "true"
