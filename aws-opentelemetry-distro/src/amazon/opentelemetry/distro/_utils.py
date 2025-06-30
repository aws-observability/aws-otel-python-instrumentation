# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from importlib.metadata import PackageNotFoundError, version
from logging import Logger, getLogger
from typing import Optional

from packaging.requirements import Requirement

_logger: Logger = getLogger(__name__)

AGENT_OBSERVABILITY_ENABLED = "AGENT_OBSERVABILITY_ENABLED"


def is_installed(req: str) -> bool:
    """Is the given required package installed?"""
    req = Requirement(req)

    try:
        dist_version = version(req.name)
    except PackageNotFoundError as exc:
        _logger.debug("Skipping instrumentation patch: package %s, exception: %s", req, exc)
        return False

    if not list(req.specifier.filter([dist_version])):
        _logger.debug(
            "instrumentation for package %s is available but version %s is installed. Skipping.",
            req,
            dist_version,
        )
        return False
    return True


def is_agent_observability_enabled() -> bool:
    """Is the Agentic AI monitoring flag set to true?"""
    return os.environ.get(AGENT_OBSERVABILITY_ENABLED, "false").lower() == "true"


IS_BOTOCORE_INSTALLED: bool = is_installed("botocore")


def get_aws_session():
    if IS_BOTOCORE_INSTALLED:
        # pylint: disable=import-outside-toplevel
        from botocore.session import Session

        return Session()
    return None


def get_aws_region() -> Optional[str]:
    botocore_session = get_aws_session()
    return botocore_session.get_config_variable("region") if botocore_session else None
