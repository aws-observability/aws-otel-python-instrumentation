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
    """
    Returns a botocore session only if botocore is installed, otherwise None.
    If AWS Region is defined in `AWS_REGION` or `AWS_DEFAULT_REGION` environment variables,
    then the region is set in the botocore session before returning.

    We do this to prevent runtime errors for ADOT customers that do not need
    any features that require botocore.
    """
    if IS_BOTOCORE_INSTALLED:
        # pylint: disable=import-outside-toplevel
        from botocore.session import Session

        session = Session()
        # Botocore only looks up AWS_DEFAULT_REGION when creating a session/client
        # See: https://docs.aws.amazon.com/sdkref/latest/guide/feature-region.html#feature-region-sdk-compat
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if region:
            session.set_config_variable("region", region)
        return session
    return None


def get_aws_region() -> Optional[str]:
    """Get AWS region from environment or botocore session.

    Returns the AWS region in the following priority order:
    1. AWS_REGION environment variable
    2. AWS_DEFAULT_REGION environment variable
    3. botocore session's region (if botocore is available)
    4. None if no region can be determined
    """
    botocore_session = get_aws_session()
    return botocore_session.get_config_variable("region") if botocore_session else None


def is_account_id(input_str: str) -> bool:
    return input_str is not None and input_str.isdigit()
