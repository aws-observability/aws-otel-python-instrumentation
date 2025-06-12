# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from logging import Logger, getLogger

import pkg_resources

_logger: Logger = getLogger(__name__)

AGENT_OBSERVABILITY_ENABLED = "AGENT_OBSERVABILITY_ENABLED"


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


def is_agent_observability_enabled() -> bool:
    """Is the Agentic AI monitoring flag set to true?"""
    return os.environ.get(AGENT_OBSERVABILITY_ENABLED, "false").lower() == "true"


def get_aws_region() -> str:
    """Get AWS region from environment or botocore session.

    Returns the AWS region in the following priority order:
    1. AWS_REGION environment variable
    2. AWS_DEFAULT_REGION environment variable
    3. botocore session's region (if botocore is available)
    4. None if no region can be determined
    """
    # Check AWS environment variables first
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region

    # Try to get region from botocore if available
    # This will automatically check AWS CLI config, instance metadata, etc.
    if is_installed("botocore"):
        try:
            from botocore import session

            botocore_session = session.Session()
            if botocore_session.region_name:
                return botocore_session.region_name
        except Exception:
            # botocore failed to determine region
            pass

    _logger.warning(
        "AWS region not found in environment variables (AWS_REGION, AWS_DEFAULT_REGION) "
        "or botocore configuration. Please set AWS_REGION environment variable explicitly."
    )
    return None
