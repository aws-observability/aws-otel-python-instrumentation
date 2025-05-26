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
    return os.environ.get(AGENT_OBSERVABILITY_ENABLED, "false").lower() == "true"


def get_aws_region() -> str:
    """Get AWS region from environment or boto3 session.

    Returns the AWS region in the following priority order:
    1. AWS_REGION environment variable
    2. AWS_DEFAULT_REGION environment variable
    3. boto3 session's region (if boto3 is available)
    4. Default to 'us-east-1' with warning
    """
    # Check AWS environment variables first
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region

    # Try to get region from boto3 if available
    # This will automatically check AWS CLI config, instance metadata, etc.
    try:
        import boto3

        session = boto3.Session()
        if session.region_name:
            return session.region_name
    except Exception:
        # boto3 not available or failed to determine region
        pass

    _logger.warning(
        "AWS region not found in environment variables (AWS_REGION, AWS_DEFAULT_REGION) "
        "or boto3 configuration. Defaulting to 'us-east-1'. "
        "This may cause issues if your resources are in a different region. "
        "Please set AWS_REGION environment variable explicitly."
    )
    return "us-east-1"
