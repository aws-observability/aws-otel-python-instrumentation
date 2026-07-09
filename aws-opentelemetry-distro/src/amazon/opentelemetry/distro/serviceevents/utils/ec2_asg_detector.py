# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Resource detector for the EC2 Auto Scaling group name (from IMDS instance tags).

The stock ``AwsEc2ResourceDetector`` only reads the instance-identity document — it does
NOT fetch instance tags, so the ASG is absent from the SDK Resource. The CloudWatch agent
reads the ASG from IMDS instance tags
(``/latest/meta-data/tags/instance/aws:autoscaling:groupName``) to resolve ``ec2:<asg>``.
This detector closes that gap so the SDK can compute the same ``aws.local.environment``
the agent would on EC2, without depending on the agent.

Instance metadata tags must be enabled on the instance
(``InstanceMetadataTags=enabled``); when they aren't (or IMDS is unreachable / not EC2),
the detector returns an empty Resource and the resolver falls back to ``ec2:default`` —
matching the agent.
"""
import logging
from urllib.error import URLError
from urllib.request import Request, urlopen

from opentelemetry.sdk.resources import Resource, ResourceDetector

logger = logging.getLogger(__name__)

_AWS_METADATA_HOST = "169.254.169.254"
_TOKEN_PATH = "/latest/api/token"
_ASG_TAG_PATH = "/latest/meta-data/tags/instance/aws:autoscaling:groupName"
_TOKEN_HEADER = "X-aws-ec2-metadata-token"
_TOKEN_TTL_HEADER = "X-aws-ec2-metadata-token-ttl-seconds"
_TIMEOUT_SECONDS = 1.0

# Resource attribute key the environment resolver reads — matches the agent's.
EC2_ASG_ATTRIBUTE = "ec2.tag.aws:autoscaling:groupName"


def _http_request(method: str, path: str, headers: dict, host: str) -> str:
    request = Request("http://" + host + path, headers=headers, method=method)
    with urlopen(request, timeout=_TIMEOUT_SECONDS) as response:  # nosec B310 - link-local IMDS
        return response.read().decode("utf-8")


class Ec2AutoScalingGroupResourceDetector(ResourceDetector):
    """Detects the EC2 Auto Scaling group via IMDS instance tags.

    Returns an empty Resource (never raises) when not on EC2, when IMDS is unreachable,
    or when instance metadata tags are not enabled.
    """

    def __init__(self, host: str = _AWS_METADATA_HOST):
        # host is overridable for tests; defaults to the EC2 link-local address.
        self._host = host

    def detect(self) -> Resource:
        try:
            token = _http_request("PUT", _TOKEN_PATH, {_TOKEN_TTL_HEADER: "60"}, self._host)
            asg = _http_request("GET", _ASG_TAG_PATH, {_TOKEN_HEADER: token}, self._host).strip()
            if asg:
                return Resource({EC2_ASG_ATTRIBUTE: asg})
        except (URLError, OSError, ValueError) as exception:
            # Not on EC2, IMDS unreachable, or instance tags not enabled — all expected,
            # non-fatal. The resolver falls back to ec2:default, matching the agent.
            logger.debug("Ec2AutoScalingGroupResourceDetector: ASG not available via IMDS: %s", exception)
        return Resource.get_empty()
