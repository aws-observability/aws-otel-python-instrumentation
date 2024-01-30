# Copyright The OpenTelemetry Authors
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from logging import getLogger
from threading import Timer
from typing import Optional, Sequence

from amazon.opentelemetry.distro.sampler.aws_xray_sampling_client import AwsXRaySamplingClient
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

DEFAULT_RULES_POLLING_INTERVAL = 300
DEFAULT_TARGET_POLLING_INTERVAL = 10
DEFAULT_SAMPLING_PROXY_ENDPOINT = "http://127.0.0.1:2000"

class AwsXRayRemoteSampler(Sampler):
    """
    Remote Sampler for OpenTelemetry that gets sampling configurations from AWS X-Ray

    Args:
        resource: OpenTelemetry Resource (Optional)
        endpoint: proxy endpoint for AWS X-Ray Sampling (Optional)
        polling_interval: Polling interval for getSamplingRules call (Optional)
        log_level: custom log level configuration for remote sampler (Optional)
    """

    __resource : Resource
    __polling_interval : int
    __xray_client : AwsXRaySamplingClient

    def __init__(self, resource=None, endpoint=DEFAULT_SAMPLING_PROXY_ENDPOINT, polling_interval=DEFAULT_RULES_POLLING_INTERVAL, log_level = None):
        # Override default log level
        if log_level is not None:
            _logger.setLevel(log_level)

        self.__xray_client = AwsXRaySamplingClient(endpoint, log_level=log_level)
        self.__polling_interval = polling_interval
        self.__resource = resource

        self.__start_sampling_rule_poller()

    def should_sample(
        self,
        parent_context: Optional["Context"],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence["Link"] = None,
        trace_state: "TraceState" = None,
    ) -> "SamplingResult":
        # TODO: add sampling functionality
        return ALWAYS_OFF

    def get_description(self) -> str:
        description = "AwsXRayRemoteSampler{remote sampling with AWS X-Ray}"
        return description

    def __get_and_update_sampling_rules(self):
        sampling_rules = self.__xray_client.get_sampling_rules()

        # TODO: Update sampling rules cache
        _logger.info(f"Got Sampling Rules: {json.dumps([ob.__dict__ for ob in sampling_rules])}")

    def __start_sampling_rule_poller(self):
        self.__get_and_update_sampling_rules()
        # Schedule the next sampling rule poll 
        self._timer = Timer(self.__polling_interval, self.__start_sampling_rule_poller)
        self._timer.daemon = True
        self._timer.start()

