# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from logging import getLogger
from threading import Timer
from typing import Optional, Sequence

from typing_extensions import override

from amazon.opentelemetry.distro.sampler._aws_xray_sampling_client import _AwsXRaySamplingClient
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

DEFAULT_RULES_POLLING_INTERVAL_SECONDS = 300
DEFAULT_TARGET_POLLING_INTERVAL_SECONDS = 10
DEFAULT_SAMPLING_PROXY_ENDPOINT = "http://127.0.0.1:2000"


class AwsXRayRemoteSampler(Sampler):
    """
    Remote Sampler for OpenTelemetry that gets sampling configurations from AWS X-Ray

    Args:
        resource: OpenTelemetry Resource (Required)
        endpoint: proxy endpoint for AWS X-Ray Sampling (Optional)
        polling_interval: Polling interval for getSamplingRules call (Optional)
        log_level: custom log level configuration for remote sampler (Optional)
    """

    __resource: Resource
    __polling_interval: int
    __xray_client: _AwsXRaySamplingClient

    def __init__(
        self,
        resource: Resource,
        endpoint=DEFAULT_SAMPLING_PROXY_ENDPOINT,
        polling_interval=DEFAULT_RULES_POLLING_INTERVAL_SECONDS,
        log_level=None,
    ):
        # Override default log level
        if log_level is not None:
            _logger.setLevel(log_level)

        self.__xray_client = _AwsXRaySamplingClient(endpoint, log_level=log_level)
        self.__polling_interval = polling_interval

        # pylint: disable=unused-private-member
        if resource is not None:
            self.__resource = resource
        else:
            _logger.warning("OTel Resource provided is `None`. Defaulting to empty resource")
            self.__resource = Resource.get_empty()

        # Schedule the next rule poll now
        # Python Timers only run once, so they need to be recreated for every poll
        self._timer = Timer(0, self.__start_sampling_rule_poller)
        self._timer.daemon = True  # Ensures that when the main thread exits, the Timer threads are killed
        self._timer.start()

    # pylint: disable=no-self-use
    @override
    def should_sample(
        self,
        parent_context: Optional["Context"],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence["Link"] = None,
        trace_state: "TraceState" = None,
    ) -> SamplingResult:
        # TODO: add sampling functionality
        return ALWAYS_OFF.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    # pylint: disable=no-self-use
    @override
    def get_description(self) -> str:
        description = "AwsXRayRemoteSampler{remote sampling with AWS X-Ray}"
        return description

    def __get_and_update_sampling_rules(self):
        sampling_rules = self.__xray_client.get_sampling_rules()

        # TODO: Update sampling rules cache
        _logger.info("Got Sampling Rules: %s", {json.dumps([ob.__dict__ for ob in sampling_rules])})

    def __start_sampling_rule_poller(self):
        self.__get_and_update_sampling_rules()
        # Schedule the next sampling rule poll
        self._timer = Timer(self.__polling_interval, self.__start_sampling_rule_poller)
        self._timer.daemon = True
        self._timer.start()
