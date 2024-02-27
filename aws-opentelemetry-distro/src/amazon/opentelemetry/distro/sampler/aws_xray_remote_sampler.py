# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import random
from logging import getLogger
from threading import Lock, Timer
from typing import Optional, Sequence

from typing_extensions import override

from amazon.opentelemetry.distro.sampler._aws_xray_sampling_client import _AwsXRaySamplingClient
from amazon.opentelemetry.distro.sampler._clock import _Clock
from amazon.opentelemetry.distro.sampler._fallback_sampler import _FallbackSampler
from amazon.opentelemetry.distro.sampler._rule_cache import DEFAULT_TARGET_POLLING_INTERVAL_SECONDS, _RuleCache
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import ParentBased, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

DEFAULT_RULES_POLLING_INTERVAL_SECONDS = 300
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

    def __init__(
        self,
        resource: Resource,
        endpoint: str = None,
        polling_interval: int = None,
        log_level=None,
    ):
        # Override default log level
        if log_level is not None:
            _logger.setLevel(log_level)

        if endpoint is None:
            _logger.info("`endpoint` is `None`. Defaulting to %s", DEFAULT_SAMPLING_PROXY_ENDPOINT)
            endpoint = DEFAULT_SAMPLING_PROXY_ENDPOINT
        if polling_interval is None or polling_interval < 10:
            _logger.info(
                "`polling_interval` is `None` or too small. Defaulting to %s", DEFAULT_RULES_POLLING_INTERVAL_SECONDS
            )
            polling_interval = DEFAULT_RULES_POLLING_INTERVAL_SECONDS

        self.__client_id = self.__generate_client_id()
        self._clock = _Clock()
        self.__xray_client = _AwsXRaySamplingClient(endpoint, log_level=log_level)
        self.__fallback_sampler = ParentBased(_FallbackSampler(self._clock))

        self.__polling_interval = polling_interval
        self.__target_polling_interval = DEFAULT_TARGET_POLLING_INTERVAL_SECONDS
        self.__rule_polling_jitter = random.uniform(0.0, 5.0)
        self.__target_polling_jitter = random.uniform(0.0, 0.1)

        if resource is not None:
            self.__resource = resource
        else:
            _logger.warning("OTel Resource provided is `None`. Defaulting to empty resource")
            self.__resource = Resource.get_empty()

        self.__rule_cache_lock = Lock()
        self.__rule_cache = _RuleCache(
            self.__resource, self.__fallback_sampler, self.__client_id, self._clock, self.__rule_cache_lock
        )

        # Schedule the next rule poll now
        # Python Timers only run once, so they need to be recreated for every poll
        self._rules_timer = Timer(0, self.__start_sampling_rule_poller)
        self._rules_timer.daemon = True  # Ensures that when the main thread exits, the Timer threads are killed
        self._rules_timer.start()

        # set up the target poller to go off once after the default interval. Subsequent polls may use new intervals.
        self._targets_timer = Timer(
            self.__target_polling_interval + self.__target_polling_jitter, self.__start_sampling_target_poller
        )
        self._targets_timer.daemon = True  # Ensures that when the main thread exits, the Timer threads are killed
        self._targets_timer.start()

    # pylint: disable=no-self-use
    @override
    def should_sample(
        self,
        parent_context: Optional[Context],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence[Link] = None,
        trace_state: TraceState = None,
    ) -> SamplingResult:
        if self.__rule_cache.expired():
            _logger.debug("Rule cache is expired so using fallback sampling strategy")
            return self.__fallback_sampler.should_sample(
                parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
            )

        return self.__rule_cache.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    # pylint: disable=no-self-use
    @override
    def get_description(self) -> str:
        description = "AwsXRayRemoteSampler{remote sampling with AWS X-Ray}"
        return description

    def __get_and_update_sampling_rules(self) -> None:
        sampling_rules = self.__xray_client.get_sampling_rules()
        self.__rule_cache.update_sampling_rules(sampling_rules)

    def __start_sampling_rule_poller(self) -> None:
        self.__get_and_update_sampling_rules()
        # Schedule the next sampling rule poll
        self._rules_timer = Timer(
            self.__polling_interval + self.__rule_polling_jitter, self.__start_sampling_rule_poller
        )
        self._rules_timer.daemon = True
        self._rules_timer.start()

    def __get_and_update_sampling_targets(self) -> None:
        all_statistics = self.__rule_cache.get_all_statistics()
        sampling_targets_response = self.__xray_client.get_sampling_targets(all_statistics)
        refresh_rules, min_polling_interval = self.__rule_cache.update_sampling_targets(sampling_targets_response)
        if refresh_rules:
            self.__get_and_update_sampling_rules()
        if min_polling_interval is not None:
            self.__target_polling_interval = min_polling_interval

    def __start_sampling_target_poller(self) -> None:
        self.__get_and_update_sampling_targets()
        # Schedule the next sampling targets poll
        self._targets_timer = Timer(
            self.__target_polling_interval + self.__target_polling_jitter, self.__start_sampling_target_poller
        )
        self._targets_timer.daemon = True
        self._targets_timer.start()

    def __generate_client_id(self) -> str:
        hex_chars = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]
        client_id_array = []
        for _ in range(0, 24):
            client_id_array.append(random.choice(hex_chars))
        return "".join(client_id_array)
