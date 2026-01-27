# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import hashlib
import re
from logging import getLogger
from threading import Lock
from typing import Callable, Dict, List, Optional, Sequence

from cachetools import TTLCache

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_LOCAL_OPERATION
from amazon.opentelemetry.distro._aws_span_processing_util import _generate_ingress_operation
from amazon.opentelemetry.distro.sampler._aws_sampling_result import _AwsSamplingResult
from amazon.opentelemetry.distro.sampler._aws_xray_adaptive_sampling_config import (
    _AnomalyConditions,
    _AWSXRayAdaptiveSamplingConfig,
    _UsageType,
)
from amazon.opentelemetry.distro.sampler._clock import _Clock
from amazon.opentelemetry.distro.sampler._fallback_sampler import _FallbackSampler
from amazon.opentelemetry.distro.sampler._rate_limiter import _RateLimiter
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from amazon.opentelemetry.distro.sampler._sampling_rule_applier import _SamplingRuleApplier
from amazon.opentelemetry.distro.sampler._sampling_target import _SamplingTarget, _SamplingTargetResponse
from opentelemetry import baggage
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, StatusCode
from opentelemetry.sdk.trace.sampling import SamplingResult
from opentelemetry.semconv._incubating.attributes.http_attributes import HTTP_STATUS_CODE
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import Link, SpanKind, get_current_span
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

RULE_CACHE_TTL_SECONDS = 3600
DEFAULT_TARGET_POLLING_INTERVAL_SECONDS = 10
NANOS_TO_MILLIS: float = 1_000_000.0
TRACE_USAGE_CACHE_MAX_SIZE = 100_000
TRACE_USAGE_CACHE_TTL_SECONDS = 600


class _RuleCache:
    def __init__(
        self, resource: Resource, fallback_sampler: _FallbackSampler, client_id: str, clock: _Clock, lock: Lock
    ):
        self.__client_id = client_id
        self.__rule_appliers: List[_SamplingRuleApplier] = []
        self.__cache_lock = lock
        self.__resource = resource
        self.__service_name = (
            resource.attributes.get(ResourceAttributes.SERVICE_NAME, "default") if resource else "default"
        )
        self._fallback_sampler = fallback_sampler
        self._clock = clock
        self._last_modified = self._clock.now()
        self._adaptive_sampling_rule_exists = False
        self._adaptive_sampling_config: Optional[_AWSXRayAdaptiveSamplingConfig] = None
        self._anomaly_capture_rate_limiter: Optional[_RateLimiter] = None
        self._trace_usage_cache_lock = Lock()
        self._trace_usage_cache: TTLCache[int, _UsageType] = TTLCache(
            maxsize=TRACE_USAGE_CACHE_MAX_SIZE,
            ttl=TRACE_USAGE_CACHE_TTL_SECONDS,
            timer=lambda: self._clock.now().timestamp(),
        )

        self._rule_to_hash_map: Dict = {}
        self._hash_to_rule_map: Dict = {}

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
        parent_span_context = get_current_span(parent_context).get_span_context()
        upstream_matched_rule_hash = parent_span_context.trace_state.get(
            _AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY
        )
        if upstream_matched_rule_hash is None:
            bag = baggage.get_all(parent_context)
            upstream_matched_rule_hash = (
                bag.get(_AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY) if bag is not None else None
            )
        result: SamplingResult = None
        rule_name_to_propagate: Optional[str] = None
        hashed_rule_name: Optional[str] = None
        for rule_applier in self.__rule_appliers:
            if rule_applier.matches(self.__resource, attributes):
                result = rule_applier.should_sample(
                    parent_context,
                    trace_id,
                    name,
                    kind=kind,
                    attributes=attributes,
                    links=links,
                    trace_state=trace_state,
                )
                if upstream_matched_rule_hash is not None:
                    rule_name_to_propagate = self._hash_to_rule_map.get(upstream_matched_rule_hash, None)
                elif parent_span_context.is_valid:
                    rule_name_to_propagate = None
                else:
                    rule_name_to_propagate = rule_applier.sampling_rule.RuleName
                hashed_rule_name = self._rule_to_hash_map.get(rule_name_to_propagate, upstream_matched_rule_hash)
                break

        if result is not None:
            return _AwsSamplingResult(
                decision=result.decision,
                attributes=result.attributes,
                trace_state=result.trace_state,
                sampling_rule_name=rule_name_to_propagate,
                sampling_rule_hash=hashed_rule_name,
                has_adaptive_sampling_config=self._adaptive_sampling_config is not None,
            )

        _logger.debug("No sampling rules were matched")
        # Should not ever reach fallback sampler as default rule is able to match
        return self._fallback_sampler.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    def set_adaptive_sampling_config(self, config: _AWSXRayAdaptiveSamplingConfig) -> None:
        if self._adaptive_sampling_config is not None:
            _logger.warning("Programming bug - Adaptive sampling config is already set")
        elif config is not None and self._adaptive_sampling_config is None:
            self._adaptive_sampling_config = config

            # Initialize anomaly capture rate limiter if error capture limit is configured
            if config.anomaly_capture_limit is not None:
                anomaly_traces_per_second = config.anomaly_capture_limit.anomaly_traces_per_second
                self._anomaly_capture_rate_limiter = _RateLimiter(
                    anomaly_traces_per_second, anomaly_traces_per_second, self._clock
                )
            else:
                self._anomaly_capture_rate_limiter = _RateLimiter(1, 1, self._clock)

    # pylint: disable=too-many-locals
    def adapt_sampling(self, span: ReadableSpan, span_batcher: Callable[[ReadableSpan], None]) -> None:
        if not self._adaptive_sampling_rule_exists and self._adaptive_sampling_config is None:
            return

        result: _AnomalyDetectionResult = self.__is_anomaly(span)
        should_boost_sampling = result.should_boost_sampling
        should_capture_anomaly_span = result.should_capture_anomaly_span

        trace_id: int = span.context.trace_id
        is_new_trace: bool = False
        with self._trace_usage_cache_lock:
            existing_usage: _UsageType = self._trace_usage_cache.get(trace_id)
            is_new_trace = existing_usage is None
            if existing_usage is None:
                self._trace_usage_cache[trace_id] = _UsageType.NEITHER

        # Anomaly Capture
        is_span_captured = False
        if _UsageType.is_used_for_anomaly_trace_capture(existing_usage) or (
            should_capture_anomaly_span
            and not span.context.is_remote
            and self._anomaly_capture_rate_limiter is not None
            and self._anomaly_capture_rate_limiter.try_spend(1)
        ):
            span_batcher(span)
            is_span_captured = True

        # Sampling Boost
        is_counted_as_anomaly_for_boost = False
        if should_boost_sampling or is_new_trace:
            trace_state_value = span.context.trace_state.get(_AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY)
            upstream_rule_name = (
                self._hash_to_rule_map.get(trace_state_value, trace_state_value) if trace_state_value else None
            )

            rule_to_report_to: Optional[_SamplingRuleApplier] = None
            matched_rule: Optional[_SamplingRuleApplier] = None
            for applier in self.__rule_appliers:
                # Rule propagated from when sampling decision was made, otherwise the matched rule
                if applier.sampling_rule.RuleName == upstream_rule_name:
                    rule_to_report_to = applier
                    break
                # spanData.getAttributes() -> span.attributes, spanData.getResource() -> self.__resource
                if matched_rule is None and applier.matches(self.__resource, span.attributes):
                    matched_rule = applier

            if rule_to_report_to is None:
                if matched_rule is None:
                    _logger.debug(
                        "No sampling rule matched the request. This is a bug in either the OpenTelemetry SDK or X-Ray."
                    )
                elif not span.parent.is_valid:
                    # Span is not from an upstream service, so we should boost the matched rule
                    rule_to_report_to = matched_rule

            if (
                should_boost_sampling
                and rule_to_report_to is not None
                and rule_to_report_to.has_boost()
                and not _UsageType.is_used_for_boost(existing_usage)
            ):
                rule_to_report_to.count_anomaly_trace(span)
                is_counted_as_anomaly_for_boost = True

            if is_new_trace and rule_to_report_to is not None and rule_to_report_to.has_boost():
                rule_to_report_to.count_trace()

        self.__update_trace_usage_cache(trace_id, is_span_captured, is_counted_as_anomaly_for_boost)

    # pylint: disable=too-many-branches
    def __is_anomaly(self, span: ReadableSpan) -> "_AnomalyDetectionResult":
        should_boost_sampling: bool = False
        should_capture_anomaly_span: bool = False
        status_code: int = span.attributes.get(HTTP_STATUS_CODE)

        anomaly_conditions: List[_AnomalyConditions] = (
            self._adaptive_sampling_config.anomaly_conditions if self._adaptive_sampling_config else None
        )
        # Empty list -> no conditions will apply and we will not do anything
        if anomaly_conditions:
            operation = span.attributes.get(AWS_LOCAL_OPERATION)
            if operation is None:
                operation = _generate_ingress_operation(span)

            # It is an iterable, but the current pylint version doesn't recognize it
            # pylint: disable=not-an-iterable
            for condition in anomaly_conditions:
                # Skip condition if it would only re-apply action already being taken
                if (should_boost_sampling and condition.usage == _UsageType.SAMPLING_BOOST) or (
                    should_capture_anomaly_span and condition.usage == _UsageType.ANOMALY_TRACE_CAPTURE
                ):
                    continue

                # Check if the operation matches any in the list or if operations list is null (match all)
                if condition.operations is not None and operation not in condition.operations:
                    continue

                # Check if any anomalyConditions detect an anomaly either through error code or latency
                is_anomaly = False

                error_code_regex = condition.error_code_regex
                if status_code is not None and error_code_regex is not None:
                    is_anomaly = re.match(error_code_regex, str(status_code)) is not None

                high_latency_ms = condition.high_latency_ms
                if high_latency_ms is not None:
                    nanos: int = span.end_time - span.start_time
                    latency_ms: float = nanos / NANOS_TO_MILLIS
                    # If both error code and latency condition defined, both must agree to consider span as anomaly
                    is_anomaly = (error_code_regex is None or is_anomaly) and latency_ms >= high_latency_ms

                if is_anomaly:
                    usage = condition.usage
                    if usage == _UsageType.BOTH:
                        should_boost_sampling = True
                        should_capture_anomaly_span = True
                    elif usage == _UsageType.SAMPLING_BOOST:
                        should_boost_sampling = True
                    elif usage == _UsageType.ANOMALY_TRACE_CAPTURE:
                        should_capture_anomaly_span = True
                    elif usage is None:  # Default to both being True if usage is undefined
                        should_boost_sampling = True
                        should_capture_anomaly_span = True

                if should_boost_sampling and should_capture_anomaly_span:
                    break
        elif (status_code is not None and status_code > 499) or (
            status_code is None and span.status is not None and span.status.status_code == StatusCode.ERROR
        ):
            should_boost_sampling = True
            should_capture_anomaly_span = True

        return _AnomalyDetectionResult(should_boost_sampling, should_capture_anomaly_span)

    def __update_trace_usage_cache(
        self, trace_id: int, is_span_captured: bool, is_counted_as_anomaly_for_boost: bool
    ) -> None:
        with self._trace_usage_cache_lock:
            existing_usage = self._trace_usage_cache.get(trace_id)

            # Any interaction with a cache entry will reset the expiration timer of that entry
            if is_span_captured and is_counted_as_anomaly_for_boost:
                self._trace_usage_cache[trace_id] = _UsageType.BOTH
            elif is_span_captured:
                if _UsageType.is_used_for_boost(existing_usage):
                    self._trace_usage_cache[trace_id] = _UsageType.BOTH
                else:
                    self._trace_usage_cache[trace_id] = _UsageType.ANOMALY_TRACE_CAPTURE
            elif is_counted_as_anomaly_for_boost:
                if _UsageType.is_used_for_anomaly_trace_capture(existing_usage):
                    self._trace_usage_cache[trace_id] = _UsageType.BOTH
                else:
                    self._trace_usage_cache[trace_id] = _UsageType.SAMPLING_BOOST
            elif existing_usage is not None:
                self._trace_usage_cache[trace_id] = existing_usage
            else:
                self._trace_usage_cache[trace_id] = _UsageType.NEITHER

    def update_sampling_rules(self, new_sampling_rules: List[_SamplingRule]) -> None:
        new_sampling_rules.sort()
        temp_rule_appliers: List[_SamplingRuleApplier] = []
        for sampling_rule in new_sampling_rules:
            if sampling_rule.RuleName == "":
                _logger.debug("sampling rule without rule name is not supported")
                continue
            if sampling_rule.Version != 1:
                _logger.debug("sampling rule without Version 1 is not supported: RuleName: %s", sampling_rule.RuleName)
                continue
            temp_rule_appliers.append(
                _SamplingRuleApplier(sampling_rule, self.__service_name, self.__client_id, self._clock)
            )

        with self.__cache_lock:
            # map list of rule appliers by each applier's sampling_rule name
            rule_applier_map: Dict[str, _SamplingRuleApplier] = {
                applier.sampling_rule.RuleName: applier for applier in self.__rule_appliers
            }

            # If a sampling rule has not changed, keep its respective applier in the cache.
            new_applier: _SamplingRuleApplier
            for index, new_applier in enumerate(temp_rule_appliers):
                rule_name_to_check = new_applier.sampling_rule.RuleName
                if rule_name_to_check in rule_applier_map:
                    old_applier = rule_applier_map[rule_name_to_check]
                    if new_applier.sampling_rule == old_applier.sampling_rule:
                        temp_rule_appliers[index] = old_applier
            self.__rule_appliers = temp_rule_appliers
            self._last_modified = self._clock.now()

            self._adaptive_sampling_rule_exists = any(
                applier.sampling_rule.SamplingRateBoost is not None for applier in temp_rule_appliers
            )
            self._rule_to_hash_map: Dict[str, str] = {
                a.sampling_rule.RuleName: self._hash_rule_name(a.sampling_rule.RuleName) for a in temp_rule_appliers
            }
            self._hash_to_rule_map: Dict[str, str] = {v: k for k, v in self._rule_to_hash_map.items()}

    def update_sampling_targets(self, sampling_targets_response: _SamplingTargetResponse) -> tuple[bool, int]:
        targets: List[_SamplingTarget] = sampling_targets_response.SamplingTargetDocuments

        with self.__cache_lock:
            next_polling_interval = DEFAULT_TARGET_POLLING_INTERVAL_SECONDS
            min_polling_interval = None

            target_map: Dict[str, _SamplingTarget] = {target.RuleName: target for target in targets}

            new_appliers = []
            applier: _SamplingRuleApplier
            for applier in self.__rule_appliers:
                if applier.sampling_rule.RuleName in target_map:
                    target = target_map[applier.sampling_rule.RuleName]
                    new_appliers.append(applier.with_target(target))

                    if target.Interval is not None:
                        if min_polling_interval is None or min_polling_interval > target.Interval:
                            min_polling_interval = target.Interval
                else:
                    new_appliers.append(applier)

            self.__rule_appliers = new_appliers

            if min_polling_interval is not None:
                next_polling_interval = min_polling_interval

            last_rule_modification = self._clock.from_timestamp(sampling_targets_response.LastRuleModification)
            refresh_rules = last_rule_modification > self._last_modified

            return (refresh_rules, next_polling_interval)

    def get_all_statistics(self) -> tuple[List[dict], List[dict]]:
        all_statistics = []
        all_boost_statistics = []
        applier: _SamplingRuleApplier
        for applier in self.__rule_appliers:
            stats, boost_stats = applier.get_then_reset_statistics()
            if stats["RequestCount"] > 0:
                all_statistics.append(stats)
            if boost_stats["ServiceName"] is not None and boost_stats["TotalCount"] > 0:
                all_boost_statistics.append(boost_stats)
        return all_statistics, all_boost_statistics

    def expired(self) -> bool:
        with self.__cache_lock:
            return self._clock.now() > self._last_modified + self._clock.time_delta(seconds=RULE_CACHE_TTL_SECONDS)

    @staticmethod
    def _hash_rule_name(rule_name: str) -> str:
        hash_bytes = hashlib.sha256(rule_name.encode("utf-8")).digest()
        return "".join(f"{byte:02x}" for byte in hash_bytes[:8])


class _AnomalyDetectionResult:
    def __init__(self, should_boost_sampling: bool, should_capture_anomaly_span: bool):
        self.should_boost_sampling = should_boost_sampling
        self.should_capture_anomaly_span = should_capture_anomaly_span
