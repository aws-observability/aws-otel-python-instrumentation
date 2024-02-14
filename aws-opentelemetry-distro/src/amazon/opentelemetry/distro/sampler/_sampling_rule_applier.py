# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from threading import Lock
from typing import Optional, Sequence
from urllib.parse import urlparse

from amazon.opentelemetry.distro.sampler._clock import _Clock
from amazon.opentelemetry.distro.sampler._matcher import _Matcher, cloud_platform_mapping
from amazon.opentelemetry.distro.sampler._rate_limiting_sampler import _RateLimitingSampler
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from amazon.opentelemetry.distro.sampler._sampling_statistics_document import _SamplingStatisticsDocument
from amazon.opentelemetry.distro.sampler._sampling_target import _SamplingTarget
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import Decision, ParentBased, SamplingResult, TraceIdRatioBased
from opentelemetry.semconv.resource import CloudPlatformValues, ResourceAttributes
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class _SamplingRuleApplier:
    def __init__(self, sampling_rule: _SamplingRule, client_id: str, clock: _Clock):
        self.__client_id = client_id
        self._clock = clock
        self.sampling_rule = sampling_rule

        self.__statistics = _SamplingStatisticsDocument(self.__client_id, self.sampling_rule.RuleName)
        self.__statistics_lock = Lock()
        self.__fixed_rate_sampler = ParentBased(TraceIdRatioBased(self.sampling_rule.FixedRate))

        # Initialize with borrowing allowed if there will be a quota > 0
        if self.sampling_rule.ReservoirSize > 0:
            self.__reservoir_sampler = self.__create_reservoir_sampler(quota=1, borrowing=True)
        else:
            self.__reservoir_sampler = self.__create_reservoir_sampler(quota=0, borrowing=False)

        self.__reservoir_expiry = self._clock.now()

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
        has_borrowed = False
        has_sampled = False

        reservoir_expired: bool = self._clock.now() >= self.__reservoir_expiry
        sampling_result = SamplingResult(decision=Decision.DROP, attributes=attributes, trace_state=trace_state)
        if reservoir_expired:
            self.__rate_limiting_sampler.borrowing = True

        sampling_result = self.__reservoir_sampler.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

        if sampling_result.decision is not Decision.DROP:
            has_borrowed = self.__rate_limiting_sampler.borrowing
            has_sampled = True
        else:
            sampling_result = self.__fixed_rate_sampler.should_sample(
                parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
            )
            if sampling_result.decision is not Decision.DROP:
                has_sampled = True

        with self.__statistics_lock:
            self.__statistics.RequestCount += 1
            self.__statistics.BorrowCount += 1 if has_borrowed else 0
            self.__statistics.SampleCount += 1 if has_sampled else 0

        return sampling_result

    def get_then_reset_statistics(self) -> dict:
        with self.__statistics_lock:
            old_stats = self.__statistics
            self.__statistics = _SamplingStatisticsDocument(self.__client_id, self.sampling_rule.RuleName)

        return old_stats.snapshot(self._clock)

    def __create_reservoir_sampler(self, quota: int, borrowing: bool) -> ParentBased:
        # Keep a reference to rate_limiting_sampler to update its `borrowing` status
        self.__rate_limiting_sampler = _RateLimitingSampler(quota, self._clock)
        self.__rate_limiting_sampler.borrowing = borrowing
        return ParentBased(self.__rate_limiting_sampler)

    def update_target(self, target: _SamplingTarget) -> None:
        new_quota = target.ReservoirQuota if target.ReservoirQuota is not None else 0
        new_fixed_rate = target.FixedRate if target.FixedRate is not None else 0
        self.__reservoir_sampler = self.__create_reservoir_sampler(new_quota, False)
        self.__fixed_rate_sampler = ParentBased(TraceIdRatioBased(new_fixed_rate))

        if target.ReservoirQuotaTTL is not None:
            self.__reservoir_expiry = self._clock.from_timestamp(target.ReservoirQuotaTTL)
        else:
            # Treat as expired
            self.__reservoir_expiry = self._clock.now()

    def matches(self, resource: Resource, attributes: Attributes) -> bool:
        url_path = None
        url_full = None
        http_request_method = None
        server_address = None
        service_name = None

        if attributes is not None:
            # If `URL_PATH/URL_FULL/HTTP_REQUEST_METHOD/SERVER_ADDRESS` are not populated
            # also check `HTTP_TARGET/HTTP_URL/HTTP_METHOD/HTTP_HOST` respectively as backup
            url_path = attributes.get(SpanAttributes.URL_PATH, attributes.get(SpanAttributes.HTTP_TARGET, None))
            url_full = attributes.get(SpanAttributes.URL_FULL, attributes.get(SpanAttributes.HTTP_URL, None))
            http_request_method = attributes.get(
                SpanAttributes.HTTP_REQUEST_METHOD, attributes.get(SpanAttributes.HTTP_METHOD, None)
            )
            server_address = attributes.get(
                SpanAttributes.SERVER_ADDRESS, attributes.get(SpanAttributes.HTTP_HOST, None)
            )

        # Resource shouldn't be none as it should default to empty resource
        if resource is not None:
            service_name = resource.attributes.get(ResourceAttributes.SERVICE_NAME, "")

        # target may be in url
        if url_path is None and url_full is not None:
            scheme_end_index = url_full.find("://")
            # For network calls, URL usually has `scheme://host[:port][path][?query][#fragment]` format
            # Per spec, url.full is always populated with scheme://
            # If scheme is not present, assume it's bad instrumentation and ignore.
            if scheme_end_index > -1:
                # urlparse("scheme://netloc/path;parameters?query#fragment")
                url_path = urlparse(url_full).path
                if url_path == "":
                    url_path = "/"
        elif url_path is None and url_full is None:
            # When missing, the URL Path is assumed to be /
            url_path = "/"

        return (
            _Matcher.attribute_match(attributes, self.sampling_rule.Attributes)
            and _Matcher.wild_card_match(url_path, self.sampling_rule.URLPath)
            and _Matcher.wild_card_match(http_request_method, self.sampling_rule.HTTPMethod)
            and _Matcher.wild_card_match(server_address, self.sampling_rule.Host)
            and _Matcher.wild_card_match(service_name, self.sampling_rule.ServiceName)
            and _Matcher.wild_card_match(self.__get_service_type(resource), self.sampling_rule.ServiceType)
            and _Matcher.wild_card_match(self.__get_arn(resource, attributes), self.sampling_rule.ResourceARN)
        )

    # pylint: disable=no-self-use
    def __get_service_type(self, resource: Resource) -> str:
        if resource is None:
            return ""

        cloud_platform = resource.attributes.get(ResourceAttributes.CLOUD_PLATFORM, None)
        if cloud_platform is None:
            return ""

        return cloud_platform_mapping.get(cloud_platform, "")

    # pylint: disable=no-self-use
    def __get_arn(self, resource: Resource, attributes: Attributes) -> str:
        if resource is not None:
            arn = resource.attributes.get(ResourceAttributes.AWS_ECS_CONTAINER_ARN, None)
            if arn is not None:
                return arn
        if (
            resource is not None
            and resource.attributes.get(ResourceAttributes.CLOUD_PLATFORM) == CloudPlatformValues.AWS_LAMBDA.value
        ):
            return self.__get_lambda_arn(resource, attributes)
        return ""

    def __get_lambda_arn(self, resource: Resource, attributes: Attributes) -> str:
        arn = resource.attributes.get(
            ResourceAttributes.CLOUD_RESOURCE_ID, resource.attributes.get(ResourceAttributes.FAAS_ID, None)
        )
        if arn is not None:
            return arn

        # Note from `SpanAttributes.CLOUD_RESOURCE_ID`:
        # "On some cloud providers, it may not be possible to determine the full ID at startup,
        # so it may be necessary to set cloud.resource_id as a span attribute instead."
        arn = attributes.get(SpanAttributes.CLOUD_RESOURCE_ID, attributes.get("faas.id", None))
        if arn is not None:
            return arn

        return ""
