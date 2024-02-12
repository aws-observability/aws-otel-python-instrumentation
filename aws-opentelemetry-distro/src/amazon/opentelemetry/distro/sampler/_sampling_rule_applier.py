# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Sequence
from urllib.parse import urlparse

from amazon.opentelemetry.distro.sampler._matcher import _Matcher, cloud_platform_mapping
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, SamplingResult
from opentelemetry.semconv.resource import CloudPlatformValues, ResourceAttributes
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class _SamplingRuleApplier:
    def __init__(self, sampling_rule: _SamplingRule):
        self.sampling_rule = sampling_rule
        # TODO add self.next_target_fetch_time from maybe time.process_time() or cache's datetime object
        # TODO add statistics
        # TODO change to rate limiter given rate, add fixed rate sampler
        self.reservoir_sampler = ALWAYS_ON
        # self.fixed_rate_sampler = None
        # TODO add clientId

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
        return self.reservoir_sampler.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    def matches(self, resource: Resource, attributes: Attributes) -> bool:
        url_path = None
        url_full = None
        http_request_method = None
        server_address = None
        service_name = None

        if attributes is not None:
            url_path = attributes.get(SpanAttributes.URL_PATH, None)
            url_full = attributes.get(SpanAttributes.URL_FULL, None)
            http_request_method = attributes.get(SpanAttributes.HTTP_REQUEST_METHOD, None)
            server_address = attributes.get(SpanAttributes.SERVER_ADDRESS, None)

        # Resource shouldn't be none as it should default to empty resource
        if resource is not None:
            service_name = resource.attributes.get(ResourceAttributes.SERVICE_NAME, "")

        # target may be in url
        if url_path is None and url_full is not None:
            scheme_end_index = url_full.find("://")
            # For network calls, URL usually has `scheme://host[:port][path][?query][#fragment]` format
            # Per spec, url.full is always populated with scheme://host/target.
            # If scheme doesn't match, assume it's bad instrumentation and ignore.
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
        if attributes is not None and self.__get_service_type(resource=resource) == cloud_platform_mapping.get(
            CloudPlatformValues.AWS_LAMBDA.value
        ):
            arn = attributes.get(SpanAttributes.CLOUD_RESOURCE_ID, None)
            if arn is not None:
                return arn
        return ""
