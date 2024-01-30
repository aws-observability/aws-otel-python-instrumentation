# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Sequence

from amazon.opentelemetry.distro.sampler._matcher import _Matcher, cloud_platform_mapping
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, SamplingResult
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class _Rule:
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
        http_target = None
        http_url = None
        http_method = None
        http_host = None
        service_name = None

        if attributes is not None:
            http_target = attributes.get(SpanAttributes.HTTP_TARGET, None)
            http_method = attributes.get(SpanAttributes.HTTP_METHOD, None)
            http_url = attributes.get(SpanAttributes.HTTP_URL, None)
            http_host = attributes.get(SpanAttributes.HTTP_HOST, None)
        # NOTE: The above span attribute keys are deprecated in favor of:
        # URL_PATH/URL_QUERY, HTTP_REQUEST_METHOD, URL_FULL, SERVER_ADDRESS/SERVER_PORT
        # For now, the old attribute keys are kept for consistency with other centralized samplers

        # Resource shouldn't be none as it should default to empty resource
        if resource is not None:
            service_name = resource.attributes.get(ResourceAttributes.SERVICE_NAME, "")

        # target may be in url
        if http_target is None and http_url is not None:
            scheme_end_index = http_url.find("://")
            # Per spec, http.url is always populated with scheme://host/target. If scheme doesn't
            # match, assume it's bad instrumentation and ignore.
            if scheme_end_index > -1:
                path_index = http_url.find("/", scheme_end_index + len("://"))
                if path_index == -1:
                    http_target = "/"
                else:
                    http_target = http_url[path_index:]

        return (
            _Matcher.attribute_match(attributes, self.sampling_rule.Attributes)
            and _Matcher.wild_card_match(http_target, self.sampling_rule.URLPath)
            and _Matcher.wild_card_match(http_method, self.sampling_rule.HTTPMethod)
            and _Matcher.wild_card_match(http_host, self.sampling_rule.Host)
            and _Matcher.wild_card_match(service_name, self.sampling_rule.ServiceName)
            and _Matcher.wild_card_match(self.get_service_type(resource), self.sampling_rule.ServiceType)
        )

    # pylint: disable=no-self-use
    def get_service_type(self, resource: Resource) -> str:
        if resource is None:
            return ""

        cloud_platform = resource.attributes.get(ResourceAttributes.CLOUD_PLATFORM, None)
        if cloud_platform is None:
            return ""

        return cloud_platform_mapping.get(cloud_platform, "")
