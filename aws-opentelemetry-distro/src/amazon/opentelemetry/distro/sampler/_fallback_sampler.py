# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Sequence

from amazon.opentelemetry.distro.sampler._clock import _Clock
from amazon.opentelemetry.distro.sampler._rate_limiting_sampler import _RateLimitingSampler
from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult, TraceIdRatioBased
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class _FallbackSampler(Sampler):
    def __init__(self, clock: _Clock):
        self.__rate_limiting_sampler = _RateLimitingSampler(1, clock)
        self.__fixed_rate_sampler = TraceIdRatioBased(0.05)

    # pylint: disable=no-self-use
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
        sampling_result = self.__rate_limiting_sampler.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )
        if sampling_result.decision is not Decision.DROP:
            return sampling_result
        return self.__fixed_rate_sampler.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    # pylint: disable=no-self-use
    def get_description(self) -> str:
        description = (
            "FallbackSampler{fallback sampling with sampling config of 1 req/sec and 5% of additional requests}"
        )
        return description
