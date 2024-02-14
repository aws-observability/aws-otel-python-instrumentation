# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Sequence

from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, Sampler, SamplingResult, TraceIdRatioBased
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class _FallbackSampler(Sampler):
    def __init__(self):
        # TODO: Add Reservoir sampler
        # pylint: disable=unused-private-member
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
        # TODO: add reservoir + fixed rate sampling
        return ALWAYS_ON.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    # pylint: disable=no-self-use
    def get_description(self) -> str:
        description = (
            "FallbackSampler{fallback sampling with sampling config of 1 req/sec and 5% of additional requests}"
        )
        return description
