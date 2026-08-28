# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional, Sequence

from typing_extensions import override

from amazon.opentelemetry.distro._gen_ai._span_context import _GEN_AI_SPAN_CONTEXT_KEY
from opentelemetry import context as otel_context
from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

# OTel designates http.request.method a sampling-relevant attribute for HTTP client spans, so instrumentations must
# populate it at span creation. New-then-old fallback mirrors other HTTP semantic convention handling in the distro.
_HTTP_METHOD_KEYS = (SpanAttributes.HTTP_REQUEST_METHOD, SpanAttributes.HTTP_METHOD)


class GenAiHttpDropSampler(Sampler):
    """Drop HTTP CLIENT spans that duplicate an enclosing native GenAI LLM span."""

    def __init__(self, root_sampler: Sampler):
        if not root_sampler:
            raise ValueError("root_sampler must not be None")
        self._root_sampler = root_sampler

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
        if (
            kind == SpanKind.CLIENT
            and otel_context.get_value(_GEN_AI_SPAN_CONTEXT_KEY, parent_context) is not None
            and any(key in (attributes or {}) for key in _HTTP_METHOD_KEYS)
        ):
            return SamplingResult(Decision.DROP, attributes, trace_state)
        return self._root_sampler.should_sample(parent_context, trace_id, name, kind, attributes, links, trace_state)

    @override
    def get_description(self) -> str:
        return "GenAiHttpDropSampler{" + self._root_sampler.get_description() + "}"
