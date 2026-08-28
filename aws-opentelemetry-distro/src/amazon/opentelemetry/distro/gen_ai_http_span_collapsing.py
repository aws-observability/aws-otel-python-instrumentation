# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import Token
from typing import Optional, Sequence

from typing_extensions import override

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter, Setter, TextMapPropagator, default_getter, default_setter
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Link, NonRecordingSpan, Span, SpanKind, set_span_in_context
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

# Holds the enclosing GenAI LLM span's SpanContext. Its presence marks a scope where the HTTP client span for the
# LLM call is redundant: the sampler drops that span, and the propagator sends this SpanContext in its place, so the
# span disappears without breaking trace continuity for an instrumented LLM endpoint.
_GENAI_LLM_SPAN_CONTEXT_KEY = otel_context.create_key("genai_llm_span_context")

# OTel designates http.request.method a sampling-relevant attribute for HTTP client spans, so instrumentations must
# populate it at span creation. New-then-old fallback mirrors other HTTP semantic convention handling in the distro.
_HTTP_METHOD_KEYS = (SpanAttributes.HTTP_REQUEST_METHOD, SpanAttributes.HTTP_METHOD)


def attach_llm_span_context(
    span: Span,
    parent_context: Optional[Context] = None,
    *,
    collapse_http_span: bool = False,
) -> Token:
    """Attach ``span`` as current and optionally mark its HTTP client span as redundant."""
    ctx = set_span_in_context(span, parent_context)
    if collapse_http_span:
        ctx = otel_context.set_value(_GENAI_LLM_SPAN_CONTEXT_KEY, span.get_span_context(), ctx)
    return otel_context.attach(ctx)


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
            and otel_context.get_value(_GENAI_LLM_SPAN_CONTEXT_KEY, parent_context) is not None
            and any(key in (attributes or {}) for key in _HTTP_METHOD_KEYS)
        ):
            return SamplingResult(Decision.DROP, attributes, trace_state)
        return self._root_sampler.should_sample(parent_context, trace_id, name, kind, attributes, links, trace_state)

    @override
    def get_description(self) -> str:
        return "GenAiHttpDropSampler{" + self._root_sampler.get_description() + "}"


class GenAiLlmContextPropagator(TextMapPropagator):
    """Send the enclosing LLM span's identity for the HTTP span the sampler dropped."""

    def __init__(self, delegate: TextMapPropagator):
        self._delegate = delegate

    @override
    def inject(self, carrier, context: Optional[Context] = None, setter: Setter = default_setter) -> None:
        llm_span_context = otel_context.get_value(_GENAI_LLM_SPAN_CONTEXT_KEY, context)
        if llm_span_context is not None:
            current = trace.get_current_span(context)
            current_span_context = current.get_span_context()
            if not current.is_recording() and current_span_context.trace_id == llm_span_context.trace_id:
                context = set_span_in_context(NonRecordingSpan(llm_span_context), context)
        self._delegate.inject(carrier, context, setter)

    @override
    def extract(self, carrier, context: Optional[Context] = None, getter: Getter = default_getter) -> Context:
        return self._delegate.extract(carrier, context, getter)

    @property
    @override
    def fields(self):
        return self._delegate.fields
