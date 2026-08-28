# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextvars import Token
from typing import Optional

from opentelemetry import context as otel_context
from opentelemetry.context import Context
from opentelemetry.trace import Span, set_span_in_context

# Holds the enclosing GenAI LLM span's SpanContext. Its presence marks a scope where the HTTP client span for the
# LLM call is redundant: the sampler drops that span, and the propagator sends this SpanContext in its place, so the
# span disappears without breaking trace continuity for an instrumented LLM endpoint.
_GENAI_LLM_SPAN_CONTEXT_KEY = otel_context.create_key("genai_llm_span_context")


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
