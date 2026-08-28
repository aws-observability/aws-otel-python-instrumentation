# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from opentelemetry import context as otel_context
from opentelemetry.context import Context
from opentelemetry.trace import Span

# Holds the enclosing GenAI span's SpanContext. Its presence marks a scope where the HTTP client span for the same
# remote call is redundant: the sampler drops that span, and the propagator sends this SpanContext in its place.
_GEN_AI_SPAN_CONTEXT_KEY = otel_context.create_key("gen_ai_span_context")


def set_http_client_span_collapsing_in_context(
    span: Span,
    context: Optional[Context] = None,
) -> Context:
    """Mark ``context`` so an HTTP CLIENT child is collapsed into ``span``."""
    return otel_context.set_value(_GEN_AI_SPAN_CONTEXT_KEY, span.get_span_context(), context)
