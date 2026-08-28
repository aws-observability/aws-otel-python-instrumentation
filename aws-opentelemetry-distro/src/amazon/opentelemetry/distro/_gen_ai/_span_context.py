# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from opentelemetry import context as otel_context
from opentelemetry.context import Context
from opentelemetry.trace import Span

# Holds the SpanContext to use for downstream propagation when a direct child span is suppressed.
_SPAN_FOR_PROPAGATION_CONTEXT_KEY = otel_context.create_key("span_for_propagation")


def set_span_for_propagation_in_context(
    span: Span,
    context: Optional[Context] = None,
) -> Context:
    """Store ``span`` as the context to use for downstream propagation."""
    return otel_context.set_value(_SPAN_FOR_PROPAGATION_CONTEXT_KEY, span.get_span_context(), context)
