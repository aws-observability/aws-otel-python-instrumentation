# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from typing_extensions import override

from amazon.opentelemetry.distro._gen_ai._span_context import _SPAN_FOR_PROPAGATION_CONTEXT_KEY
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter, Setter, TextMapPropagator, default_getter, default_setter
from opentelemetry.trace import NonRecordingSpan, set_span_in_context


class GenAiSpanContextPropagator(TextMapPropagator):
    """Send the enclosing GenAI span's identity for the HTTP span the sampler dropped."""

    def __init__(self, delegate: TextMapPropagator):
        self._delegate = delegate

    @override
    def inject(self, carrier, context: Optional[Context] = None, setter: Setter = default_setter) -> None:
        span_for_propagation = otel_context.get_value(_SPAN_FOR_PROPAGATION_CONTEXT_KEY, context)
        if span_for_propagation is not None:
            current = trace.get_current_span(context)
            current_span_context = current.get_span_context()
            if not current.is_recording() and current_span_context.trace_id == span_for_propagation.trace_id:
                context = set_span_in_context(NonRecordingSpan(span_for_propagation), context)
        self._delegate.inject(carrier, context, setter)

    @override
    def extract(self, carrier, context: Optional[Context] = None, getter: Getter = default_getter) -> Context:
        return self._delegate.extract(carrier, context, getter)

    @property
    @override
    def fields(self):
        return self._delegate.fields
