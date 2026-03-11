# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GenAiOperationNameValues,
)

# these are otel attributes that the invoke_agent span SHOULD have
_REQUIRED_AGENT_ATTRS = (GEN_AI_PROVIDER_NAME, GEN_AI_REQUEST_MODEL, GEN_AI_REQUEST_TEMPERATURE)


class LangChainSpanProcessor(SpanProcessor):

    def __init__(self, scope_name: str) -> None:
        self._scope_name = scope_name
        self._nearest_agent_span: dict[int, Span] = {}

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        if not span.context or not self._is_langchain_span(span):
            return
        span_id = span.context.span_id
        parent_id = span.parent.span_id if span.parent else None

        if span.name and GenAiOperationNameValues.INVOKE_AGENT.value in span.name:
            self._nearest_agent_span[span_id] = span
        elif parent_id and parent_id in self._nearest_agent_span:
            self._nearest_agent_span[span_id] = self._nearest_agent_span[parent_id]

    def on_end(self, span: ReadableSpan) -> None:
        span_id = span.context.span_id if span.context else None
        if span_id is None:
            return

        op = span.attributes.get(GEN_AI_OPERATION_NAME) if span.attributes else None
        if op in (GenAiOperationNameValues.CHAT.value, GenAiOperationNameValues.TEXT_COMPLETION.value):
            agent_span = self._nearest_agent_span.get(span_id)
            if agent_span:
                for attr in _REQUIRED_AGENT_ATTRS:
                    val = span.attributes.get(attr) if span.attributes else None
                    if val is not None:
                        agent_span.set_attribute(attr, val)

        self._nearest_agent_span.pop(span_id, None)

    def shutdown(self) -> None:
        self._nearest_agent_span.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def _is_langchain_span(self, span: Span | ReadableSpan) -> bool:  # pylint: disable=no-self-use
        return span.instrumentation_scope is not None and span.instrumentation_scope.name == self._scope_name