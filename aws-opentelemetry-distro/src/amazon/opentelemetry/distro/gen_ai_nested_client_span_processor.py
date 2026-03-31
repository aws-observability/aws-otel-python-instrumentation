# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import DictWithLock
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind


class GenAiNestedClientSpanProcessor(SpanProcessor):
    # OTel GenAI semantic conventions require outgoing LLM calls to be CLIENT spans.
    # However, the same call can be instrumented by both the agentic framework
    # and the underlying LLM client SDK, producing nested CLIENT spans for a single request.
    # This processor converts the outer span to INTERNAL so only the innermost
    # SDK spans remains CLIENT, avoiding the nested CLIENT anti-pattern.

    def __init__(self):
        self._has_gen_ai_client_child: DictWithLock = DictWithLock()

    def on_start(self, span: Span, parent_context=None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        if span.kind != SpanKind.CLIENT:
            return

        parent_span_id = span.parent.span_id if span.parent else None
        if parent_span_id:
            self._has_gen_ai_client_child.put(parent_span_id, True)

        is_llm_span = (span.attributes or {}).get(GEN_AI_OPERATION_NAME) in (
            GenAiOperationNameValues.CHAT.value,
            GenAiOperationNameValues.TEXT_COMPLETION.value,
            GenAiOperationNameValues.GENERATE_CONTENT.value,
            GenAiOperationNameValues.EMBEDDINGS.value,
        )
        if is_llm_span and span.context and self._has_gen_ai_client_child.pop(span.context.span_id):
            span._kind = SpanKind.INTERNAL  # noqa: SLF001

    def shutdown(self) -> None:
        self._has_gen_ai_client_child.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True
