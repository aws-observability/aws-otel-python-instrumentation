# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import DictWithLock
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind


class GenAINestedClientSpanProcessor(SpanProcessor):
    # OTel GenAI semantic conventions require outgoing LLM calls to be CLIENT spans.
    # Framework and SDK instrumentation can produce nested CLIENT spans for the same call.
    # Demote the outer span to INTERNAL only when the child has the same GenAI
    # inference operation; HTTP and different-operation children leave it CLIENT.

    def __init__(self):
        # Maps (parent span ID, operation) to whether a matching GenAI CLIENT child ended.
        self._has_gen_ai_client_child: DictWithLock = DictWithLock()

    def on_start(self, span: Span, parent_context=None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        gen_ai_inference_operations = (
            GenAiOperationNameValues.CHAT.value,
            GenAiOperationNameValues.TEXT_COMPLETION.value,
            GenAiOperationNameValues.GENERATE_CONTENT.value,
            GenAiOperationNameValues.EMBEDDINGS.value,
        )
        # Clean up before early returns so child state cannot leak for non-GenAI parents.
        span_id = span.context.span_id if span.context else None
        child_operations = {
            operation
            for operation in gen_ai_inference_operations
            if span_id and self._has_gen_ai_client_child.pop((span_id, operation))
        }

        if span.kind != SpanKind.CLIENT:
            return

        operation = (span.attributes or {}).get(GEN_AI_OPERATION_NAME)
        if operation not in gen_ai_inference_operations:
            return

        parent_span_id = span.parent.span_id if span.parent else None
        if parent_span_id:
            self._has_gen_ai_client_child.put((parent_span_id, operation), True)

        if operation in child_operations:
            span._kind = SpanKind.INTERNAL  # noqa: SLF001

    def shutdown(self) -> None:
        self._has_gen_ai_client_child.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=no-self-use
        return True
