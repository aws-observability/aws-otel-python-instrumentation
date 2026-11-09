# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import NamedTuple
from weakref import ReferenceType, ref

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import DictWithLock
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind


class _ClientSpanState(NamedTuple):
    span_ref: ReferenceType[Span]
    child_operations: set[str]


class GenAINestedClientSpanProcessor(SpanProcessor):
    # OTel GenAI semantic conventions require outgoing LLM calls to be CLIENT spans.
    # However, the same call can be instrumented by both the agentic framework
    # and the underlying LLM client SDK, producing nested CLIENT spans for a single request.
    # This processor converts the outer span to INTERNAL only when its child has
    # the same GenAI inference operation.

    def __init__(self) -> None:
        # State belongs only to active local CLIENT spans. Weak references also
        # release it when a span is garbage collected without being ended.
        self._active_client_spans: DictWithLock = DictWithLock()

    def on_start(self, span: Span, parent_context=None) -> None:
        if span.kind != SpanKind.CLIENT or not span.context:
            return

        span_key = (span.context.trace_id, span.context.span_id)

        def remove_collected_span(span_ref: ReferenceType[Span]) -> None:
            # A stale callback must not remove a replacement with the same key.
            self._active_client_spans.pop_if_matches(span_key, lambda state: state.span_ref is span_ref)

        self._active_client_spans.put(span_key, _ClientSpanState(ref(span, remove_collected_span), set()))

    def on_end(self, span: ReadableSpan) -> None:
        gen_ai_inference_operations = (
            GenAiOperationNameValues.CHAT.value,
            GenAiOperationNameValues.TEXT_COMPLETION.value,
            GenAiOperationNameValues.GENERATE_CONTENT.value,
            GenAiOperationNameValues.EMBEDDINGS.value,
        )
        if not span.context:
            return

        state = self._active_client_spans.pop((span.context.trace_id, span.context.span_id))
        child_operations = state.child_operations if state else set()

        if span.kind != SpanKind.CLIENT:
            return

        operation = (span.attributes or {}).get(GEN_AI_OPERATION_NAME)
        if operation not in gen_ai_inference_operations:
            return

        # The conditional update is atomic with parent cleanup. A late child
        # must never recreate an ended parent's state.
        if span.parent and not span.parent.is_remote:
            self._active_client_spans.update_if_present(
                (span.context.trace_id, span.parent.span_id),
                lambda parent_state: parent_state.child_operations.add(operation),
            )

        if operation in child_operations:
            span._kind = SpanKind.INTERNAL  # noqa: SLF001

    def shutdown(self) -> None:
        self._active_client_spans.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=no-self-use
        return True
