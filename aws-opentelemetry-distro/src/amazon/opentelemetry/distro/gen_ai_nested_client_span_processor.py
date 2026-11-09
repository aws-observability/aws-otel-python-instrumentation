# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from types import MappingProxyType
from typing import Mapping, NamedTuple
from weakref import ReferenceType, ref

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import DictWithLock
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind


class _ParentSpanState(NamedTuple):
    """Record a local CLIENT span and the operations of its completed GenAI CLIENT children."""

    # Removes the span's map entry when the span is garbage collected.
    span_ref: ReferenceType[Span]
    # Inference operation names from completed GenAI CLIENT children.
    # The parent becomes INTERNAL if its own inference operation is in this set.
    child_operations: set[str]


class GenAINestedClientSpanProcessor(SpanProcessor):
    # OTel GenAI semantic conventions require outgoing LLM calls to be CLIENT spans.
    # However, the same call can be instrumented by both the agentic framework
    # and the underlying LLM client SDK, producing nested CLIENT spans for a single request.
    # This processor converts the outer span to INTERNAL only when its child has
    # the same GenAI inference operation.

    def __init__(self) -> None:
        # Tracks which parent spans have a GenAI CLIENT child for each operation.
        # Add local CLIENT spans in on_start(); remove their entries in on_end().
        # Children can update only a parent that still has an entry in this map.
        # The stored weak reference does not keep the span alive. If the span is
        # garbage collected without end(), its callback removes the entry instead.
        self._active_parent_span_id_and_operation_to_gen_ai_client_child: DictWithLock = DictWithLock()

    def on_start(self, span: Span, parent_context=None) -> None:
        if span.kind != SpanKind.CLIENT or not span.context:
            return

        span_key = (span.context.trace_id, span.context.span_id)

        def remove_collected_span(span_ref: ReferenceType[Span]) -> None:
            # Delete only the entry containing this span's weak reference.
            # If its key was overwritten, this callback must leave the newer
            # span's state alone.
            self._active_parent_span_id_and_operation_to_gen_ai_client_child.pop_if_matches(
                span_key, lambda state: state.span_ref is span_ref
            )

        self._active_parent_span_id_and_operation_to_gen_ai_client_child.put(
            span_key, _ParentSpanState(ref(span, remove_collected_span), set())
        )

    def on_end(self, span: ReadableSpan) -> None:
        gen_ai_inference_operations = (
            GenAiOperationNameValues.CHAT.value,
            GenAiOperationNameValues.TEXT_COMPLETION.value,
            GenAiOperationNameValues.GENERATE_CONTENT.value,
            GenAiOperationNameValues.EMBEDDINGS.value,
        )
        if not span.context:
            return

        # Clean up before early returns so child state cannot leak for non-GenAI parents.
        state = self._active_parent_span_id_and_operation_to_gen_ai_client_child.pop(
            (span.context.trace_id, span.context.span_id)
        )
        child_operations = state.child_operations if state else set()

        if span.kind != SpanKind.CLIENT:
            return

        operation = (span.attributes or {}).get(GEN_AI_OPERATION_NAME)
        if operation not in gen_ai_inference_operations:
            return

        # A child may finish after its parent, e.g. in another thread or async task.
        # A separate parent lookup followed by put() could race with the parent's
        # pop(): the parent removes its state after the lookup, then the child
        # recreates it. The parent will not end again to remove that stale entry.
        # update_if_present checks and updates under the same lock as pop(), so
        # a late child cannot recreate an entry that its parent already removed.
        if span.parent and not span.parent.is_remote:
            self._active_parent_span_id_and_operation_to_gen_ai_client_child.update_if_present(
                (span.context.trace_id, span.parent.span_id),
                lambda parent_state: parent_state.child_operations.add(operation),
            )

        if operation in child_operations:
            span._kind = SpanKind.INTERNAL  # noqa: SLF001

    def shutdown(self) -> None:
        self._active_parent_span_id_and_operation_to_gen_ai_client_child.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=no-self-use
        return True

    # Used by tests to inspect tracked operations without changing processor state.
    def get_active_parent_span_id_and_operation_to_gen_ai_client_child(
        self,
    ) -> Mapping[tuple[int, int], frozenset[str]]:
        """Return an immutable snapshot of active parent span operations."""
        return MappingProxyType(
            self._active_parent_span_id_and_operation_to_gen_ai_client_child.get_dict(
                lambda state: frozenset(state.child_operations)
            )
        )
