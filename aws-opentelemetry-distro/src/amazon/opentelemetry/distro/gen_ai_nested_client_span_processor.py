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

    # Add local CLIENT spans in on_start() and remove their entries in on_end().
    # Add a weak reference to remove each entry when its span is garbage collected.
    span_ref: ReferenceType[Span]
    # Record inference operation names from completed GenAI CLIENT children.
    # Convert the parent to INTERNAL if its own inference operation is in this set.
    # Mutate this set only while holding the parent state map's lock.
    child_operations: set[str]


class GenAINestedClientSpanProcessor(SpanProcessor):
    # OTel GenAI semantic conventions require outgoing LLM calls to be CLIENT spans.
    # However, the same call can be instrumented by both the agentic framework
    # and the underlying LLM client SDK, producing nested CLIENT spans for a single request.
    # This processor converts the outer span to INTERNAL only when its child has
    # the same GenAI inference operation.

    def __init__(self) -> None:
        # Tracks which parent spans have a GenAI CLIENT child for each operation.
        self._active_trace_id_and_span_id_to_parent_span_state: DictWithLock = DictWithLock()

    def on_start(self, span: Span, parent_context=None) -> None:
        if span.kind != SpanKind.CLIENT or not span.context:
            return

        # Track every CLIENT span because GenAI attributes can be added after on_start().
        span_key = (span.context.trace_id, span.context.span_id)

        def remove_collected_span(span_ref: ReferenceType[Span]) -> None:
            # Remove the entry only if it still contains this span's weak reference.
            # Keep any newer entry that has replaced it under the same key.
            self._active_trace_id_and_span_id_to_parent_span_state.pop_if_matches(
                span_key, lambda state: state.span_ref is span_ref
            )

        self._active_trace_id_and_span_id_to_parent_span_state.put(
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
        state = self._active_trace_id_and_span_id_to_parent_span_state.pop(
            (span.context.trace_id, span.context.span_id)
        )
        child_operations = state.child_operations if state else set()

        if span.kind != SpanKind.CLIENT:
            return

        operation = (span.attributes or {}).get(GEN_AI_OPERATION_NAME)
        if operation not in gen_ai_inference_operations:
            return

        # Check and update the parent entry under the same lock used by pop().
        # A child in another thread or async task could otherwise look up its parent,
        # race with the parent's end() removing that entry, then put() it back.
        # Update only an existing entry so the child cannot recreate an ended parent.
        if span.parent and not span.parent.is_remote:
            self._active_trace_id_and_span_id_to_parent_span_state.update_if_present(
                (span.context.trace_id, span.parent.span_id),
                lambda parent_state: parent_state.child_operations.add(operation),
            )

        if operation in child_operations:
            span._kind = SpanKind.INTERNAL  # noqa: SLF001

    def shutdown(self) -> None:
        self._active_trace_id_and_span_id_to_parent_span_state.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=no-self-use
        return True

    # Used by tests to inspect tracked operations without changing processor state.
    def get_active_parent_operations(self) -> Mapping[tuple[int, int], frozenset[str]]:
        """Return an immutable snapshot of active parent span operations."""
        return MappingProxyType(
            self._active_trace_id_and_span_id_to_parent_span_state.get_dict(
                lambda state: frozenset(state.child_operations)
            )
        )
