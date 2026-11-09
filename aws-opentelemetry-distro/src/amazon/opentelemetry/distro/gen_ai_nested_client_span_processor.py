# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
import weakref
from collections import deque
from typing import Deque, Dict, Optional, Set, Tuple

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind

_GEN_AI_INFERENCE_OPERATIONS = (
    GenAiOperationNameValues.CHAT.value,
    GenAiOperationNameValues.TEXT_COMPLETION.value,
    GenAiOperationNameValues.GENERATE_CONTENT.value,
    GenAiOperationNameValues.EMBEDDINGS.value,
)

_SpanKey = Tuple[int, int]


class GenAINestedClientSpanProcessor(SpanProcessor):
    # OTel GenAI semantic conventions require outgoing LLM calls to be CLIENT spans.
    # However, the same call can be instrumented by both the agentic framework
    # and the underlying LLM client SDK, producing nested CLIENT spans for a single request.
    # This processor converts the outer span to INTERNAL only when its child has
    # the same GenAI inference operation.
    #
    # State is kept only for local parents that are still open when a CLIENT child starts, and is
    # released when the parent ends or is garbage-collected. A remote parent, or a parent that has
    # already ended, gets no state, so children of such parents cannot leave entries behind.
    # Garbage collection can run at any allocation of a tracked object and can end spans on this thread,
    # for example by closing an abandoned generator that holds a span open, which calls on_end again.
    # Objects are therefore built before the lock is taken, and the lock is re-entrant, so a span ended
    # from inside a collection cannot deadlock on it. The finalizer never takes the lock. It only queues
    # the parent's key on a thread-safe deque, which every lifecycle call drains.

    def __init__(self):
        self._lock = threading.RLock()
        # (trace_id, span_id) of an open local parent -> (inference operations of its ended CLIENT
        # children, finalizer). Guarded by _lock.
        self._parents: Dict[_SpanKey, Tuple[Set[str], weakref.finalize]] = {}
        # Keys of parents collected without ending. Appended by finalizers without the lock.
        self._collected: Deque[_SpanKey] = deque()

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        # Only a CLIENT child can demote its parent. Span kind is fixed at start in this SDK.
        if span.kind != SpanKind.CLIENT or span.parent is None or span.parent.is_remote:
            self._remove_collected()
            return
        parent = trace.get_current_span(parent_context)
        if not isinstance(parent, Span) or not parent.is_recording():
            self._remove_collected()
            return
        parent_span_context = parent.get_span_context()
        key = (parent_span_context.trace_id, parent_span_context.span_id)
        with self._lock:
            self._remove_collected_locked()
            tracked = key in self._parents
        if tracked:
            return
        finalizer = weakref.finalize(parent, self._collected.append, key)
        finalizer.atexit = False
        entry: Tuple[Set[str], weakref.finalize] = (set(), finalizer)
        with self._lock:
            # Another child of the same parent may have started in between.
            added = self._parents.setdefault(key, entry) is entry
        if not added:
            finalizer.detach()
        elif not parent.is_recording():
            # The parent ended on another thread after the check above, so its on_end may already have run.
            with self._lock:
                if self._parents.get(key) is entry:
                    del self._parents[key]
            finalizer.detach()

    def on_end(self, span: ReadableSpan) -> None:
        operation = _client_inference_operation(span)
        key = (span.context.trace_id, span.context.span_id) if span.context else None
        parent_key = (span.parent.trace_id, span.parent.span_id) if operation and span.parent else None
        with self._lock:
            self._remove_collected_locked()
            entry = self._parents.pop(key, None) if key else None
            if parent_key is not None:
                # Only an open tracked parent records the child. A parent that already ended has no entry,
                # and none is created for it.
                parent_entry = self._parents.get(parent_key)
                if parent_entry is not None:
                    parent_entry[0].add(operation)
        if entry is None:
            return
        child_operations, finalizer = entry
        finalizer.detach()
        if operation is not None and operation in child_operations:
            span._kind = SpanKind.INTERNAL  # noqa: SLF001

    def shutdown(self) -> None:
        with self._lock:
            entries = list(self._parents.values())
            self._parents.clear()
            self._collected.clear()
        for _, finalizer in entries:
            finalizer.detach()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        self._remove_collected()
        return True

    def _remove_collected(self) -> None:
        # Checking the deque without the lock keeps the common case, nothing collected, lock-free.
        if self._collected:
            with self._lock:
                self._remove_collected_locked()

    def _remove_collected_locked(self) -> None:
        while self._collected:
            self._parents.pop(self._collected.popleft(), None)


def _client_inference_operation(span: ReadableSpan) -> Optional[str]:
    if span.kind != SpanKind.CLIENT:
        return None
    operation = (span.attributes or {}).get(GEN_AI_OPERATION_NAME)
    return operation if operation in _GEN_AI_INFERENCE_OPERATIONS else None
