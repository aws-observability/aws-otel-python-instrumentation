# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import weakref
from typing import Callable, Dict, Mapping, Optional, Tuple

from typing_extensions import override

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GEN_AI_INFERENCE_OPERATIONS
from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import DictWithLock
from opentelemetry import baggage
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import GEN_AI_OPERATION_NAME
from opentelemetry.trace import SpanKind

_logger = logging.getLogger(__name__)

AWS_AGENT_LINEAGE = "aws.agent.lineage"
INFERENCE_SCOPE = "inference"


def is_gen_ai_inference_span(span: ReadableSpan) -> bool:
    """A CLIENT span for a GenAI inference operation (chat, text_completion, generate_content, embeddings)."""
    if span.kind != SpanKind.CLIENT:
        return False
    return (span.attributes or {}).get(GEN_AI_OPERATION_NAME) in GEN_AI_INFERENCE_OPERATIONS


# Internal scope registry: scope name -> predicate evaluated when the span ends. Predicates run only at
# end because instrumentations and processors can change a span's kind and attributes after it starts.
_SCOPES: Dict[str, Callable[[ReadableSpan], bool]] = {
    INFERENCE_SCOPE: is_gen_ai_inference_span,
}


class ScopedBaggageSpanProcessor(SpanProcessor):
    """
    Copies selected baggage entries to span attributes, but only on spans that match a scope.

    Unlike the upstream BaggageSpanProcessor, which copies keys to every span at start, a scope's
    predicate depends on attributes that instrumentations often set after the span starts (for example
    ``gen_ai.operation.name``). Baggage is captured in ``on_start`` from the parent context, because the
    context active when the span ends may not hold the same baggage, and applied in ``on_end`` once the
    scope matches.

    The SDK has made the span immutable by ``on_end``, so the attribute is written through the
    attributes container's lock, the same way AttributeRedactingSpanProcessor updates finished spans.
    This processor must be registered after GenAINestedClientSpanProcessor, so it sees the corrected
    span kind, and before the batch processors, so exporters see the new attribute.

    Pending baggage is released when the span ends, or when the span is garbage-collected without
    ending, so abandoned spans do not retain it.

    A key is only set when the span does not already have it, so a value set by the instrumentation wins.
    """

    def __init__(self, scoped_keys: Mapping[str, str]) -> None:
        """
        Args:
            scoped_keys: Mapping of baggage key to scope name. Keys with unknown scopes are ignored.
        """
        self._scoped_keys: Dict[str, Callable[[ReadableSpan], bool]] = {}
        for key, scope_name in scoped_keys.items():
            predicate = _SCOPES.get(scope_name)
            if predicate is None:
                _logger.warning("Unknown baggage scope '%s' for key '%s'. The key will not be copied.", scope_name, key)
                continue
            self._scoped_keys[key] = predicate
        # (trace_id, span_id) -> (pending baggage, finalizer that drops the entry if the span is collected)
        self._pending_by_span: DictWithLock = DictWithLock()

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        if not self._scoped_keys or span.context is None:
            return
        entries = baggage.get_all(parent_context)
        pending = {key: value for key, value in entries.items() if key in self._scoped_keys}
        if not pending:
            return
        span_key = _span_key(span)
        # The SDK passes a separate ReadableSpan to on_end, so the entry is keyed by span identity. The
        # finalizer removes it if the span is garbage-collected without ending.
        finalizer = weakref.finalize(span, self._pending_by_span.pop, span_key)
        finalizer.atexit = False
        self._pending_by_span.put(span_key, (pending, finalizer))

    @override
    def on_end(self, span: ReadableSpan) -> None:
        if span.context is None:
            return
        entry = self._pending_by_span.pop(_span_key(span))
        if entry is None:
            return
        pending, finalizer = entry
        finalizer.detach()
        for key, value in pending.items():
            try:
                matches = self._scoped_keys[key](span)
            except Exception:  # pylint: disable=broad-exception-caught
                _logger.debug("Baggage scope check failed for key '%s'", key, exc_info=True)
                continue
            if matches:
                _set_attribute_if_absent(span, key, value)

    @override
    def shutdown(self) -> None:
        for _, (_, finalizer) in self._pending_by_span.pop_items_if_matches(lambda _key, _value: True):
            finalizer.detach()

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def _span_key(span: ReadableSpan) -> Tuple[int, int]:
    return span.context.trace_id, span.context.span_id


def _set_attribute_if_absent(span: ReadableSpan, key: str, value: str) -> None:
    attributes = span._attributes  # noqa: SLF001
    if isinstance(attributes, BoundedAttributes):
        # Finished spans expose immutable BoundedAttributes, so the public setter raises TypeError.
        # Write under its lock and keep its value-length limit. When the container is full, skip the
        # key rather than evict an attribute the instrumentation set.
        cleaned = str(value)
        if attributes.max_value_len is not None:
            cleaned = cleaned[: attributes.max_value_len]
        with attributes._lock:  # noqa: SLF001
            if key in attributes._dict:  # noqa: SLF001
                return
            if attributes.maxlen is not None and len(attributes._dict) >= attributes.maxlen:  # noqa: SLF001
                attributes.dropped += 1
                return
            attributes._dict[key] = cleaned  # noqa: SLF001
    elif isinstance(attributes, dict):
        attributes.setdefault(key, value)
