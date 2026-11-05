# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Callable, Dict, Mapping, NamedTuple, Optional

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


def _is_client_span(span: Span) -> bool:
    return span.kind == SpanKind.CLIENT


class _Scope(NamedTuple):
    # Cheap check at span start. Spans that fail it are not tracked, which keeps memory bounded for
    # spans that start but never end. It may only use what is fixed at start, such as span kind.
    may_match: Callable[[Span], bool]
    # Full check when the span ends, after instrumentations have set their attributes.
    matches: Callable[[ReadableSpan], bool]


# Internal scope registry: scope name -> scope.
_SCOPES: Dict[str, _Scope] = {
    # Span kind only changes after start from CLIENT to INTERNAL, so non-CLIENT spans can never match.
    INFERENCE_SCOPE: _Scope(may_match=_is_client_span, matches=is_gen_ai_inference_span),
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

    A key is only set when the span does not already have it, so a value set by the instrumentation wins.
    """

    def __init__(self, scoped_keys: Mapping[str, str]) -> None:
        """
        Args:
            scoped_keys: Mapping of baggage key to scope name. Keys with unknown scopes are ignored.
        """
        self._scoped_keys: Dict[str, _Scope] = {}
        for key, scope_name in scoped_keys.items():
            scope = _SCOPES.get(scope_name)
            if scope is None:
                _logger.warning("Unknown baggage scope '%s' for key '%s'. The key will not be copied.", scope_name, key)
                continue
            self._scoped_keys[key] = scope
        self._pending_by_span_id: DictWithLock = DictWithLock()

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        if not self._scoped_keys or span.context is None:
            return
        entries = baggage.get_all(parent_context)
        pending = {
            key: value
            for key, value in entries.items()
            if key in self._scoped_keys and self._scoped_keys[key].may_match(span)
        }
        if pending:
            self._pending_by_span_id.put(span.context.span_id, pending)

    @override
    def on_end(self, span: ReadableSpan) -> None:
        if span.context is None:
            return
        pending = self._pending_by_span_id.pop(span.context.span_id)
        if not pending:
            return
        for key, value in pending.items():
            try:
                matches = self._scoped_keys[key].matches(span)
            except Exception:  # pylint: disable=broad-exception-caught
                _logger.debug("Baggage scope check failed for key '%s'", key, exc_info=True)
                continue
            if matches:
                _set_attribute_if_absent(span, key, value)

    @override
    def shutdown(self) -> None:
        self._pending_by_span_id.clear()

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


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
