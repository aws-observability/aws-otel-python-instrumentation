# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import threading
import weakref
from collections import deque
from typing import Callable, Collection, Deque, Dict, FrozenSet, Mapping, Optional, Set, Tuple

from typing_extensions import override

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GEN_AI_INFERENCE_OPERATIONS
from opentelemetry import baggage
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import GEN_AI_OPERATION_NAME
from opentelemetry.trace import SpanKind

_logger = logging.getLogger(__name__)

AWS_AGENT_LINEAGE = "aws.agent.lineage"
# Scope names with this prefix are reserved for built-in scopes, so a future built-in cannot collide
# with a scope an application registered.
RESERVED_SCOPE_PREFIX = "aws."
INFERENCE_SCOPE = "aws.inference"


def is_gen_ai_inference_span(span: ReadableSpan) -> bool:
    """A CLIENT span for a GenAI inference operation (chat, text_completion, generate_content, embeddings).

    When a framework span wraps the model SDK span with the same operation, GenAINestedClientSpanProcessor
    demotes the outer span to INTERNAL before this check runs, so only the innermost span matches.
    """
    if span.kind != SpanKind.CLIENT:
        return False
    return (span.attributes or {}).get(GEN_AI_OPERATION_NAME) in GEN_AI_INFERENCE_OPERATIONS


ScopePredicate = Callable[[ReadableSpan], bool]

# Built-in scopes: scope name -> predicate evaluated when the span ends. Names use the reserved prefix.
_BUILTIN_SCOPES: Mapping[str, ScopePredicate] = {
    INFERENCE_SCOPE: is_gen_ai_inference_span,
}

# Scopes registered by applications with register_baggage_scope(). Replaced as a whole on registration
# (copy-on-write), so span processing reads it without a lock.
_registered_scopes: Dict[str, ScopePredicate] = {}
_registered_scopes_lock = threading.Lock()


def register_baggage_scope(name: str, predicate: ScopePredicate) -> bool:
    """Define a baggage scope for use in OTEL_BAGGAGE_SPAN_ATTRIBUTE_KEYS. Not part of the documented API.

    After registration, ``OTEL_BAGGAGE_SPAN_ATTRIBUTE_KEYS=my.key:<name>`` copies baggage entry ``my.key``
    only to spans for which ``predicate`` returns True. The predicate receives the ending span. It runs
    synchronously at the end of every span that carries such a key, so it must be fast and must not modify
    the span. If it raises, the scope does not match that span.

    Scope names are looked up when each span ends, so the scope can be registered at application startup,
    after the distro has read the environment variable. Registering an existing custom name replaces it.

    Returns False, without registering, when the name is empty, has surrounding whitespace, contains ``:``
    or ``,``, or starts with the reserved prefix ``aws.``, or when ``predicate`` is not callable.
    """
    if not isinstance(name, str) or not name.strip() or ":" in name or "," in name or name != name.strip():
        _logger.warning("Invalid baggage scope name '%s'.", name)
        return False
    if name.startswith(RESERVED_SCOPE_PREFIX):
        _logger.warning("Baggage scope names starting with '%s' are reserved: '%s'.", RESERVED_SCOPE_PREFIX, name)
        return False
    if not callable(predicate):
        _logger.warning("Baggage scope '%s' needs a callable predicate.", name)
        return False
    global _registered_scopes  # pylint: disable=global-statement
    with _registered_scopes_lock:
        scopes = dict(_registered_scopes)
        scopes[name] = predicate
        _registered_scopes = scopes
    return True


def _clear_registered_scopes() -> None:
    """Remove all application-registered scopes. For tests."""
    global _registered_scopes  # pylint: disable=global-statement
    with _registered_scopes_lock:
        _registered_scopes = {}


def _get_scope(name: str) -> Optional[ScopePredicate]:
    return _BUILTIN_SCOPES.get(name) or _registered_scopes.get(name)


class _PendingSpan:
    """Baggage captured at span start, held until the span ends."""

    __slots__ = ("baggage", "finalizer")

    def __init__(self, captured: Dict[str, str], finalizer: weakref.finalize) -> None:
        self.baggage = captured
        self.finalizer = finalizer


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
    ending, so abandoned spans do not retain it. Garbage collection can run a finalizer at any
    allocation, including while this processor holds its lock, so the finalizer never takes the lock.
    It only queues the span's key on a thread-safe deque. Every lifecycle call (``on_start``,
    ``on_end``, ``force_flush``) removes queued entries, whether or not the span carries scoped baggage.

    A key is only set when the span does not already have it, so a value set by the instrumentation wins.
    """

    def __init__(self, scoped_keys: Mapping[str, Collection[str]]) -> None:
        """
        Args:
            scoped_keys: Mapping of baggage key to scope names. The key is copied to a span that matches
                any of its scopes. A scope can be built in or registered later with
                register_baggage_scope(). An unknown scope matches no span.
        """
        self._scoped_keys: Dict[str, FrozenSet[str]] = {
            key: frozenset([scopes] if isinstance(scopes, str) else scopes) for key, scopes in scoped_keys.items()
        }
        self._warned_scopes: Set[str] = set()
        self._lock = threading.Lock()
        # (trace_id, span_id) -> pending state. Guarded by _lock.
        self._pending_by_span: Dict[Tuple[int, int], _PendingSpan] = {}
        # Keys of spans collected without ending. Appended by finalizers without the lock.
        self._collected: Deque[Tuple[int, int]] = deque()

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        scoped_keys = self._scoped_keys
        if not scoped_keys or span.context is None:
            return
        entries = baggage.get_all(parent_context)
        pending = {key: value for key, value in entries.items() if key in scoped_keys}
        if not pending:
            self._remove_collected()
            return
        span_key = _span_key(span)
        # The SDK passes a separate ReadableSpan to on_end, so the entry is keyed by span identity. The
        # finalizer queues the key for removal if the span is garbage-collected without ending.
        finalizer = weakref.finalize(span, self._collected.append, span_key)
        finalizer.atexit = False
        with self._lock:
            self._remove_collected_locked()
            self._pending_by_span[span_key] = _PendingSpan(pending, finalizer)

    @override
    def on_end(self, span: ReadableSpan) -> None:
        if span.context is None:
            return
        with self._lock:
            self._remove_collected_locked()
            entry = self._pending_by_span.pop(_span_key(span), None)
        if entry is None:
            return
        entry.finalizer.detach()
        for key, value in entry.baggage.items():
            if any(self._scope_matches(scope, span) for scope in self._scoped_keys[key]):
                _set_attribute_if_absent(span, key, value)

    def _scope_matches(self, scope_name: str, span: ReadableSpan) -> bool:
        predicate = _get_scope(scope_name)
        if predicate is None:
            self._warn_unknown_scope(scope_name)
            return False
        try:
            return bool(predicate(span))
        except Exception:  # pylint: disable=broad-exception-caught
            _logger.debug("Baggage scope '%s' check failed", scope_name, exc_info=True)
            return False

    def _warn_unknown_scope(self, scope_name: str) -> None:
        if scope_name in self._warned_scopes:
            return
        self._warned_scopes.add(scope_name)
        _logger.warning(
            "Unknown baggage scope '%s' in OTEL_BAGGAGE_SPAN_ATTRIBUTE_KEYS. It matches no span.", scope_name
        )

    @override
    def shutdown(self) -> None:
        with self._lock:
            entries = list(self._pending_by_span.values())
            self._pending_by_span.clear()
            self._collected.clear()
        for entry in entries:
            entry.finalizer.detach()

    def _remove_collected(self) -> None:
        # Checking the deque without the lock keeps the common case, nothing collected, lock-free.
        if self._collected:
            with self._lock:
                self._remove_collected_locked()

    def _remove_collected_locked(self) -> None:
        while self._collected:
            self._pending_by_span.pop(self._collected.popleft(), None)

    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        self._remove_collected()
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
