# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Demote the outer of two nested GenAI inference CLIENT spans to INTERNAL (a post step).

The upstream LangChain ``chat`` CLIENT span wraps the botocore CLIENT span of the same model call. The CloudWatch
console sums token usage over CLIENT spans, so the tokens count twice (and span metrics get two CLIENT series).

Unlike ADOT's ``GenAINestedClientSpanProcessor``, which demotes only for a child with the same operation and tracks
every CLIENT span, any inference child demotes (LangChain says ``chat`` where botocore says ``text_completion`` for
some models), and only CLIENT spans with an inference operation at start are tracked (util-genai and botocore set
it there). The demotion runs in the SDK's private ``_on_ending`` hook, which ``Span.end`` calls on every processor,
on the live span, before any processor's ``on_end``. So exporters and the span-metrics connector all see INTERNAL,
whatever the order the processors were added in.

The hook and the ``_kind`` field behind ``ReadableSpan.kind`` exist on every supported SDK (1.42 and later); an
SDK without the hook would simply never call it.
"""

import threading
import weakref
from typing import Dict, List, Optional, Tuple

from plugins.opentelemetry.cloudwatch.genai._pipeline import (
    NOT_SDK,
    genai_instrumentation_active,
    has_span_processor,
    sdk_tracer_provider,
)

from opentelemetry.context import Context
from opentelemetry.sdk.trace import Span, SpanProcessor
from opentelemetry.trace import SpanKind

GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
INFERENCE_OPERATIONS = frozenset(("chat", "text_completion", "generate_content", "embeddings"))
ON = "on"


def _is_inference_client(span: Span) -> bool:
    return span.kind is SpanKind.CLIENT and (span.attributes or {}).get(GEN_AI_OPERATION_NAME) in INFERENCE_OPERATIONS


# CW-PLUGIN-WORKAROUND(expires: 2027-03-31; retire-when: botocore Bedrock spans defer to an upstream GenAI Bedrock
# instrumentation (one CLIENT span per call, open-telemetry/opentelemetry-python-contrib#5266), or the CloudWatch
# console sums tokens over leaf CLIENT spans only)
class NestedGenAIClientSpanProcessor(SpanProcessor):
    """Demotes an inference CLIENT span to INTERNAL when one of its local children is an inference CLIENT span.
    Spans from a remote parent, and spans already INTERNAL, are untouched."""

    def __init__(self) -> None:
        # (trace_id, span_id) of each open inference CLIENT span -> [weak reference, has an inference CLIENT child].
        # The weak reference drops the entry of a span collected without ending.
        self._open: Dict[Tuple[int, int], List] = {}
        # Reentrant: the weak-reference callback can run during garbage collection while this thread holds it.
        self._lock = threading.RLock()

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        if not _is_inference_client(span):
            return
        context = span.get_span_context()
        key = (context.trace_id, context.span_id)

        def forget(_ref) -> None:  # the span was collected without ending
            with self._lock:
                self._open.pop(key, None)

        with self._lock:
            self._open[key] = [weakref.ref(span, forget), False]

    def _on_ending(self, span: Span) -> None:
        if not self._open:  # nothing tracked, so neither this span nor its parent is: skip the lock
            return
        context = span.get_span_context()
        with self._lock:
            entry = self._open.pop((context.trace_id, context.span_id), None)
        if not _is_inference_client(span):
            return
        parent = span.parent
        if parent is not None and not parent.is_remote:
            with self._lock:
                # Only an open parent is marked, so a child never recreates an entry for an ended span.
                parent_entry = self._open.get((parent.trace_id, parent.span_id))
                if parent_entry is not None:
                    parent_entry[1] = True
        if entry is not None and entry[1]:
            span._kind = SpanKind.INTERNAL  # pylint: disable=protected-access

    def shutdown(self) -> None:
        with self._lock:
            self._open.clear()


def nested_client_step() -> Optional[str]:
    """The ``nested chat demotion`` post step: the summary outcome, or None when no bridged GenAI instrumentation
    is active."""
    if not genai_instrumentation_active():
        return None
    provider = sdk_tracer_provider()
    if provider is None:
        return NOT_SDK
    if not has_span_processor(provider, NestedGenAIClientSpanProcessor):
        provider.add_span_processor(NestedGenAIClientSpanProcessor())
    return ON
