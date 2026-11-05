# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gc
import unittest

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GenAINestedClientSpanProcessor
from amazon.opentelemetry.distro.scoped_baggage_span_processor import (
    AWS_AGENT_LINEAGE,
    INFERENCE_SCOPE,
    ScopedBaggageSpanProcessor,
    is_gen_ai_inference_span,
)
from opentelemetry import baggage, context, trace
from opentelemetry.processor.baggage import BaggageSpanProcessor
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind

LINEAGE = "travel_planner,booking_agent"


class TestScopedBaggageSpanProcessor(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemorySpanExporter()
        self.processor = ScopedBaggageSpanProcessor({AWS_AGENT_LINEAGE: INFERENCE_SCOPE})
        self.provider = TracerProvider()
        self.provider.add_span_processor(self.processor)
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer("test")
        self.token = context.attach(baggage.set_baggage(AWS_AGENT_LINEAGE, LINEAGE))

    def tearDown(self):
        context.detach(self.token)

    def _start(self, name="chat model", op=GenAiOperationNameValues.CHAT.value, kind=SpanKind.CLIENT, ctx=None):
        span = self.tracer.start_span(name, kind=kind, context=ctx)
        if op is not None:
            span.set_attribute(GEN_AI_OPERATION_NAME, op)
        return span

    def _finished(self, name):
        return next(s for s in self.exporter.get_finished_spans() if s.name == name)

    def test_copies_to_client_inference_span(self):
        for op in (
            GenAiOperationNameValues.CHAT.value,
            GenAiOperationNameValues.TEXT_COMPLETION.value,
            GenAiOperationNameValues.GENERATE_CONTENT.value,
            GenAiOperationNameValues.EMBEDDINGS.value,
        ):
            self._start(name=op, op=op).end()
            self.assertEqual(self._finished(op).attributes[AWS_AGENT_LINEAGE], LINEAGE)

    def test_not_copied_to_non_inference_spans(self):
        self._start(name="internal chat", kind=SpanKind.INTERNAL).end()
        self._start(name="tool", op="execute_tool").end()
        self._start(name="http", op=None).end()

        for name in ("internal chat", "tool", "http"):
            self.assertNotIn(AWS_AGENT_LINEAGE, self._finished(name).attributes)

    def test_operation_set_after_start_still_matches(self):
        # ADOT's LangChain handler sets gen_ai.operation.name after the span starts.
        span = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        self.assertEqual(self._finished("chat model").attributes[AWS_AGENT_LINEAGE], LINEAGE)

    def test_baggage_captured_at_start_used_when_span_ends_elsewhere(self):
        span = self._start()
        # Simulate a span ended from a context without the baggage, e.g. a stream consumer.
        detach_token = context.attach(baggage.remove_baggage(AWS_AGENT_LINEAGE))
        try:
            span.end()
        finally:
            context.detach(detach_token)

        self.assertEqual(self._finished("chat model").attributes[AWS_AGENT_LINEAGE], LINEAGE)

    def test_does_not_overwrite_instrumentation_value(self):
        span = self._start()
        span.set_attribute(AWS_AGENT_LINEAGE, "set_by_instrumentation")
        span.end()

        self.assertEqual(self._finished("chat model").attributes[AWS_AGENT_LINEAGE], "set_by_instrumentation")

    def test_no_baggage_no_attribute(self):
        empty_token = context.attach(baggage.remove_baggage(AWS_AGENT_LINEAGE))
        try:
            self._start().end()
        finally:
            context.detach(empty_token)

        self.assertNotIn(AWS_AGENT_LINEAGE, self._finished("chat model").attributes)

    def test_pending_state_cleared_for_non_matching_spans(self):
        self._start(name="tool", op="execute_tool").end()
        self._start().end()

        self.assertEqual(self.processor._pending_by_span._data, {})  # noqa: SLF001

    def test_full_attribute_container_skips_without_evicting(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider(span_limits=SpanLimits(max_span_attributes=1))
        provider.add_span_processor(ScopedBaggageSpanProcessor({AWS_AGENT_LINEAGE: INFERENCE_SCOPE}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        span = provider.get_tracer("test").start_span("chat model", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        finished = exporter.get_finished_spans()[0]
        self.assertEqual(finished.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.CHAT.value)
        self.assertNotIn(AWS_AGENT_LINEAGE, finished.attributes)
        self.assertEqual(finished.dropped_attributes, 1)

    def test_value_truncated_to_attribute_length_limit(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider(span_limits=SpanLimits(max_span_attribute_length=6))
        provider.add_span_processor(ScopedBaggageSpanProcessor({AWS_AGENT_LINEAGE: INFERENCE_SCOPE}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        span = provider.get_tracer("test").start_span("chat model", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        self.assertEqual(exporter.get_finished_spans()[0].attributes[AWS_AGENT_LINEAGE], LINEAGE[:6])

    def test_pending_baggage_released_when_unended_span_is_collected(self):
        # Spans of any kind that start but never end must not retain pending baggage once collected.
        for kind in (SpanKind.CLIENT, SpanKind.INTERNAL):
            for _ in range(100):
                self.tracer.start_span("abandoned", kind=kind)
        gc.collect()

        self.assertEqual(self.processor._pending_by_span._data, {})  # noqa: SLF001

    def test_span_promoted_to_client_after_start_still_matches(self):
        # A span can start as INTERNAL and be changed to CLIENT before it ends.
        span = self._start(kind=SpanKind.INTERNAL)
        span._kind = SpanKind.CLIENT  # noqa: SLF001
        span.end()

        self.assertEqual(self._finished("chat model").attributes[AWS_AGENT_LINEAGE], LINEAGE)

    def test_shutdown_clears_pending_state(self):
        self._start()
        self.processor.shutdown()

        self.assertEqual(self.processor._pending_by_span._data, {})  # noqa: SLF001

    def test_unknown_scope_is_ignored(self):
        with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING"):
            processor = ScopedBaggageSpanProcessor({"my.key": "no_such_scope"})
        self.assertEqual(processor._scoped_keys, {})  # noqa: SLF001

    def test_only_innermost_client_span_gets_lineage_with_nested_processor(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(GenAINestedClientSpanProcessor())
        provider.add_span_processor(ScopedBaggageSpanProcessor({AWS_AGENT_LINEAGE: INFERENCE_SCOPE}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")

        framework = tracer.start_span("framework chat", kind=SpanKind.CLIENT)
        framework.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        sdk = tracer.start_span("bedrock chat", kind=SpanKind.CLIENT, context=trace.set_span_in_context(framework))
        sdk.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        sdk.end()
        framework.end()

        spans = {s.name: s for s in exporter.get_finished_spans()}
        self.assertEqual(spans["framework chat"].kind, SpanKind.INTERNAL)
        self.assertNotIn(AWS_AGENT_LINEAGE, spans["framework chat"].attributes)
        self.assertEqual(spans["bedrock chat"].attributes[AWS_AGENT_LINEAGE], LINEAGE)

    def test_unscoped_keys_still_copied_to_every_span(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(ScopedBaggageSpanProcessor({AWS_AGENT_LINEAGE: INFERENCE_SCOPE}))
        provider.add_span_processor(BaggageSpanProcessor(lambda key: key == "session.id"))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")

        session_token = context.attach(baggage.set_baggage("session.id", "s-1"))
        try:
            tracer.start_span("agent", kind=SpanKind.INTERNAL).end()
            chat = tracer.start_span("chat model", kind=SpanKind.CLIENT)
            chat.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            chat.end()
        finally:
            context.detach(session_token)

        spans = {s.name: s for s in exporter.get_finished_spans()}
        self.assertEqual(spans["agent"].attributes["session.id"], "s-1")
        self.assertNotIn(AWS_AGENT_LINEAGE, spans["agent"].attributes)
        self.assertEqual(spans["chat model"].attributes["session.id"], "s-1")
        self.assertEqual(spans["chat model"].attributes[AWS_AGENT_LINEAGE], LINEAGE)


class TestIsGenAiInferenceSpan(unittest.TestCase):
    def test_requires_client_kind_and_inference_operation(self):
        provider = TracerProvider()
        tracer = provider.get_tracer("test")

        cases = [
            (SpanKind.CLIENT, GenAiOperationNameValues.CHAT.value, True),
            (SpanKind.INTERNAL, GenAiOperationNameValues.CHAT.value, False),
            (SpanKind.CLIENT, "execute_tool", False),
            (SpanKind.CLIENT, None, False),
        ]
        for kind, op, expected in cases:
            span = tracer.start_span("s", kind=kind)
            if op is not None:
                span.set_attribute(GEN_AI_OPERATION_NAME, op)
            span.end()
            self.assertEqual(is_gen_ai_inference_span(span), expected, (kind, op))
