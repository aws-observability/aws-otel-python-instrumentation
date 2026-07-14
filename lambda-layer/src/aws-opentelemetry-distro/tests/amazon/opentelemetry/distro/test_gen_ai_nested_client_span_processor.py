# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GenAiNestedClientSpanProcessor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind


class TestGenAiNestedClientSpanProcessor(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(GenAiNestedClientSpanProcessor())
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer("test")

    def _make_llm_span(self, name="chat model", op=GenAiOperationNameValues.CHAT.value, kind=SpanKind.CLIENT, ctx=None):
        span = self.tracer.start_span(name, kind=kind, context=ctx)
        span.set_attribute(GEN_AI_OPERATION_NAME, op)
        return span

    def test_nested_llm_client_child_converts_parent_to_internal(self):
        parent = self._make_llm_span()
        ctx = trace.set_span_in_context(parent)
        child = self._make_llm_span(ctx=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        child_span = spans[0]
        parent_span = spans[1]
        self.assertEqual(child_span.kind, SpanKind.CLIENT)
        self.assertEqual(parent_span.kind, SpanKind.INTERNAL)

    def test_http_child_converts_parent(self):
        parent = self._make_llm_span()
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("POST", kind=SpanKind.CLIENT, context=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        parent_span = next(s for s in spans if s.name == "chat model")
        self.assertEqual(parent_span.kind, SpanKind.INTERNAL)

    def test_no_child_stays_client(self):
        span = self._make_llm_span()
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_text_completion_converted_by_child(self):
        parent = self._make_llm_span(op=GenAiOperationNameValues.TEXT_COMPLETION.value)
        ctx = trace.set_span_in_context(parent)
        child = self._make_llm_span(op=GenAiOperationNameValues.TEXT_COMPLETION.value, ctx=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.INTERNAL)

    def test_embeddings_converted_by_child(self):
        parent = self._make_llm_span(op=GenAiOperationNameValues.EMBEDDINGS.value)
        ctx = trace.set_span_in_context(parent)
        child = self._make_llm_span(op=GenAiOperationNameValues.EMBEDDINGS.value, ctx=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.INTERNAL)

    def test_generate_content_converted_by_child(self):
        parent = self._make_llm_span(op=GenAiOperationNameValues.GENERATE_CONTENT.value)
        ctx = trace.set_span_in_context(parent)
        child = self._make_llm_span(op=GenAiOperationNameValues.GENERATE_CONTENT.value, ctx=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.INTERNAL)

    def test_non_llm_operation_ignored(self):
        span = self.tracer.start_span("invoke_agent MyAgent", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_internal_span_ignored(self):
        span = self.tracer.start_span("chat model", kind=SpanKind.INTERNAL)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.INTERNAL)

    def test_span_without_gen_ai_attr_ignored(self):
        span = self.tracer.start_span("some-span", kind=SpanKind.CLIENT)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_no_parent_stays_client(self):
        span = self._make_llm_span()
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_shutdown_clears_state(self):
        processor = GenAiNestedClientSpanProcessor()
        processor._has_gen_ai_client_child.put(123, True)
        self.assertEqual(len(processor._has_gen_ai_client_child), 1)
        processor.shutdown()
        self.assertEqual(len(processor._has_gen_ai_client_child), 0)

    def test_force_flush_returns_true(self):
        processor = GenAiNestedClientSpanProcessor()
        self.assertTrue(processor.force_flush())


if __name__ == "__main__":
    unittest.main()
