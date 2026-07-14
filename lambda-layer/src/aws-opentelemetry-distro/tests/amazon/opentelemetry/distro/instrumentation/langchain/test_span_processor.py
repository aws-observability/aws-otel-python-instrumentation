# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.instrumentation.langchain.span_processor import LangChainSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan, Span, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GenAiOperationNameValues,
)

SCOPE_NAME = "test-langchain-scope"


class TestLangChainSpanProcessor(TestCase):
    def setUp(self):
        self.processor = LangChainSpanProcessor(SCOPE_NAME)
        self.provider = TracerProvider()
        self.exporter = InMemorySpanExporter()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.provider.add_span_processor(self.processor)
        self.tracer = self.provider.get_tracer(SCOPE_NAME)

    def tearDown(self):
        self.processor.shutdown()
        self.exporter.clear()

    def test_on_start_ignores_non_langchain_span(self):
        other_tracer = self.provider.get_tracer("other-scope")
        with other_tracer.start_as_current_span("invoke_agent TestAgent"):
            pass
        spans = self.exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(len(self.processor._span_id_to_nearest_invoke_agent_span_map), 0)

    def test_shutdown_clears_state(self):
        span = self.tracer.start_span("invoke_agent test")
        assert isinstance(span, Span)
        self.processor._span_id_to_nearest_invoke_agent_span_map.put(12345, span)
        span.end()
        self.assertEqual(len(self.processor._span_id_to_nearest_invoke_agent_span_map), 1)
        self.processor.shutdown()
        self.assertEqual(len(self.processor._span_id_to_nearest_invoke_agent_span_map), 0)

    def test_force_flush_returns_true(self):
        self.assertTrue(self.processor.force_flush())

    def test_agent_span_receives_chat_attributes(self):
        with self.tracer.start_as_current_span("invoke_agent TestAgent") as agent_span:
            agent_span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)

            with self.tracer.start_as_current_span("chat openai") as chat_span:
                chat_span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
                chat_span.set_attribute(GEN_AI_REQUEST_MODEL, "gpt-4")
                chat_span.set_attribute(GEN_AI_REQUEST_TEMPERATURE, 0.7)
                chat_span.set_attribute(GEN_AI_PROVIDER_NAME, "openai")

        spans = self.exporter.get_finished_spans()
        agent = next(s for s in spans if "invoke_agent" in s.name)
        attrs = agent.attributes
        self.assertIsNotNone(attrs)
        assert attrs is not None
        self.assertEqual(attrs.get(GEN_AI_REQUEST_MODEL), "gpt-4")
        self.assertEqual(attrs.get(GEN_AI_REQUEST_TEMPERATURE), 0.7)
        self.assertEqual(attrs.get(GEN_AI_PROVIDER_NAME), "openai")

    def test_does_not_propagate_to_non_recording_agent_span(self):
        agent_span = self.tracer.start_span("invoke_agent TestAgent")
        assert isinstance(agent_span, Span)
        agent_span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        agent_span.end()
        self.assertFalse(agent_span.is_recording())

        chat_span = self.tracer.start_span("chat openai")
        assert isinstance(chat_span, Span)
        chat_span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        chat_span.set_attribute(GEN_AI_REQUEST_MODEL, "gpt-4")
        assert chat_span.context is not None
        self.processor._span_id_to_nearest_invoke_agent_span_map.put(chat_span.context.span_id, agent_span)
        chat_span.end()

        spans = self.exporter.get_finished_spans()
        agent = next(s for s in spans if "invoke_agent" in s.name)
        attrs = agent.attributes
        self.assertIsNotNone(attrs)
        assert attrs is not None
        self.assertIsNone(attrs.get(GEN_AI_REQUEST_MODEL))

    def test_on_end_ignores_span_without_context(self):
        span = ReadableSpan("no-context")
        self.assertIsNone(span.context)
        self.processor.on_end(span)
        self.assertEqual(len(self.processor._span_id_to_nearest_invoke_agent_span_map), 0)

    def test_non_agent_child_does_not_propagate(self):
        with self.tracer.start_as_current_span("chat openai") as chat_span:
            chat_span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            chat_span.set_attribute(GEN_AI_REQUEST_MODEL, "gpt-4")

        spans = self.exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
