# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gc
import unittest
import weakref
from unittest.mock import patch

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GenAINestedClientSpanProcessor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind, TraceFlags


class TestGenAINestedClientSpanProcessor(unittest.TestCase):  # pylint: disable=too-many-public-methods
    def setUp(self):
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.addCleanup(self.provider.shutdown)
        self.processor = GenAINestedClientSpanProcessor()
        self.provider.add_span_processor(self.processor)
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer("test")

    def test_on_end_nested_llm_client_child_converts_parent_to_internal(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=ctx)
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        child_span = spans[0]
        parent_span = spans[1]
        self.assertEqual(child_span.kind, SpanKind.CLIENT)
        self.assertEqual(parent_span.kind, SpanKind.INTERNAL)

    def test_on_end_http_child_leaves_parent_client(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("POST", kind=SpanKind.CLIENT, context=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        parent_span = next(s for s in spans if s.name == "chat model")
        self.assertEqual(parent_span.kind, SpanKind.CLIENT)

    def test_on_end_no_child_stays_client(self):
        span = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_on_end_text_completion_converted_by_child(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.TEXT_COMPLETION.value)
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=ctx)
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.TEXT_COMPLETION.value)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.INTERNAL)

    def test_on_end_embeddings_converted_by_child(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EMBEDDINGS.value)
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=ctx)
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EMBEDDINGS.value)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.INTERNAL)

    def test_on_end_generate_content_converted_by_child(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.GENERATE_CONTENT.value)
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=ctx)
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.GENERATE_CONTENT.value)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.INTERNAL)

    def test_on_end_different_llm_operation_child_leaves_parent_client(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EMBEDDINGS.value)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.CLIENT)

    def test_on_end_non_llm_operation_ignored(self):
        span = self.tracer.start_span("invoke_agent MyAgent", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_on_end_llm_child_leaves_non_llm_parent_client(self):
        parent = self.tracer.start_span("invoke_agent MyAgent", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.CLIENT)

    def test_on_end_internal_span_ignored(self):
        span = self.tracer.start_span("chat model", kind=SpanKind.INTERNAL)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.INTERNAL)

    def test_on_end_span_without_gen_ai_attr_ignored(self):
        span = self.tracer.start_span("some-span", kind=SpanKind.CLIENT)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_on_end_no_parent_stays_client(self):
        span = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_on_end_remote_parents_do_not_accumulate_state(self):
        for span_id in range(1, 1001):
            parent = NonRecordingSpan(
                SpanContext(
                    trace_id=span_id, span_id=span_id, is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED)
                )
            )
            child = self.tracer.start_span(
                "chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent)
            )
            child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            child.end()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_end_nonrecording_local_parent_does_not_accumulate_state(self):
        parent = NonRecordingSpan(
            SpanContext(trace_id=1, span_id=1, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))
        )
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_end_parent_from_another_provider_does_not_accumulate_state(self):
        other_provider = TracerProvider(sampler=ALWAYS_ON)
        self.addCleanup(other_provider.shutdown)
        parent = other_provider.get_tracer("other").start_span("parent", kind=SpanKind.CLIENT)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        parent.end()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_end_child_ending_after_parent_does_not_recreate_state(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        parent.end()
        child.end()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)
        self.assertTrue(all(span.kind == SpanKind.CLIENT for span in self.exporter.get_finished_spans()))

    def test_on_end_nonclient_parent_does_not_accumulate_state(self):
        parent = self.tracer.start_span("parent", kind=SpanKind.INTERNAL)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)
        parent.end()
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_end_non_llm_client_parent_cleans_up_child_state(self):
        parent = self.tracer.start_span("parent", kind=SpanKind.CLIENT)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        parent.end()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_start_abandoned_parent_releases_child_state(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        parent_ref = weakref.ref(parent)
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 1)

        del parent
        gc.collect()

        self.assertIsNone(parent_ref())
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_start_abandoned_child_releases_state(self):
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child_ref = weakref.ref(child)

        del child
        gc.collect()

        self.assertIsNone(child_ref())
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_end_same_span_id_in_different_traces_does_not_share_state(self):
        with patch.object(self.provider.id_generator, "generate_trace_id", side_effect=[1, 2]):
            with patch.object(self.provider.id_generator, "generate_span_id", return_value=123):
                first_parent = self.tracer.start_span("first parent", kind=SpanKind.CLIENT)
                first_parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
                second_parent = self.tracer.start_span("second parent", kind=SpanKind.CLIENT)
                second_parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span(
            "chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(first_parent)
        )
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        first_parent.end()
        second_parent.end()

        parents = {span.name: span for span in self.exporter.get_finished_spans()}
        self.assertEqual(parents["first parent"].kind, SpanKind.INTERNAL)
        self.assertEqual(parents["second parent"].kind, SpanKind.CLIENT)
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_on_end_three_nested_llm_clients_preserve_conversion(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        grandchild = self.tracer.start_span(
            "chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(child)
        )
        grandchild.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        grandchild.end()
        child.end()
        parent.end()

        self.assertEqual(
            [span.kind for span in self.exporter.get_finished_spans()],
            [SpanKind.CLIENT, SpanKind.INTERNAL, SpanKind.INTERNAL],
        )
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_shutdown_clears_state(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 1)
        child_operations = self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child().get(
            (parent.context.trace_id, parent.context.span_id)
        )
        self.assertEqual(child_operations, frozenset({GenAiOperationNameValues.CHAT.value}))

        self.processor.shutdown()

        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)
        parent.end()
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_force_flush_returns_true(self):
        processor = GenAINestedClientSpanProcessor()
        self.assertTrue(processor.force_flush())

    def test_force_flush_preserves_active_parent_state(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()

        self.assertTrue(self.processor.force_flush())
        parent.end()

        self.assertEqual(self.exporter.get_finished_spans()[-1].kind, SpanKind.INTERNAL)
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)

    def test_get_active_parent_span_id_and_operation_to_gen_ai_client_child_returns_immutable_snapshot(self):
        parent = self.tracer.start_span("chat model", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child = self.tracer.start_span("chat model", kind=SpanKind.CLIENT, context=trace.set_span_in_context(parent))
        child.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        child.end()
        span_key = (parent.context.trace_id, parent.context.span_id)
        snapshot = self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()

        with self.assertRaises(TypeError):
            snapshot[span_key] = frozenset()
        with self.assertRaises(AttributeError):
            snapshot[span_key].add(GenAiOperationNameValues.EMBEDDINGS.value)

        parent.end()
        self.assertEqual(self.exporter.get_finished_spans()[-1].kind, SpanKind.INTERNAL)
        self.assertEqual(len(self.processor.get_active_parent_span_id_and_operation_to_gen_ai_client_child()), 0)


if __name__ == "__main__":
    unittest.main()
