# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gc
import random
import subprocess
import sys
import textwrap
import threading
import unittest

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GenAINestedClientSpanProcessor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind, TraceFlags


class TestGenAINestedClientSpanProcessor(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.processor = GenAINestedClientSpanProcessor()
        self.provider.add_span_processor(self.processor)
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

    def test_http_child_leaves_parent_client(self):
        parent = self._make_llm_span()
        ctx = trace.set_span_in_context(parent)
        child = self.tracer.start_span("POST", kind=SpanKind.CLIENT, context=ctx)
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        parent_span = next(s for s in spans if s.name == "chat model")
        self.assertEqual(parent_span.kind, SpanKind.CLIENT)

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

    def test_different_llm_operation_child_leaves_parent_client(self):
        parent = self._make_llm_span()
        child = self._make_llm_span(
            op=GenAiOperationNameValues.EMBEDDINGS.value,
            ctx=trace.set_span_in_context(parent),
        )
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.CLIENT)

    def test_non_llm_operation_ignored(self):
        span = self.tracer.start_span("invoke_agent MyAgent", kind=SpanKind.CLIENT)
        span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        span.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)

    def test_llm_child_leaves_non_llm_parent_client(self):
        parent = self.tracer.start_span("invoke_agent MyAgent", kind=SpanKind.CLIENT)
        parent.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
        child = self._make_llm_span(ctx=trace.set_span_in_context(parent))
        child.end()
        parent.end()

        spans = self.exporter.get_finished_spans()
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[1].kind, SpanKind.CLIENT)

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

    def _finished(self, name):
        return next(s for s in self.exporter.get_finished_spans() if s.name == name)

    def test_three_levels_demote_all_but_innermost(self):
        top = self._make_llm_span("top")
        middle = self._make_llm_span("middle", ctx=trace.set_span_in_context(top))
        inner = self._make_llm_span("inner", ctx=trace.set_span_in_context(middle))
        inner.end()
        middle.end()
        top.end()

        self.assertEqual(self._finished("top").kind, SpanKind.INTERNAL)
        self.assertEqual(self._finished("middle").kind, SpanKind.INTERNAL)
        self.assertEqual(self._finished("inner").kind, SpanKind.CLIENT)
        self.assertEqual(self.processor._parents, {})  # noqa: SLF001

    def test_remote_parent_keeps_no_state(self):
        for _ in range(100):
            remote = SpanContext(
                random.getrandbits(128), random.getrandbits(64), is_remote=True, trace_flags=TraceFlags(1)
            )
            self._make_llm_span(ctx=trace.set_span_in_context(NonRecordingSpan(remote))).end()

        self.assertEqual(self.processor._parents, {})  # noqa: SLF001

    def test_child_ending_after_parent_keeps_no_state(self):
        for _ in range(100):
            parent = self.tracer.start_span("agent")
            child = self._make_llm_span(ctx=trace.set_span_in_context(parent))
            parent.end()
            child.end()
            # A child that starts after its parent ended.
            self._make_llm_span(ctx=trace.set_span_in_context(parent)).end()

        self.assertEqual(self.processor._parents, {})  # noqa: SLF001

    def test_non_client_children_keep_no_state(self):
        parent = self.tracer.start_span("agent")
        self.tracer.start_span("step", context=trace.set_span_in_context(parent))

        self.assertEqual(self.processor._parents, {})  # noqa: SLF001

    def _abandon_parents(self, count):
        # Parents that never end, each with a CLIENT child that does end. Dropped together, like requests.
        parents = [self.tracer.start_span("agent") for _ in range(count)]
        for parent in parents:
            self._make_llm_span(ctx=trace.set_span_in_context(parent)).end()
        del parents
        gc.collect()

    def test_state_released_for_parents_collected_without_ending(self):
        self._abandon_parents(100)
        self.assertEqual(len(self.processor._collected), 100)  # noqa: SLF001

        # Any later span, even one that does not touch this processor's state, releases the collected entries.
        self.tracer.start_span("unrelated").end()

        self.assertEqual(self.processor._parents, {})  # noqa: SLF001
        self.assertEqual(len(self.processor._collected), 0)  # noqa: SLF001

    def test_force_flush_releases_collected_parents(self):
        self._abandon_parents(10)
        self.assertTrue(self.processor.force_flush())
        self.assertEqual(self.processor._parents, {})  # noqa: SLF001

    def test_finalizer_does_not_deadlock_while_processor_holds_its_lock(self):
        parent = self.tracer.start_span("agent")
        self._make_llm_span(ctx=trace.set_span_in_context(parent)).end()
        cycle = [parent]
        cycle.append(cycle)  # Keep the parent alive until the cyclic collector runs.
        del parent, cycle

        def collect_inside_lock():
            with self.processor._lock:  # noqa: SLF001
                gc.collect()

        worker = threading.Thread(target=collect_inside_lock, daemon=True)
        worker.start()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive(), "finalizer deadlocked on the processor lock")
        self.assertEqual(len(self.processor._collected), 1)  # noqa: SLF001

    def test_span_ended_while_lock_is_held_on_the_same_thread_does_not_deadlock(self):
        # Garbage collection can end a span on the thread that holds the processor's lock, for example by
        # closing an abandoned generator that holds a span open. on_end must be able to re-enter.
        parent = self.tracer.start_span("agent")
        child = self._make_llm_span(ctx=trace.set_span_in_context(parent))

        def end_inside_lock():
            with self.processor._lock:  # noqa: SLF001
                child.end()
                parent.end()

        worker = threading.Thread(target=end_inside_lock, daemon=True)
        worker.start()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive(), "on_end deadlocked on the processor lock")
        self.assertEqual(self.processor._parents, {})  # noqa: SLF001

    def test_spans_ended_by_garbage_collection_do_not_deadlock(self):
        # Abandoned streaming generators that hold a span open are closed by the cyclic collector, which can
        # run inside this processor's own calls and end a span on the same thread. Runs in a subprocess so a
        # deadlock fails the test instead of hanging the test run.
        script = textwrap.dedent(
            """
            import gc, os, threading
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.trace import SpanKind
            from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import (
                GenAINestedClientSpanProcessor,
            )

            gc.set_threshold(50)
            provider = TracerProvider()
            provider.add_span_processor(GenAINestedClientSpanProcessor())
            tracer = provider.get_tracer("t")

            def stream():
                with tracer.start_as_current_span("stream", kind=SpanKind.CLIENT) as span:
                    span.set_attribute("gen_ai.operation.name", "chat")
                    yield 1
                    yield 2

            def run():
                for _ in range(200):
                    parent = tracer.start_span("agent")
                    ctx = trace.set_span_in_context(parent)
                    for _ in range(20):
                        generator = stream()
                        next(generator)
                        cycle = [generator]
                        cycle.append(cycle)
                        del generator, cycle
                        chat = tracer.start_span("chat", kind=SpanKind.CLIENT, context=ctx)
                        chat.set_attribute("gen_ai.operation.name", "chat")
                        chat.end()
                    parent.end()

            worker = threading.Thread(target=run, daemon=True)
            worker.start()
            worker.join(20)
            os._exit(1 if worker.is_alive() else 0)
            """
        )
        try:
            result = subprocess.run([sys.executable, "-c", script], timeout=40, check=False)
        except subprocess.TimeoutExpired:
            self.fail("subprocess did not finish")
        self.assertEqual(result.returncode, 0, "a span ended by garbage collection deadlocked the processor")

    def test_shutdown_clears_state(self):
        parent = self.tracer.start_span("agent")
        self._make_llm_span(ctx=trace.set_span_in_context(parent))
        self.assertEqual(len(self.processor._parents), 1)  # noqa: SLF001
        finalizer = next(iter(self.processor._parents.values()))[1]  # noqa: SLF001

        self.processor.shutdown()

        self.assertEqual(self.processor._parents, {})  # noqa: SLF001
        self.assertFalse(finalizer.alive)

    def test_force_flush_returns_true(self):
        processor = GenAINestedClientSpanProcessor()
        self.assertTrue(processor.force_flush())


if __name__ == "__main__":
    unittest.main()
