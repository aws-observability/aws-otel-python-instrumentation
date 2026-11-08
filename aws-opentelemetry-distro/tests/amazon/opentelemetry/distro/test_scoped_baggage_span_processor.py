# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import gc
import threading
import unittest

from amazon.opentelemetry.distro.gen_ai_nested_client_span_processor import GenAINestedClientSpanProcessor
from amazon.opentelemetry.distro.scoped_baggage_span_processor import (
    AWS_AGENT_LINEAGE,
    INFERENCE_SCOPE,
    ScopedBaggageSpanProcessor,
    _clear_registered_scopes,
    is_gen_ai_inference_span,
    register_baggage_scope,
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

        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001

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
        # Collected entries are removed the next time the processor takes its lock.
        self._start().end()

        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001

    def _abandon_batch(self, count):
        # Hold the spans together, then drop them at once, like concurrent requests that never end.
        spans = [self.tracer.start_span("abandoned", kind=SpanKind.CLIENT) for _ in range(count)]
        del spans
        gc.collect()

    def test_collected_entries_removed_when_later_spans_have_no_lineage(self):
        self._abandon_batch(100)
        self.assertEqual(len(self.processor._collected), 100)  # noqa: SLF001
        self.assertEqual(len(self.processor._pending_by_span), 100)  # noqa: SLF001

        # Later traffic without the scoped baggage must still release the collected entries.
        no_lineage = context.attach(baggage.remove_baggage(AWS_AGENT_LINEAGE))
        try:
            self.tracer.start_span("no lineage", kind=SpanKind.CLIENT)
        finally:
            context.detach(no_lineage)

        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001
        self.assertEqual(len(self.processor._collected), 0)  # noqa: SLF001

    def test_on_end_and_force_flush_remove_collected_entries(self):
        held = self._start(name="held")
        self._abandon_batch(10)
        held.end()
        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001

        self._abandon_batch(10)
        self.assertEqual(len(self.processor._pending_by_span), 10)  # noqa: SLF001
        self.assertTrue(self.processor.force_flush())
        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001

    def test_finalizer_does_not_deadlock_while_processor_holds_its_lock(self):
        # Garbage collection can run a finalizer while the processor holds its lock, for example during an
        # allocation inside shutdown(). The finalizer must not try to take the lock.
        span = self.tracer.start_span("abandoned", kind=SpanKind.CLIENT)
        cycle = [span]
        cycle.append(cycle)  # Keep the span alive until the cyclic collector runs.
        del span, cycle

        def collect_inside_lock():
            with self.processor._lock:  # noqa: SLF001
                gc.collect()

        worker = threading.Thread(target=collect_inside_lock, daemon=True)
        worker.start()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive(), "finalizer deadlocked on the processor lock")
        self.assertEqual(len(self.processor._collected), 1)  # noqa: SLF001

        self.processor.shutdown()
        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001

    def test_span_promoted_to_client_after_start_still_matches(self):
        # A span can start as INTERNAL and be changed to CLIENT before it ends.
        span = self._start(kind=SpanKind.INTERNAL)
        span._kind = SpanKind.CLIENT  # noqa: SLF001
        span.end()

        self.assertEqual(self._finished("chat model").attributes[AWS_AGENT_LINEAGE], LINEAGE)

    def test_shutdown_clears_pending_state_and_detaches_finalizers(self):
        span = self._start()
        finalizer = self.processor._pending_by_span[
            (span.context.trace_id, span.context.span_id)
        ].finalizer  # noqa: SLF001
        self.processor.shutdown()

        self.assertEqual(self.processor._pending_by_span, {})  # noqa: SLF001
        self.assertFalse(finalizer.alive)
        span.end()

    def test_unknown_scope_skips_the_key_and_warns_once(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(ScopedBaggageSpanProcessor({AWS_AGENT_LINEAGE: "no_such_scope"}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")

        with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING") as logs:
            for _ in range(3):
                tracer.start_span("s").end()

        self.assertEqual(len(logs.records), 1)
        self.assertTrue(all(AWS_AGENT_LINEAGE not in span.attributes for span in exporter.get_finished_spans()))

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

    def _nested_chat_spans(self, tracer, outer_op=GenAiOperationNameValues.CHAT.value):
        framework = tracer.start_span("framework", kind=SpanKind.CLIENT)
        framework.set_attribute(GEN_AI_OPERATION_NAME, outer_op)
        sdk = tracer.start_span("sdk", kind=SpanKind.CLIENT, context=trace.set_span_in_context(framework))
        sdk.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
        sdk.end()
        framework.end()

    def test_only_innermost_client_span_gets_lineage_without_nested_processor(self):
        # Agent observability off: GenAINestedClientSpanProcessor is not installed, so both spans stay CLIENT.
        self._nested_chat_spans(self.tracer)

        self.assertEqual(self._finished("framework").kind, SpanKind.CLIENT)
        self.assertNotIn(AWS_AGENT_LINEAGE, self._finished("framework").attributes)
        self.assertEqual(self._finished("sdk").attributes[AWS_AGENT_LINEAGE], LINEAGE)
        self.assertEqual(self.processor._pending_by_span, {})

    def test_nested_inference_spans_with_different_operations_both_match(self):
        # Same rule as GenAINestedClientSpanProcessor: only a child with the same operation demotes the parent.
        self._nested_chat_spans(self.tracer, outer_op=GenAiOperationNameValues.EMBEDDINGS.value)

        self.assertEqual(self._finished("framework").attributes[AWS_AGENT_LINEAGE], LINEAGE)
        self.assertEqual(self._finished("sdk").attributes[AWS_AGENT_LINEAGE], LINEAGE)

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


class TestRegisterBaggageScope(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemorySpanExporter()
        provider = TracerProvider()
        # As configured from OTEL_BAGGAGE_SPAN_ATTRIBUTE_KEYS=my.key:tools before the scope is registered.
        provider.add_span_processor(ScopedBaggageSpanProcessor({"my.key": "tools"}))
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = provider.get_tracer("test")
        self.token = context.attach(baggage.set_baggage("my.key", "v1"))

    def tearDown(self):
        context.detach(self.token)
        _clear_registered_scopes()

    def _finished(self, name):
        return next(s for s in self.exporter.get_finished_spans() if s.name == name)

    @staticmethod
    def _is_tool_span(span):
        return (span.attributes or {}).get(GEN_AI_OPERATION_NAME) == "execute_tool"

    def _tool_span(self, name="tool"):
        span = self.tracer.start_span(name)
        span.set_attribute(GEN_AI_OPERATION_NAME, "execute_tool")
        return span

    def test_registered_scope_applies_to_keys_mapped_in_env(self):
        self.assertTrue(register_baggage_scope("tools", self._is_tool_span))

        self._tool_span().end()
        self.tracer.start_span("other").end()

        self.assertEqual(self._finished("tool").attributes["my.key"], "v1")
        self.assertNotIn("my.key", self._finished("other").attributes)

    def test_scope_registered_after_span_start_applies_at_end(self):
        span = self._tool_span()
        register_baggage_scope("tools", self._is_tool_span)
        span.end()

        self.assertEqual(self._finished("tool").attributes["my.key"], "v1")

    def test_key_not_copied_before_scope_is_registered(self):
        with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING"):
            self._tool_span("before").end()
        register_baggage_scope("tools", self._is_tool_span)
        self._tool_span("after").end()

        self.assertNotIn("my.key", self._finished("before").attributes)
        self.assertEqual(self._finished("after").attributes["my.key"], "v1")

    def test_re_registering_replaces_the_scope(self):
        register_baggage_scope("tools", lambda span: False)
        register_baggage_scope("tools", self._is_tool_span)
        self._tool_span().end()

        self.assertEqual(self._finished("tool").attributes["my.key"], "v1")

    def test_predicate_errors_skip_the_key(self):
        def broken(_span):
            raise ValueError("boom")

        register_baggage_scope("tools", broken)
        self._tool_span().end()

        self.assertNotIn("my.key", self._finished("tool").attributes)

    def test_key_matching_any_of_its_scopes_is_copied(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(ScopedBaggageSpanProcessor({"my.key": {"tools", INFERENCE_SCOPE, "no_such_scope"}}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")
        register_baggage_scope("tools", self._is_tool_span)

        with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING"):
            tool = tracer.start_span("tool")
            tool.set_attribute(GEN_AI_OPERATION_NAME, "execute_tool")
            tool.end()
            chat = tracer.start_span("chat", kind=SpanKind.CLIENT)
            chat.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            chat.end()
            tracer.start_span("other").end()

        spans = {s.name: s for s in exporter.get_finished_spans()}
        self.assertEqual(spans["tool"].attributes["my.key"], "v1")
        self.assertEqual(spans["chat"].attributes["my.key"], "v1")
        self.assertNotIn("my.key", spans["other"].attributes)

    def test_predicate_error_does_not_block_other_scopes(self):
        def broken(_span):
            raise ValueError("boom")

        register_baggage_scope("broken", broken)
        register_baggage_scope("tools", self._is_tool_span)
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(ScopedBaggageSpanProcessor({"my.key": {"broken", "tools"}}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tool = provider.get_tracer("test").start_span("tool")
        tool.set_attribute(GEN_AI_OPERATION_NAME, "execute_tool")
        tool.end()

        self.assertEqual(exporter.get_finished_spans()[0].attributes["my.key"], "v1")

    def test_invalid_registrations_are_rejected(self):
        for name in ("", " tools", "a:b", "a,b", INFERENCE_SCOPE, "aws.tools"):
            with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING"):
                self.assertFalse(register_baggage_scope(name, self._is_tool_span), name)
        with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING"):
            self.assertFalse(register_baggage_scope("tools", "not callable"))

    def test_builtin_scope_still_works_after_rejected_override(self):
        with self.assertLogs("amazon.opentelemetry.distro.scoped_baggage_span_processor", level="WARNING"):
            register_baggage_scope(INFERENCE_SCOPE, lambda span: True)
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(ScopedBaggageSpanProcessor({"my.key": INFERENCE_SCOPE}))
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        provider.get_tracer("test").start_span("internal").end()

        self.assertNotIn("my.key", exporter.get_finished_spans()[0].attributes)

    def test_unprefixed_inference_is_a_custom_scope_name(self):
        self.assertTrue(register_baggage_scope("inference", self._is_tool_span))


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
