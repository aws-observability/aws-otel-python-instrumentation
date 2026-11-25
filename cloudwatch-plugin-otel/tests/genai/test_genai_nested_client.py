# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Demoting the outer of two nested GenAI inference CLIENT spans (no upstream packages needed)."""
import gc
import threading

import pytest
from plugins.opentelemetry.cloudwatch.genai import nested_client
from plugins.opentelemetry.cloudwatch.span_metrics._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.span_metrics.connector import SpanMetricsConnector

from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind

OP = "gen_ai.operation.name"
CLIENT, INTERNAL = SpanKind.CLIENT, SpanKind.INTERNAL
DEMOTED = [("inner", CLIENT), ("outer", INTERNAL)]


class RecordingExporter(SpanExporter):
    """Records ``(name, kind)`` when ``export()`` is called, not the span object, so a kind changed later (say
    by a processor's ``on_end`` after the export) is not seen."""

    def __init__(self):
        self.exported = []

    def export(self, spans):
        self.exported.extend((span.name, span.kind) for span in spans)
        return SpanExportResult.SUCCESS


class RecordingBatchSpanProcessor(BatchSpanProcessor):
    """Also records ``(name, kind)`` in ``on_end``: the batch exports later, from its own thread."""

    def __init__(self, exporter):
        super().__init__(exporter)
        self.ended = []

    def on_end(self, span):
        self.ended.append((span.name, span.kind))
        super().on_end(span)


@pytest.fixture
def provider():
    """A fresh SDK TracerProvider (not global) with an exporter registered before the plugin's processor."""
    tracer_provider = TracerProvider()
    tracer_provider.exporter = InMemorySpanExporter()
    tracer_provider.add_span_processor(SimpleSpanProcessor(tracer_provider.exporter))
    tracer_provider.demotion = nested_client.NestedGenAIClientSpanProcessor()
    tracer_provider.add_span_processor(tracer_provider.demotion)
    yield tracer_provider
    tracer_provider.shutdown()


def _nested(tracer, outer_op="chat", inner_op="chat", outer_kind=CLIENT):
    """An outer span with one inner CLIENT span, each with its operation at start (as util-genai and botocore)."""
    with tracer.start_as_current_span("outer", kind=outer_kind, attributes={OP: outer_op}):
        with tracer.start_as_current_span("inner", kind=CLIENT, attributes={OP: inner_op}):
            pass


def _kinds(exporter):
    return {span.name: span.kind for span in exporter.get_finished_spans()}


def test_a_simple_exporter_registered_earlier_exports_internal():
    exporter = RecordingExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))  # exports inside its own on_end
    tracer_provider.add_span_processor(nested_client.NestedGenAIClientSpanProcessor())

    _nested(tracer_provider.get_tracer("test"))

    assert exporter.exported == DEMOTED
    tracer_provider.shutdown()


def test_a_batch_exporter_registered_earlier_sees_internal():
    exporter = RecordingExporter()
    batch = RecordingBatchSpanProcessor(exporter)
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(batch)
    tracer_provider.add_span_processor(nested_client.NestedGenAIClientSpanProcessor())

    _nested(tracer_provider.get_tracer("test"))
    tracer_provider.force_flush()

    assert batch.ended == DEMOTED and exporter.exported == DEMOTED
    tracer_provider.shutdown()


def test_span_metrics_get_a_client_series_for_the_inner_span_only():
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SpanMetricsConnector(meter_provider=meter_provider))
    tracer_provider.add_span_processor(nested_client.NestedGenAIClientSpanProcessor())

    _nested(tracer_provider.get_tracer("test"))

    (calls,) = [
        metric
        for resource_metrics in reader.get_metrics_data().resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == _SpanMetrics.CALLS_NAME
    ]
    series = {
        (p.attributes[_SpanMetrics.SPAN_NAME], p.attributes[_SpanMetrics.SPAN_KIND]) for p in calls.data.data_points
    }
    assert series == {("inner", "CLIENT"), ("outer", "INTERNAL")}
    tracer_provider.shutdown()
    meter_provider.shutdown()


@pytest.mark.parametrize("outer_op", sorted(nested_client.INFERENCE_OPERATIONS))
@pytest.mark.parametrize("inner_op", sorted(nested_client.INFERENCE_OPERATIONS))
def test_any_inference_child_demotes_an_inference_parent(provider, outer_op, inner_op):
    """For example LangChain's ``chat`` over botocore's ``text_completion`` (a Titan text model)."""
    _nested(provider.get_tracer("test"), outer_op, inner_op)
    assert _kinds(provider.exporter) == dict(DEMOTED)


@pytest.mark.parametrize(
    "outer_op,inner_op", [("invoke_agent", "chat"), ("chat", "invoke_agent"), ("execute_tool", "execute_tool")]
)
def test_non_inference_operations_are_kept(provider, outer_op, inner_op):
    _nested(provider.get_tracer("test"), outer_op, inner_op)
    assert _kinds(provider.exporter) == {"inner": CLIENT, "outer": CLIENT}


def test_an_internal_outer_span_is_untouched(provider):
    _nested(provider.get_tracer("test"), outer_kind=INTERNAL)
    assert _kinds(provider.exporter) == dict(DEMOTED)


def test_an_inner_span_from_a_remote_parent_demotes_nothing(provider):
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("outer", kind=CLIENT, attributes={OP: "chat"}) as outer:
        own = outer.get_span_context()
        # The same ids, but arriving from another process.
        remote = SpanContext(own.trace_id, own.span_id, is_remote=True, trace_flags=own.trace_flags)
        with tracer.start_as_current_span(
            "inner", context=trace.set_span_in_context(NonRecordingSpan(remote)), kind=CLIENT, attributes={OP: "chat"}
        ):
            pass
    assert _kinds(provider.exporter) == {"inner": CLIENT, "outer": CLIENT}


def test_only_spans_with_an_inference_operation_at_start_are_tracked(provider):
    tracer = provider.get_tracer("test")
    spans = [
        tracer.start_span("http", kind=CLIENT),
        tracer.start_span("agent", kind=CLIENT, attributes={OP: "invoke_agent"}),
        tracer.start_span("server", kind=SpanKind.SERVER, attributes={OP: "chat"}),
    ]
    late = tracer.start_span("late", kind=CLIENT)
    late.set_attribute(OP, "chat")
    tracked = tracer.start_span("chat", kind=CLIENT, attributes={OP: "chat"})

    assert list(provider.demotion._open) == [(tracked.context.trace_id, tracked.context.span_id)]
    for span in spans + [late, tracked]:
        span.end()


def test_a_collected_unended_span_is_forgotten(provider):
    span = provider.get_tracer("test").start_span("never ended", kind=CLIENT, attributes={OP: "chat"})
    assert len(provider.demotion._open) == 1
    del span
    gc.collect()
    assert not provider.demotion._open


def test_a_child_ending_after_its_parent_leaves_no_entry(provider):
    tracer = provider.get_tracer("test")
    outer = tracer.start_span("outer", kind=CLIENT, attributes={OP: "chat"})
    inner = tracer.start_span("inner", context=trace.set_span_in_context(outer), kind=CLIENT, attributes={OP: "chat"})
    outer.end()
    inner.end()
    assert _kinds(provider.exporter) == {"inner": CLIENT, "outer": CLIENT}
    assert not provider.demotion._open


def test_no_entry_is_left_after_the_spans_end(provider):
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("server", kind=SpanKind.SERVER):
        _nested(tracer)
        with tracer.start_as_current_span("embeddings", kind=CLIENT, attributes={OP: "embeddings"}):
            pass
    assert not provider.demotion._open


def test_span_ends_skip_the_lock_while_nothing_is_tracked(provider, monkeypatch):
    monkeypatch.setattr(provider.demotion, "_lock", None)  # any use of the lock would raise
    with provider.get_tracer("test").start_as_current_span("http", kind=CLIENT):
        pass
    assert _kinds(provider.exporter) == {"http": CLIENT}


def test_concurrent_requests(provider):
    tracer = provider.get_tracer("test")
    barrier = threading.Barrier(8)

    def request():
        barrier.wait()
        for _ in range(50):
            _nested(tracer)

    threads = [threading.Thread(target=request) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    spans = provider.exporter.get_finished_spans()
    assert len(spans) == 800
    assert {(span.name, span.kind) for span in spans} == set(DEMOTED)
    assert not provider.demotion._open


def test_the_step_adds_one_processor(genai_active, span_exporter):
    assert nested_client.nested_client_step() == nested_client.ON
    assert nested_client.nested_client_step() == nested_client.ON
    processors = trace.get_tracer_provider()._active_span_processor._span_processors
    assert len([p for p in processors if isinstance(p, nested_client.NestedGenAIClientSpanProcessor)]) == 1

    _nested(trace.get_tracer("test"))
    assert _kinds(span_exporter) == dict(DEMOTED)


def test_a_processor_registered_by_an_earlier_run_is_not_duplicated(genai_active, span_exporter):
    """The check finds only the plugin's own processor; it makes a repeated post hook idempotent."""
    provider = trace.get_tracer_provider()
    existing = nested_client.NestedGenAIClientSpanProcessor()
    provider.add_span_processor(existing)

    assert nested_client.nested_client_step() == nested_client.ON
    processors = provider._active_span_processor._span_processors
    assert [p for p in processors if isinstance(p, nested_client.NestedGenAIClientSpanProcessor)] == [existing]
