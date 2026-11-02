# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
from unittest.mock import patch

from plugins.opentelemetry.cloudwatch.span_metrics._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.span_metrics.instrumentor import SpanMetricsInstrumentor

from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, Decision
from opentelemetry.test.globals_test import reset_trace_globals
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import ProxyTracerProvider, set_tracer_provider
from opentelemetry.util._importlib_metadata import EntryPoint, entry_points


class TestSpanMetricsInstrumentor(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = SpanMetricsInstrumentor()
        self.instrumentor.uninstrument()

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def test_instrument_registers_processor_and_derives_metrics(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self._record_span("op")

        calls = self._metric_point(_SpanMetrics.CALLS_NAME, "op")
        self.assertEqual(calls.value, 1)
        duration = self._metric_point(_SpanMetrics.DURATION_NAME, "op")
        self.assertEqual(duration.count, 1)

        span = self.get_finished_spans().by_name("op")
        self.assertEqual(span.attributes[_SpanMetrics.SCHEMA], _SpanMetrics.SCHEMA_VERSION)
        self.assertIn(_SpanMetrics.LIB_VERSION, span.attributes)

    def test_appending_after_batch_processor_is_order_safe(self):
        exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))

        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self._record_span("ordered")

        exported = [s for s in exporter.get_finished_spans() if s.name == "ordered"]
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0].attributes[_SpanMetrics.SCHEMA], _SpanMetrics.SCHEMA_VERSION)
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "ordered").value, 1)

    def test_proxy_tracer_provider_is_skipped(self):
        proxy = ProxyTracerProvider()
        self.assertFalse(hasattr(proxy, "add_span_processor"))

        self.instrumentor.instrument(tracer_provider=proxy, meter_provider=self.meter_provider)

        with proxy.get_tracer(__name__).start_as_current_span("proxied"):
            pass

        self.assertEqual(self.get_sorted_metrics(), [])

    def test_instrumentation_dependencies_gate_the_sdk(self):
        deps = SpanMetricsInstrumentor().instrumentation_dependencies()
        self.assertTrue(any("opentelemetry-sdk" in dep for dep in deps))

    def test_uninstrument_stops_deriving_metrics(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self._record_span("before_uninstrument")
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "before_uninstrument").value, 1)

        self.instrumentor.uninstrument()

        self._record_span("after_uninstrument")
        after_points = [
            point
            for metric in self.get_sorted_metrics()
            if metric.name == _SpanMetrics.CALLS_NAME
            for point in metric.data.data_points
            if point.attributes.get(_SpanMetrics.SPAN_NAME) == "after_uninstrument"
        ]
        self.assertEqual(after_points, [])

    def test_uninstrument_safe_when_never_instrumented(self):
        original_sampler = self.tracer_provider.sampler

        self.instrumentor.uninstrument()

        self.assertIs(self.tracer_provider.sampler, original_sampler)
        self._record_span("never_instrumented")
        self.assertEqual(self.get_sorted_metrics(), [])

    def test_double_instrument_registers_once(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self._record_span("once")
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "once").value, 1)
        self.assertEqual(self._metric_point(_SpanMetrics.DURATION_NAME, "once").count, 1)

    def test_instrument_wraps_provider_sampler_to_record_dropped_spans(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        self.assertFalse(tracer_provider.get_tracer("baseline").start_span("dropped").is_recording())

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        tracer = tracer_provider.get_tracer("instrumented")
        self.assertTrue(tracer.start_span("after_instrument").is_recording())
        with tracer.start_as_current_span("wrapped"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "wrapped").value, 1)

    def test_uninstrument_restores_original_sampler(self):
        original_sampler = self.tracer_provider.sampler

        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.instrumentor.uninstrument()

        self.assertIs(self.tracer_provider.sampler, original_sampler)

    def test_uninstrument_disables_sampler_held_by_cached_tracers(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)
        cached_tracer = tracer_provider.get_tracer(__name__)

        self.assertTrue(cached_tracer.start_span("before_uninstrument").is_recording())
        with cached_tracer.start_as_current_span("before_uninstrument"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "before_uninstrument").value, 1)

        self.instrumentor.uninstrument()

        self.assertFalse(cached_tracer.start_span("after_uninstrument").is_recording())

    def test_reinstrument_after_uninstrument_derives_metrics_again(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)
        tracer = tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("first_cycle"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "first_cycle").value, 1)

        self.instrumentor.uninstrument()
        self.assertFalse(tracer.start_span("between_cycles").is_recording())

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        self.assertTrue(tracer.start_span("after_reinstrument").is_recording())
        with tracer.start_as_current_span("second_cycle"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "second_cycle").value, 1)
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "first_cycle").value, 1)

    def test_dropping_sampler_still_derives_full_metrics(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        exporter = InMemorySpanExporter()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        tracer = tracer_provider.get_tracer(__name__)
        for _ in range(5):
            with tracer.start_as_current_span("dropped"):
                pass

        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "dropped").value, 5)
        self.assertEqual(self._metric_point(_SpanMetrics.DURATION_NAME, "dropped").count, 5)
        self.assertEqual([s for s in exporter.get_finished_spans() if s.name == "dropped"], [])

    def test_dropping_sampler_records_but_does_not_sample(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        result = tracer_provider.sampler.should_sample(None, 0, "dropped")
        self.assertEqual(result.decision, Decision.RECORD_ONLY)

    def test_double_instrument_does_not_double_wrap_sampler(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)
        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        tracer = tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("double_wrapped"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "double_wrapped").value, 1)

        self.instrumentor.uninstrument()
        self.assertFalse(tracer.start_span("after_uninstrument").is_recording())

    def test_dropped_spans_derive_metrics_with_measured_duration(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        exporter = InMemorySpanExporter()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        sleep_seconds = 0.01
        started = time.perf_counter()
        for _ in range(4):
            span = self._record_span("dropped", tracer_provider=tracer_provider, sleep_seconds=sleep_seconds)
            self.assertFalse(span.is_recording())
        expected_seconds = time.perf_counter() - started

        calls = self._metric_point(_SpanMetrics.CALLS_NAME, "dropped")
        self.assertEqual(calls.value, 4)
        self.assertEqual(calls.attributes[_SpanMetrics.SPAN_KIND], "INTERNAL")
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "UNSET")
        self.assertEqual(calls.attributes[_SpanMetrics.SCHEMA], _SpanMetrics.SCHEMA_VERSION)

        duration = self._metric_point(_SpanMetrics.DURATION_NAME, "dropped")
        self.assertEqual(duration.count, 4)
        self.assertGreaterEqual(duration.min, sleep_seconds)
        self.assertLess(duration.max, expected_seconds)
        self.assertLess(duration.sum, expected_seconds)
        self.assertEqual(dict(duration.attributes), dict(calls.attributes))

        self.assertEqual([s for s in exporter.get_finished_spans() if s.name == "dropped"], [])

    def test_dropped_spans_stop_deriving_metrics_after_uninstrument(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        original_sampler = tracer_provider.sampler
        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        self.assertFalse(self._record_span("dropped", tracer_provider=tracer_provider).is_recording())
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "dropped").value, 1)

        self.instrumentor.uninstrument()
        self.assertIs(tracer_provider.sampler, original_sampler)

        self.assertFalse(self._record_span("dropped", tracer_provider=tracer_provider).is_recording())
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "dropped").value, 1)

    def _record_span(self, name, tracer_provider=None, sleep_seconds=0.0):
        tracer = (tracer_provider or self.tracer_provider).get_tracer(__name__)
        with tracer.start_as_current_span(name) as span:
            if sleep_seconds:
                time.sleep(sleep_seconds)
            return span

    def _metric_point(self, metric_name, span_name):
        point = None
        for metric in self.get_sorted_metrics():
            if metric.name != metric_name:
                continue
            for candidate in metric.data.data_points:
                if candidate.attributes.get(_SpanMetrics.SPAN_NAME) == span_name:
                    self.assertIsNone(point, f"multiple {metric_name} points for {span_name!r}")
                    point = candidate
        self.assertIsNotNone(point, f"no {metric_name} point for {span_name!r}")
        return point


class TestSpanMetricsAutoInstrumentation(TestBase):
    _ENTRY_POINT_NAME = "span_metrics"
    _ENTRY_POINT_VALUE = "plugins.opentelemetry.cloudwatch.span_metrics.instrumentor:SpanMetricsInstrumentor"

    def setUp(self):
        super().setUp()
        SpanMetricsInstrumentor().uninstrument()

    def tearDown(self):
        SpanMetricsInstrumentor().uninstrument()
        super().tearDown()

    def test_entry_point_registered_for_auto_instrumentation(self):
        matches = [ep for ep in entry_points(group="opentelemetry_instrumentor") if ep.name == self._ENTRY_POINT_NAME]
        if not matches:
            self.skipTest("cloudwatch-plugin-otel is not installed as a distribution; entry points are unavailable")
        self.assertEqual(matches[0].value, self._ENTRY_POINT_VALUE)
        self.assertIs(matches[0].load(), SpanMetricsInstrumentor)

    def test_auto_instrumentation_loader_derives_metrics_for_dropped_spans(self):
        self.tracer_provider, self.memory_exporter = self.create_tracer_provider(sampler=ALWAYS_OFF)
        reset_trace_globals()
        set_tracer_provider(self.tracer_provider)

        self._run_auto_instrumentation()

        span = self.tracer_provider.get_tracer("probe").start_span("auto_span")
        self.assertTrue(span.is_recording())
        span.end()

        calls = self._calls_for("auto_span")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].value, 1)
        exported = [s for s in self.memory_exporter.get_finished_spans() if s.name == "auto_span"]
        self.assertEqual(exported, [])

    def test_auto_instrumentation_loader_respects_disabled_list(self):
        with patch.dict("os.environ", {"OTEL_PYTHON_DISABLED_INSTRUMENTATIONS": self._ENTRY_POINT_NAME}):
            self._run_auto_instrumentation()

        with self.tracer_provider.get_tracer("probe").start_as_current_span("auto_span"):
            pass

        self.assertEqual(self._calls_for("auto_span"), [])

    def _run_auto_instrumentation(self):
        entry_point = EntryPoint(self._ENTRY_POINT_NAME, self._ENTRY_POINT_VALUE, "opentelemetry_instrumentor")

        def only_span_metrics(group):
            return (entry_point,) if group == "opentelemetry_instrumentor" else ()

        with patch.object(_load, "entry_points", side_effect=only_span_metrics), patch.object(
            _load, "get_dist_dependency_conflicts", return_value=None
        ):
            _load._load_instrumentors(DefaultDistro())

    def _calls_for(self, span_name):
        return [
            point
            for metric in self.get_sorted_metrics()
            if metric.name == _SpanMetrics.CALLS_NAME
            for point in metric.data.data_points
            if point.attributes.get(_SpanMetrics.SPAN_NAME) == span_name
        ]
