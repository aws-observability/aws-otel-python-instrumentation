# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from plugins.opentelemetry.cloudwatch.connector._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.connector.instrumentor import SpanMetricsInstrumentor
from plugins.opentelemetry.cloudwatch.connector.span_metrics_connector import SpanMetricsConnector
from plugins.opentelemetry.cloudwatch.sampler.always_record_sampler import AlwaysRecordSampler

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, Decision
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import ProxyTracerProvider


class TestSpanMetricsInstrumentor(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = SpanMetricsInstrumentor()
        self.instrumentor.uninstrument()

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _record_span(self, name):
        tracer = self.tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span(name):
            pass

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

        self.instrumentor.instrument(tracer_provider=proxy)
        self.assertIsNone(self.instrumentor._processor)

    def test_instrumentation_dependencies_gate_the_sdk(self):
        deps = SpanMetricsInstrumentor().instrumentation_dependencies()
        self.assertTrue(any("opentelemetry-sdk" in dep for dep in deps))

    def test_uninstrument_shuts_down_processor(self):
        shutdown_calls = []

        class _SpyProcessor(SpanMetricsConnector):
            def shutdown(self):
                shutdown_calls.append(True)
                super().shutdown()

        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.instrumentor._processor = _SpyProcessor(meter_provider=self.meter_provider)

        self.instrumentor.uninstrument()

        self.assertEqual(shutdown_calls, [True])
        self.assertFalse(self.instrumentor._processor.enabled)

    def test_uninstrument_safe_when_never_instrumented(self):
        self.assertIsNone(self.instrumentor._processor)
        self.instrumentor.uninstrument()

    def test_double_instrument_registers_once(self):
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        first = self.instrumentor._processor
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.assertIs(self.instrumentor._processor, first)

        self._record_span("once")
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "once").value, 1)

    def test_instrument_wraps_provider_sampler_with_always_record(self):
        original_sampler = self.tracer_provider.sampler
        self.assertNotIsInstance(original_sampler, AlwaysRecordSampler)

        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self.assertIsInstance(self.tracer_provider.sampler, AlwaysRecordSampler)
        self.assertIs(self.tracer_provider.sampler._root_sampler, original_sampler)

    def test_uninstrument_restores_original_sampler(self):
        original_sampler = self.tracer_provider.sampler

        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.instrumentor.uninstrument()

        self.assertIs(self.tracer_provider.sampler, original_sampler)

    def test_uninstrument_disables_sampler_held_by_cached_tracers(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)
        installed_sampler = tracer_provider.sampler
        cached_tracer = tracer_provider.get_tracer(__name__)

        with cached_tracer.start_as_current_span("before_uninstrument"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "before_uninstrument").value, 1)

        self.instrumentor.uninstrument()

        self.assertFalse(installed_sampler.enabled)
        self.assertIs(cached_tracer.sampler, installed_sampler)
        self.assertFalse(cached_tracer.start_span("after_uninstrument").is_recording())

    def test_reinstrument_after_uninstrument_derives_metrics_again(self):
        tracer_provider = TracerProvider(sampler=ALWAYS_OFF)
        original_sampler = tracer_provider.sampler

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)
        with tracer_provider.get_tracer(__name__).start_as_current_span("first_cycle"):
            pass
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "first_cycle").value, 1)

        self.instrumentor.uninstrument()
        self.assertIs(tracer_provider.sampler, original_sampler)

        self.instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=self.meter_provider)

        self.assertIsInstance(tracer_provider.sampler, AlwaysRecordSampler)
        self.assertTrue(tracer_provider.sampler.enabled)
        self.assertIs(tracer_provider.sampler._root_sampler, original_sampler)

        with tracer_provider.get_tracer(__name__).start_as_current_span("second_cycle"):
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
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        wrapped = self.tracer_provider.sampler

        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self.assertIs(self.tracer_provider.sampler, wrapped)
        self.assertNotIsInstance(wrapped._root_sampler, AlwaysRecordSampler)

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
