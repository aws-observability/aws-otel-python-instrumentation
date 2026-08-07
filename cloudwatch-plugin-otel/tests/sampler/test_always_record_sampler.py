# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from flask import Flask
from plugins.opentelemetry.cloudwatch.sampler.always_record_sampler import AlwaysRecordSampler
from plugins.opentelemetry.cloudwatch.span_metrics._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.span_metrics.instrumentor import SpanMetricsInstrumentor

from opentelemetry.context import Context
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME as RESOURCE_SERVICE_NAME
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, Decision, ParentBased, Sampler, SamplingResult, StaticSampler
from opentelemetry.semconv.attributes.http_attributes import HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE, HTTP_ROUTE
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class TestAlwaysRecordSampler(TestCase):
    def setUp(self):
        self.mock_sampler: Sampler = MagicMock()
        self.sampler: Sampler = AlwaysRecordSampler(self.mock_sampler)

    def test_none_root_sampler_raises(self):
        with self.assertRaises(ValueError):
            AlwaysRecordSampler(None)

    def test_get_description(self):
        static_sampler: Sampler = StaticSampler(Decision.DROP)
        test_sampler: Sampler = AlwaysRecordSampler(static_sampler)
        self.assertEqual("AlwaysRecordSampler{AlwaysOffSampler}", test_sampler.get_description())

    def test_record_and_sample_sampling_decision(self):
        self.validate_should_sample(Decision.RECORD_AND_SAMPLE, Decision.RECORD_AND_SAMPLE)

    def test_record_only_sampling_decision(self):
        self.validate_should_sample(Decision.RECORD_ONLY, Decision.RECORD_ONLY)

    def test_drop_sampling_decision(self):
        self.validate_should_sample(Decision.DROP, Decision.RECORD_ONLY)

    def test_drop_with_both_none_attributes(self):
        root_result = SamplingResult(decision=Decision.DROP, attributes=None, trace_state=TraceState())
        self.mock_sampler.should_sample.return_value = root_result

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=None,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(len(actual_result.attributes), 0)

    def test_drop_with_both_empty_attributes(self):
        root_result = self._build_root_sampling_result(Decision.DROP, {})
        self.mock_sampler.should_sample.return_value = root_result

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes={},
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {})

    def test_drop_decision_merges_attributes_with_sampler_precedence(self):
        root_result = self._build_root_sampling_result(
            Decision.DROP, {"shared_key": "sampler_value", "sampler_only": "yes"}
        )
        self.mock_sampler.should_sample.return_value = root_result

        original_attributes = {"shared_key": "original_value", "original_only": "yes"}
        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes.get("shared_key"), "sampler_value")
        self.assertEqual(actual_result.attributes.get("sampler_only"), "yes")
        self.assertEqual(actual_result.attributes.get("original_only"), "yes")

    def test_drop_with_original_none_uses_sampler_attributes(self):
        root_result = self._build_root_sampling_result(Decision.DROP, {"sampler_key": "sampler_value"})
        self.mock_sampler.should_sample.return_value = root_result

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=None,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {"sampler_key": "sampler_value"})

    def test_drop_with_sampler_none_uses_original_attributes(self):
        root_result = SamplingResult(decision=Decision.DROP, attributes=None, trace_state=TraceState())
        self.mock_sampler.should_sample.return_value = root_result

        original_attributes = {"original_key": "original_value"}
        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {"original_key": "original_value"})

    def test_drop_merges_disjoint_attributes(self):
        root_result = self._build_root_sampling_result(Decision.DROP, {"c": "3", "d": "4"})
        self.mock_sampler.should_sample.return_value = root_result

        original_attributes = {"a": "1", "b": "2"}
        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        self.assertEqual(actual_result.decision, Decision.RECORD_ONLY)
        self.assertEqual(actual_result.attributes, {"a": "1", "b": "2", "c": "3", "d": "4"})

    def test_disabled_sampler_passes_drop_through(self):
        root_result = self._build_root_sampling_result(Decision.DROP)
        self.mock_sampler.should_sample.return_value = root_result
        self.sampler.enabled = False

        actual_result = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes={"key": "value"},
            trace_state=TraceState(),
        )

        self.assertIs(actual_result, root_result)
        self.assertEqual(actual_result.decision, Decision.DROP)

    def validate_should_sample(self, root_decision: Decision, expected_decision: Decision):
        root_result: SamplingResult = self._build_root_sampling_result(root_decision)
        self.mock_sampler.should_sample.return_value = root_result
        original_attributes = {"key": root_decision.name}
        actual_result: SamplingResult = self.sampler.should_sample(
            parent_context=Context(),
            trace_id=0,
            name="name",
            kind=SpanKind.CLIENT,
            attributes=original_attributes,
            trace_state=TraceState(),
        )

        if root_decision == expected_decision:
            self.assertEqual(actual_result, root_result)
            self.assertEqual(actual_result.decision, root_decision)
        else:
            self.assertNotEqual(actual_result, root_result)
            self.assertEqual(actual_result.decision, expected_decision)

        if root_decision != Decision.DROP:
            self.assertEqual(actual_result.attributes, root_result.attributes)
        self.assertEqual(actual_result.trace_state, root_result.trace_state)

    def _build_root_sampling_result(self, sampling_decision: Decision, attributes: Attributes = None):
        sampling_attr: Attributes = attributes if attributes is not None else {"key": sampling_decision.name}
        sampling_trace_state: TraceState = TraceState()
        sampling_trace_state.add("key", sampling_decision.name)
        return SamplingResult(decision=sampling_decision, attributes=sampling_attr, trace_state=sampling_trace_state)


class TestAlwaysRecordSamplerInstrumentation(TestBase):
    def setUp(self):
        super().setUp()
        self.metric_reader = InMemoryMetricReader()
        self.meter_provider = MeterProvider(metric_readers=[self.metric_reader])

        self.span_exporter = InMemorySpanExporter()
        self.batch_processor = BatchSpanProcessor(self.span_exporter)
        self.tracer_provider = TracerProvider(
            sampler=ParentBased(ALWAYS_OFF),
            resource=Resource.create({RESOURCE_SERVICE_NAME: "test-service"}),
        )
        self.tracer_provider.add_span_processor(self.batch_processor)
        self.original_sampler = self.tracer_provider.sampler

        self.instrumentor = SpanMetricsInstrumentor()
        self.instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

        self.app = Flask(__name__)

        @self.app.route("/orders/<order_id>")
        def get_order(order_id):
            return {"status": "ok"}

        FlaskInstrumentor().instrument_app(
            self.app,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )
        self.client = self.app.test_client()

    def tearDown(self):
        FlaskInstrumentor().uninstrument_app(self.app)
        self.instrumentor.uninstrument()
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()
        super().tearDown()

    def test_sampler_is_wrapped_for_recording(self):
        self.assertIsInstance(self.tracer_provider.sampler, AlwaysRecordSampler)
        self.assertIs(self.tracer_provider.sampler._root_sampler, self.original_sampler)

    def test_all_requests_produce_metrics_and_no_exported_spans(self):
        for order_id in range(4):
            response = self.client.get(f"/orders/{order_id}")
            self.assertEqual(response.status_code, 200)

        self.assertTrue(self.batch_processor.force_flush(5000))
        self.assertEqual(self.span_exporter.get_finished_spans(), ())

        calls = self._metric_point(_SpanMetrics.CALLS_NAME, "GET /orders/<order_id>")
        self.assertEqual(calls.value, 4)
        self.assertEqual(calls.attributes[_SpanMetrics.SPAN_KIND], "SERVER")
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "UNSET")
        self.assertEqual(calls.attributes[HTTP_REQUEST_METHOD], "GET")
        self.assertEqual(calls.attributes[HTTP_RESPONSE_STATUS_CODE], 200)
        self.assertEqual(calls.attributes[HTTP_ROUTE], "/orders/<order_id>")
        self.assertEqual(calls.attributes[_SpanMetrics.SCHEMA], _SpanMetrics.SCHEMA_VERSION)

        duration = self._metric_point(_SpanMetrics.DURATION_NAME, "GET /orders/<order_id>")
        self.assertEqual(duration.count, 4)
        self.assertGreater(duration.sum, 0)

    def test_uninstrument_stops_recording_metrics(self):
        self.client.get("/orders/1")
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "GET /orders/<order_id>").value, 1)

        self.instrumentor.uninstrument()

        self.assertIs(self.tracer_provider.sampler, self.original_sampler)

        self.client.get("/orders/2")
        self.assertEqual(self._metric_point(_SpanMetrics.CALLS_NAME, "GET /orders/<order_id>").value, 1)

        tracer = self.tracer_provider.get_tracer("after_uninstrument")
        self.assertFalse(tracer.start_span("dropped_again").is_recording())

        self.assertTrue(self.batch_processor.force_flush(5000))
        self.assertEqual(self.span_exporter.get_finished_spans(), ())

    def _metric_point(self, metric_name, span_name):
        point = None
        for resource_metric in self.metric_reader.get_metrics_data().resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name != metric_name:
                        continue
                    for candidate in metric.data.data_points:
                        if candidate.attributes.get(_SpanMetrics.SPAN_NAME) == span_name:
                            self.assertIsNone(point, f"multiple {metric_name} points for {span_name!r}")
                            point = candidate
        self.assertIsNotNone(point, f"no {metric_name} point for {span_name!r}")
        return point
