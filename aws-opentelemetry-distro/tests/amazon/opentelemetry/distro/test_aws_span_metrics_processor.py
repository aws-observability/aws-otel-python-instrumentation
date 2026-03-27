# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from enum import Enum
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock, call

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_REMOTE_SERVICE
from amazon.opentelemetry.distro._aws_span_processing_util import (
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)
from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from amazon.opentelemetry.distro.metric_attribute_generator import (
    DEPENDENCY_METRIC,
    SERVICE_METRIC,
    MetricAttributeGenerator,
)
from opentelemetry.context import Context
from opentelemetry.metrics import Histogram
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, Status, StatusCode
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import INVALID_SPAN_CONTEXT, SpanContext, SpanKind
from opentelemetry.util.types import Attributes

# Test constants
_CONTAINS_ATTRIBUTES: bool = True
_CONTAINS_NO_ATTRIBUTES: bool = False
_TEST_LATENCY_MILLIS: float = 150.0
_TEST_LATENCY_NANOS: int = 150000000


class TestAwsSpanMetricsProcessor(TestCase):
    # Tests can safely rely on an empty resource
    test_resource = Resource.get_empty()

    # Useful enum for indicating expected HTTP status code-related metrics
    class ExpectedStatusMetric(Enum):
        ERROR = 1
        FAULT = 2
        NEITHER = 3

    def setUp(self):
        self.error_histogram_mock: Histogram = MagicMock()
        self.fault_histogram_mock: Histogram = MagicMock()
        self.latency_histogram_mock: Histogram = MagicMock()
        self.generator_mock: MetricAttributeGenerator = MagicMock()
        self.aws_span_metrics_processor: AwsSpanMetricsProcessor = AwsSpanMetricsProcessor(
            error_histogram=self.error_histogram_mock,
            fault_histogram=self.fault_histogram_mock,
            latency_histogram=self.latency_histogram_mock,
            generator=self.generator_mock,
            resource=self.test_resource,
        )

    def test_start_does_nothing_to_span(self):
        parent_context_mock: Context = MagicMock()
        span_mock: Span = MagicMock()
        self.aws_span_metrics_processor.on_start(span_mock, parent_context_mock)
        self.assertEqual(span_mock.mock_calls, [])
        self.assertEqual(parent_context_mock.mock_calls, [])

    def test_tear_down(self):
        self.assertIsNone(self.aws_span_metrics_processor.shutdown())
        self.assertTrue(self.aws_span_metrics_processor.force_flush())

    # Tests starting with test_on_end_metrics_generation are testing the logic in
    # AwsSpanMetricsProcessor's onEnd method pertaining to metrics generation.

    def test_on_end_metrics_generation_without_span_attributes(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 1, 0)

    def test_on_end_metrics_generation_without_metrics_attributes(self):
        span_attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: 500}
        span: ReadableSpan = _build_readable_span_mock(span_attributes)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_NO_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 0, 0)

    def test_on_end_metrics_generation_local_root_server_span(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.SERVER, INVALID_SPAN_CONTEXT)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 1, 0)

    def test_on_end_metrics_generation_local_root_consumer_span(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.CONSUMER, INVALID_SPAN_CONTEXT)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 1, 1)

    def test_on_end_metrics_generation_local_root_client_span(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.CLIENT, INVALID_SPAN_CONTEXT)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 1, 1)

    def test_on_end_metrics_generation_local_root_producer_span(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.PRODUCER, INVALID_SPAN_CONTEXT)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 1, 1)

    def test_on_end_metrics_generation_local_root_internal_span(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.INTERNAL, INVALID_SPAN_CONTEXT)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 1, 0)

    def test_on_end_metrics_generation_local_root_producer_span_without_metric_attributes(self):
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.PRODUCER, INVALID_SPAN_CONTEXT)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_NO_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 0, 0)

    def test_on_end_metrics_generation_client_span(self):
        span_context = SpanContext(1, 1, False)
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.CLIENT, span_context)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 0, 1)

    def test_on_end_metrics_generation_producer_span(self):
        span_context = SpanContext(1, 1, False)
        span_attributes: Attributes = _build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = _build_readable_span_mock(span_attributes, SpanKind.PRODUCER, span_context)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 0, 1)

    def test_on_end_metrics_generation_without_end_required(self):
        span_attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: 500}
        span: ReadableSpan = _build_readable_span_mock(span_attributes)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)

        self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
        self.fault_histogram_mock.assert_has_calls([call.record(1, metric_attributes_dict.get(SERVICE_METRIC))])
        self.latency_histogram_mock.assert_has_calls(
            [call.record(_TEST_LATENCY_MILLIS, metric_attributes_dict.get(SERVICE_METRIC))]
        )

        self.error_histogram_mock.record.assert_called_once()
        self.fault_histogram_mock.record.assert_called_once()
        self.latency_histogram_mock.record.assert_called_once()

    def test_on_end_metrics_generation_with_latency(self):
        span_attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: 200}
        span: ReadableSpan = _build_readable_span_mock(span_attributes)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        # To Mock a latency of 5500000 Nano Second / 5.5ms
        span.start_time = 0
        span.end_time = 5500000

        self.aws_span_metrics_processor.on_end(span)

        self.error_histogram_mock.record.assert_called_once()
        self.fault_histogram_mock.record.assert_called_once()
        self.latency_histogram_mock.record.assert_called_once()

        self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
        self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
        self.latency_histogram_mock.assert_has_calls([call.record(5.5, metric_attributes_dict.get(SERVICE_METRIC))])

    def test_on_end_metrics_generation_with_http_status_codes(self):
        # INVALID HTTP STATUS CODE
        self._validate_metrics_generated_for_http_status_code(None, self.ExpectedStatusMetric.NEITHER)

        # VALID HTTP STATUS CODE
        self._validate_metrics_generated_for_http_status_code(200, self.ExpectedStatusMetric.NEITHER)
        self._validate_metrics_generated_for_http_status_code(399, self.ExpectedStatusMetric.NEITHER)
        self._validate_metrics_generated_for_http_status_code(400, self.ExpectedStatusMetric.ERROR)
        self._validate_metrics_generated_for_http_status_code(499, self.ExpectedStatusMetric.ERROR)
        self._validate_metrics_generated_for_http_status_code(500, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_http_status_code(599, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_http_status_code(600, self.ExpectedStatusMetric.NEITHER)

    def test_on_end_metrics_generation_with_status_data_error(self):
        # INVALID HTTP STATUS CODE
        self._validate_metrics_generated_for_status_data_error(None, self.ExpectedStatusMetric.FAULT)

        # VALID HTTP STATUS CODE
        self._validate_metrics_generated_for_status_data_error(200, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_error(399, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_error(400, self.ExpectedStatusMetric.ERROR)
        self._validate_metrics_generated_for_status_data_error(499, self.ExpectedStatusMetric.ERROR)
        self._validate_metrics_generated_for_status_data_error(500, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_error(599, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_error(600, self.ExpectedStatusMetric.FAULT)

    def test_on_end_metrics_generation_with_status_data_ok(self):
        # Empty Status and HTTP with OK status
        self._validate_metrics_generated_for_status_data_ok(None, self.ExpectedStatusMetric.NEITHER)

        # Valid HTTP with OK Status
        self._validate_metrics_generated_for_status_data_ok(200, self.ExpectedStatusMetric.NEITHER)
        self._validate_metrics_generated_for_status_data_ok(399, self.ExpectedStatusMetric.NEITHER)
        self._validate_metrics_generated_for_status_data_ok(400, self.ExpectedStatusMetric.ERROR)
        self._validate_metrics_generated_for_status_data_ok(499, self.ExpectedStatusMetric.ERROR)
        self._validate_metrics_generated_for_status_data_ok(500, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_ok(599, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_ok(599, self.ExpectedStatusMetric.FAULT)
        self._validate_metrics_generated_for_status_data_ok(600, self.ExpectedStatusMetric.NEITHER)

    def test_on_end_metrics_generation_from_ec2_metadata_api(self):
        span_attributes: Attributes = {AWS_REMOTE_SERVICE: "169.254.169.254"}
        span: ReadableSpan = _build_readable_span_mock(span_attributes)
        metric_attributes_dict = _build_ec2_metadata_api_metric_attributes()
        self._configure_mock_for_on_end(span, metric_attributes_dict)

        self.aws_span_metrics_processor.on_end(span)
        self._verify_histogram_record(metric_attributes_dict, 0, 0)

    def _configure_mock_for_on_end(self, span: Span, attribute_map: {str: Attributes}):
        def generate_m_a_from_span_side_effect(input_span, resource):
            if input_span == span and resource == self.test_resource:
                return attribute_map
            return None

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = generate_m_a_from_span_side_effect

    def _verify_histogram_record(
        self,
        metric_attributes_dict: {str: Attributes},
        wanted_service_metric_invocation: int,
        wanted_dependency_metric_invocation: int,
    ):
        service_metric_calls = [call.record(0, metric_attributes_dict.get(SERVICE_METRIC))]
        dependency_metric_calls = [call.record(0, metric_attributes_dict.get(DEPENDENCY_METRIC))]
        service_metric_latency_calls = [call.record(_TEST_LATENCY_MILLIS, metric_attributes_dict.get(SERVICE_METRIC))]
        dependency_metric_latency_calls = [
            call.record(_TEST_LATENCY_MILLIS, metric_attributes_dict.get(SERVICE_METRIC))
        ]

        self.error_histogram_mock.assert_has_calls(service_metric_calls * wanted_service_metric_invocation)
        self.fault_histogram_mock.assert_has_calls(service_metric_calls * wanted_service_metric_invocation)
        self.latency_histogram_mock.assert_has_calls(service_metric_latency_calls * wanted_service_metric_invocation)

        self.error_histogram_mock.assert_has_calls(dependency_metric_calls * wanted_dependency_metric_invocation)
        self.fault_histogram_mock.assert_has_calls(dependency_metric_calls * wanted_dependency_metric_invocation)
        self.latency_histogram_mock.assert_has_calls(dependency_metric_latency_calls * wanted_service_metric_invocation)

        total_wanted_invocations: int = wanted_service_metric_invocation + wanted_dependency_metric_invocation
        self.assertEqual(self.error_histogram_mock.record.call_count, total_wanted_invocations)
        self.assertEqual(self.fault_histogram_mock.record.call_count, total_wanted_invocations)
        self.assertEqual(self.latency_histogram_mock.record.call_count, total_wanted_invocations)

    def _validate_metrics_generated_for_status_data_error(
        self, http_status_code, expected_status_metric: ExpectedStatusMetric
    ):
        attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: http_status_code}
        span: ReadableSpan = _build_readable_span_mock(attributes, SpanKind.PRODUCER, None, Status(StatusCode.ERROR))
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)

        self._configure_mock_for_on_end(span, metric_attributes_dict)
        self.aws_span_metrics_processor.on_end(span)
        self._valid_metrics(metric_attributes_dict, expected_status_metric)

    def _validate_metrics_generated_for_status_data_ok(
        self, http_status_code, expected_status_metric: ExpectedStatusMetric
    ):
        attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: http_status_code}
        span: ReadableSpan = _build_readable_span_mock(attributes, SpanKind.PRODUCER, None, Status(StatusCode.OK))
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)

        self._configure_mock_for_on_end(span, metric_attributes_dict)
        self.aws_span_metrics_processor.on_end(span)
        self._valid_metrics(metric_attributes_dict, expected_status_metric)

    def _validate_metrics_generated_for_http_status_code(
        self, http_status_code, expected_status_metric: ExpectedStatusMetric
    ):
        attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: http_status_code}
        span: ReadableSpan = _build_readable_span_mock(attributes, SpanKind.PRODUCER)
        metric_attributes_dict = _build_metric_attributes(_CONTAINS_ATTRIBUTES, span)

        self._configure_mock_for_on_end(span, metric_attributes_dict)
        self.aws_span_metrics_processor.on_end(span)
        self._valid_metrics(metric_attributes_dict, expected_status_metric)

    def _valid_metrics(self, metric_attributes_dict, expected_status_metric: ExpectedStatusMetric):
        if expected_status_metric == self.ExpectedStatusMetric.ERROR:
            self.error_histogram_mock.assert_has_calls([call.record(1, metric_attributes_dict.get(SERVICE_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
            self.error_histogram_mock.assert_has_calls([call.record(1, metric_attributes_dict.get(DEPENDENCY_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(DEPENDENCY_METRIC))])

        if expected_status_metric == self.ExpectedStatusMetric.FAULT:
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(1, metric_attributes_dict.get(SERVICE_METRIC))])
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(DEPENDENCY_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(1, metric_attributes_dict.get(DEPENDENCY_METRIC))])

        if expected_status_metric == self.ExpectedStatusMetric.NEITHER:
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(SERVICE_METRIC))])
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(DEPENDENCY_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_dict.get(DEPENDENCY_METRIC))])

        self.latency_histogram_mock.assert_has_calls(
            [
                call.record(_TEST_LATENCY_MILLIS, metric_attributes_dict.get(SERVICE_METRIC)),
                call.record(_TEST_LATENCY_MILLIS, metric_attributes_dict.get(DEPENDENCY_METRIC)),
            ]
        )

        self.error_histogram_mock.reset_mock()
        self.fault_histogram_mock.reset_mock()
        self.latency_histogram_mock.reset_mock()


def _build_span_attributes(contains_attribute: bool) -> Attributes:
    attribute: Attributes = {}
    if contains_attribute:
        attribute = {"original key": "original value"}
    return attribute


def _build_readable_span_mock(
    span_attributes: Attributes,
    span_kind: Optional[SpanKind] = SpanKind.SERVER,
    parent_span_context: Optional[SpanContext] = None,
    status_data: Optional[Status] = Status(status_code=StatusCode.UNSET),
) -> ReadableSpan:
    mock_span: ReadableSpan = MagicMock()
    mock_span.instrumentation_scope = InstrumentationScope("aws-sdk", "version")
    mock_span.attributes = span_attributes
    mock_span.kind = span_kind
    mock_span.parent = parent_span_context
    mock_span.status = status_data

    # Simulate Latency
    mock_span.start_time = 0
    mock_span.end_time = 0 + _TEST_LATENCY_NANOS

    return mock_span


def _build_metric_attributes(contain_attributes: bool, span: Span) -> Attributes:
    attribute_map: Attributes = {}
    if contain_attributes:
        if should_generate_service_metric_attributes(span):
            attributes = {"new service key": "new service value"}
            attribute_map[SERVICE_METRIC] = attributes
        if should_generate_dependency_metric_attributes(span):
            attributes = {"new dependency key": "new dependency value"}
            attribute_map[DEPENDENCY_METRIC] = attributes
    return attribute_map


def _build_ec2_metadata_api_metric_attributes() -> Attributes:
    attribute_map: Attributes = {}
    attributes = {AWS_REMOTE_SERVICE: "169.254.169.254"}
    attribute_map[DEPENDENCY_METRIC] = attributes
    return attribute_map
