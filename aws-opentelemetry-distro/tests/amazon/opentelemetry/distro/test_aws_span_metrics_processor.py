# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock, call

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
from opentelemetry.metrics import Histogram
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, Status, StatusCode
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.util.types import Attributes


class TestAwsSpanMetricsProcessor(TestCase):
    # Test constants
    CONTAINS_ATTRIBUTES = True
    CONTAINS_NO_ATTRIBUTES = False
    TEST_LATENCY_MILLIS = 150.0
    TEST_LATENCY_NANOS = 150000000

    # Resource is not mockable, but tests can safely rely on an empty resource.
    # Assuming Resource is a class from a library you're using
    test_resource = Resource.get_empty()

    # Useful enum for indicating expected HTTP status code-related metrics
    from enum import Enum

    class ExpectedStatusMetric(Enum):
        ERROR = 1
        FAULT = 2
        NEITHER = 3

    def setUp(self):
        self.error_histogram_mock: Histogram = MagicMock()
        self.fault_histogram_mock: Histogram = MagicMock()
        self.late_histogram_mock: Histogram = MagicMock()
        self.generator_mock: MetricAttributeGenerator = MagicMock()
        self.aws_span_metrics_processor: AwsSpanMetricsProcessor = AwsSpanMetricsProcessor(
            error_histogram=self.error_histogram_mock,
            fault_histogram=self.fault_histogram_mock,
            latency_histogram=self.late_histogram_mock,
            generator=self.generator_mock,
            resource=self.test_resource,
        )

    def test_start_does_nothing_to_span(self):
        parent_context_mock: Span = MagicMock()
        span_mock: Span = MagicMock()
        self.aws_span_metrics_processor.on_start(parent_context_mock, span_mock)
        self.assertNotEqual(span_mock.parent, parent_context_mock)

    def test_tear_down(self):
        self.assertIsNone(self.aws_span_metrics_processor.shutdown())
        self.assertTrue(self.aws_span_metrics_processor.force_flush())

    def test_on_end_metrics_generation_without_span_attributes(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 1, 0)

    def test_on_end_metrics_generation_without_metrics_attributes(self):
        span_attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: 500}
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 0, 0)

    def test_on_end_metrics_generation_local_root_server_span(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.SERVER)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 1, 0)

    def test_on_end_metrics_generation_local_root_consumer_span(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.CONSUMER)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 1, 1)

    def test_on_end_metrics_generation_local_root_client_span(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.CLIENT)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 1, 1)

    def test_on_end_metrics_generation_local_root_producer_span(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.PRODUCER)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 1, 1)

    def test_on_end_metrics_generation_local_root_internal_span(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.INTERNAL)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 1, 0)

    def test_on_end_metrics_generation_local_root_producer_span_without_metric_attributes(self):
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.PRODUCER)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 0, 0)

    def test_on_end_metrics_generation_client_span(self):
        mock_span_context = MagicMock()
        mock_span_context.is_valid.return_value = True
        mock_span_context.is_remote.return_value = False
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.CLIENT, mock_span_context)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 0, 1)

    def test_on_end_metrics_generation_producer_span(self):
        mock_span_context = MagicMock()
        mock_span_context.is_valid.return_value = True
        mock_span_context.is_remote.return_value = False
        span_attributes: Attributes = self.__build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes, SpanKind.PRODUCER, mock_span_context)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)
        self.__verify_histogram_record(metric_attributes_map, 0, 1)

    def test_on_end_metrics_generation_without_end_required(self):
        span_attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: 500}
        span: ReadableSpan = self.__build_readable_span_mock(span_attributes)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        self.__configure_mock_for_on_end(span, metric_attributes_map)

        self.aws_span_metrics_processor.on_end(span)

        self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(SERVICE_METRIC))])
        self.fault_histogram_mock.assert_has_calls([call.record(1, metric_attributes_map.get(SERVICE_METRIC))])

        self.error_histogram_mock.assert_has_calls([])
        self.fault_histogram_mock.assert_has_calls([])

    def test_on_end_metrics_generation_with_aws_status_codes(self):
        self.__validate_metrics_generated_for_attributes_status_code(None, self.ExpectedStatusMetric.NEITHER)

        self.__validate_metrics_generated_for_attributes_status_code(399, self.ExpectedStatusMetric.NEITHER)
        self.__validate_metrics_generated_for_attributes_status_code(400, self.ExpectedStatusMetric.ERROR)
        self.__validate_metrics_generated_for_attributes_status_code(499, self.ExpectedStatusMetric.ERROR)
        self.__validate_metrics_generated_for_attributes_status_code(500, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_attributes_status_code(599, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_attributes_status_code(600, self.ExpectedStatusMetric.NEITHER)

    def test_on_end_metrics_generation_with_http_status_codes(self):
        self.__validate_metrics_generated_for_http_status_code(None, self.ExpectedStatusMetric.NEITHER)

        self.__validate_metrics_generated_for_http_status_code(200, self.ExpectedStatusMetric.NEITHER)
        self.__validate_metrics_generated_for_http_status_code(399, self.ExpectedStatusMetric.NEITHER)
        self.__validate_metrics_generated_for_http_status_code(400, self.ExpectedStatusMetric.ERROR)
        self.__validate_metrics_generated_for_http_status_code(499, self.ExpectedStatusMetric.ERROR)
        self.__validate_metrics_generated_for_http_status_code(500, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_http_status_code(599, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_http_status_code(600, self.ExpectedStatusMetric.NEITHER)

    def test_on_end_metrics_generation_with_status_data_error(self):
        self.__validate_metrics_generated_for_status_data_error(None, self.ExpectedStatusMetric.FAULT)

        self.__validate_metrics_generated_for_status_data_error(200, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_status_data_error(399, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_status_data_error(400, self.ExpectedStatusMetric.ERROR)
        self.__validate_metrics_generated_for_status_data_error(499, self.ExpectedStatusMetric.ERROR)
        self.__validate_metrics_generated_for_status_data_error(500, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_status_data_error(599, self.ExpectedStatusMetric.FAULT)
        self.__validate_metrics_generated_for_status_data_error(600, self.ExpectedStatusMetric.FAULT)

    def __build_span_attributes(self, contains_attribute):
        attribute: Attributes = {}
        if contains_attribute:
            attribute = {"original key": "original value"}
        return attribute

    def __build_readable_span_mock(
        self,
        span_attributes: Attributes,
        span_kind: Optional[SpanKind] = SpanKind.SERVER,
        parent_span_context: Optional[SpanContext] = None,
        status_data: Optional[Status] = Status(status_code=StatusCode.UNSET),
    ):
        mock_span_data: ReadableSpan = MagicMock()
        mock_span_data.instrumentation_scope = InstrumentationScope("aws-sdk", "version")
        mock_span_data.attributes = span_attributes
        mock_span_data.kind = span_kind
        mock_span_data.parent = parent_span_context
        mock_span_data.status = status_data
        return mock_span_data

    def __build_metric_attributes(self, contain_attributes: bool, span: Span):
        attribute_map: Attributes = {}
        if contain_attributes:
            if should_generate_service_metric_attributes(span):
                attributes = {"new service key": "new service value"}
                attribute_map[SERVICE_METRIC] = attributes
            if should_generate_dependency_metric_attributes(span):
                attributes = {"new dependency key": "new dependency value"}
                attribute_map[DEPENDENCY_METRIC] = attributes
        return attribute_map

    def __configure_mock_for_on_end(self, span: Span, attribute_map: {str: Attributes}):
        def generate_m_a_from_span_side_effect(input_span, resource):
            if input_span == span and resource == self.test_resource:
                return attribute_map
            return None

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = generate_m_a_from_span_side_effect

    def __verify_histogram_record(
        self,
        metric_attributes_map: {str: Attributes},
        wanted_service_metric_invocation: int,
        wanted_dependency_metric_invocation: int,
    ):
        service_metric_calls = [call.record(0, metric_attributes_map.get(SERVICE_METRIC))]
        dependency_metric_calls = [call.record(0, metric_attributes_map.get(DEPENDENCY_METRIC))]

        self.error_histogram_mock.assert_has_calls(service_metric_calls * wanted_service_metric_invocation)
        self.fault_histogram_mock.assert_has_calls(service_metric_calls * wanted_service_metric_invocation)

        self.error_histogram_mock.assert_has_calls(dependency_metric_calls * wanted_dependency_metric_invocation)
        self.fault_histogram_mock.assert_has_calls(dependency_metric_calls * wanted_dependency_metric_invocation)

    def __validate_metrics_generated_for_status_data_error(
        self, http_status_code, expected_status_metric: ExpectedStatusMetric
    ):
        attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: http_status_code}
        span: ReadableSpan = self.__build_readable_span_mock(
            attributes, SpanKind.PRODUCER, None, Status(StatusCode.ERROR)
        )
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)

        self.__configure_mock_for_on_end(span, metric_attributes_map)
        self.aws_span_metrics_processor.on_end(span)
        self.__valid_metrics(metric_attributes_map, expected_status_metric)

    def __validate_metrics_generated_for_http_status_code(
        self, http_status_code, expected_status_metric: ExpectedStatusMetric
    ):
        attributes: Attributes = {SpanAttributes.HTTP_STATUS_CODE: http_status_code}
        span: ReadableSpan = self.__build_readable_span_mock(attributes, SpanKind.PRODUCER)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)

        self.__configure_mock_for_on_end(span, metric_attributes_map)
        self.aws_span_metrics_processor.on_end(span)
        self.__valid_metrics(metric_attributes_map, expected_status_metric)

    def __validate_metrics_generated_for_attributes_status_code(
        self, aws_status_code, expected_status_metric: ExpectedStatusMetric
    ):
        attributes: Attributes = {"new key": "new value"}
        span: ReadableSpan = self.__build_readable_span_mock(attributes, SpanKind.PRODUCER)
        metric_attributes_map = self.__build_metric_attributes(self.CONTAINS_ATTRIBUTES, span)
        if aws_status_code is not None:
            attr_temp_service = {
                "new service key": "new service value",
                SpanAttributes.HTTP_STATUS_CODE: aws_status_code,
            }
            metric_attributes_map[SERVICE_METRIC] = attr_temp_service
            attr_temp_dependency = {
                "new dependency key": "new dependency value",
                SpanAttributes.HTTP_STATUS_CODE: aws_status_code,
            }
            metric_attributes_map[DEPENDENCY_METRIC] = attr_temp_dependency
        self.__configure_mock_for_on_end(span, metric_attributes_map)
        self.aws_span_metrics_processor.on_end(span)
        self.__valid_metrics(metric_attributes_map, expected_status_metric)

    def __valid_metrics(self, metric_attributes_map, expected_status_metric: ExpectedStatusMetric):

        if expected_status_metric == self.ExpectedStatusMetric.ERROR:
            self.error_histogram_mock.assert_has_calls([call.record(1, metric_attributes_map.get(SERVICE_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(SERVICE_METRIC))])
            self.error_histogram_mock.assert_has_calls([call.record(1, metric_attributes_map.get(DEPENDENCY_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(DEPENDENCY_METRIC))])

        if expected_status_metric == self.ExpectedStatusMetric.FAULT:
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(SERVICE_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(1, metric_attributes_map.get(SERVICE_METRIC))])
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(DEPENDENCY_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(1, metric_attributes_map.get(DEPENDENCY_METRIC))])

        if expected_status_metric == self.ExpectedStatusMetric.NEITHER:
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(SERVICE_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(SERVICE_METRIC))])
            self.error_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(DEPENDENCY_METRIC))])
            self.fault_histogram_mock.assert_has_calls([call.record(0, metric_attributes_map.get(DEPENDENCY_METRIC))])
