# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock, call

from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.util.types import Attributes
from opentelemetry.trace import SpanKind, SpanContext

from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from opentelemetry.metrics import Histogram
from opentelemetry.sdk.trace import Span, Status, StatusCode, ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationScope

from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator, SERVICE_METRIC, \
    DEPENDENCY_METRIC
from amazon.opentelemetry.distro._aws_span_processing_util import (
    LOCAL_ROOT,
    UNKNOWN_OPERATION,
    UNKNOWN_REMOTE_OPERATION,
    UNKNOWN_REMOTE_SERVICE,
    UNKNOWN_SERVICE,
    extract_api_path_value,
    get_egress_operation,
    get_ingress_operation,
    is_key_present,
    is_local_root,
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)


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
            resource=self.test_resource
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


    def __build_span_attributes(self, contains_attribute):
        attribute: Attributes = {}
        if contains_attribute:
            attribute = {"original key": "original value"}
        return attribute

    def __build_readable_span_mock(self, span_attributes: Attributes,
                                   span_kind: SpanKind | None = SpanKind.SERVER,
                                   parent_span_context: SpanContext | None = None,
                                   status_data: Status| None = Status(status_code=StatusCode.UNSET)):
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
                attribute_map = {SERVICE_METRIC: attributes}
            if should_generate_dependency_metric_attributes(span):
                attributes = {"new dependency key": "new dependency value"}
                attribute_map = {DEPENDENCY_METRIC: attributes}
        return attribute_map

    def __configure_mock_for_on_end(self, span: Span, attribute_map: {str: Attributes}):
        def generate_m_a_from_span_side_effect(input_span, resource):
            if input_span == span and resource == self.test_resource:
                return attribute_map
            return None
        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = generate_m_a_from_span_side_effect

    def __verify_histogram_record(self,
                                  metric_attributes_map: {str: Attributes},
                                  wanted_service_metric_invocation: int,
                                  wanted_dependency_metric_invocation: int):
        service_metric_calls = [
            call.record(0, metric_attributes_map.get(SERVICE_METRIC))
        ]
        dependency_metric_calls = [
            call.record(0, metric_attributes_map.get(DEPENDENCY_METRIC))
        ]

        self.error_histogram_mock.assert_has_calls(service_metric_calls * wanted_service_metric_invocation)
        self.fault_histogram_mock.assert_has_calls(service_metric_calls * wanted_service_metric_invocation)

        self.error_histogram_mock.assert_has_calls(dependency_metric_calls * wanted_dependency_metric_invocation)
        self.fault_histogram_mock.assert_has_calls(dependency_metric_calls * wanted_dependency_metric_invocation)

