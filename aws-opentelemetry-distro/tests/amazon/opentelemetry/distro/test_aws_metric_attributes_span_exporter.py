# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import copy
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock, call, Mock

from opentelemetry.attributes import BoundedAttributes

from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator, \
    _generate_service_metric_attributes, _generate_dependency_metric_attributes
from amazon.opentelemetry.distro._aws_span_processing_util import should_generate_dependency_metric_attributes, \
    should_generate_service_metric_attributes
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from amazon.opentelemetry.distro.metric_attribute_generator import (
    DEPENDENCY_METRIC,
    SERVICE_METRIC,
    MetricAttributeGenerator,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import SpanKind, SpanContext, Status, StatusCode
from opentelemetry.util.types import Attributes


def build_span_attributes(contains_attribute):
    attribute: Attributes = {}
    if contains_attribute:
        attribute = {"original key": "original value"}
    return attribute


def build_metric_attributes(contains_attribute):
    attribute: Attributes = {}
    if contains_attribute:
        attribute["new key"] = "new value"
    return attribute


def build_readable_span_mock(span_attributes: Attributes):
    mock_span_data: ReadableSpan = MagicMock()
    mock_span_data._attributes = span_attributes
    mock_span_data._kind = SpanKind.SERVER
    mock_span_data._parent = None
    mock_span_data.attributes = mock_span_data._attributes
    return mock_span_data


class TestAwsMetricAttributesSpanExporter(TestCase):
    CONTAINS_ATTRIBUTES = True
    CONTAINS_NO_ATTRIBUTES = False

    def setUp(self):
        self.delegate_mock: ConsoleSpanExporter = MagicMock()
        self.generator_mock: _AwsMetricAttributeGenerator = MagicMock()
        self.test_resource: Resource = Resource.get_empty()
        self.aws_metric_attributes_span_exporter: AwsMetricAttributesSpanExporter = AwsMetricAttributesSpanExporter(
            self.delegate_mock, self.generator_mock, self.test_resource
        )

    def test_pass_through_delegations(self):
        self.aws_metric_attributes_span_exporter.force_flush()
        self.aws_metric_attributes_span_exporter.shutdown()
        self.delegate_mock.assert_has_calls([call.force_flush(30000), call.shutdown()])

    def test_export_delegation_without_attributes_or_modification(self):
        span_attributes: Attributes = build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span_data_mock: ReadableSpan = build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES)
        self.__configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])

    def test_export_delegation_with_attributes_but_without_modification(self):
        span_attributes: Attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock: ReadableSpan = build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES)
        self.__configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])

    def test_export_delegation_without_attributes_but_with_modification(self):
        span_attributes: Attributes = build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span_data_mock: ReadableSpan = build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = build_metric_attributes(self.CONTAINS_ATTRIBUTES)
        self.__configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args = self.delegate_mock.export.call_args[0][0]
        exported_span = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        exported_attributes = exported_span._attributes
        self.assertEquals(len(exported_attributes), len(metric_attributes))
        for key, value in metric_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)

    def test_export_delegation_with_attributes_and_modification(self):
        span_attributes: Attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock: ReadableSpan = build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = build_metric_attributes(self.CONTAINS_ATTRIBUTES)
        self.__configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args = self.delegate_mock.export.call_args[0][0]
        exported_span = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        for key, value in metric_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)
        for key, value in span_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)

    def test_export_delegation_with_multiple_spans(self):
        span_attributes1: Attributes = build_span_attributes(self.CONTAINS_NO_ATTRIBUTES)
        span_data_mock1: ReadableSpan = build_readable_span_mock(span_attributes1)
        metric_attributes1: Attributes = build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES)


        span_attributes2: Attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock2: ReadableSpan = build_readable_span_mock(span_attributes2)
        metric_attributes2: Attributes = build_metric_attributes(self.CONTAINS_ATTRIBUTES)

        span_attributes3: Attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock3: ReadableSpan = build_readable_span_mock(span_attributes3)
        metric_attributes3: Attributes = build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES)

        self.__configure_mock_for_export_with_multiple_side_effect([span_data_mock1, span_data_mock2, span_data_mock3],
                                                                   [metric_attributes1, metric_attributes2, metric_attributes3])

        self.aws_metric_attributes_span_exporter.export([span_data_mock1, span_data_mock2, span_data_mock3])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock1, span_data_mock2, span_data_mock3])])
        exported_spans_args = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans_args), 3)

        exported_span1 = exported_spans_args[0]
        exported_span2 = exported_spans_args[1]
        exported_span3 = exported_spans_args[2]
        self.assertEqual(exported_span1, span_data_mock1)
        self.assertEqual(exported_span3, span_data_mock3)

        expected_attribute_count = len(metric_attributes2) + len(span_attributes2)
        self.assertEqual(len(exported_span2._attributes), expected_attribute_count)
        for key, value in metric_attributes2.items():
            self.assertEqual(exported_span2._attributes[key], value)
        for key, value in span_attributes2.items():
            self.assertEqual(exported_span2._attributes[key], value)

    def test_overridden_attributes(self):
        span_attributes = {
            "key1": "old value1",
            "key2": "old value2",
        }
        span_data_mock = build_readable_span_mock(span_attributes)
        metric_attributes = {
            "key1": "new value1",
            "key3": "new value3",
        }
        self.__configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args = self.delegate_mock.export.call_args[0][0]
        exported_span = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        # 验证导出的Span属性
        self.assertEqual(exported_span._attributes["key1"], "new value1")
        self.assertEqual(exported_span._attributes["key2"], "old value2")
        self.assertEqual(exported_span._attributes["key3"], "new value3")


    def __configure_mock_for_export(self, span_data_mock: ReadableSpan, metric_attributes: Attributes):
        attribute_map: Attributes = {}
        if should_generate_service_metric_attributes(span_data_mock):
            attribute_map[SERVICE_METRIC] = copy.deepcopy(metric_attributes)
        if should_generate_dependency_metric_attributes(span_data_mock):
            attribute_map[DEPENDENCY_METRIC] = copy.deepcopy(metric_attributes)
        print(should_generate_service_metric_attributes(span_data_mock))
        print(should_generate_dependency_metric_attributes(span_data_mock))

        def generate_metric_attribute_map_side_effect(span, resource):
            if span == span_data_mock and resource == self.test_resource:
                return attribute_map
            return {}

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = generate_metric_attribute_map_side_effect

    ## Since the side_effect design of the python cause reference issue, make another helper that allow multiple span && attr pair

    def __configure_mock_for_export_with_multiple_side_effect(self, span_data_mocks: [ReadableSpan], metric_attributes_list: [Attributes]):
        attributes_map_list = []
        for span in span_data_mocks:
            attribute_map: Attributes = {}
            if should_generate_service_metric_attributes(span):
                attribute_map[SERVICE_METRIC] = copy.deepcopy(metric_attributes_list[span_data_mocks.index(span)])
            if should_generate_dependency_metric_attributes(span):
                attribute_map[DEPENDENCY_METRIC] = copy.deepcopy(metric_attributes_list[span_data_mocks.index(span)])
            print(should_generate_service_metric_attributes(span))
            print(should_generate_dependency_metric_attributes(span))
            attributes_map_list.append(attribute_map)

        def side_effect(span, resource):
            if span in span_data_mocks and resource == self.test_resource:
                return attributes_map_list[span_data_mocks.index(span)]
            else:
                return {}

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = side_effect
