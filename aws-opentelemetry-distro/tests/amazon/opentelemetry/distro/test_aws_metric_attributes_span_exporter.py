# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import copy
from unittest import TestCase
from unittest.mock import MagicMock, call, Mock

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.trace import SpanKind
from opentelemetry.semconv.trace import SpanAttributes, MessagingOperationValues
from opentelemetry.util.types import Attributes

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_SPAN_KIND, AWS_CONSUMER_PARENT_SPAN_KIND
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro._aws_span_processing_util import should_generate_dependency_metric_attributes, \
    should_generate_service_metric_attributes
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from amazon.opentelemetry.distro.metric_attribute_generator import (
    DEPENDENCY_METRIC,
    SERVICE_METRIC,
)


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
        self.assertEqual(exported_span._attributes["key1"], "new value1")
        self.assertEqual(exported_span._attributes["key2"], "old value2")
        self.assertEqual(exported_span._attributes["key3"], "new value3")

    def test_export_delegating_span_data_behaviour(self):
        span_attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock = build_readable_span_mock(span_attributes)
        metric_attributes = build_metric_attributes(self.CONTAINS_ATTRIBUTES)
        self.__configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        exported_span = exported_spans[0]

        span_context_mock = Mock()
        span_data_mock.get_span_context.return_value = span_context_mock
        self.assertEqual(exported_span.get_span_context(), span_context_mock)

        parent_span_context_mock = Mock()
        span_data_mock.parent = parent_span_context_mock
        self.assertEqual(exported_span.parent, parent_span_context_mock)

        span_data_mock.resource = self.test_resource
        self.assertEqual(exported_span.resource, self.test_resource)

        test_instrumentation_scope_info = MagicMock()
        span_data_mock.instrumentation_scope = test_instrumentation_scope_info
        self.assertEqual(exported_span.instrumentation_scope, test_instrumentation_scope_info)

        test_name = "name"
        span_data_mock.name = test_name
        self.assertEqual(exported_span.name, test_name)

        kind_mock = Mock()
        span_data_mock.kind = kind_mock
        self.assertEqual(exported_span.kind, kind_mock)

        events_mock = [Mock()]
        span_data_mock.events = events_mock
        self.assertEqual(exported_span.events, events_mock)

        links_mock = [Mock()]
        span_data_mock.links = links_mock
        self.assertEqual(exported_span.links, links_mock)

        status_mock = Mock()
        span_data_mock.status = status_mock
        self.assertEqual(exported_span.status, status_mock)

    def test_export_delegation_with_two_metrics(self):
        span_attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)

        span_data_mock = MagicMock()
        span_data_mock._attributes = span_attributes
        span_data_mock.kind = SpanKind.PRODUCER
        span_data_mock.parent_span_context = None
        span_data_mock.attributes = span_attributes

        dependency_metric: BoundedAttributes = BoundedAttributes(attributes={"new dependency key": "new dependency value", AWS_SPAN_KIND: SpanKind.PRODUCER})

        attribute_map = {
            SERVICE_METRIC: {"new service key": "new service value"},
            DEPENDENCY_METRIC: dependency_metric
        }

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = lambda span, resource: attribute_map if span == span_data_mock and resource == self.test_resource else {}

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        exported_span = exported_spans[0]

        # Check the number of attributes and specific attributes
        expected_attribute_count = sum(len(attrs) for attrs in attribute_map.values()) + len(span_attributes)
        self.assertEqual(len(exported_span._attributes), expected_attribute_count)

    def test_consumer_process_span_has_empty_attribute(self):
        attributes_mock: Attributes = {}
        span_data_mock: ReadableSpan = MagicMock()
        parent_span_context_mock: ReadableSpan = MagicMock()

        attributes_mock[AWS_CONSUMER_PARENT_SPAN_KIND] = SpanKind.CONSUMER
        attributes_mock[SpanAttributes.MESSAGING_OPERATION] = MessagingOperationValues.PROCESS
        span_data_mock.kind = SpanKind.CONSUMER
        span_data_mock._attributes = attributes_mock
        span_data_mock.parent_span_context = parent_span_context_mock
        parent_span_context_mock.is_valid.return_value = True
        parent_span_context_mock.is_remote.return_value = False

        dependency_metric: BoundedAttributes = MagicMock()
        dependency_metric.attributes = {}
        dependency_metric.maxlen = None

        attribute_map = {DEPENDENCY_METRIC: dependency_metric}
        self.generator_mock.generate_metric_attributes_dict_from_span.return_value = attribute_map

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        attribute_map[DEPENDENCY_METRIC].assert_has_calls([])

        exported_span = exported_spans[0]
        self.assertEqual(exported_span, span_data_mock)

    def test_export_delegation_with_dependency_metrics(self):
        span_attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock = Mock()
        span_context_mock = Mock()
        span_context_mock.is_remote.return_value = False
        span_context_mock.is_valid.return_value = True
        span_data_mock.attributes = span_attributes
        span_data_mock.kind = SpanKind.PRODUCER
        span_data_mock.parent_span_context = span_context_mock

        dependency_metric: BoundedAttributes = MagicMock()
        dependency_metric.attributes = {"new service key": "new dependency value"}
        dependency_metric.maxlen = None

        attribute_map = {DEPENDENCY_METRIC: dependency_metric}
        self.generator_mock.generate_metric_attributes_dict_from_span.return_value = attribute_map

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.export.assert_has_calls([call.export([span_data_mock])])
        exported_spans = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        exported_span = exported_spans[0]

        expected_attribute_count = len(dependency_metric.attributes) + len(span_attributes)
        self.assertEqual(len(exported_span._attributes), expected_attribute_count)

    def __configure_mock_for_export(self, span_data_mock: ReadableSpan, metric_attributes: Attributes):
        attribute_map: Attributes = {}
        if should_generate_service_metric_attributes(span_data_mock):
            attribute_map[SERVICE_METRIC] = copy.deepcopy(metric_attributes)
        if should_generate_dependency_metric_attributes(span_data_mock):
            attribute_map[DEPENDENCY_METRIC] = copy.deepcopy(metric_attributes)

        def generate_metric_attribute_map_side_effect(span, resource):
            if span == span_data_mock and resource == self.test_resource:
                return attribute_map
            return {}

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = generate_metric_attribute_map_side_effect

    # Since the side_effect design of the python cause reference issue, make another helper that allow multiple span && attr pair
    def __configure_mock_for_export_with_multiple_side_effect(self, span_data_mocks: [ReadableSpan], metric_attributes_list: [Attributes]):
        attributes_map_list = []
        for span in span_data_mocks:
            attribute_map: Attributes = {}
            if should_generate_service_metric_attributes(span):
                attribute_map[SERVICE_METRIC] = copy.deepcopy(metric_attributes_list[span_data_mocks.index(span)])
            if should_generate_dependency_metric_attributes(span):
                attribute_map[DEPENDENCY_METRIC] = copy.deepcopy(metric_attributes_list[span_data_mocks.index(span)])
            attributes_map_list.append(attribute_map)

        def side_effect(span, resource):
            if span in span_data_mocks and resource == self.test_resource:
                return attributes_map_list[span_data_mocks.index(span)]
            else:
                return {}

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = side_effect
