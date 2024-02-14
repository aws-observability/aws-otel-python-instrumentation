# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import copy
from unittest import TestCase
from unittest.mock import MagicMock, call

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_CONSUMER_PARENT_SPAN_KIND, AWS_SPAN_KIND
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro._aws_span_processing_util import (
    LOCAL_ROOT,
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from amazon.opentelemetry.distro.metric_attribute_generator import DEPENDENCY_METRIC, SERVICE_METRIC
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.util.types import Attributes

_CONTAINS_ATTRIBUTES: bool = True
_CONTAINS_NO_ATTRIBUTES: bool = False


# pylint: disable=no-self-use
class TestAwsMetricAttributesSpanExporter(TestCase):
    def setUp(self):
        self.delegate_mock: SpanExporter = MagicMock()
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
        span_attributes: Attributes = self._build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span_data_mock: ReadableSpan = self._build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = self._build_metric_attributes(_CONTAINS_NO_ATTRIBUTES)
        self._configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        exported_span: ReadableSpan = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        self.assertEqual(span_data_mock, exported_span)

    def test_export_delegation_with_attributes_but_without_modification(self):
        span_attributes: Attributes = self._build_span_attributes(_CONTAINS_ATTRIBUTES)
        span_data_mock: ReadableSpan = self._build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = self._build_metric_attributes(_CONTAINS_NO_ATTRIBUTES)
        self._configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        exported_span: ReadableSpan = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        self.assertEqual(span_data_mock, exported_span)

    def test_export_delegation_without_attributes_but_with_modification(self):
        span_attributes: Attributes = self._build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        span_data_mock: ReadableSpan = self._build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = self._build_metric_attributes(_CONTAINS_ATTRIBUTES)
        self._configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        exported_span: ReadableSpan = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        exported_attributes: Attributes = exported_span._attributes
        self.assertEqual(len(exported_attributes), len(metric_attributes))
        for key, value in metric_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)

    def test_export_delegation_with_attributes_and_modification(self):
        span_attributes: Attributes = self._build_span_attributes(_CONTAINS_ATTRIBUTES)
        span_data_mock: ReadableSpan = self._build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = self._build_metric_attributes(_CONTAINS_ATTRIBUTES)
        self._configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        exported_span: ReadableSpan = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        for key, value in metric_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)
        for key, value in span_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)

    def test_export_delegation_with_multiple_spans(self):
        span_data_mock1: ReadableSpan = self._build_readable_span_mock(
            self._build_span_attributes(_CONTAINS_NO_ATTRIBUTES)
        )
        metric_attributes1: Attributes = self._build_metric_attributes(_CONTAINS_NO_ATTRIBUTES)

        span_attributes2: Attributes = self._build_span_attributes(_CONTAINS_ATTRIBUTES)
        span_data_mock2: ReadableSpan = self._build_readable_span_mock(span_attributes2)
        metric_attributes2: Attributes = self._build_metric_attributes(_CONTAINS_ATTRIBUTES)

        span_data_mock3: ReadableSpan = self._build_readable_span_mock(
            self._build_span_attributes(_CONTAINS_ATTRIBUTES)
        )
        metric_attributes3: Attributes = self._build_metric_attributes(_CONTAINS_NO_ATTRIBUTES)

        self._configure_mock_for_export_with_multiple_side_effect(
            [span_data_mock1, span_data_mock2, span_data_mock3],
            [metric_attributes1, metric_attributes2, metric_attributes3],
        )

        self.aws_metric_attributes_span_exporter.export([span_data_mock1, span_data_mock2, span_data_mock3])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock1, span_data_mock2, span_data_mock3])])
        exported_spans_args: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans_args), 3)

        exported_span1: ReadableSpan = exported_spans_args[0]
        exported_span2: ReadableSpan = exported_spans_args[1]
        exported_span3: ReadableSpan = exported_spans_args[2]
        self.assertEqual(exported_span1, span_data_mock1)
        self.assertEqual(exported_span3, span_data_mock3)

        self.assertEqual(len(exported_span2._attributes), len(metric_attributes2) + len(span_attributes2))
        for key, value in metric_attributes2.items():
            self.assertEqual(exported_span2._attributes[key], value)
        for key, value in span_attributes2.items():
            self.assertEqual(exported_span2._attributes[key], value)

    def test_overridden_attributes(self):
        span_attributes: Attributes = {
            "key1": "old value1",
            "key2": "old value2",
        }
        span_data_mock: ReadableSpan = self._build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = {
            "key1": "new value1",
            "key3": "new value3",
        }
        self._configure_mock_for_export(span_data_mock, metric_attributes)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans_args: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        exported_span: ReadableSpan = exported_spans_args[0]
        self.assertEqual(len(exported_spans_args), 1)
        self.assertEqual(exported_span._attributes["key1"], "new value1")
        self.assertEqual(exported_span._attributes["key2"], "old value2")
        self.assertEqual(exported_span._attributes["key3"], "new value3")

    def test_export_delegation_with_two_metrics(self):
        span_attributes: Attributes = self._build_span_attributes(_CONTAINS_ATTRIBUTES)

        span_data_mock: ReadableSpan = MagicMock()
        span_data_mock._attributes = span_attributes
        span_data_mock.kind = SpanKind.PRODUCER
        span_data_mock.parent_span_context = None
        span_data_mock.attributes = span_attributes

        dependency_metric: BoundedAttributes = BoundedAttributes(
            attributes={"new dependency key": "new dependency value", AWS_SPAN_KIND: SpanKind.PRODUCER}
        )

        attribute_map: {str: Attributes} = {
            SERVICE_METRIC: {"new service key": "new service value"},
            DEPENDENCY_METRIC: dependency_metric,
        }

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = lambda span, resource: (
            attribute_map if span == span_data_mock and resource == self.test_resource else {}
        )

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        exported_span: ReadableSpan = exported_spans[0]

        # Check the number of attributes and specific attributes
        expected_attribute_count: int = sum(len(attrs) for attrs in attribute_map.values()) + len(span_attributes)
        expected_attribute_count: int = sum(len(attrs) for attrs in attribute_map.values()) + len(span_attributes)
        self.assertEqual(len(exported_span._attributes), expected_attribute_count)

        # Check that all expected attributes are present
        for key, value in span_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)

        for key, value in dependency_metric.items():
            if key == AWS_SPAN_KIND:
                self.assertNotEqual(exported_span._attributes[key], value)
            else:
                self.assertEqual(exported_span._attributes[key], value)

        self.assertEqual(exported_span._attributes[AWS_SPAN_KIND], LOCAL_ROOT)

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

        # The dependencyAttributesMock will only be used if
        # AwsSpanProcessingUtil.shouldGenerateDependencyMetricAttributes(span) is true.
        # It shouldn't have any interaction since the spanData is a consumer process with parent span
        # of consumer
        attribute_map: {str: BoundedAttributes} = {DEPENDENCY_METRIC: dependency_metric}
        self.generator_mock.generate_metric_attributes_dict_from_span.return_value = attribute_map

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans: [ReadableSpan] = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        attribute_map[DEPENDENCY_METRIC].assert_not_called()

        exported_span: ReadableSpan = exported_spans[0]
        self.assertEqual(exported_span, span_data_mock)

    def test_export_delegation_with_dependency_metrics(self):
        span_attributes: Attributes = self._build_span_attributes(_CONTAINS_ATTRIBUTES)
        span_data_mock: ReadableSpan = MagicMock()
        span_context_mock: SpanContext = SpanContext(1, 1, False)
        span_data_mock.attributes = span_attributes
        span_data_mock.kind = SpanKind.PRODUCER
        span_data_mock.parent = span_context_mock

        dependency_metric: BoundedAttributes = BoundedAttributes(attributes={"new service key": "new dependency value"})

        attribute_map: {str: BoundedAttributes} = {DEPENDENCY_METRIC: dependency_metric}

        def generate_metric_side_effect(span, resource):
            if span == span_data_mock and resource == self.test_resource:
                return attribute_map
            return None

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = generate_metric_side_effect

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
        self.delegate_mock.assert_has_calls([call.export([span_data_mock])])
        exported_spans = self.delegate_mock.export.call_args[0][0]
        self.assertEqual(len(exported_spans), 1)

        exported_span: ReadableSpan = exported_spans[0]

        expected_attribute_count = len(dependency_metric) + len(span_attributes.items())
        self.assertEqual(len(exported_span._attributes), expected_attribute_count)

        for key, value in span_attributes.items():
            self.assertEqual(exported_span._attributes[key], value)

        for key, value in dependency_metric._dict.items():
            self.assertEqual(exported_span._attributes[key], value)

    def _configure_mock_for_export(self, span_data_mock: ReadableSpan, metric_attributes: Attributes):
        attribute_map: Attributes = {}
        if should_generate_service_metric_attributes(span_data_mock):
            attribute_map[SERVICE_METRIC] = copy.deepcopy(metric_attributes)
        if should_generate_dependency_metric_attributes(span_data_mock):
            attribute_map[DEPENDENCY_METRIC] = copy.deepcopy(metric_attributes)

        def generate_metric_attribute_map_side_effect(span, resource):
            if span == span_data_mock and resource == self.test_resource:
                return attribute_map
            return {}

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = (
            generate_metric_attribute_map_side_effect
        )

    def _configure_mock_for_export_with_multiple_side_effect(
        self, span_data_mocks: [ReadableSpan], metric_attributes_list: [Attributes]
    ):
        attributes_map_list: list = []
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
            return {}

        self.generator_mock.generate_metric_attributes_dict_from_span.side_effect = side_effect

    def _build_span_attributes(self, contains_attribute: bool) -> Attributes:
        attribute: Attributes = {}
        if contains_attribute:
            attribute = {"original key": "original value"}
        return attribute

    def _build_metric_attributes(self, contains_attribute: bool) -> Attributes:
        attribute: Attributes = {}
        if contains_attribute:
            attribute["new key"] = "new value"
        return attribute

    def _build_readable_span_mock(self, span_attributes: Attributes) -> ReadableSpan:
        mock_span_data: ReadableSpan = MagicMock()
        mock_span_data._attributes = span_attributes
        mock_span_data._kind = SpanKind.SERVER
        mock_span_data._parent = None
        mock_span_data.attributes = mock_span_data._attributes
        return mock_span_data
