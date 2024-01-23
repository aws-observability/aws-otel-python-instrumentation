# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock, call

from amazon.opentelemetry.distro._aws_span_processing_util import should_generate_dependency_metric_attributes, should_generate_service_metric_attributes
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
    mock_span_data.attributes = span_attributes
    mock_span_data.kind = SpanKind.SERVER
    mock_span_data.parent = None
    return mock_span_data


def configure_mock_for_export(span_data_mock: ReadableSpan, metric_attributes: Attributes):
    attribute_map: Attributes = {}
    if should_generate_service_metric_attributes(span_data_mock):
        attribute_map[SERVICE_METRIC] = metric_attributes
    if should_generate_dependency_metric_attributes(span_data_mock):
        attribute_map[DEPENDENCY_METRIC] = metric_attributes
    return attribute_map


class TestAwsMetricAttributesSpanExporter(TestCase):
    CONTAINS_ATTRIBUTES = True
    CONTAINS_NO_ATTRIBUTES = False

    def setUp(self):
        self.delegate_mock: ConsoleSpanExporter = MagicMock()
        self.generator_mock: MetricAttributeGenerator = MagicMock()
        self.test_resource: Resource = Resource.get_empty()
        self.aws_metric_attributes_span_exporter: AwsMetricAttributesSpanExporter = AwsMetricAttributesSpanExporter(
            self.delegate_mock, self.generator_mock, self.test_resource
        )

    def test_pass_through_delegations(self):
        self.aws_metric_attributes_span_exporter.force_flush()
        self.aws_metric_attributes_span_exporter.shutdown()
        self.delegate_mock.assert_has_calls([call.force_flush(30000), call.shutdown()])

    def test_export_delegation_without_attributes_or_modification(self):
        span_attributes: Attributes = build_span_attributes(self.CONTAINS_ATTRIBUTES)
        span_data_mock: ReadableSpan = build_readable_span_mock(span_attributes)
        metric_attributes: Attributes = build_metric_attributes(self.CONTAINS_NO_ATTRIBUTES)

        self.aws_metric_attributes_span_exporter.export([span_data_mock])
