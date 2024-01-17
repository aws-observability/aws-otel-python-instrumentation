# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import SpanExporter


class AwsMetricAttributesSpanExporterBuilder:
    _DEFAULT_GENERATOR: MetricAttributeGenerator = _AwsMetricAttributeGenerator()

    def __init__(self, delegate: SpanExporter, resource: Resource):
        self.delegate: SpanExporter = delegate
        self.resource: Resource = resource
        self.generator: MetricAttributeGenerator = self._DEFAULT_GENERATOR

    def set_generator(self, generator: MetricAttributeGenerator) -> "AwsMetricAttributesSpanExporterBuilder":
        """
        Sets the generator used to generate attributes used spans exported by the exporter.
        If unset, defaults to _DEFAULT_GENERATOR. Must not be None.
        """
        if generator is None:
            raise ValueError("generator must not be None")
        self.generator = generator
        return self

    def build(self) -> AwsMetricAttributesSpanExporter:
        return AwsMetricAttributesSpanExporter(self.delegate, self.generator, self.resource)
