# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter_builder import (
    AwsMetricAttributesSpanExporterBuilder,
)


class TestAwsMetricAttributesSpanExporterBuilder(TestCase):
    def test_basic(self):
        generator: _AwsMetricAttributeGenerator = _AwsMetricAttributeGenerator()
        builder: AwsMetricAttributesSpanExporterBuilder = AwsMetricAttributesSpanExporterBuilder(None, None)
        self.assertIs(builder.set_generator(generator), builder)
