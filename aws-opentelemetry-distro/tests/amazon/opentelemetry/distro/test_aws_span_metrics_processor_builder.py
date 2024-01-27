# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator
from opentelemetry.sdk.metrics import MeterProvider


class TestAwsSpanMetricsProcessorBuilder(TestCase):
    def test_basic(self):
        meter_provider: MeterProvider = MeterProvider()
        builder: AwsSpanMetricsProcessorBuilder = AwsSpanMetricsProcessorBuilder(meter_provider, None)
        self.assertIs(builder.set_scope_name("test"), builder)

        generator_mock: MetricAttributeGenerator = MagicMock()
        builder.set_generator(generator_mock)
        builder.set_scope_name("test scope name")
        metric_processor: AwsSpanMetricsProcessor = builder.build()
        self.assertEqual(metric_processor._error_histogram.record(1, None), None)
