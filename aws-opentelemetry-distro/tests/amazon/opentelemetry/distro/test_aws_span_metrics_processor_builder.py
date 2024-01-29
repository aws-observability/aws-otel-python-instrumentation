# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator
from opentelemetry.sdk.metrics import MeterProvider


class TestAwsSpanMetricsProcessorBuilder(TestCase):
    def test_all_methods(self):
        # Basic functionality tests for constructor, setters, and build(). Mostly these tests exist to validate the
        # code can be run, as the implementation is fairly trivial and does not require robust unit tests.
        meter_provider: MeterProvider = MeterProvider()
        builder: AwsSpanMetricsProcessorBuilder = AwsSpanMetricsProcessorBuilder(meter_provider, None)
        generator_mock: MetricAttributeGenerator = MagicMock()
        self.assertIs(builder.set_generator(generator_mock), builder)
        self.assertIs(builder.set_scope_name("test"), builder)
        metric_processor: AwsSpanMetricsProcessor = builder.build()
        self.assertIsNotNone(metric_processor)
