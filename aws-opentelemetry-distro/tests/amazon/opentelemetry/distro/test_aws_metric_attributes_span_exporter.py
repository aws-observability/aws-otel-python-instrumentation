# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from opentelemetry.sdk.trace.export import ConsoleSpanExporter


class TestAwsMetricAttributesSpanExporter(TestCase):
    def test_basic(self):
        console_span_exporter: ConsoleSpanExporter = ConsoleSpanExporter()
        span_exporter: AwsMetricAttributesSpanExporter = AwsMetricAttributesSpanExporter(
            console_span_exporter, None, None
        )
        self.assertTrue(span_exporter.force_flush)
