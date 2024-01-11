# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder


class TestAwsSpanMetricsProcessorBuilder(TestCase):
    def test_basic(self):
        builder: AwsSpanMetricsProcessorBuilder = AwsSpanMetricsProcessorBuilder(None, None)
        self.assertIs(builder.set_scope_name("test"), builder)
