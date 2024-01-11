# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor


class TestAwsSpanMetricsProcessor(TestCase):
    def test_basic(self):
        processor: AwsSpanMetricsProcessor = AwsSpanMetricsProcessor(None, None, None, None, None)
        self.assertTrue(processor.force_flush)
