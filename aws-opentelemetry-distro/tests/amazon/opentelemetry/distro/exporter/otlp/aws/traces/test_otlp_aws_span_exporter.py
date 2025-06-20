# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from opentelemetry.sdk._logs import LoggerProvider


class TestOTLPAwsSpanExporter(TestCase):
    def test_init_with_logger_provider(self):
        # Test initialization with logger_provider
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"

        exporter = OTLPAwsSpanExporter(endpoint=endpoint, logger_provider=mock_logger_provider)

        self.assertEqual(exporter._logger_provider, mock_logger_provider)
        self.assertEqual(exporter._aws_region, "us-east-1")

    def test_init_without_logger_provider(self):
        # Test initialization without logger_provider (default behavior)
        endpoint = "https://xray.us-west-2.amazonaws.com/v1/traces"

        exporter = OTLPAwsSpanExporter(endpoint=endpoint)

        self.assertIsNone(exporter._logger_provider)
        self.assertEqual(exporter._aws_region, "us-west-2")
