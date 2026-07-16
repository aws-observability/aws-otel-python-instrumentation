# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the _build_log_otlp_exporter helper in serviceevents_instrumentation.

Kept in a separate file to avoid interactions with the stateful
ServiceEventsInstrumentation lifecycle tests (which initialize real collectors).
"""

from unittest import TestCase


class TestBuildLogOtlpExporter(TestCase):
    """Verify _build_log_otlp_exporter builds a plain OTLPLogExporter for every endpoint.

    ServiceEvents exports through the collector-proxied OTLP endpoint; the direct-to-CloudWatch
    SigV4 path (``OTLPAwsLogRecordExporter``) was removed, so every endpoint yields the plain
    upstream exporter.
    """

    def test_collector_proxied_endpoint_returns_plain_exporter(self):
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "http://localhost:4316/v1/logs",
            {"x-aws-log-group": "g", "x-aws-log-stream": "s"},
            Compression.NoCompression,
        )
        self.assertIs(type(exp), OTLPLogExporter)

    def test_arbitrary_https_endpoint_returns_plain_exporter(self):
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "https://my-collector.example.com/v1/logs",
            {},
            Compression.Gzip,
        )
        self.assertIs(type(exp), OTLPLogExporter)

    def test_cloudwatch_shaped_endpoint_returns_plain_exporter(self):
        """A ``logs.{region}.amazonaws.com`` endpoint no longer routes to a SigV4 exporter."""
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "https://logs.us-east-2.amazonaws.com/v1/logs",
            {"x-aws-log-group": "/my/group", "x-aws-log-stream": "my-stream"},
            Compression.Gzip,
        )
        self.assertIs(type(exp), OTLPLogExporter)
