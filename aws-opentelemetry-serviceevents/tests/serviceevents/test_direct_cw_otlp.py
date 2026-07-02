# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the _build_log_otlp_exporter helper in serviceevents_instrumentation.

Kept in a separate file to avoid interactions with the stateful
ServiceEventsInstrumentation lifecycle tests (which initialize real collectors).
"""

from unittest import TestCase


class TestBuildLogOtlpExporter(TestCase):
    """Verify _build_log_otlp_exporter routes correctly based on endpoint shape."""

    def test_collector_proxied_endpoint_returns_plain_exporter(self):
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "http://localhost:4316/v1/logs",
            {"x-aws-log-group": "g", "x-aws-log-stream": "s"},
            Compression.NoCompression,
        )
        self.assertIs(type(exp), OTLPLogExporter)

    def test_arbitrary_https_endpoint_returns_plain_exporter(self):
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "https://my-collector.example.com/v1/logs",
            {},
            Compression.Gzip,
        )
        self.assertIs(type(exp), OTLPLogExporter)

