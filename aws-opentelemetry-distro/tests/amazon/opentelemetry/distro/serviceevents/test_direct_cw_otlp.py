# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the _build_log_otlp_exporter helper in serviceevents_instrumentation.

Kept in a separate file to avoid interactions with the stateful
ServiceEventsInstrumentation lifecycle tests (which initialize real collectors).
"""

import sys
import types
from unittest import TestCase
from unittest.mock import MagicMock, patch


class TestBuildLogOtlpExporter(TestCase):
    """Verify _build_log_otlp_exporter routes correctly based on endpoint shape."""

    def test_collector_proxied_endpoint_returns_plain_exporter(self):
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "http://localhost:4316/v1/logs",
            {"x-aws-log-group": "g", "x-aws-log-stream": "s"},
            Compression.NoCompression,
        )
        # OTLPAwsLogRecordExporter subclasses OTLPLogExporter, so use `type is`
        # rather than `isinstance` to distinguish the plain upstream class.
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

    def test_cloudwatch_endpoint_routes_to_sigv4_exporter(self):
        """Mock the SigV4 exporter to avoid AWS credential resolution in CI.

        Both ``boto3`` and the AWS log-record exporter module are stubbed via
        ``sys.modules`` rather than ``mock.patch("dotted.path")``. That
        matters because ``mock.patch`` resolves its target by dotted-name
        lookup, which walks ``sys.meta_path`` finders. Other tests in this
        suite (``TestServiceEventsModes``) install the ServiceEvents AST import hook
        and never uninstall it, so accumulated finders turn that lookup
        into pathological recursion. Pre-populating ``sys.modules`` sidesteps
        meta_path entirely — the lazy ``import`` inside the function under
        test resolves directly from the cache.
        """
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression

        stub_boto3 = types.ModuleType("boto3")
        stub_boto3.Session = MagicMock(return_value=MagicMock(name="stub-boto3-session"))

        exporter_module_path = "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_log_record_exporter"
        stub_exporter_module = types.ModuleType(exporter_module_path)
        mock_ctor = MagicMock(name="OTLPAwsLogRecordExporter")
        stub_exporter_module.OTLPAwsLogRecordExporter = mock_ctor

        with patch.dict(
            sys.modules,
            {"boto3": stub_boto3, exporter_module_path: stub_exporter_module},
        ):
            _build_log_otlp_exporter(
                "https://logs.us-east-2.amazonaws.com/v1/logs",
                {"x-aws-log-group": "/my/group", "x-aws-log-stream": "my-stream"},
                Compression.Gzip,
            )
        mock_ctor.assert_called_once()
        kwargs = mock_ctor.call_args.kwargs
        # Region was extracted from the endpoint hostname.
        self.assertEqual(kwargs["aws_region"], "us-east-2")
        self.assertEqual(kwargs["endpoint"], "https://logs.us-east-2.amazonaws.com/v1/logs")
        self.assertEqual(
            kwargs["headers"],
            {"x-aws-log-group": "/my/group", "x-aws-log-stream": "my-stream"},
        )
