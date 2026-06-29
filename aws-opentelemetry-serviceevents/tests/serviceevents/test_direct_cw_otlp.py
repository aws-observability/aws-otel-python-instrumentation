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
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
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
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        exp = _build_log_otlp_exporter(
            "https://my-collector.example.com/v1/logs",
            {},
            Compression.Gzip,
        )
        self.assertIs(type(exp), OTLPLogExporter)

    def test_cloudwatch_endpoint_routes_to_sigv4_exporter(self):
        """The CloudWatch endpoint builds the SigV4 exporter via a botocore session.

        Regression guard for the boto3-vs-botocore finding: the SigV4 path must
        obtain its session from ``get_aws_session()`` (which returns a
        ``botocore.session.Session``) and must NOT ``import boto3``. ``boto3`` is
        not a declared distro dependency, so we assert it stays absent from
        ``sys.modules`` throughout — if the production code reintroduced
        ``import boto3`` this would fail in a botocore-only environment.

        The AWS log-record exporter module is stubbed via ``sys.modules`` rather
        than ``mock.patch("dotted.path")``. That matters because ``mock.patch``
        resolves its target by dotted-name lookup, which walks
        ``sys.meta_path`` finders. Other tests in this suite
        (``TestServiceEventsModes``) install the ServiceEvents AST import hook
        and never uninstall it, so accumulated finders turn that lookup into
        pathological recursion. Pre-populating ``sys.modules`` sidesteps
        meta_path entirely — the lazy ``import`` inside the function under test
        resolves directly from the cache. ``get_aws_session`` is patched at its
        source module so the function picks up the stub on its lazy import.
        """
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression

        stub_session = MagicMock(name="stub-botocore-session")

        exporter_module_path = "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_log_record_exporter"
        stub_exporter_module = types.ModuleType(exporter_module_path)
        mock_ctor = MagicMock(name="OTLPAwsLogRecordExporter")
        stub_exporter_module.OTLPAwsLogRecordExporter = mock_ctor

        # Guard: boto3 must never be imported by the SigV4 path.
        had_boto3 = "boto3" in sys.modules
        if had_boto3:
            self.addCleanup(lambda saved=sys.modules["boto3"]: sys.modules.__setitem__("boto3", saved))
            del sys.modules["boto3"]

        with patch.dict(sys.modules, {exporter_module_path: stub_exporter_module}), patch(
            "amazon.opentelemetry.distro._utils.get_aws_session", return_value=stub_session
        ):
            _build_log_otlp_exporter(
                "https://logs.us-east-2.amazonaws.com/v1/logs",
                {"x-aws-log-group": "/my/group", "x-aws-log-stream": "my-stream"},
                Compression.Gzip,
            )

        # boto3 must not have been imported as a side effect of the SigV4 build.
        self.assertNotIn("boto3", sys.modules)

        mock_ctor.assert_called_once()
        kwargs = mock_ctor.call_args.kwargs
        # The exporter receives the botocore session from get_aws_session(),
        # not a boto3.Session().
        self.assertIs(kwargs["session"], stub_session)
        # Region was extracted from the endpoint hostname.
        self.assertEqual(kwargs["aws_region"], "us-east-2")
        self.assertEqual(kwargs["endpoint"], "https://logs.us-east-2.amazonaws.com/v1/logs")
        self.assertEqual(
            kwargs["headers"],
            {"x-aws-log-group": "/my/group", "x-aws-log-stream": "my-stream"},
        )

    def test_cloudwatch_endpoint_without_botocore_falls_back_to_plain_exporter(self):
        """When botocore is unavailable the CloudWatch path must degrade gracefully.

        ``get_aws_session()`` returns ``None`` when botocore is not installed.
        The SigV4 path must then fall back to a plain (unsigned) ``OTLPLogExporter``
        against the same endpoint — never return ``None`` (the caller does not
        handle it) and never raise into the host app. This is the core
        regression: previously the path did ``import boto3`` and an
        ``ImportError`` was swallowed by ``initialize()``'s broad-except,
        silently disabling all ServiceEvents telemetry.
        """
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import _build_log_otlp_exporter
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        with patch("amazon.opentelemetry.distro._utils.get_aws_session", return_value=None):
            exp = _build_log_otlp_exporter(
                "https://logs.us-east-2.amazonaws.com/v1/logs",
                {"x-aws-log-group": "/my/group", "x-aws-log-stream": "my-stream"},
                Compression.Gzip,
            )

        # Falls back to the plain upstream exporter (not the SigV4 subclass),
        # and crucially is not None.
        self.assertIsNotNone(exp)
        self.assertIs(type(exp), OTLPLogExporter)
