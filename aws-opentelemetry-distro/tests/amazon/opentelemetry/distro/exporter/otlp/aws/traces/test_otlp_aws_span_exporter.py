# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

import requests
from botocore.session import Session

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common._aws_http_headers import _OTLP_AWS_HTTP_HEADERS
from amazon.opentelemetry.distro.exporter.otlp.aws.environment_variables import (
    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE,
)
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._configuration import _get_exporter_names, _import_exporters
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_HEADERS,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TIMEOUT,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    OTEL_EXPORTER_OTLP_TRACES_HEADERS,
    OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult


class TestOTLPAwsSpanExporter(TestCase):
    def setUp(self) -> None:
        self.environment = patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-access-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret-key",
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_CONFIG_FILE": os.devnull,
                "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            },
            clear=True,
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_init_with_logger_provider(self):
        # Test initialization with logger_provider
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"

        exporter = OTLPAwsSpanExporter(
            session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint, logger_provider=mock_logger_provider
        )

        self.assertEqual(exporter._logger_provider, mock_logger_provider)
        self.assertEqual(exporter._aws_region, "us-east-1")

    def test_init_without_logger_provider(self):
        # Test initialization without logger_provider (default behavior)
        endpoint = "https://xray.us-west-2.amazonaws.com/v1/traces"

        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-west-2", endpoint=endpoint)

        self.assertIsNone(exporter._logger_provider)
        self.assertEqual(exporter._aws_region, "us-west-2")
        self.assertIsNone(exporter._llo_handler)

    def test_aws_headers_applied(self):
        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        custom_headers = {"X-Custom-Header": "custom-value"}

        exporter = OTLPAwsSpanExporter(
            session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint, headers=custom_headers
        )

        # Upstream sends these as per-request headers (lowercased keys), which take precedence over session headers.
        request_headers = exporter._client._headers
        for key, value in _OTLP_AWS_HTTP_HEADERS.items():
            self.assertEqual(request_headers[key.lower()], value)

        self.assertEqual(request_headers["x-custom-header"], "custom-value")

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    def test_ensure_llo_handler_when_disabled(self, mock_is_enabled):
        # Test _ensure_llo_handler when agent observability is disabled
        mock_is_enabled.return_value = False
        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"

        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)
        result = exporter._ensure_llo_handler()

        self.assertFalse(result)
        self.assertIsNone(exporter._llo_handler)
        mock_is_enabled.assert_called_once()

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.LLOHandler")
    def test_ensure_llo_handler_lazy_initialization(
        self, mock_llo_handler_class, mock_is_enabled, mock_get_logger_provider
    ):
        # Test lazy initialization of LLO handler when enabled
        mock_is_enabled.return_value = True
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        mock_get_logger_provider.return_value = mock_logger_provider
        mock_llo_handler = MagicMock()
        mock_llo_handler_class.return_value = mock_llo_handler

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        # First call should initialize
        result = exporter._ensure_llo_handler()

        self.assertTrue(result)
        self.assertEqual(exporter._llo_handler, mock_llo_handler)
        mock_llo_handler_class.assert_called_once_with(mock_logger_provider)
        mock_get_logger_provider.assert_called_once()

        # Second call should not re-initialize
        mock_llo_handler_class.reset_mock()
        mock_get_logger_provider.reset_mock()

        result = exporter._ensure_llo_handler()

        self.assertTrue(result)
        mock_llo_handler_class.assert_not_called()
        mock_get_logger_provider.assert_not_called()

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    def test_ensure_llo_handler_with_existing_logger_provider(self, mock_is_enabled, mock_get_logger_provider):
        # Test when logger_provider is already provided
        mock_is_enabled.return_value = True
        mock_logger_provider = MagicMock(spec=LoggerProvider)

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(
            session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint, logger_provider=mock_logger_provider
        )

        with patch(
            "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.LLOHandler"
        ) as mock_llo_handler_class:
            mock_llo_handler = MagicMock()
            mock_llo_handler_class.return_value = mock_llo_handler

            result = exporter._ensure_llo_handler()

            self.assertTrue(result)
            self.assertEqual(exporter._llo_handler, mock_llo_handler)
            mock_llo_handler_class.assert_called_once_with(mock_logger_provider)
            mock_get_logger_provider.assert_not_called()

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    def test_ensure_llo_handler_get_logger_provider_fails(self, mock_is_enabled, mock_get_logger_provider):
        # Test when get_logger_provider raises exception
        mock_is_enabled.return_value = True
        mock_get_logger_provider.side_effect = Exception("Failed to get logger provider")

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        result = exporter._ensure_llo_handler()

        self.assertFalse(result)
        self.assertIsNone(exporter._llo_handler)

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    def test_export_with_llo_disabled(self, mock_is_enabled):
        # Test export when LLO is disabled
        mock_is_enabled.return_value = False
        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"

        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        # Mock the parent class export method
        with patch.object(OTLPSpanExporter, "export") as mock_parent_export:
            mock_parent_export.return_value = SpanExportResult.SUCCESS

            spans = [MagicMock(spec=ReadableSpan), MagicMock(spec=ReadableSpan)]
            result = exporter.export(spans)

            self.assertEqual(result, SpanExportResult.SUCCESS)
            mock_parent_export.assert_called_once_with(spans)
            self.assertIsNone(exporter._llo_handler)

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.LLOHandler")
    def test_export_with_llo_enabled(self, mock_llo_handler_class, mock_get_logger_provider, mock_is_enabled):
        # Test export when LLO is enabled and successfully processes spans
        mock_is_enabled.return_value = True
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        mock_get_logger_provider.return_value = mock_logger_provider

        mock_llo_handler = MagicMock()
        mock_llo_handler_class.return_value = mock_llo_handler

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        # Mock spans and processed spans
        original_spans = [MagicMock(spec=ReadableSpan), MagicMock(spec=ReadableSpan)]
        processed_spans = [MagicMock(spec=ReadableSpan), MagicMock(spec=ReadableSpan)]
        mock_llo_handler.process_spans.return_value = processed_spans

        # Mock the parent class export method
        with patch.object(OTLPSpanExporter, "export") as mock_parent_export:
            mock_parent_export.return_value = SpanExportResult.SUCCESS

            result = exporter.export(original_spans)

            self.assertEqual(result, SpanExportResult.SUCCESS)
            mock_llo_handler.process_spans.assert_called_once_with(original_spans)
            mock_parent_export.assert_called_once_with(processed_spans)

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.LLOHandler")
    def test_export_with_llo_processing_failure(
        self, mock_llo_handler_class, mock_get_logger_provider, mock_is_enabled
    ):
        # Test export when LLO processing fails
        mock_is_enabled.return_value = True
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        mock_get_logger_provider.return_value = mock_logger_provider

        mock_llo_handler = MagicMock()
        mock_llo_handler_class.return_value = mock_llo_handler
        mock_llo_handler.process_spans.side_effect = Exception("LLO processing failed")

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        spans = [MagicMock(spec=ReadableSpan), MagicMock(spec=ReadableSpan)]

        result = exporter.export(spans)

        self.assertEqual(result, SpanExportResult.FAILURE)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter."
        "is_genai_content_extraction_opted_out"
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.LLOHandler")
    def test_export_skips_llo_when_content_extraction_opted_out(
        self, mock_llo_handler_class, mock_get_logger_provider, mock_is_enabled, mock_opted_out
    ):
        mock_is_enabled.return_value = True
        mock_opted_out.return_value = True
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        mock_get_logger_provider.return_value = mock_logger_provider

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        original_spans = [MagicMock(spec=ReadableSpan)]

        with patch.object(OTLPSpanExporter, "export") as mock_parent_export:
            mock_parent_export.return_value = SpanExportResult.SUCCESS

            result = exporter.export(original_spans)

            self.assertEqual(result, SpanExportResult.SUCCESS)
            mock_parent_export.assert_called_once_with(original_spans)
            mock_llo_handler_class.assert_not_called()

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter."
        "is_genai_content_extraction_opted_out"
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_logger_provider")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.LLOHandler")
    def test_export_processes_llo_when_content_extraction_not_opted_out(
        self, mock_llo_handler_class, mock_get_logger_provider, mock_is_enabled, mock_opted_out
    ):
        mock_is_enabled.return_value = True
        mock_opted_out.return_value = False
        mock_logger_provider = MagicMock(spec=LoggerProvider)
        mock_get_logger_provider.return_value = mock_logger_provider

        mock_llo_handler = MagicMock()
        mock_llo_handler_class.return_value = mock_llo_handler

        endpoint = "https://xray.us-east-1.amazonaws.com/v1/traces"
        exporter = OTLPAwsSpanExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=endpoint)

        original_spans = [MagicMock(spec=ReadableSpan)]
        processed_spans = [MagicMock(spec=ReadableSpan)]
        mock_llo_handler.process_spans.return_value = processed_spans

        with patch.object(OTLPSpanExporter, "export") as mock_parent_export:
            mock_parent_export.return_value = SpanExportResult.SUCCESS

            result = exporter.export(original_spans)

            self.assertEqual(result, SpanExportResult.SUCCESS)
            mock_llo_handler.process_spans.assert_called_once_with(original_spans)
            mock_parent_export.assert_called_once_with(processed_spans)

    # pylint: disable=protected-access
    @patch.dict(
        os.environ,
        {
            OTEL_TRACES_EXPORTER: "otlp/sigv4",
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "https://xray.us-west-2.amazonaws.com/v1/traces",
        },
    )
    def test_otlp_sigv4_span_exporter_should_load_from_entry_point(self):
        loaded = _import_exporters(
            _get_exporter_names("traces"), _get_exporter_names("metrics"), _get_exporter_names("logs")
        )
        exporters = loaded[0]
        self.assertIs(exporters["otlp/sigv4"], OTLPAwsSpanExporter)
        exporter = exporters["otlp/sigv4"]()
        self.addCleanup(exporter.shutdown)
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        with patch.object(requests.Session, "request", return_value=response) as request:
            exporter._session.post(exporter._client._endpoint, data=b"payload")
        self.assertEqual(request.call_count, 1)
        headers = request.call_args.kwargs["headers"]
        authorization = [value for (key, value) in headers.items() if key.lower() == "authorization"]
        self.assertEqual(len(authorization), 1)
        self.assertTrue(authorization[0].startswith("AWS4-HMAC-SHA256"))
        self.assertIn("/us-west-2/xray/aws4_request", authorization[0])
        self.assertEqual(request.call_args.kwargs["url"], "https://xray.us-west-2.amazonaws.com/v1/traces")
        self.assertTrue(request.call_args.kwargs["data"])

    @patch.dict(
        os.environ,
        {
            OTEL_TRACES_EXPORTER: "otlp/sigv4,otlp",
            OTEL_METRICS_EXPORTER: "none",
            OTEL_LOGS_EXPORTER: "none",
            OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
        },
    )
    def test_otlp_sigv4_span_exporter_should_load_alongside_otlp_exporter(self):
        loaded = _import_exporters(
            _get_exporter_names("traces"), _get_exporter_names("metrics"), _get_exporter_names("logs")
        )
        exporters = loaded[0]
        self.assertEqual(set(exporters), {"otlp/sigv4", "otlp_proto_http"})
        self.assertIs(exporters["otlp/sigv4"], OTLPAwsSpanExporter)
        self.assertIs(exporters["otlp_proto_http"], OTLPSpanExporter)
        self.assertFalse(loaded[1])
        self.assertFalse(loaded[2])

    def test_otlp_sigv4_span_exporter_should_resolve_region_from_endpoint_across_partitions(self):
        partitions = (
            ("us-west-2", "amazonaws.com"),
            ("cn-north-1", "amazonaws.com.cn"),
            ("eusc-de-east-1", "amazonaws.eu"),
            ("us-iso-east-1", "c2s.ic.gov"),
        )
        for region, suffix in partitions:
            endpoint = f"https://xray.{region}.{suffix}/v1/traces"
            with self.subTest(region=region), patch.dict(os.environ, {OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: endpoint}):
                exporter = OTLPAwsSpanExporter()
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._aws_region, region)

    def test_otlp_sigv4_span_exporter_should_use_generic_endpoint_and_environment_region(self):
        with patch.dict(
            os.environ,
            {OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example.com", "AWS_DEFAULT_REGION": "us-east-2"},
        ):
            exporter = OTLPAwsSpanExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._aws_region, "us-east-2")
            self.assertEqual(exporter._client._endpoint, "https://collector.example.com/v1/traces")

    def test_otlp_sigv4_span_exporter_should_resolve_region_from_generic_aws_endpoint(self):
        with patch.dict(os.environ, {OTEL_EXPORTER_OTLP_ENDPOINT: "https://xray.us-west-2.amazonaws.com/base/"}):
            exporter = OTLPAwsSpanExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._aws_region, "us-west-2")
            self.assertEqual(exporter._client._endpoint, "https://xray.us-west-2.amazonaws.com/base/v1/traces")

    def test_otlp_sigv4_span_exporter_should_apply_signing_service_precedence(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        test_cases = (
            ({}, None, "xray"),
            ({OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service"}, None, "shared-service"),
            (
                {
                    OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service",
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                None,
                "traces-service",
            ),
            (
                {OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service", OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: ""},
                None,
                "shared-service",
            ),
            ({OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "", OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: ""}, None, "xray"),
            (
                {
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                None,
                "xray",
            ),
            (
                {
                    OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service",
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                "explicit-service",
                "explicit-service",
            ),
        )
        for environment, explicit_service, expected_service in test_cases:
            with self.subTest(environment=environment, explicit_service=explicit_service), patch.dict(
                os.environ,
                {
                    "AWS_REGION": "us-west-2",
                    OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example.com",
                    **environment,
                },
            ):
                exporter = OTLPAwsSpanExporter(aws_service=explicit_service)
                self.addCleanup(exporter.shutdown)
                with patch.object(requests.Session, "request", return_value=response) as request:
                    exporter._session.post(exporter._client._endpoint, data=b"payload")
                self.assertEqual(request.call_count, 1)
                self.assertIn(
                    f"/us-west-2/{expected_service}/aws4_request", request.call_args.kwargs["headers"]["Authorization"]
                )

    def test_otlp_sigv4_span_exporter_should_preserve_explicit_region_and_session(self):
        session = Session()
        exporter = OTLPAwsSpanExporter(
            aws_region="us-east-1",
            session=session,
            endpoint="https://xray.us-west-2.amazonaws.com/v1/traces",
        )
        self.addCleanup(exporter.shutdown)
        self.assertEqual(exporter._aws_region, "us-east-1")
        self.assertIs(exporter._session._session, session)

    def test_otlp_sigv4_span_exporter_should_use_environment_settings_with_constructor_overrides(self):
        with patch.dict(
            os.environ,
            {
                "AWS_REGION": "us-east-1",
                OTEL_EXPORTER_OTLP_ENDPOINT: "https://generic.example.com",
                OTEL_EXPORTER_OTLP_HEADERS: "x-custom=generic",
                OTEL_EXPORTER_OTLP_TIMEOUT: "10",
                OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "https://signal.example.com",
                OTEL_EXPORTER_OTLP_TRACES_HEADERS: "x-custom=signal",
                OTEL_EXPORTER_OTLP_TRACES_TIMEOUT: "20",
            },
        ):
            exporter = OTLPAwsSpanExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._client._endpoint, "https://signal.example.com")
            self.assertEqual(exporter._client._headers["x-custom"], "signal")
            self.assertEqual(exporter._client._timeout, 20)
            exporter = OTLPAwsSpanExporter(
                endpoint="https://explicit.example.com", headers={"x-custom": "explicit"}, timeout=30
            )
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._client._endpoint, "https://explicit.example.com")
            self.assertEqual(exporter._client._headers["x-custom"], "explicit")
            self.assertEqual(exporter._client._timeout, 30)

    def test_otlp_sigv4_span_exporter_should_fail_to_initialize_when_region_cannot_be_resolved(self):
        session = Session()
        session.set_config_variable("region", None)
        with self.assertRaisesRegex(ValueError, "requires an AWS endpoint region"):
            OTLPAwsSpanExporter(session=session, endpoint="https://collector.example.com")
