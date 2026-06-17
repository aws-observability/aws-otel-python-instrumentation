# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._utils import OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE, get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common._aws_http_headers import _OTLP_AWS_HTTP_HEADERS
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter_auto import AutoOTLPAwsSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._configuration import _import_exporters
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult

OTLP_TRACES_SIGV4_EXPORTER = "otlp/sigv4"


class TestOTLPAwsSpanExporter(TestCase):
    def test_import_exporters_resolves_otlp_sigv4_entry_point(self):
        """Tests that the 'otlp/sigv4' entry point resolves to AutoOTLPAwsSpanExporter."""
        trace_exporters, _, _ = _import_exporters(
            trace_exporter_names=[OTLP_TRACES_SIGV4_EXPORTER],
            metric_exporter_names=[],
            log_exporter_names=[],
        )

        self.assertIs(trace_exporters[OTLP_TRACES_SIGV4_EXPORTER], AutoOTLPAwsSpanExporter)

    def test_auto_exporter_resolves_from_environment(self):
        """The entry point is constructed with no args; region/service resolve from env into the SigV4 session."""
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE] = "aps"
        os.environ[OTEL_EXPORTER_OTLP_TRACES_ENDPOINT] = "https://collector.example.com/v1/traces"
        try:
            exporter = AutoOTLPAwsSpanExporter()
            self.assertIsInstance(exporter, OTLPAwsSpanExporter)
            self.assertEqual(exporter._aws_region, "us-east-1")
            self.assertEqual(exporter._aws_service, "aps")
            self.assertEqual(exporter._session._service, "aps")
            self.assertEqual(exporter._session._aws_region, "us-east-1")
        finally:
            os.environ.pop("AWS_REGION", None)
            os.environ.pop(OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE, None)
            os.environ.pop(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, None)

    def test_auto_exporter_defaults_service_to_xray(self):
        """Without OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE, the signing service defaults to xray."""
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ[OTEL_EXPORTER_OTLP_TRACES_ENDPOINT] = "https://collector.example.com/v1/traces"
        os.environ.pop(OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE, None)
        try:
            exporter = AutoOTLPAwsSpanExporter()
            self.assertEqual(exporter._aws_service, "xray")
            self.assertEqual(exporter._session._service, "xray")
        finally:
            os.environ.pop("AWS_REGION", None)
            os.environ.pop(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, None)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.get_aws_session",
        return_value=None,
    )
    def test_auto_exporter_without_botocore_falls_back_to_unsigned(self, _mock_session):
        """botocore is optional; without a session the entry point yields an unsigned OTLP exporter, not a crash."""
        os.environ["AWS_REGION"] = "us-east-1"
        try:
            exporter = AutoOTLPAwsSpanExporter()
        finally:
            os.environ.pop("AWS_REGION", None)

        # Falls back to the base OTLP exporter (not the SigV4 AwsAuthSession variant).
        self.assertNotIsInstance(exporter, OTLPAwsSpanExporter)
        self.assertIsInstance(exporter, OTLPSpanExporter)
        self.assertNotEqual(type(exporter._session).__name__, "AwsAuthSession")

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter_auto.get_aws_region",
        return_value=None,
    )
    def test_auto_exporter_without_region_falls_back_to_unsigned(self, _mock_region):
        """Without a resolvable AWS region the entry point yields an unsigned OTLP exporter, not a crash."""
        exporter = AutoOTLPAwsSpanExporter()

        self.assertNotIsInstance(exporter, OTLPAwsSpanExporter)
        self.assertIsInstance(exporter, OTLPSpanExporter)
        self.assertNotEqual(type(exporter._session).__name__, "AwsAuthSession")

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

        for key in _OTLP_AWS_HTTP_HEADERS.keys():
            self.assertIn(key, exporter._session.headers)

        self.assertEqual(exporter._session.headers["X-Custom-Header"], "custom-value")
        self.assertIn("User-Agent", exporter._session.headers)

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
