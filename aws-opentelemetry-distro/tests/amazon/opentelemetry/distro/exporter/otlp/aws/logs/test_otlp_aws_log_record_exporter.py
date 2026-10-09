# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

import requests
from botocore.session import Session
from requests.structures import CaseInsensitiveDict

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.environment_variables import (
    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE,
)
from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_log_record_exporter import OTLPAwsLogRecordExporter
from opentelemetry._logs._internal import LogRecord
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._configuration import _get_exporter_names, _import_exporters
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import LogRecordExportResult
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_HEADERS,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_LOGS_HEADERS,
    OTEL_EXPORTER_OTLP_LOGS_TIMEOUT,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TIMEOUT,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags

# Mirrors the upstream OTLP HTTP client's max attempts per export.
_MAX_RETRIES = 6


class TestOTLPAwsLogsExporter(TestCase):
    _ENDPOINT = "https://logs.us-west-2.amazonaws.com/v1/logs"

    def setUp(self):
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

        self.logs = self.generate_test_log_data()
        self.exporter = OTLPAwsLogRecordExporter(
            session=get_aws_session(), aws_region="us-east-1", endpoint=self._ENDPOINT
        )

        self.good_response = requests.Response()
        self.good_response.status_code = 200

        self.non_retryable_response = requests.Response()
        self.non_retryable_response.status_code = 404

        self.retryable_response_no_header = requests.Response()
        self.retryable_response_no_header.status_code = 429

        self.retryable_response_header = requests.Response()
        self.retryable_response_header.headers = CaseInsensitiveDict({"Retry-After": "10"})
        self.retryable_response_header.status_code = 503

    def _mock_backoff_wait(self, interrupted: bool = False) -> MagicMock:
        """Replaces the upstream client's interruptible backoff wait and lifts its deadline."""
        # pylint: disable=protected-access
        self.exporter._client._timeout = 10000  # Large timeout to avoid early exit
        mock_event = MagicMock()
        mock_event.is_set.return_value = False
        mock_event.wait.side_effect = lambda _: interrupted
        self.exporter._client._shutdown_event = mock_event
        return mock_event

    def test_uses_aws_auth_session_with_logs_service(self):
        # pylint: disable=protected-access
        self.assertIsInstance(self.exporter._session, AwsAuthSession)
        self.assertIs(self.exporter._client._transport._session, self.exporter._session)
        self.assertEqual(self.exporter._session._service, "logs")
        self.assertEqual(self.exporter._session._aws_region, "us-east-1")

    @patch("requests.Session.request")
    def test_export_success(self, mock_request):
        mock_request.return_value = self.good_response
        """Tests that the exporter always compresses the serialized logs with gzip before exporting."""
        result = self.exporter.export(self.logs)

        mock_request.assert_called_once()

        _, kwargs = mock_request.call_args
        data = kwargs.get("data", None)

        self.assertEqual(result, LogRecordExportResult.SUCCESS)
        self.assertEqual(kwargs["headers"]["Content-Encoding"], "gzip")

        # Gzip first 10 bytes are reserved for metadata headers:
        # https://www.loc.gov/preservation/digital/formats/fdd/fdd000599.shtml?loclr=blogsig
        self.assertIsNotNone(data)
        self.assertTrue(len(data) >= 10)
        self.assertEqual(data[0:2], b"\x1f\x8b")

    @patch("requests.Session.request")
    def test_should_not_export_if_shutdown(self, mock_request):
        mock_request.return_value = self.good_response
        """Tests that no export request is made if the exporter is shutdown."""
        self.exporter.shutdown()
        result = self.exporter.export(self.logs)

        mock_request.assert_not_called()
        self.assertEqual(result, LogRecordExportResult.FAILURE)

    @patch("requests.Session.request")
    def test_should_not_export_again_if_not_retryable(self, mock_request):
        mock_request.return_value = self.non_retryable_response
        """Tests that only one export request is made if the response status code is non-retryable."""
        result = self.exporter.export(self.logs)
        mock_request.assert_called_once()

        self.assertEqual(result, LogRecordExportResult.FAILURE)

    @patch("requests.Session.request")
    def test_should_export_again_with_backoff_if_retryable_and_no_retry_after_header(self, mock_request):
        mock_request.return_value = self.retryable_response_no_header
        """Tests that multiple export requests are made with exponential delay if the response status code is 429
        and there is no Retry-After header."""
        mock_event = self._mock_backoff_wait()

        result = self.exporter.export(self.logs)

        self.assertEqual(mock_event.wait.call_count, _MAX_RETRIES - 1)

        for index, delay in enumerate(mock_event.wait.call_args_list):
            expected_base = 2**index
            actual_delay = delay[0][0]
            # Assert delay is within jitter range: base * [0.8, 1.2]
            self.assertGreaterEqual(actual_delay, expected_base * 0.8)
            self.assertLessEqual(actual_delay, expected_base * 1.2)

        self.assertEqual(mock_request.call_count, _MAX_RETRIES)
        self.assertEqual(result, LogRecordExportResult.FAILURE)

    @patch("requests.Session.request")
    def test_should_export_again_with_server_delay_if_retryable_and_retry_after_header(self, mock_request):
        mock_request.side_effect = [
            self.retryable_response_header,
            self.retryable_response_header,
            self.retryable_response_header,
            self.good_response,
        ]
        """Tests that multiple export requests are made with the server's suggested
        delay if the response status code is retryable and there is a Retry-After header."""
        mock_event = self._mock_backoff_wait()

        result = self.exporter.export(self.logs)

        for delay in mock_event.wait.call_args_list:
            self.assertEqual(delay[0][0], 10)

        self.assertEqual(mock_event.wait.call_count, 3)
        self.assertEqual(mock_request.call_count, 4)
        self.assertEqual(result, LogRecordExportResult.SUCCESS)

    @patch("requests.Session.request")
    def test_export_connection_error_retry(self, mock_request):
        mock_request.side_effect = [requests.exceptions.ConnectionError(), self.good_response]
        """Tests that the exporter retries on ConnectionError."""
        result = self.exporter.export(self.logs)

        self.assertEqual(mock_request.call_count, 2)
        self.assertEqual(result, LogRecordExportResult.SUCCESS)

    @patch("requests.Session.request")
    def test_export_interrupted_by_shutdown(self, mock_request):
        mock_request.return_value = self.retryable_response_no_header
        """Tests that export can be interrupted by shutdown during retry wait."""
        self._mock_backoff_wait(interrupted=True)

        result = self.exporter.export(self.logs)

        # Should make one request, then get interrupted during retry wait
        self.assertEqual(mock_request.call_count, 1)
        self.assertEqual(result, LogRecordExportResult.FAILURE)

    @patch("requests.Session.request")
    def test_export_with_log_group_and_stream_headers(self, mock_request):
        mock_request.return_value = self.good_response
        """Tests that log_group and log_stream are properly set as headers when provided."""
        log_group = "test-log-group"
        log_stream = "test-log-stream"

        exporter = OTLPAwsLogRecordExporter(
            session=get_aws_session(),
            aws_region="us-east-1",
            endpoint=self._ENDPOINT,
            log_group=log_group,
            log_stream=log_stream,
        )

        result = exporter.export(self.logs)

        mock_request.assert_called_once()
        self.assertEqual(result, LogRecordExportResult.SUCCESS)

        # Verify headers sent with the request contain log group and stream
        request_headers = mock_request.call_args.kwargs["headers"]
        self.assertEqual(request_headers["x-aws-log-group"], log_group)
        self.assertEqual(request_headers["x-aws-log-stream"], log_stream)

    # pylint: disable=protected-access
    @patch.dict(
        os.environ,
        {
            OTEL_LOGS_EXPORTER: "otlp/sigv4",
            OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: "https://logs.us-west-2.amazonaws.com/v1/logs",
            OTEL_EXPORTER_OTLP_LOGS_HEADERS: "x-aws-log-group=test-group,x-aws-log-stream=test-stream",
        },
    )
    def test_should_load_otlp_sigv4_log_record_exporter(self):
        loaded = _import_exporters(
            _get_exporter_names("traces"), _get_exporter_names("metrics"), _get_exporter_names("logs")
        )
        exporters = loaded[2]
        self.assertIs(exporters["otlp/sigv4"], OTLPAwsLogRecordExporter)
        exporter = exporters["otlp/sigv4"]()
        self.addCleanup(exporter.shutdown)
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        with patch.object(requests.Session, "request", return_value=response) as request:
            self.assertEqual(exporter.export(self.logs), LogRecordExportResult.SUCCESS)
        self.assertEqual(request.call_count, 1)
        headers = request.call_args.kwargs["headers"]
        authorization = [value for (key, value) in headers.items() if key.lower() == "authorization"]
        self.assertEqual(len(authorization), 1)
        self.assertTrue(authorization[0].startswith("AWS4-HMAC-SHA256"))
        self.assertIn("/us-west-2/logs/aws4_request", authorization[0])
        self.assertEqual(request.call_args.kwargs["url"], "https://logs.us-west-2.amazonaws.com/v1/logs")
        self.assertTrue(request.call_args.kwargs["data"])
        self.assertEqual(headers["x-aws-log-group"], "test-group")
        self.assertEqual(headers["x-aws-log-stream"], "test-stream")
        self.assertEqual(headers["Content-Encoding"], "gzip")

    @patch.dict(
        os.environ,
        {
            OTEL_TRACES_EXPORTER: "none",
            OTEL_METRICS_EXPORTER: "none",
            OTEL_LOGS_EXPORTER: "otlp/sigv4,otlp",
            OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
        },
    )
    def test_should_load_otlp_sigv4_log_record_exporter_alongside_otlp_exporter(self):
        loaded = _import_exporters(
            _get_exporter_names("traces"), _get_exporter_names("metrics"), _get_exporter_names("logs")
        )
        exporters = loaded[2]
        self.assertEqual(set(exporters), {"otlp/sigv4", "otlp_proto_http"})
        self.assertIs(exporters["otlp/sigv4"], OTLPAwsLogRecordExporter)
        self.assertIs(exporters["otlp_proto_http"], OTLPLogExporter)
        self.assertFalse(loaded[0])
        self.assertFalse(loaded[1])

    def test_should_resolve_otlp_sigv4_log_record_exporter_region_from_endpoint_across_partitions(self):
        partitions = (
            ("us-west-2", "amazonaws.com"),
            ("cn-north-1", "amazonaws.com.cn"),
            ("eusc-de-east-1", "amazonaws.eu"),
            ("us-iso-east-1", "c2s.ic.gov"),
        )
        for region, suffix in partitions:
            endpoint = f"https://logs.{region}.{suffix}/v1/logs"
            with self.subTest(region=region), patch.dict(os.environ, {OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: endpoint}):
                exporter = OTLPAwsLogRecordExporter()
                self.addCleanup(exporter.shutdown)
                self.assertEqual(exporter._aws_region, region)

    def test_should_use_otlp_sigv4_log_record_exporter_with_generic_endpoint_and_environment_region(self):
        with patch.dict(
            os.environ,
            {OTEL_EXPORTER_OTLP_ENDPOINT: "https://collector.example.com", "AWS_DEFAULT_REGION": "us-east-2"},
        ):
            exporter = OTLPAwsLogRecordExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._aws_region, "us-east-2")
            self.assertEqual(exporter._client._endpoint, "https://collector.example.com/v1/logs")

    def test_should_resolve_otlp_sigv4_log_record_exporter_region_from_generic_aws_endpoint(self):
        with patch.dict(os.environ, {OTEL_EXPORTER_OTLP_ENDPOINT: "https://xray.us-west-2.amazonaws.com/base/"}):
            exporter = OTLPAwsLogRecordExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._aws_region, "us-west-2")
            self.assertEqual(exporter._client._endpoint, "https://xray.us-west-2.amazonaws.com/base/v1/logs")

    def test_should_apply_otlp_sigv4_log_record_exporter_signing_service_precedence(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        test_cases = (
            ({}, None, "logs"),
            ({OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service"}, None, "shared-service"),
            (
                {
                    OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service",
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                    OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: "logs-service",
                },
                None,
                "logs-service",
            ),
            (
                {OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "shared-service", OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: ""},
                None,
                "shared-service",
            ),
            ({OTEL_EXPORTER_OTLP_SIGV4_SERVICE: "", OTEL_EXPORTER_OTLP_LOGS_SIGV4_SERVICE: ""}, None, "logs"),
            (
                {
                    OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE: "traces-service",
                    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE: "metrics-service",
                },
                None,
                "logs",
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
                exporter = OTLPAwsLogRecordExporter(aws_service=explicit_service)
                self.addCleanup(exporter.shutdown)
                with patch.object(requests.Session, "request", return_value=response) as request:
                    self.assertEqual(exporter.export(self.logs), LogRecordExportResult.SUCCESS)
                self.assertEqual(request.call_count, 1)
                self.assertIn(
                    f"/us-west-2/{expected_service}/aws4_request", request.call_args.kwargs["headers"]["Authorization"]
                )

    def test_should_preserve_otlp_sigv4_log_record_exporter_explicit_region_and_session(self):
        session = Session()
        exporter = OTLPAwsLogRecordExporter(
            aws_region="us-east-1",
            session=session,
            endpoint="https://logs.us-west-2.amazonaws.com/v1/logs",
        )
        self.addCleanup(exporter.shutdown)
        self.assertEqual(exporter._aws_region, "us-east-1")
        self.assertIs(exporter._session._session, session)

    def test_should_configure_otlp_sigv4_log_record_exporter_from_environment_with_constructor_overrides(self):
        with patch.dict(
            os.environ,
            {
                "AWS_REGION": "us-east-1",
                OTEL_EXPORTER_OTLP_ENDPOINT: "https://generic.example.com",
                OTEL_EXPORTER_OTLP_HEADERS: "x-custom=generic",
                OTEL_EXPORTER_OTLP_TIMEOUT: "10",
                OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: "https://signal.example.com",
                OTEL_EXPORTER_OTLP_LOGS_HEADERS: "x-custom=signal",
                OTEL_EXPORTER_OTLP_LOGS_TIMEOUT: "20",
            },
        ):
            exporter = OTLPAwsLogRecordExporter()
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._client._endpoint, "https://signal.example.com")
            self.assertEqual(exporter._client._headers["x-custom"], "signal")
            self.assertEqual(exporter._client._timeout, 20)
            exporter = OTLPAwsLogRecordExporter(
                endpoint="https://explicit.example.com", headers={"x-custom": "explicit"}, timeout=30
            )
            self.addCleanup(exporter.shutdown)
            self.assertEqual(exporter._client._endpoint, "https://explicit.example.com")
            self.assertEqual(exporter._client._headers["x-custom"], "explicit")
            self.assertEqual(exporter._client._timeout, 30)

    def test_should_fail_to_initialize_otlp_sigv4_log_record_exporter_when_region_cannot_be_resolved(self):
        session = Session()
        session.set_config_variable("region", None)
        with self.assertRaisesRegex(ValueError, "requires an AWS endpoint region"):
            OTLPAwsLogRecordExporter(session=session, endpoint="https://collector.example.com")

    @staticmethod
    def generate_test_log_data(count=5):
        logs = []
        for index in range(count):
            record = LogRecord(
                timestamp=int(time.time_ns()),
                trace_id=int(f"0x{index + 1:032x}", 16),
                span_id=int(f"0x{index + 1:016x}", 16),
                trace_flags=TraceFlags(1),
                severity_text="INFO",
                severity_number=SeverityNumber.INFO,
                body=f"Test log {index + 1}",
                attributes={"test.attribute": f"value-{index + 1}"},
            )

            log_data = ReadableLogRecord(
                log_record=record,
                resource=Resource.create(),
                instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"),
            )

            logs.append(log_data)

        return logs
