import time
from unittest import TestCase
from unittest.mock import patch

import requests
from requests.structures import CaseInsensitiveDict

from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.sdk._logs import LogData, LogRecord
from opentelemetry.sdk._logs.export import (
    LogExportResult,
)
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags


class TestOTLPAwsLogsExporter(TestCase):
    _ENDPOINT = "https://logs.us-west-2.amazonaws.com/v1/logs"
    good_response = requests.Response()
    good_response.status_code = 200

    non_retryable_response = requests.Response()
    non_retryable_response.status_code = 404

    retryable_response_no_header = requests.Response()
    retryable_response_no_header.status_code = 429

    retryable_response_header = requests.Response()
    retryable_response_header.headers = CaseInsensitiveDict({"Retry-After": "10"})
    retryable_response_header.status_code = 503

    retryable_response_bad_header = requests.Response()
    retryable_response_bad_header.headers = CaseInsensitiveDict({"Retry-After": "-12"})
    retryable_response_bad_header.status_code = 503

    def setUp(self):
        self.logs = self.generate_test_log_data()
        self.exporter = OTLPAwsLogExporter(endpoint=self._ENDPOINT)

    @patch("requests.Session.request", return_value=good_response)
    def test_export_success(self, mock_request):
        """Tests that the exporter always compresses the serialized logs with gzip before exporting."""
        result = self.exporter.export(self.logs)

        mock_request.assert_called_once()

        _, kwargs = mock_request.call_args
        data = kwargs.get("data", None)

        self.assertEqual(result, LogExportResult.SUCCESS)

        # Gzip first 10 bytes are reserved for metadata headers:
        # https://www.loc.gov/preservation/digital/formats/fdd/fdd000599.shtml?loclr=blogsig
        self.assertIsNotNone(data)
        self.assertTrue(len(data) >= 10)
        self.assertEqual(data[0:2], b"\x1f\x8b")

    @patch("requests.Session.request", return_value=good_response)
    def test_export_gen_ai_logs(self, mock_request):
        """Tests that when set_gen_ai_log_flag is set, the exporter includes the LLO header in the request."""

        self.exporter.set_gen_ai_log_flag()

        result = self.exporter.export(self.logs)

        mock_request.assert_called_once()

        _, kwargs = mock_request.call_args
        headers = kwargs.get("headers", None)

        self.assertEqual(result, LogExportResult.SUCCESS)
        self.assertIsNotNone(headers)
        self.assertIn(self.exporter._LARGE_LOG_HEADER, headers)
        self.assertEqual(headers[self.exporter._LARGE_LOG_HEADER], self.exporter._LARGE_GEN_AI_LOG_PATH_HEADER)

    @patch("requests.Session.request", return_value=good_response)
    def test_should_not_export_if_shutdown(self, mock_request):
        """Tests that no export request is made if the exporter is shutdown."""
        self.exporter.shutdown()
        result = self.exporter.export(self.logs)

        mock_request.assert_not_called()
        self.assertEqual(result, LogExportResult.FAILURE)

    @patch("requests.Session.request", return_value=non_retryable_response)
    def test_should_not_export_again_if_not_retryable(self, mock_request):
        """Tests that only one export request is made if the response status code is non-retryable."""
        result = self.exporter.export(self.logs)
        mock_request.assert_called_once()

        self.assertEqual(result, LogExportResult.FAILURE)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.sleep", side_effect=lambda x: None
    )
    @patch("requests.Session.request", return_value=retryable_response_no_header)
    def test_should_export_again_with_backoff_if_retryable_and_no_retry_after_header(self, mock_request, mock_sleep):
        """Tests that multiple export requests are made with exponential delay if the response status code is retryable.
        But there is no Retry-After header."""
        result = self.exporter.export(self.logs)

        # 1, 2, 4, 8, 16, 32 delays
        self.assertEqual(mock_sleep.call_count, 6)

        delays = mock_sleep.call_args_list

        for i in range(len(delays)):
            self.assertEqual(delays[i][0][0], 2**i)

        # Number of calls: 1 + len(1, 2, 4, 8, 16, 32 delays)
        self.assertEqual(mock_request.call_count, 7)
        self.assertEqual(result, LogExportResult.FAILURE)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.sleep", side_effect=lambda x: None
    )
    @patch(
        "requests.Session.request",
        side_effect=[retryable_response_header, retryable_response_header, retryable_response_header, good_response],
    )
    def test_should_export_again_with_server_delay_if_retryable_and_retry_after_header(self, mock_request, mock_sleep):
        """Tests that multiple export requests are made with the server's suggested
        delay if the response status code is retryable and there is a Retry-After header."""
        result = self.exporter.export(self.logs)
        delays = mock_sleep.call_args_list

        for i in range(len(delays)):
            self.assertEqual(delays[i][0][0], 10)

        self.assertEqual(mock_sleep.call_count, 3)
        self.assertEqual(mock_request.call_count, 4)
        self.assertEqual(result, LogExportResult.SUCCESS)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.sleep", side_effect=lambda x: None
    )
    @patch(
        "requests.Session.request",
        side_effect=[
            retryable_response_bad_header,
            retryable_response_bad_header,
            retryable_response_bad_header,
            good_response,
        ],
    )
    def test_should_export_again_with_backoff_delay_if_retryable_and_bad_retry_after_header(
        self, mock_request, mock_sleep
    ):
        """Tests that multiple export requests are made with exponential delay if the response status code is retryable.
        but the Retry-After header ins invalid or malformed."""
        result = self.exporter.export(self.logs)
        delays = mock_sleep.call_args_list

        for i in range(len(delays)):
            self.assertEqual(delays[i][0][0], 2**i)

        self.assertEqual(mock_sleep.call_count, 3)
        self.assertEqual(mock_request.call_count, 4)
        self.assertEqual(result, LogExportResult.SUCCESS)

    def generate_test_log_data(self, count=5):
        logs = []
        for i in range(count):
            record = LogRecord(
                timestamp=int(time.time_ns()),
                trace_id=int(f"0x{i + 1:032x}", 16),
                span_id=int(f"0x{i + 1:016x}", 16),
                trace_flags=TraceFlags(1),
                severity_text="INFO",
                severity_number=SeverityNumber.INFO,
                body=f"Test log {i + 1}",
                attributes={"test.attribute": f"value-{i + 1}"},
            )

            log_data = LogData(log_record=record, instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"))

            logs.append(log_data)

        return logs
