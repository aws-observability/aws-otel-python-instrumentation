# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
from unittest import TestCase
from unittest.mock import patch

import requests
from requests.structures import CaseInsensitiveDict

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import _MAX_RETRYS, OTLPAwsLogExporter
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.sdk._logs import LogData, LogRecord
from opentelemetry.sdk._logs.export import LogExportResult
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags


class TestOTLPAwsLogsExporter(TestCase):
    _ENDPOINT = "https://logs.us-west-2.amazonaws.com/v1/logs"

    def setUp(self):
        self.logs = self.generate_test_log_data()
        self.exporter = OTLPAwsLogExporter(session=get_aws_session(), aws_region="us-east-1", endpoint=self._ENDPOINT)

        self.good_response = requests.Response()
        self.good_response.status_code = 200

        self.non_retryable_response = requests.Response()
        self.non_retryable_response.status_code = 404

        self.retryable_response_no_header = requests.Response()
        self.retryable_response_no_header.status_code = 429

        self.retryable_response_header = requests.Response()
        self.retryable_response_header.headers = CaseInsensitiveDict({"Retry-After": "10"})
        self.retryable_response_header.status_code = 503

        self.retryable_response_bad_header = requests.Response()
        self.retryable_response_bad_header.headers = CaseInsensitiveDict({"Retry-After": "-12"})
        self.retryable_response_bad_header.status_code = 503

    @patch("requests.Session.post")
    def test_export_success(self, mock_request):
        mock_request.return_value = self.good_response
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

    @patch("requests.Session.post")
    def test_should_not_export_if_shutdown(self, mock_request):
        mock_request.return_value = self.good_response
        """Tests that no export request is made if the exporter is shutdown."""
        self.exporter.shutdown()
        result = self.exporter.export(self.logs)

        mock_request.assert_not_called()
        self.assertEqual(result, LogExportResult.FAILURE)

    @patch("requests.Session.post")
    def test_should_not_export_again_if_not_retryable(self, mock_request):
        mock_request.return_value = self.non_retryable_response
        """Tests that only one export request is made if the response status code is non-retryable."""
        result = self.exporter.export(self.logs)
        mock_request.assert_called_once()

        self.assertEqual(result, LogExportResult.FAILURE)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.Event.wait",
        side_effect=lambda x: False,
    )
    @patch("requests.Session.post")
    def test_should_export_again_with_backoff_if_retryable_and_no_retry_after_header(self, mock_request, mock_wait):
        mock_request.return_value = self.retryable_response_no_header
        """Tests that multiple export requests are made with exponential delay if the response status code is retryable.
        But there is no Retry-After header."""
        self.exporter._timeout = 10000  # Large timeout to avoid early exit

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.time") as mock_time:
            mock_time.return_value = 0  # Keep time constant to avoid timeout issues
            result = self.exporter.export(self.logs)

        self.assertEqual(mock_wait.call_count, _MAX_RETRYS - 1)

        delays = mock_wait.call_args_list

        for index, delay in enumerate(delays):
            expected_base = 2**index
            actual_delay = delay[0][0]
            # Assert delay is within jitter range: base * [0.8, 1.2]
            self.assertGreaterEqual(actual_delay, expected_base * 0.8)
            self.assertLessEqual(actual_delay, expected_base * 1.2)

        self.assertEqual(mock_request.call_count, _MAX_RETRYS)
        self.assertEqual(result, LogExportResult.FAILURE)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.Event.wait",
        side_effect=lambda x: False,
    )
    @patch("requests.Session.post")
    def test_should_export_again_with_server_delay_if_retryable_and_retry_after_header(self, mock_request, mock_wait):
        mock_request.side_effect = [
            self.retryable_response_header,
            self.retryable_response_header,
            self.retryable_response_header,
            self.good_response,
        ]
        """Tests that multiple export requests are made with the server's suggested
        delay if the response status code is retryable and there is a Retry-After header."""
        self.exporter._timeout = 10000  # Large timeout to avoid early exit

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.time") as mock_time:
            mock_time.return_value = 0  # Keep time constant to avoid timeout issues
            result = self.exporter.export(self.logs)

        delays = mock_wait.call_args_list

        for delay in delays:
            self.assertEqual(delay[0][0], 10)

        self.assertEqual(mock_wait.call_count, 3)
        self.assertEqual(mock_request.call_count, 4)
        self.assertEqual(result, LogExportResult.SUCCESS)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.Event.wait",
        side_effect=lambda x: False,
    )
    @patch("requests.Session.post")
    def test_should_export_again_with_backoff_delay_if_retryable_and_bad_retry_after_header(
        self, mock_request, mock_wait
    ):
        mock_request.side_effect = [
            self.retryable_response_bad_header,
            self.retryable_response_bad_header,
            self.retryable_response_bad_header,
            self.good_response,
        ]
        """Tests that multiple export requests are made with exponential delay if the response status code is retryable.
        but the Retry-After header is invalid or malformed."""
        self.exporter._timeout = 10000  # Large timeout to avoid early exit

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.time") as mock_time:
            mock_time.return_value = 0  # Keep time constant to avoid timeout issues
            result = self.exporter.export(self.logs)

        delays = mock_wait.call_args_list

        for index, delay in enumerate(delays):
            expected_base = 2**index
            actual_delay = delay[0][0]
            # Assert delay is within jitter range: base * [0.8, 1.2]
            self.assertGreaterEqual(actual_delay, expected_base * 0.8)
            self.assertLessEqual(actual_delay, expected_base * 1.2)

        self.assertEqual(mock_wait.call_count, 3)
        self.assertEqual(mock_request.call_count, 4)
        self.assertEqual(result, LogExportResult.SUCCESS)

    @patch("requests.Session.post")
    def test_export_connection_error_retry(self, mock_request):
        mock_request.side_effect = [requests.exceptions.ConnectionError(), self.good_response]
        """Tests that the exporter retries on ConnectionError."""
        result = self.exporter.export(self.logs)

        self.assertEqual(mock_request.call_count, 2)
        self.assertEqual(result, LogExportResult.SUCCESS)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.Event.wait",
        side_effect=lambda x: False,
    )
    @patch("requests.Session.post")
    def test_should_stop_retrying_when_deadline_exceeded(self, mock_request, mock_wait):
        mock_request.return_value = self.retryable_response_no_header
        """Tests that the exporter stops retrying when the deadline is exceeded."""
        self.exporter._timeout = 5  # Short timeout to trigger deadline check

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.time") as mock_time:
            # First call returns start time, subsequent calls simulate time passing
            mock_time.side_effect = [0, 0, 1, 2, 4, 8]  # Exponential backoff would be 1, 2, 4 seconds

            result = self.exporter.export(self.logs)

            # Should stop before max retries due to deadline
            self.assertLess(mock_wait.call_count, _MAX_RETRYS)
            self.assertLess(mock_request.call_count, _MAX_RETRYS + 1)
            self.assertEqual(result, LogExportResult.FAILURE)

            # Verify total time passed is at the timeout limit
            self.assertGreaterEqual(5, self.exporter._timeout)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.Event.wait",
        side_effect=lambda x: True,
    )
    @patch("requests.Session.post")
    def test_export_interrupted_by_shutdown(self, mock_request, mock_wait):
        mock_request.return_value = self.retryable_response_no_header
        """Tests that export can be interrupted by shutdown during retry wait."""
        self.exporter._timeout = 10000

        with patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.time") as mock_time:
            mock_time.return_value = 0  # Keep time constant to avoid timeout issues
            result = self.exporter.export(self.logs)

        # Should make one request, then get interrupted during retry wait
        self.assertEqual(mock_request.call_count, 1)
        self.assertEqual(result, LogExportResult.FAILURE)

    @patch("requests.Session.post")
    def test_export_with_log_group_and_stream_headers(self, mock_request):
        mock_request.return_value = self.good_response
        """Tests that log_group and log_stream are properly set as headers when provided."""
        log_group = "test-log-group"
        log_stream = "test-log-stream"

        exporter = OTLPAwsLogExporter(
            session=get_aws_session(),
            aws_region="us-east-1",
            endpoint=self._ENDPOINT,
            log_group=log_group,
            log_stream=log_stream,
        )

        result = exporter.export(self.logs)

        mock_request.assert_called_once()
        self.assertEqual(result, LogExportResult.SUCCESS)

        # Verify headers contain log group and stream
        session_headers = exporter._session.headers
        self.assertIn("x-aws-log-group", session_headers)
        self.assertIn("x-aws-log-stream", session_headers)
        self.assertEqual(session_headers["x-aws-log-group"], log_group)
        self.assertEqual(session_headers["x-aws-log-stream"], log_stream)

    @patch("requests.Session.post")
    def test_should_not_make_request_when_deadline_exceeded_immediately(self, mock_request):
        """Tests that no HTTP request is made when deadline is already exceeded before first attempt."""
        self.exporter._timeout = 0  # Deadline already exceeded

        result = self.exporter.export(self.logs)

        # Should not make any HTTP requests
        mock_request.assert_not_called()
        self.assertEqual(result, LogExportResult.FAILURE)

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

            log_data = LogData(log_record=record, instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"))

            logs.append(log_data)

        return logs
