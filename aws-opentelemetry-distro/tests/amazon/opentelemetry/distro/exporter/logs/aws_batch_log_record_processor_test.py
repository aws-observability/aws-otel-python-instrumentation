import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor import (
    BASE_LOG_BUFFER_BYTE_SIZE,
    MAX_LOG_REQUEST_BYTE_SIZE,
    AwsBatchLogRecordProcessor,
    BatchLogExportStrategy,
)
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.sdk._logs import LogData, LogRecord
from opentelemetry.sdk._logs.export import LogExportResult
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags


class TestAwsBatchLogRecordProcessor(unittest.TestCase):

    def setUp(self):
        self.mock_exporter = MagicMock()
        self.mock_exporter.export.return_value = LogExportResult.SUCCESS

        self.processor = AwsBatchLogRecordProcessor(exporter=self.mock_exporter)
        self.logs = self.generate_test_log_data()

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.set_value")
    def test_export_single_batch_under_size_limit(self, _, __, ___):
        """Tests that export is only called once if a single batch is under the size limit"""
        log_count = 10
        test_logs = self.generate_test_log_data(count=log_count)
        total_data_size = 0

        for log in test_logs:
            total_data_size += self.processor._get_size_of_log(log)
            self.processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchLogExportStrategy.EXPORT_ALL)
        args, kwargs = self.mock_exporter.export.call_args
        actual_batch = args[0]

        self.assertLess(total_data_size, MAX_LOG_REQUEST_BYTE_SIZE)
        self.assertEqual(len(self.processor._queue), 0)
        self.assertEqual(len(actual_batch), log_count)
        self.mock_exporter.export.assert_called_once()
        self.mock_exporter.set_gen_ai_flag.assert_not_called()

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.set_value")
    def test_export_single_batch_all_logs_over_size_limit(self, _, __, ___):
        """Should make multiple export calls of batch size 1 to export logs of size > 1 MB"""
        test_logs = self.generate_test_log_data(count=3, body_size=(MAX_LOG_REQUEST_BYTE_SIZE + 1))

        for log in test_logs:
            self.processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchLogExportStrategy.EXPORT_ALL)

        self.assertEqual(len(self.processor._queue), 0)
        self.assertEqual(self.mock_exporter.export.call_count, 3)
        self.assertEqual(self.mock_exporter.set_gen_ai_flag.call_count, 3)

        batches = self.mock_exporter.export.call_args_list

        for batch in batches:
            self.assertEquals(len(batch[0]), 1)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.set_value")
    def test_export_single_batch_some_logs_over_size_limit(self, _, __, ___):
        """Should make calls to export smaller sub-batch logs"""
        test_logs = self.generate_test_log_data(count=3, body_size=(MAX_LOG_REQUEST_BYTE_SIZE + 1))
        # 1st, 2nd, 3rd batch = size 1
        # 4th batch = size 10
        # 5th batch = size 2
        small_logs = self.generate_test_log_data(count=12, body_size=(104857 - BASE_LOG_BUFFER_BYTE_SIZE))

        test_logs.extend(small_logs)

        for log in test_logs:
            self.processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchLogExportStrategy.EXPORT_ALL)

        self.assertEqual(len(self.processor._queue), 0)
        self.assertEqual(self.mock_exporter.export.call_count, 5)
        self.assertEqual(self.mock_exporter.set_gen_ai_flag.call_count, 3)

        batches = self.mock_exporter.export.call_args_list

        expected_sizes = {
            0: 1,  # 1st batch (index 1) should have 1 log
            1: 1,  # 2nd batch (index 1) should have 1 log
            2: 1,  # 3rd batch (index 2) should have 1 log
            3: 10,  # 4th batch (index 3) should have 10 logs
            4: 2,  # 5th batch (index 4) should have 2 logs
        }

        for i, call in enumerate(batches):
            batch = call[0][0]
            expected_size = expected_sizes[i]
            self.assertEqual(len(batch), expected_size)

    def generate_test_log_data(self, count=5, body_size=100):
        logs = []
        for i in range(count):
            body = "X" * body_size

            record = LogRecord(
                timestamp=int(time.time_ns()),
                trace_id=int(f"0x{i + 1:032x}", 16),
                span_id=int(f"0x{i + 1:016x}", 16),
                trace_flags=TraceFlags(1),
                severity_text="INFO",
                severity_number=SeverityNumber.INFO,
                body=body,
                attributes={"test.attribute": f"value-{i + 1}"},
            )

            log_data = LogData(log_record=record, instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"))
            logs.append(log_data)

        return logs
