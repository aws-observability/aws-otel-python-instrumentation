import time
import unittest
from typing import List
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor import (
    AwsBatchLogRecordProcessor,
    BatchLogExportStrategy,
)
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.sdk._logs import LogData, LogRecord
from opentelemetry.sdk._logs.export import LogExportResult
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags
from opentelemetry.util.types import AnyValue


class TestAwsBatchLogRecordProcessor(unittest.TestCase):

    def setUp(self):
        self.mock_exporter = MagicMock()
        self.mock_exporter.export.return_value = LogExportResult.SUCCESS

        self.processor = AwsBatchLogRecordProcessor(exporter=self.mock_exporter)

    def test_process_log_data_nested_structure(self):
        """Tests that the processor correctly handles nested structures (dict/list)"""
        message_size = 400
        depth = 2

        nested_dict_log_body = self.generate_nested_log_body(
            depth=depth, expected_body="X" * message_size, create_map=True
        )
        nested_array_log_body = self.generate_nested_log_body(
            depth=depth, expected_body="X" * message_size, create_map=False
        )

        dict_size = self.processor._get_any_value_size(val=nested_dict_log_body, depth=depth)
        array_size = self.processor._get_any_value_size(val=nested_array_log_body, depth=depth)

        # Asserting almost equal to account for key lengths in the Log object body
        self.assertAlmostEqual(dict_size, message_size, delta=20)
        self.assertAlmostEqual(array_size, message_size, delta=20)

    def test_process_log_data_nested_structure_exceeds_depth(self):
        """Tests that the processor returns 0 for nested structure that exceeds the depth limit"""
        message_size = 400
        log_body = "X" * message_size

        nested_dict_log_body = self.generate_nested_log_body(depth=4, expected_body=log_body, create_map=True)
        nested_array_log_body = self.generate_nested_log_body(depth=4, expected_body=log_body, create_map=False)

        dict_size = self.processor._get_any_value_size(val=nested_dict_log_body, depth=3)
        array_size = self.processor._get_any_value_size(val=nested_array_log_body, depth=3)

        self.assertEqual(dict_size, 0)
        self.assertEqual(array_size, 0)

    def test_process_log_data_nested_structure_size_exceeds_max_log_size(self):
        """Tests that the processor returns prematurely if the size already exceeds _MAX_LOG_REQUEST_BYTE_SIZE"""
        log_body = {
            "smallKey": "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE // 2),
            "bigKey": "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE + 1),
        }

        nested_dict_log_body = self.generate_nested_log_body(depth=0, expected_body=log_body, create_map=True)
        nested_array_log_body = self.generate_nested_log_body(depth=0, expected_body=log_body, create_map=False)

        dict_size = self.processor._get_any_value_size(val=nested_dict_log_body)
        array_size = self.processor._get_any_value_size(val=nested_array_log_body)

        self.assertAlmostEqual(dict_size, self.processor._MAX_LOG_REQUEST_BYTE_SIZE, delta=20)
        self.assertAlmostEqual(array_size, self.processor._MAX_LOG_REQUEST_BYTE_SIZE, delta=20)

    def test_process_log_data_primitive(self):

        primitives: List[AnyValue] = ["test", b"test", 1, 1.2, True, False, None]
        expected_sizes = [4, 4, 1, 3, 4, 5, 0]

        for i in range(len(primitives)):
            body = primitives[i]
            expected_size = expected_sizes[i]

            actual_size = self.processor._get_any_value_size(body)
            self.assertEqual(actual_size, expected_size)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.set_value")
    def test_export_single_batch_under_size_limit(self, _, __, ___):
        """Tests that export is only called once if a single batch is under the size limit"""
        log_count = 10
        log_body = "test"
        test_logs = self.generate_test_log_data(count=log_count, log_body=log_body)
        total_data_size = 0

        for log in test_logs:
            size = self.processor._get_any_value_size(log.log_record.body)
            total_data_size += size
            self.processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchLogExportStrategy.EXPORT_ALL)
        args, _ = self.mock_exporter.export.call_args
        actual_batch = args[0]

        self.assertLess(total_data_size, self.processor._MAX_LOG_REQUEST_BYTE_SIZE)
        self.assertEqual(len(self.processor._queue), 0)
        self.assertEqual(len(actual_batch), log_count)
        self.mock_exporter.export.assert_called_once()
        self.mock_exporter.set_gen_ai_log_flag.assert_not_called()

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.set_value")
    def test_export_single_batch_all_logs_over_size_limit(self, _, __, ___):
        """Should make multiple export calls of batch size 1 to export logs of size > 1 MB.
        But should only call set_gen_ai_log_flag if it's a Gen AI log event."""

        large_log_body = "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE + 1)
        non_gen_ai_test_logs = self.generate_test_log_data(count=3, log_body=large_log_body)
        gen_ai_test_logs = []

        gen_ai_scopes = [
            "openinference.instrumentation.langchain",
            "openinference.instrumentation.crewai",
            "opentelemetry.instrumentation.langchain",
            "crewai.telemetry",
            "openlit.otel.tracing",
        ]

        for gen_ai_scope in gen_ai_scopes:
            gen_ai_test_logs.extend(
                self.generate_test_log_data(
                    count=1, log_body=large_log_body, instrumentation_scope=InstrumentationScope(gen_ai_scope, "1.0.0")
                )
            )

        test_logs = gen_ai_test_logs + non_gen_ai_test_logs

        for log in test_logs:
            self.processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchLogExportStrategy.EXPORT_ALL)

        self.assertEqual(len(self.processor._queue), 0)
        self.assertEqual(self.mock_exporter.export.call_count, 3 + len(gen_ai_test_logs))
        self.assertEqual(self.mock_exporter.set_gen_ai_log_flag.call_count, len(gen_ai_test_logs))

        batches = self.mock_exporter.export.call_args_list

        for batch in batches:
            self.assertEqual(len(batch[0]), 1)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs.aws_batch_log_record_processor.set_value")
    def test_export_single_batch_some_logs_over_size_limit(self, _, __, ___):
        """Should make calls to export smaller sub-batch logs"""
        large_log_body = "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE + 1)
        gen_ai_scope = InstrumentationScope("openinference.instrumentation.langchain", "1.0.0")
        small_log_body = "X" * (
            int(self.processor._MAX_LOG_REQUEST_BYTE_SIZE / 10) - self.processor._BASE_LOG_BUFFER_BYTE_SIZE
        )
        test_logs = self.generate_test_log_data(count=3, log_body=large_log_body, instrumentation_scope=gen_ai_scope)
        # 1st, 2nd, 3rd batch = size 1
        # 4th batch = size 10
        # 5th batch = size 2
        small_logs = self.generate_test_log_data(count=12, log_body=small_log_body, instrumentation_scope=gen_ai_scope)

        test_logs.extend(small_logs)

        for log in test_logs:
            self.processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchLogExportStrategy.EXPORT_ALL)

        self.assertEqual(len(self.processor._queue), 0)
        self.assertEqual(self.mock_exporter.export.call_count, 5)
        self.assertEqual(self.mock_exporter.set_gen_ai_log_flag.call_count, 3)

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

    def generate_test_log_data(
        self, log_body: AnyValue, count=5, instrumentation_scope=InstrumentationScope("test-scope", "1.0.0")
    ) -> List[LogData]:
        logs = []
        for i in range(count):
            record = LogRecord(
                timestamp=int(time.time_ns()),
                trace_id=int(f"0x{i + 1:032x}", 16),
                span_id=int(f"0x{i + 1:016x}", 16),
                trace_flags=TraceFlags(1),
                severity_text="INFO",
                severity_number=SeverityNumber.INFO,
                body=log_body,
                attributes={"test.attribute": f"value-{i + 1}"},
            )

            log_data = LogData(log_record=record, instrumentation_scope=instrumentation_scope)
            logs.append(log_data)

        return logs

    @staticmethod
    def generate_nested_log_body(depth=0, expected_body: AnyValue = "test", create_map=True):
        if depth < 0:
            return expected_body

        if create_map:
            return {
                "key": TestAwsBatchLogRecordProcessor.generate_nested_log_body(depth - 1, expected_body, create_map)
            }

        return [TestAwsBatchLogRecordProcessor.generate_nested_log_body(depth - 1, expected_body, create_map)]
