# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
import unittest
from typing import List
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor import (
    AwsCloudWatchOtlpBatchLogRecordProcessor,
)
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.sdk._logs import LogData, LogRecord
from opentelemetry.sdk._logs.export import LogExportResult
from opentelemetry.sdk._shared_internal import BatchExportStrategy
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags
from opentelemetry.util.types import AnyValue


class TestAwsBatchLogRecordProcessor(unittest.TestCase):

    def setUp(self):
        self.mock_exporter = MagicMock()
        self.mock_exporter.export.return_value = LogExportResult.SUCCESS

        self.processor = AwsCloudWatchOtlpBatchLogRecordProcessor(exporter=self.mock_exporter)

    def test_process_log_data_nested_structure(self):
        """Tests that the processor correctly handles nested structures (dict/list)"""
        log_body = "X" * 400
        log_key = "test"
        log_depth = 2

        nested_dict_log = self.generate_test_log_data(
            log_body=log_body, log_key=log_key, log_body_depth=log_depth, count=1, create_map=True
        )
        nested_array_log = self.generate_test_log_data(
            log_body=log_body, log_key=log_key, log_body_depth=log_depth, count=1, create_map=False
        )

        expected_dict_size = len(log_key) * log_depth + len(log_body)
        expected_array_size = len(log_body)

        dict_size = self.processor._estimate_log_size(log=nested_dict_log[0], depth=log_depth)
        array_size = self.processor._estimate_log_size(log=nested_array_log[0], depth=log_depth)

        self.assertEqual(dict_size - self.processor._BASE_LOG_BUFFER_BYTE_SIZE, expected_dict_size)
        self.assertEqual(array_size - self.processor._BASE_LOG_BUFFER_BYTE_SIZE, expected_array_size)

    def test_process_log_data_with_attributes(self):
        """Tests that the processor correctly handles both body and attributes"""
        log_body = "test_body"
        attr_key = "attr_key"
        attr_value = "attr_value"

        record = LogRecord(
            timestamp=int(time.time_ns()),
            trace_id=0x123456789ABCDEF0123456789ABCDEF0,
            span_id=0x123456789ABCDEF0,
            trace_flags=TraceFlags(1),
            severity_text="INFO",
            severity_number=SeverityNumber.INFO,
            body=log_body,
            attributes={attr_key: attr_value},
        )
        log_data = LogData(log_record=record, instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"))

        expected_size = len(log_body) + len(attr_key) + len(attr_value)
        actual_size = self.processor._estimate_log_size(log_data)

        self.assertEqual(actual_size - self.processor._BASE_LOG_BUFFER_BYTE_SIZE, expected_size)

    def test_process_log_data_nested_structure_exceeds_depth(self):
        """Tests that the processor cuts off calculation for nested structure that exceeds the depth limit"""
        max_depth = 0
        calculated_body = "X" * 400
        log_body = {
            "calculated": "X" * 400,
            "restOfThisLogWillBeTruncated": {"truncated": {"test": "X" * self.processor._MAX_LOG_REQUEST_BYTE_SIZE}},
        }

        expected_size = self.processor._BASE_LOG_BUFFER_BYTE_SIZE + (
            len("calculated") + len(calculated_body) + len("restOfThisLogWillBeTruncated")
        )

        test_logs = self.generate_test_log_data(log_body=log_body, count=1)

        # Only calculates log size of up to depth of 0
        dict_size = self.processor._estimate_log_size(log=test_logs[0], depth=max_depth)

        self.assertEqual(dict_size, expected_size)

    def test_process_log_data_nested_structure_size_exceeds_max_log_size(self):
        """Tests that the processor returns prematurely if the size already exceeds _MAX_LOG_REQUEST_BYTE_SIZE"""
        # Should stop calculation at bigKey + biggerKey and not calculate the content of biggerKey
        log_body = {
            "bigKey": "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE),
            "biggerKey": "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE * 100),
        }

        expected_size = (
            self.processor._BASE_LOG_BUFFER_BYTE_SIZE
            + self.processor._MAX_LOG_REQUEST_BYTE_SIZE
            + len("bigKey")
            + len("biggerKey")
        )

        nest_dict_log = self.generate_test_log_data(log_body=log_body, count=1, create_map=True)
        nest_array_log = self.generate_test_log_data(log_body=log_body, count=1, create_map=False)

        dict_size = self.processor._estimate_log_size(log=nest_dict_log[0])
        array_size = self.processor._estimate_log_size(log=nest_array_log[0])

        self.assertEqual(dict_size, expected_size)
        self.assertEqual(array_size, expected_size)

    def test_process_log_data_primitive(self):

        primitives: List[AnyValue] = ["test", b"test", 1, 1.2, True, False, None, "深入 Python", "calfé"]
        expected_sizes = [4, 4, 1, 3, 4, 5, 0, 2 * 4 + len(" Python"), 1 * 4 + len("calf")]

        for index, primitive in enumerate(primitives):
            log = self.generate_test_log_data(log_body=primitive, count=1)
            expected_size = self.processor._BASE_LOG_BUFFER_BYTE_SIZE + expected_sizes[index]
            actual_size = self.processor._estimate_log_size(log[0])
            self.assertEqual(actual_size, expected_size)

    def test_process_log_data_with_cycle(self):
        """Test that processor handles processing logs with circular references only once"""
        cyclic_dict: dict = {"data": "test"}
        cyclic_dict["self_ref"] = cyclic_dict

        log = self.generate_test_log_data(log_body=cyclic_dict, count=1)
        expected_size = self.processor._BASE_LOG_BUFFER_BYTE_SIZE + len("data") + len("self_ref") + len("test")
        actual_size = self.processor._estimate_log_size(log[0])
        self.assertEqual(actual_size, expected_size)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.set_value")
    def test_export_single_batch_under_size_limit(self, _, __, ___):
        """Tests that export is only called once if a single batch is under the size limit"""
        log_count = 10
        log_body = "test"
        test_logs = self.generate_test_log_data(log_body=log_body, count=log_count)
        total_data_size = 0

        for log in test_logs:
            size = self.processor._estimate_log_size(log)
            total_data_size += size
            self.processor._batch_processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchExportStrategy.EXPORT_ALL)
        args, _ = self.mock_exporter.export.call_args
        actual_batch = args[0]

        self.assertLess(total_data_size, self.processor._MAX_LOG_REQUEST_BYTE_SIZE)
        self.assertEqual(len(self.processor._batch_processor._queue), 0)
        self.assertEqual(len(actual_batch), log_count)
        self.mock_exporter.export.assert_called_once()

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.set_value")
    def test_export_single_batch_all_logs_over_size_limit(self, _, __, ___):
        """Should make multiple export calls of batch size 1 to export logs of size > 1 MB."""

        large_log_body = "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE + 1)
        test_logs = self.generate_test_log_data(log_body=large_log_body, count=15)

        for log in test_logs:
            self.processor._batch_processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchExportStrategy.EXPORT_ALL)

        self.assertEqual(len(self.processor._batch_processor._queue), 0)
        self.assertEqual(self.mock_exporter.export.call_count, len(test_logs))

        batches = self.mock_exporter.export.call_args_list

        for batch in batches:
            self.assertEqual(len(batch[0]), 1)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.set_value")
    def test_export_single_batch_some_logs_over_size_limit(self, _, __, ___):
        """Should make calls to export smaller sub-batch logs"""
        large_log_body = "X" * (self.processor._MAX_LOG_REQUEST_BYTE_SIZE + 1)
        small_log_body = "X" * (
            self.processor._MAX_LOG_REQUEST_BYTE_SIZE // 10 - self.processor._BASE_LOG_BUFFER_BYTE_SIZE
        )

        large_logs = self.generate_test_log_data(log_body=large_log_body, count=3)
        small_logs = self.generate_test_log_data(log_body=small_log_body, count=12)

        # 1st, 2nd, 3rd batch = size 1
        # 4th batch = size 10
        # 5th batch = size 2
        test_logs = large_logs + small_logs

        for log in test_logs:
            self.processor._batch_processor._queue.appendleft(log)

        self.processor._export(batch_strategy=BatchExportStrategy.EXPORT_ALL)

        self.assertEqual(len(self.processor._batch_processor._queue), 0)
        self.assertEqual(self.mock_exporter.export.call_count, 5)

        batches = self.mock_exporter.export.call_args_list

        expected_sizes = {
            0: 1,  # 1st batch (index 1) should have 1 log
            1: 1,  # 2nd batch (index 1) should have 1 log
            2: 1,  # 3rd batch (index 2) should have 1 log
            3: 10,  # 4th batch (index 3) should have 10 logs
            4: 2,  # 5th batch (index 4) should have 2 logs
        }

        for index, call in enumerate(batches):
            batch = call[0][0]
            expected_size = expected_sizes[index]
            self.assertEqual(len(batch), expected_size)

    def test_force_flush_returns_false_when_shutdown(self):
        """Tests that force_flush returns False when processor is shutdown"""
        self.processor.shutdown()
        result = self.processor.force_flush()

        # Verify force_flush returns False and no export is called
        self.assertFalse(result)
        self.mock_exporter.export.assert_not_called()

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.set_value")
    def test_force_flush_exports_only_one_batch(self, _, __, ___):
        """Tests that force_flush should try to at least export one batch of logs. Rest of the logs will be dropped"""
        # Set max_export_batch_size to 5 to limit batch size
        self.processor._batch_processor._max_export_batch_size = 5
        self.processor._batch_processor._shutdown = False

        # Add 6 logs to queue, after the export there should be 1 log remaining
        log_count = 6
        test_logs = self.generate_test_log_data(log_body="test message", count=log_count)

        for log in test_logs:
            self.processor._batch_processor._queue.appendleft(log)

        self.assertEqual(len(self.processor._batch_processor._queue), log_count)

        result = self.processor.force_flush()

        self.assertTrue(result)
        self.assertEqual(len(self.processor._batch_processor._queue), 1)
        self.mock_exporter.export.assert_called_once()

        # Verify only one batch of 5 logs was exported
        args, _ = self.mock_exporter.export.call_args
        exported_batch = args[0]
        self.assertEqual(len(exported_batch), 5)

    @patch(
        "amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.attach",
        return_value=MagicMock(),
    )
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.detach")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor.set_value")
    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor._logger")
    def test_export_handles_exception_gracefully(self, mock_logger, _, __, ___):
        """Tests that exceptions during export are caught and logged"""
        # Setup exporter to raise an exception
        self.mock_exporter.export.side_effect = Exception("Export failed")

        # Add logs to queue
        test_logs = self.generate_test_log_data(log_body="test message", count=2)
        for log in test_logs:
            self.processor._batch_processor._queue.appendleft(log)

        # Call _export - should not raise exception
        self.processor._export(batch_strategy=BatchExportStrategy.EXPORT_ALL)

        # Verify exception was logged
        mock_logger.exception.assert_called_once()
        call_args = mock_logger.exception.call_args[0]
        self.assertIn("Exception while exporting logs:", call_args[0])

        # Queue should be empty even though export failed
        self.assertEqual(len(self.processor._batch_processor._queue), 0)

    @patch("amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor._logger")
    def test_estimate_log_size_debug_logging_on_depth_exceeded(self, mock_logger):
        """Tests that debug logging occurs when depth limit is exceeded"""
        # Create deeply nested structure that exceeds depth limit
        depth_limit = 1
        log_body = {"level1": {"level2": {"level3": {"level4": "this should trigger debug log"}}}}

        test_logs = self.generate_test_log_data(log_body=log_body, count=1)

        # Call with limited depth that will be exceeded
        self.processor._estimate_log_size(log=test_logs[0], depth=depth_limit)

        # Verify debug logging was called
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        self.assertIn("Max log depth of", call_args[0])
        self.assertIn("exceeded", call_args[0])

    def test_estimate_utf8_size_static_method(self):
        """Tests the _estimate_utf8_size static method with various strings"""
        # Test ASCII only string
        ascii_result = AwsCloudWatchOtlpBatchLogRecordProcessor._estimate_utf8_size("hello")
        self.assertEqual(ascii_result, 5)  # 5 ASCII chars = 5 bytes

        # Test mixed ASCII and non-ASCII
        mixed_result = AwsCloudWatchOtlpBatchLogRecordProcessor._estimate_utf8_size("café")
        self.assertEqual(mixed_result, 7)  # 3 ASCII + 1 non-ASCII (4 bytes) = 7 bytes

        # Test non-ASCII only
        non_ascii_result = AwsCloudWatchOtlpBatchLogRecordProcessor._estimate_utf8_size("深入")
        self.assertEqual(non_ascii_result, 8)  # 2 non-ASCII chars * 4 bytes = 8 bytes

        # Test empty string
        empty_result = AwsCloudWatchOtlpBatchLogRecordProcessor._estimate_utf8_size("")
        self.assertEqual(empty_result, 0)

    def test_constructor_with_custom_parameters(self):
        """Tests constructor with custom parameters"""
        custom_processor = AwsCloudWatchOtlpBatchLogRecordProcessor(
            exporter=self.mock_exporter,
            schedule_delay_millis=5000,
            max_export_batch_size=100,
            export_timeout_millis=10000,
            max_queue_size=2000,
        )

        # Verify exporter is stored
        self.assertEqual(custom_processor._exporter, self.mock_exporter)

        # Verify parameters are passed to parent constructor
        self.assertEqual(custom_processor._batch_processor._max_export_batch_size, 100)

    @staticmethod
    def generate_test_log_data(
        log_body,
        log_key="key",
        log_body_depth=0,
        count=5,
        create_map=True,
    ) -> List[LogData]:

        def generate_nested_value(depth, value, create_map=True) -> AnyValue:
            if depth <= 0:
                return value

            if create_map:
                return {log_key: generate_nested_value(depth - 1, value, True)}

            return [generate_nested_value(depth - 1, value, False)]

        logs = []

        for _ in range(count):
            record = LogRecord(
                timestamp=int(time.time_ns()),
                trace_id=0x123456789ABCDEF0123456789ABCDEF0,
                span_id=0x123456789ABCDEF0,
                trace_flags=TraceFlags(1),
                severity_text="INFO",
                severity_number=SeverityNumber.INFO,
                body=generate_nested_value(log_body_depth, log_body, create_map),
            )

            log_data = LogData(log_record=record, instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"))
            logs.append(log_data)

        return logs
