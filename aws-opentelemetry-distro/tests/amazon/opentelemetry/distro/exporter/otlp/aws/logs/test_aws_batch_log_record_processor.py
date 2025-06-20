# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
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
        self.max_log_size = self.processor._MAX_LOG_REQUEST_BYTE_SIZE
        self.base_log_size = self.processor._BASE_LOG_BUFFER_BYTE_SIZE

    def test_process_log_data_nested_structure(self):
        """Tests that the processor correctly handles nested structures (dict/list)"""
        message_size = 400
        message = "X" * message_size

        nest_dict_log = self.generate_test_log_data(
            log_body=message, attr_key="t", attr_val=message, log_body_depth=2, attr_depth=2, count=1, create_map=True
        )
        nest_array_log = self.generate_test_log_data(
            log_body=message, attr_key="t", attr_val=message, log_body_depth=2, attr_depth=2, count=1, create_map=False
        )

        expected_size = self.base_log_size + message_size * 2

        dict_size = self.processor._estimate_log_size(log=nest_dict_log[0], depth=2)
        array_size = self.processor._estimate_log_size(log=nest_array_log[0], depth=2)

        # Asserting almost equal to account for dictionary keys in the Log object
        self.assertAlmostEqual(dict_size, expected_size, delta=10)
        self.assertAlmostEqual(array_size, expected_size, delta=10)

    def test_process_log_data_nested_structure_exceeds_depth(self):
        """Tests that the processor cuts off calculation for nested structure that exceeds the depth limit"""
        calculated = "X" * 400
        message = {"calculated": calculated, "truncated": {"truncated": {"test": "X" * self.max_log_size}}}

        # *2 since we set this message in both body and attributes
        expected_size = self.base_log_size + (len("calculated") + len(calculated) + len("truncated")) * 2

        nest_dict_log = self.generate_test_log_data(
            log_body=message, attr_key="t", attr_val=message, log_body_depth=3, attr_depth=3, count=1, create_map=True
        )
        nest_array_log = self.generate_test_log_data(
            log_body=message, attr_key="t", attr_val=message, log_body_depth=3, attr_depth=3, count=1, create_map=False
        )

        # Only calculates log size of up to depth of 4
        dict_size = self.processor._estimate_log_size(log=nest_dict_log[0], depth=4)
        array_size = self.processor._estimate_log_size(log=nest_array_log[0], depth=4)

        # Asserting almost equal to account for dictionary keys in the Log object body
        self.assertAlmostEqual(dict_size, expected_size, delta=10)
        self.assertAlmostEqual(array_size, expected_size, delta=10)

    def test_process_log_data_nested_structure_size_exceeds_max_log_size(self):
        """Tests that the processor returns prematurely if the size already exceeds _MAX_LOG_REQUEST_BYTE_SIZE"""
        # Should stop calculation at bigKey
        message = {
            "bigKey": "X" * (self.max_log_size),
            "smallKey": "X" * (self.max_log_size * 10),
        }

        expected_size = self.base_log_size + self.max_log_size + len("bigKey")

        nest_dict_log = self.generate_test_log_data(
            log_body=message, attr_key="", attr_val="", log_body_depth=-1, attr_depth=-1, count=1, create_map=True
        )
        nest_array_log = self.generate_test_log_data(
            log_body=message, attr_key="", attr_val="", log_body_depth=-1, attr_depth=-1, count=1, create_map=False
        )

        dict_size = self.processor._estimate_log_size(log=nest_dict_log[0])
        array_size = self.processor._estimate_log_size(log=nest_array_log[0])

        self.assertAlmostEqual(dict_size, expected_size, delta=10)
        self.assertAlmostEqual(array_size, expected_size, delta=10)

    def test_process_log_data_primitive(self):

        primitives: List[AnyValue] = ["test", b"test", 1, 1.2, True, False, None]
        expected_sizes = [4, 4, 1, 3, 4, 5, 0]

        for i in range(len(primitives)):
            log = self.generate_test_log_data(
                log_body=primitives[i],
                attr_key="",
                attr_val="",
                log_body_depth=-1,
                attr_depth=-1,
                count=1,
            )

            expected_size = self.base_log_size + expected_sizes[i]
            actual_size = self.processor._estimate_log_size(log[0])

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
        test_logs = self.generate_test_log_data(
            log_body=log_body, attr_key="", attr_val="", log_body_depth=-1, attr_depth=-1, count=log_count
        )
        total_data_size = 0

        for log in test_logs:
            size = self.processor._estimate_log_size(log)
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
        non_gen_ai_test_logs = self.generate_test_log_data(
            log_body=large_log_body, attr_key="", attr_val="", log_body_depth=-1, attr_depth=-1, count=3
        )
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
                    log_body=large_log_body,
                    attr_key="",
                    attr_val="",
                    log_body_depth=-1,
                    attr_depth=-1,
                    count=3,
                    instrumentation_scope=InstrumentationScope(gen_ai_scope, "1.0.0"),
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
        large_log_body = "X" * (self.max_log_size + 1)
        small_log_body = "X" * (self.max_log_size // 10 - self.base_log_size)

        gen_ai_scope = InstrumentationScope("openinference.instrumentation.langchain", "1.0.0")

        large_logs = self.generate_test_log_data(
            log_body=large_log_body,
            attr_key="",
            attr_val="",
            log_body_depth=-1,
            attr_depth=-1,
            count=3,
            instrumentation_scope=gen_ai_scope,
        )

        small_logs = self.generate_test_log_data(
            log_body=small_log_body,
            attr_key="",
            attr_val="",
            log_body_depth=-1,
            attr_depth=-1,
            count=12,
            instrumentation_scope=gen_ai_scope,
        )

        # 1st, 2nd, 3rd batch = size 1
        # 4th batch = size 10
        # 5th batch = size 2
        test_logs = large_logs + small_logs

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

    @staticmethod
    def generate_test_log_data(
        log_body,
        attr_key,
        attr_val,
        log_body_depth=3,
        attr_depth=3,
        count=5,
        create_map=True,
        instrumentation_scope=InstrumentationScope("test-scope", "1.0.0"),
    ) -> List[LogData]:

        def generate_nested_value(depth, value, create_map=True) -> AnyValue:
            if depth < 0:
                return value

            if create_map:
                return {"t": generate_nested_value(depth - 1, value, True)}

            return [generate_nested_value(depth - 1, value, False)]

        logs = []

        for i in range(count):
            record = LogRecord(
                timestamp=int(time.time_ns()),
                trace_id=int(f"0x{i + 1:032x}", 16),
                span_id=int(f"0x{i + 1:016x}", 16),
                trace_flags=TraceFlags(1),
                severity_text="INFO",
                severity_number=SeverityNumber.INFO,
                body=generate_nested_value(log_body_depth, log_body, create_map),
                attributes={attr_key: generate_nested_value(attr_depth, attr_val, create_map)},
            )

            log_data = LogData(log_record=record, instrumentation_scope=instrumentation_scope)
            logs.append(log_data)

        return logs
