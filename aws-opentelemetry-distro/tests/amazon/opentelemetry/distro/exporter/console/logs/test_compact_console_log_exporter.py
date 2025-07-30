# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import unittest
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.exporter.console.logs.compact_console_log_exporter import CompactConsoleLogExporter
from opentelemetry.sdk._logs.export import LogExportResult


class TestCompactConsoleLogExporter(unittest.TestCase):

    def setUp(self):
        self.exporter = CompactConsoleLogExporter()

    @patch("builtins.print")
    def test_export_compresses_json(self, mock_print):
        # Mock log data
        mock_log_data = Mock()
        mock_log_record = Mock()
        mock_log_data.log_record = mock_log_record

        # Mock formatted JSON with whitespace
        formatted_json = '{\n    "body": "test message",\n    "severity_number": 9,\n    "attributes": {\n        "key": "value"\n    }\n}'  # noqa: E501
        self.exporter.formatter = Mock(return_value=formatted_json)

        # Call export
        result = self.exporter.export([mock_log_data])

        # Verify result
        self.assertEqual(result, LogExportResult.SUCCESS)

        # Verify print calls
        self.assertEqual(mock_print.call_count, 1)
        mock_print.assert_called_with(
            '{"body":"test message","severity_number":9,"attributes":{"key":"value"}}', flush=True
        )

    @patch("builtins.print")
    def test_export_multiple_records(self, mock_print):
        # Mock multiple log data
        mock_log_data1 = Mock()
        mock_log_data2 = Mock()
        mock_log_data1.log_record = Mock()
        mock_log_data2.log_record = Mock()

        formatted_json = '{\n    "body": "test"\n}'
        self.exporter.formatter = Mock(return_value=formatted_json)

        # Call export
        result = self.exporter.export([mock_log_data1, mock_log_data2])

        # Verify result
        self.assertEqual(result, LogExportResult.SUCCESS)

        # Verify print calls
        self.assertEqual(mock_print.call_count, 2)  # 2 records
        # Each record should print compact JSON
        expected_calls = [unittest.mock.call('{"body":"test"}', flush=True)] * 2
        mock_print.assert_has_calls(expected_calls)

    @patch("builtins.print")
    def test_export_empty_batch(self, mock_print):
        # Call export with empty batch
        result = self.exporter.export([])

        # Verify result
        self.assertEqual(result, LogExportResult.SUCCESS)

        # Verify print calls
        mock_print.assert_not_called()  # No records, no prints
