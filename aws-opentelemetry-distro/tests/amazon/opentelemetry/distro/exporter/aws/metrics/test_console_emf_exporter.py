# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter import ConsoleEmfExporter
from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics.export import AggregationTemporality, MetricExportResult, MetricsData


class TestConsoleEmfExporter(unittest.TestCase):
    """Test ConsoleEmfExporter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = ConsoleEmfExporter()

    def test_namespace_initialization(self):
        """Test exporter initialization with different namespace scenarios."""
        # Test default namespace
        exporter = ConsoleEmfExporter()
        self.assertEqual(exporter.namespace, "default")

        # Test custom namespace
        exporter = ConsoleEmfExporter(namespace="CustomNamespace")
        self.assertEqual(exporter.namespace, "CustomNamespace")

        # Test None namespace (should default to 'default')
        exporter = ConsoleEmfExporter(namespace=None)
        self.assertEqual(exporter.namespace, "default")

        # Test empty string namespace (should remain empty)
        exporter = ConsoleEmfExporter(namespace="")
        self.assertEqual(exporter.namespace, "")

    def test_initialization_with_parameters(self):
        """Test exporter initialization with optional parameters."""
        # Test with preferred_temporality
        preferred_temporality = {Counter: AggregationTemporality.CUMULATIVE}
        exporter = ConsoleEmfExporter(namespace="TestNamespace", preferred_temporality=preferred_temporality)
        self.assertEqual(exporter.namespace, "TestNamespace")
        self.assertEqual(exporter._preferred_temporality[Counter], AggregationTemporality.CUMULATIVE)

        # Test with preferred_aggregation
        preferred_aggregation = {Counter: "TestAggregation"}
        exporter = ConsoleEmfExporter(preferred_aggregation=preferred_aggregation)
        self.assertEqual(exporter._preferred_aggregation[Counter], "TestAggregation")

        # Test with additional kwargs
        exporter = ConsoleEmfExporter(namespace="TestNamespace", extra_param="ignored")  # Should be ignored
        self.assertEqual(exporter.namespace, "TestNamespace")

    def test_export_log_event_success(self):
        """Test that log events are properly sent to console output."""
        # Create a simple log event with EMF-formatted message
        test_message = (
            '{"_aws":{"Timestamp":1640995200000,"CloudWatchMetrics":[{"Namespace":"TestNamespace",'
            '"Dimensions":[["Service"]],"Metrics":[{"Name":"TestMetric","Unit":"Count"}]}]},'
            '"Service":"test-service","TestMetric":42}'
        )
        log_event = {"message": test_message, "timestamp": 1640995200000}

        # Capture stdout to verify the output
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            self.exporter._export(log_event)

        # Verify the message was printed to stdout with flush
        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, test_message)

    def test_export_log_event_empty_message(self):
        """Test handling of log events with empty messages."""
        log_event = {"message": "", "timestamp": 1640995200000}

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
                self.exporter._export(log_event)

        # Should not print anything for empty message
        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, "")

        # Should log a warning
        mock_logger.warning.assert_called_once()

    def test_export_log_event_missing_message(self):
        """Test handling of log events without message key."""
        log_event = {"timestamp": 1640995200000}

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
                self.exporter._export(log_event)

        # Should not print anything when message is missing
        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, "")

        # Should log a warning
        mock_logger.warning.assert_called_once()

    def test_export_log_event_with_none_message(self):
        """Test handling of log events with None message."""
        log_event = {"message": None, "timestamp": 1640995200000}

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
                self.exporter._export(log_event)

        # Should not print anything when message is None
        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, "")

        # Should log a warning
        mock_logger.warning.assert_called_once()

    def test_export_log_event_print_exception(self):
        """Test error handling when print() raises an exception."""
        log_event = {"message": "test message", "timestamp": 1640995200000}

        with patch("builtins.print", side_effect=Exception("Print failed")):
            with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
                self.exporter._export(log_event)

        # Should log the error
        mock_logger.error.assert_called_once()
        args = mock_logger.error.call_args[0]
        self.assertIn("Failed to write EMF log to console", args[0])
        self.assertEqual(args[1], log_event)
        self.assertIn("Print failed", str(args[2]))

    def test_export_log_event_various_message_types(self):
        """Test _export with various message types."""
        # Test with JSON string
        json_message = '{"key": "value"}'
        log_event = {"message": json_message, "timestamp": 1640995200000}

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            self.exporter._export(log_event)

        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, json_message)

        # Test with plain string
        plain_message = "Simple log message"
        log_event = {"message": plain_message, "timestamp": 1640995200000}

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            self.exporter._export(log_event)

        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, plain_message)

    def test_force_flush(self):
        """Test force_flush method."""
        with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
            # Test with default timeout
            result = self.exporter.force_flush()
            self.assertTrue(result)
            mock_logger.debug.assert_called_once()

            # Reset mock for next call
            mock_logger.reset_mock()

            # Test with custom timeout
            result = self.exporter.force_flush(timeout_millis=5000)
            self.assertTrue(result)
            mock_logger.debug.assert_called_once()

    def test_shutdown(self):
        """Test shutdown method."""
        with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
            # Test with no timeout
            result = self.exporter.shutdown()
            self.assertTrue(result)
            mock_logger.debug.assert_called_once_with("ConsoleEmfExporter shutdown called with timeout_millis=%s", None)

            # Reset mock for next call
            mock_logger.reset_mock()

            # Test with timeout
            result = self.exporter.shutdown(timeout_millis=3000)
            self.assertTrue(result)
            mock_logger.debug.assert_called_once_with("ConsoleEmfExporter shutdown called with timeout_millis=%s", 3000)

            # Reset mock for next call
            mock_logger.reset_mock()

            # Test with additional kwargs
            result = self.exporter.shutdown(timeout_millis=3000, extra_arg="ignored")
            self.assertTrue(result)
            mock_logger.debug.assert_called_once()

    def test_integration_with_metrics_data(self):
        """Test the full export flow with actual MetricsData."""
        # Create a mock MetricsData
        mock_metrics_data = MagicMock(spec=MetricsData)
        mock_resource_metrics = MagicMock()
        mock_scope_metrics = MagicMock()
        mock_metric = MagicMock()

        # Set up the mock hierarchy
        mock_metrics_data.resource_metrics = [mock_resource_metrics]
        mock_resource_metrics.scope_metrics = [mock_scope_metrics]
        mock_scope_metrics.metrics = [mock_metric]

        # Mock the metric to have no data_points to avoid complex setup
        mock_metric.data = None

        with patch("sys.stdout", new_callable=StringIO):
            result = self.exporter.export(mock_metrics_data)

        # Should succeed even with no actual metrics
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_integration_export_success(self):
        """Test export method returns success."""
        # Create empty MetricsData
        mock_metrics_data = MagicMock(spec=MetricsData)
        mock_metrics_data.resource_metrics = []

        result = self.exporter.export(mock_metrics_data)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_integration_export_with_timeout(self):
        """Test export method with timeout parameter."""
        mock_metrics_data = MagicMock(spec=MetricsData)
        mock_metrics_data.resource_metrics = []

        result = self.exporter.export(mock_metrics_data, timeout_millis=5000)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_export_failure_handling(self):
        """Test export method handles exceptions and returns failure."""
        # Create a mock that raises an exception
        mock_metrics_data = MagicMock(spec=MetricsData)
        mock_metrics_data.resource_metrics = [MagicMock()]

        # Make the resource_metrics access raise an exception
        type(mock_metrics_data).resource_metrics = property(
            lambda self: (_ for _ in ()).throw(Exception("Test exception"))
        )

        # Patch the logger in the base_emf_exporter since that's where the error logging happens
        with patch("amazon.opentelemetry.distro.exporter.aws.metrics.base_emf_exporter.logger") as mock_logger:
            result = self.exporter.export(mock_metrics_data)

            self.assertEqual(result, MetricExportResult.FAILURE)
            mock_logger.error.assert_called_once()
            self.assertIn("Failed to export metrics", mock_logger.error.call_args[0][0])

    def test_flush_output_verification(self):
        """Test that print is called with flush=True."""
        log_event = {"message": "test message", "timestamp": 1640995200000}

        with patch("builtins.print") as mock_print:
            self.exporter._export(log_event)

        # Verify print was called with flush=True
        mock_print.assert_called_once_with("test message", flush=True)

    def test_logger_levels(self):
        """Test that appropriate log levels are used."""
        # Test debug logging in force_flush
        with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
            self.exporter.force_flush()
            mock_logger.debug.assert_called_once()

        # Test debug logging in shutdown
        with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
            self.exporter.shutdown()
            mock_logger.debug.assert_called_once()

        # Test warning logging for empty message
        with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
            self.exporter._export({"message": "", "timestamp": 123})
            mock_logger.warning.assert_called_once()

        # Test error logging for exception
        with patch("builtins.print", side_effect=Exception("Test")):
            with patch("amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.logger") as mock_logger:
                self.exporter._export({"message": "test", "timestamp": 123})
                mock_logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()
