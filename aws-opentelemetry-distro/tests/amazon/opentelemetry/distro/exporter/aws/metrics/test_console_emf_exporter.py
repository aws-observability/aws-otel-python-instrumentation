# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import patch
from io import StringIO

from amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter import ConsoleEmfExporter


class TestConsoleEmfExporter(unittest.TestCase):
    """Test ConsoleEmfExporter class."""

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

    def test_send_log_event(self):
        """Test that log events are properly sent to console output."""
        exporter = ConsoleEmfExporter()
        
        # Create a simple log event with EMF-formatted message
        test_message = '{"_aws":{"Timestamp":1640995200000,"CloudWatchMetrics":[{"Namespace":"TestNamespace","Dimensions":[["Service"]],"Metrics":[{"Name":"TestMetric","Unit":"Count"}]}]},"Service":"test-service","TestMetric":42}'
        log_event = {
            "message": test_message,
            "timestamp": 1640995200000
        }
        
        # Capture stdout to verify the output
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            exporter._send_log_event(log_event)
            
        # Verify the message was printed to stdout
        captured_output = mock_stdout.getvalue().strip()
        self.assertEqual(captured_output, test_message)


if __name__ == "__main__":
    unittest.main()
