# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import time
import unittest
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.exporter.otlp.aws.metrics.otlp_aws_emf_exporter import (
    CloudWatchEMFExporter,
    create_emf_exporter
)
from opentelemetry.sdk.metrics.export import MetricExportResult
from opentelemetry.sdk.resources import Resource


class MockDataPoint:
    """Mock datapoint for testing."""
    def __init__(self, value=10.0, attributes=None, time_unix_nano=None):
        self.value = value
        self.attributes = attributes or {}
        self.time_unix_nano = time_unix_nano or int(time.time() * 1_000_000_000)


class MockMetric:
    """Mock metric for testing."""
    def __init__(self, name="test_metric", unit="1", description="Test metric"):
        self.name = name
        self.unit = unit
        self.description = description


class MockGaugeData:
    """Mock gauge data that passes isinstance checks."""
    def __init__(self, data_points=None):
        self.data_points = data_points or []

# Create a mock Gauge class for isinstance checks
class MockGauge:
    pass

class MockResourceMetrics:
    """Mock resource metrics for testing."""
    def __init__(self, resource=None, scope_metrics=None):
        self.resource = resource or Resource.create({"service.name": "test-service"})
        self.scope_metrics = scope_metrics or []


class MockScopeMetrics:
    """Mock scope metrics for testing."""
    def __init__(self, scope=None, metrics=None):
        self.scope = scope or Mock()
        self.metrics = metrics or []


class TestCreateEMFExporter(unittest.TestCase):
    """Test the create_emf_exporter function."""
    
    @patch('botocore.session.Session')
    def test_create_emf_exporter_default_args(self, mock_session):
        """Test creating exporter with default arguments."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client
        mock_client.describe_log_groups.return_value = {"logGroups": []}
        mock_client.create_log_group.return_value = {}
        
        exporter = create_emf_exporter()
        
        self.assertIsInstance(exporter, CloudWatchEMFExporter)
        self.assertEqual(exporter.namespace, "OTelPython")
    
    @patch('botocore.session.Session')
    def test_create_emf_exporter_custom_args(self, mock_session):
        """Test creating exporter with custom arguments."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client
        mock_client.describe_log_groups.return_value = {"logGroups": []}
        mock_client.create_log_group.return_value = {}
        
        exporter = create_emf_exporter(
            namespace="CustomNamespace",
            log_group_name="/custom/log/group",
            aws_region="us-west-2"
        )
        
        self.assertIsInstance(exporter, CloudWatchEMFExporter)
        self.assertEqual(exporter.namespace, "CustomNamespace")
        self.assertEqual(exporter.log_group_name, "/custom/log/group")
    
    @patch('botocore.session.Session')
    @patch('logging.basicConfig')
    def test_create_emf_exporter_debug_mode(self, mock_logging_config, mock_session):
        """Test creating exporter with debug mode enabled."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client
        mock_client.describe_log_groups.return_value = {"logGroups": []}
        mock_client.create_log_group.return_value = {}
        
        exporter = create_emf_exporter(debug=True)
        
        self.assertIsInstance(exporter, CloudWatchEMFExporter)
        mock_logging_config.assert_called_once()


class TestCloudWatchEMFExporter(unittest.TestCase):
    """Test CloudWatchEMFExporter class."""
    
    @patch('botocore.session.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client
        mock_client.describe_log_groups.return_value = {"logGroups": []}
        mock_client.create_log_group.return_value = {}
        
        self.exporter = CloudWatchEMFExporter(
            namespace="TestNamespace",
            log_group_name="test-log-group"
        )
    
    def test_initialization(self):
        """Test exporter initialization."""
        self.assertEqual(self.exporter.namespace, "TestNamespace")
        self.assertIsNotNone(self.exporter.log_stream_name)
        self.assertEqual(self.exporter.metric_declarations, [])
    
    @patch('botocore.session.Session')
    def test_initialization_with_custom_params(self, mock_session):
        """Test exporter initialization with custom parameters."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client
        mock_client.describe_log_groups.return_value = {"logGroups": []}
        mock_client.create_log_group.return_value = {}
        
        exporter = CloudWatchEMFExporter(
            namespace="CustomNamespace",
            log_group_name="custom-log-group",
            log_stream_name="custom-stream",
            aws_region="us-west-2"
        )
        self.assertEqual(exporter.namespace, "CustomNamespace")
        self.assertEqual(exporter.log_group_name, "custom-log-group")
        self.assertEqual(exporter.log_stream_name, "custom-stream")
    
    def test_get_unit_mapping(self):
        """Test unit mapping functionality."""
        # Test known units
        self.assertEqual(self.exporter._get_unit(Mock(unit="ms")), "Milliseconds")
        self.assertEqual(self.exporter._get_unit(Mock(unit="s")), "Seconds")
        self.assertEqual(self.exporter._get_unit(Mock(unit="By")), "Bytes")
        self.assertEqual(self.exporter._get_unit(Mock(unit="%")), "Percent")
        
        # Test unknown unit
        self.assertEqual(self.exporter._get_unit(Mock(unit="unknown")), "unknown")
        
        # Test empty unit (should return None due to falsy check)
        self.assertIsNone(self.exporter._get_unit(Mock(unit="")))
        
        # Test None unit
        self.assertIsNone(self.exporter._get_unit(Mock(unit=None)))
    
    def test_get_metric_name(self):
        """Test metric name extraction."""
        # Test with record that has instrument.name
        record = Mock()
        record.instrument = Mock()
        record.instrument.name = "test_metric"
        del record.name  # Ensure record.name doesn't exist
        
        result = self.exporter._get_metric_name(record)
        self.assertEqual(result, "test_metric")
        
        # Test with record that has direct name attribute
        record_with_name = Mock()
        record_with_name.name = "direct_metric"
        
        result2 = self.exporter._get_metric_name(record_with_name)
        self.assertEqual(result2, "direct_metric")
    
    def test_get_dimension_names(self):
        """Test dimension names extraction."""
        attributes = {"service.name": "test-service", "env": "prod", "region": "us-east-1"}
        
        result = self.exporter._get_dimension_names(attributes)
        
        # Should return all attribute keys
        self.assertEqual(set(result), {"service.name", "env", "region"})
    
    def test_get_attributes_key(self):
        """Test attributes key generation."""
        attributes = {"service": "test", "env": "prod"}
        
        result = self.exporter._get_attributes_key(attributes)
        
        # Should be a string representation of sorted attributes
        self.assertIsInstance(result, str)
        self.assertIn("service", result)
        self.assertIn("test", result)
        self.assertIn("env", result)
        self.assertIn("prod", result)
    
    def test_get_attributes_key_consistent(self):
        """Test that attributes key generation is consistent."""
        # Same attributes in different order should produce same key
        attrs1 = {"b": "2", "a": "1"}
        attrs2 = {"a": "1", "b": "2"}
        
        key1 = self.exporter._get_attributes_key(attrs1)
        key2 = self.exporter._get_attributes_key(attrs2)
        
        self.assertEqual(key1, key2)
    
    def test_group_by_attributes_and_timestamp(self):
        """Test grouping by attributes and timestamp."""
        record = Mock()
        record.attributes = {"env": "test"}
        timestamp_ms = 1234567890
        
        result = self.exporter._group_by_attributes_and_timestamp(record, timestamp_ms)
        
        # Should return a tuple with attributes key and timestamp
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1], timestamp_ms)
    
    def test_generate_log_stream_name(self):
        """Test log stream name generation."""
        name1 = self.exporter._generate_log_stream_name()
        name2 = self.exporter._generate_log_stream_name()
        
        # Should generate unique names
        self.assertNotEqual(name1, name2)
        self.assertTrue(name1.startswith("otel-python-"))
        self.assertTrue(name2.startswith("otel-python-"))
    
    def test_normalize_timestamp(self):
        """Test timestamp normalization."""
        timestamp_ns = 1609459200000000000  # 2021-01-01 00:00:00 in nanoseconds
        expected_ms = 1609459200000  # Same time in milliseconds
        
        result = self.exporter._normalize_timestamp(timestamp_ns)
        self.assertEqual(result, expected_ms)
    
    def test_create_metric_record(self):
        """Test metric record creation."""
        record = self.exporter._create_metric_record("test_metric", "Count", "Test description")
        
        self.assertIsNotNone(record)
        self.assertIsNotNone(record.instrument)
        self.assertEqual(record.instrument.name, "test_metric")
        self.assertEqual(record.instrument.unit, "Count")
        self.assertEqual(record.instrument.description, "Test description")
    
    def test_convert_gauge(self):
        """Test gauge conversion."""
        metric = MockMetric("gauge_metric", "Count", "Gauge description")
        dp = MockDataPoint(value=42.5, attributes={"key": "value"})
        
        record, timestamp = self.exporter._convert_gauge(metric, dp)
        
        self.assertIsNotNone(record)
        self.assertEqual(record.instrument.name, "gauge_metric")
        self.assertEqual(record.value, 42.5)
        self.assertEqual(record.attributes, {"key": "value"})
        self.assertIsInstance(timestamp, int)
    
    def test_create_emf_log(self):
        """Test EMF log creation."""
        # Create test records
        gauge_record = self.exporter._create_metric_record("gauge_metric", "Count", "Gauge")
        gauge_record.value = 50.0
        gauge_record.timestamp = int(time.time() * 1000)
        gauge_record.attributes = {"env": "test"}
        
        records = [gauge_record]
        resource = Resource.create({"service.name": "test-service"})
        
        result = self.exporter._create_emf_log(records, resource)
        
        self.assertIsInstance(result, dict)
        
        # Check that the result is JSON serializable
        json.dumps(result)  # Should not raise exception
    
    @patch('botocore.session.Session')
    def test_export_success(self, mock_session):
        """Test successful export."""
        # Mock CloudWatch Logs client
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client
        mock_client.put_log_events.return_value = {"nextSequenceToken": "12345"}
        
        # Create empty metrics data to test basic export flow
        metrics_data = Mock()
        metrics_data.resource_metrics = []
        
        result = self.exporter.export(metrics_data)
        
        self.assertEqual(result, MetricExportResult.SUCCESS)
    
    def test_export_failure(self):
        """Test export failure handling."""
        # Create metrics data that will cause an exception during iteration
        metrics_data = Mock()
        # Make resource_metrics raise an exception when iterated over
        metrics_data.resource_metrics = Mock()
        metrics_data.resource_metrics.__iter__ = Mock(side_effect=Exception("Test exception"))
        
        result = self.exporter.export(metrics_data)
        
        self.assertEqual(result, MetricExportResult.FAILURE)
    
    def test_force_flush_no_pending_events(self):
        """Test force flush functionality with no pending events."""
        result = self.exporter.force_flush()
        
        self.assertTrue(result)
    
    @patch.object(CloudWatchEMFExporter, 'force_flush')
    def test_shutdown(self, mock_force_flush):
        """Test shutdown functionality."""
        mock_force_flush.return_value = True
        
        result = self.exporter.shutdown(timeout_millis=5000)
        
        self.assertTrue(result)
        mock_force_flush.assert_called_once_with(5000)


if __name__ == "__main__":
    unittest.main()