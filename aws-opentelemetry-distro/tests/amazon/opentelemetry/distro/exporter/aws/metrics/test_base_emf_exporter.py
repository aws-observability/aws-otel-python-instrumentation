# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import Mock

from amazon.opentelemetry.distro.exporter.aws.metrics.base_emf_exporter import BaseEmfExporter, MetricRecord
from opentelemetry.sdk.metrics.export import MetricExportResult
from opentelemetry.sdk.resources import Resource


class ConcreteEmfExporter(BaseEmfExporter):
    """Concrete implementation of BaseEmfExporter for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exported_logs = []

    def _export(self, log_event):
        """Implementation that stores exported logs for testing."""
        self.exported_logs.append(log_event)

    def force_flush(self, timeout_millis=None):  # pylint: disable=no-self-use
        """Force flush implementation for testing."""
        return True

    def shutdown(self, timeout_millis=None):  # pylint: disable=no-self-use
        """Shutdown implementation for testing."""
        return True


class TestMetricRecord(unittest.TestCase):
    """Test MetricRecord class."""

    def test_metric_record_initialization(self):
        """Test MetricRecord initialization with basic values."""
        record = MetricRecord("test_metric", "Count", "Test description")

        self.assertEqual(record.name, "test_metric")
        self.assertEqual(record.unit, "Count")
        self.assertEqual(record.description, "Test description")
        self.assertIsNone(record.timestamp)
        self.assertEqual(record.attributes, {})
        self.assertIsNone(record.value)
        self.assertIsNone(record.sum_data)
        self.assertIsNone(record.histogram_data)
        self.assertIsNone(record.exp_histogram_data)


class TestBaseEmfExporter(unittest.TestCase):
    """Test BaseEmfExporter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = ConcreteEmfExporter(namespace="TestNamespace")

    def test_initialization(self):
        """Test exporter initialization."""
        exporter = ConcreteEmfExporter()
        self.assertEqual(exporter.namespace, "default")

        exporter_custom = ConcreteEmfExporter(namespace="CustomNamespace")
        self.assertEqual(exporter_custom.namespace, "CustomNamespace")

    def test_get_metric_name(self):
        """Test metric name extraction."""
        # Test with valid name
        record = Mock()
        record.name = "test_metric"
        result = self.exporter._get_metric_name(record)
        self.assertEqual(result, "test_metric")

        # Test with empty name
        record.name = ""
        result = self.exporter._get_metric_name(record)
        self.assertIsNone(result)

        # Test with None name
        record.name = None
        result = self.exporter._get_metric_name(record)
        self.assertIsNone(result)

    def test_get_unit(self):
        """Test unit mapping functionality."""
        # Test EMF supported units (should return as-is)
        record = MetricRecord("test", "Count", "desc")
        self.assertEqual(self.exporter._get_unit(record), "Count")

        record = MetricRecord("test", "Percent", "desc")
        self.assertEqual(self.exporter._get_unit(record), "Percent")

        # Test OTel unit mapping
        record = MetricRecord("test", "ms", "desc")
        self.assertEqual(self.exporter._get_unit(record), "Milliseconds")

        record = MetricRecord("test", "s", "desc")
        self.assertEqual(self.exporter._get_unit(record), "Seconds")

        record = MetricRecord("test", "By", "desc")
        self.assertEqual(self.exporter._get_unit(record), "Bytes")

        # Test units that map to empty string
        record = MetricRecord("test", "1", "desc")
        self.assertEqual(self.exporter._get_unit(record), "")

        # Test unknown unit
        record = MetricRecord("test", "unknown", "desc")
        self.assertIsNone(self.exporter._get_unit(record))

        # Test None unit
        record = MetricRecord("test", None, "desc")
        self.assertIsNone(self.exporter._get_unit(record))

    def test_get_dimension_names(self):
        """Test dimension names extraction."""
        attributes = {"service": "test", "env": "prod"}
        result = self.exporter._get_dimension_names(attributes)
        self.assertEqual(set(result), {"service", "env"})

        # Test empty attributes
        result = self.exporter._get_dimension_names({})
        self.assertEqual(result, [])

    def test_get_attributes_key(self):
        """Test attributes key generation."""
        attrs1 = {"b": "2", "a": "1"}
        attrs2 = {"a": "1", "b": "2"}

        key1 = self.exporter._get_attributes_key(attrs1)
        key2 = self.exporter._get_attributes_key(attrs2)

        # Keys should be consistent regardless of order
        self.assertEqual(key1, key2)
        self.assertIsInstance(key1, str)

    def test_normalize_timestamp(self):
        """Test timestamp normalization."""
        timestamp_ns = 1609459200000000000  # nanoseconds
        expected_ms = 1609459200000  # milliseconds

        result = self.exporter._normalize_timestamp(timestamp_ns)
        self.assertEqual(result, expected_ms)

    def test_create_metric_record(self):
        """Test metric record creation."""
        record = self.exporter._create_metric_record("test_metric", "Count", "Description")

        self.assertIsInstance(record, MetricRecord)
        self.assertEqual(record.name, "test_metric")
        self.assertEqual(record.unit, "Count")
        self.assertEqual(record.description, "Description")

    def test_convert_gauge_and_sum(self):
        """Test gauge and sum conversion."""
        metric = Mock()
        metric.name = "test_gauge"
        metric.unit = "Count"
        metric.description = "Test gauge"

        data_point = Mock()
        data_point.value = 42.0
        data_point.attributes = {"key": "value"}
        data_point.time_unix_nano = 1609459200000000000

        record = self.exporter._convert_gauge_and_sum(metric, data_point)

        self.assertEqual(record.name, "test_gauge")
        self.assertEqual(record.value, 42.0)
        self.assertEqual(record.attributes, {"key": "value"})
        self.assertEqual(record.timestamp, 1609459200000)

    def test_convert_histogram(self):
        """Test histogram conversion."""
        metric = Mock()
        metric.name = "test_histogram"
        metric.unit = "ms"
        metric.description = "Test histogram"

        data_point = Mock()
        data_point.count = 5
        data_point.sum = 25.0
        data_point.min = 1.0
        data_point.max = 10.0
        data_point.attributes = {"service": "test"}
        data_point.time_unix_nano = 1609459200000000000

        record = self.exporter._convert_histogram(metric, data_point)

        self.assertEqual(record.name, "test_histogram")
        expected_data = {"Count": 5, "Sum": 25.0, "Min": 1.0, "Max": 10.0}
        self.assertEqual(record.histogram_data, expected_data)
        self.assertEqual(record.attributes, {"service": "test"})

    def test_convert_exp_histogram(self):
        """Test exponential histogram conversion."""
        metric = Mock()
        metric.name = "test_exp_histogram"
        metric.unit = "s"
        metric.description = "Test exponential histogram"

        data_point = Mock()
        data_point.count = 10
        data_point.sum = 50.0
        data_point.min = 1.0
        data_point.max = 20.0
        data_point.scale = 1
        data_point.zero_count = 0
        data_point.attributes = {"env": "test"}
        data_point.time_unix_nano = 1609459200000000000

        # Mock buckets
        data_point.positive = Mock()
        data_point.positive.offset = 0
        data_point.positive.bucket_counts = [1, 2, 1]

        data_point.negative = Mock()
        data_point.negative.offset = 0
        data_point.negative.bucket_counts = []

        record = self.exporter._convert_exp_histogram(metric, data_point)

        self.assertEqual(record.name, "test_exp_histogram")
        self.assertIsNotNone(record.exp_histogram_data)
        self.assertIn("Values", record.exp_histogram_data)
        self.assertIn("Counts", record.exp_histogram_data)
        self.assertEqual(record.exp_histogram_data["Count"], 10)
        self.assertEqual(record.exp_histogram_data["Sum"], 50.0)

    def test_group_by_attributes_and_timestamp(self):
        """Test grouping by attributes and timestamp."""
        record = Mock()
        record.attributes = {"env": "test"}
        record.timestamp = 1234567890

        result = self.exporter._group_by_attributes_and_timestamp(record)

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1], 1234567890)

    def test_create_emf_log(self):
        """Test EMF log creation."""
        # Create a simple metric record
        record = self.exporter._create_metric_record("test_metric", "Count", "Test")
        record.value = 50.0
        record.timestamp = 1234567890
        record.attributes = {"env": "test"}

        records = [record]
        resource = Resource.create({"service.name": "test-service"})

        result = self.exporter._create_emf_log(records, resource, 1234567890)

        # Check basic EMF structure
        self.assertIn("_aws", result)
        self.assertIn("CloudWatchMetrics", result["_aws"])
        self.assertEqual(result["_aws"]["Timestamp"], 1234567890)
        self.assertEqual(result["Version"], "1")

        # Check metric value
        self.assertEqual(result["test_metric"], 50.0)

        # Check resource attributes
        self.assertEqual(result["otel.resource.service.name"], "test-service")

        # Check CloudWatch metrics
        cw_metrics = result["_aws"]["CloudWatchMetrics"][0]
        self.assertEqual(cw_metrics["Namespace"], "TestNamespace")
        self.assertEqual(cw_metrics["Metrics"][0]["Name"], "test_metric")

    def test_export_empty_metrics(self):
        """Test export with empty metrics data."""
        metrics_data = Mock()
        metrics_data.resource_metrics = []

        result = self.exporter.export(metrics_data)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_export_failure_handling(self):
        """Test export failure handling."""
        metrics_data = Mock()
        # Make iteration fail
        metrics_data.resource_metrics = Mock()
        metrics_data.resource_metrics.__iter__ = Mock(side_effect=Exception("Test exception"))

        result = self.exporter.export(metrics_data)
        self.assertEqual(result, MetricExportResult.FAILURE)


if __name__ == "__main__":
    unittest.main()
