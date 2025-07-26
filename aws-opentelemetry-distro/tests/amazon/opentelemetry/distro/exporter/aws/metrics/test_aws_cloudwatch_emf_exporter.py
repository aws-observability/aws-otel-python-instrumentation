# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import time
import unittest
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter import AwsCloudWatchEmfExporter
from opentelemetry.sdk.metrics.export import Gauge, Histogram, MetricExportResult, Sum
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


class MockHistogramDataPoint(MockDataPoint):
    """Mock histogram datapoint for testing."""

    def __init__(self, count=5, sum_val=25.0, min_val=1.0, max_val=10.0, **kwargs):
        super().__init__(**kwargs)
        self.count = count
        self.sum = sum_val
        self.min = min_val
        self.max = max_val


class MockExpHistogramDataPoint(MockDataPoint):
    """Mock exponential histogram datapoint for testing."""

    def __init__(self, count=10, sum_val=50.0, min_val=1.0, max_val=20.0, scale=2, **kwargs):
        super().__init__(**kwargs)
        self.count = count
        self.sum = sum_val
        self.min = min_val
        self.max = max_val
        self.scale = scale

        # Mock positive buckets
        self.positive = Mock()
        self.positive.offset = 0
        self.positive.bucket_counts = [1, 2, 3, 4]

        # Mock negative buckets
        self.negative = Mock()
        self.negative.offset = 0
        self.negative.bucket_counts = []

        # Mock zero count
        self.zero_count = 0


class MockGaugeData:
    """Mock gauge data that passes isinstance checks."""

    def __init__(self, data_points=None):
        self.data_points = data_points or []


class MockSumData:
    """Mock sum data that passes isinstance checks."""

    def __init__(self, data_points=None):
        self.data_points = data_points or []


class MockHistogramData:
    """Mock histogram data that passes isinstance checks."""

    def __init__(self, data_points=None):
        self.data_points = data_points or []


class MockExpHistogramData:
    """Mock exponential histogram data that passes isinstance checks."""

    def __init__(self, data_points=None):
        self.data_points = data_points or []


class MockMetricWithData:
    """Mock metric with data attribute."""

    def __init__(self, name="test_metric", unit="1", description="Test metric", data=None):
        self.name = name
        self.unit = unit
        self.description = description
        self.data = data or MockGaugeData()


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


# pylint: disable=too-many-public-methods
class TestAwsCloudWatchEmfExporter(unittest.TestCase):
    """Test AwsCloudWatchEmfExporter class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the botocore session to avoid AWS calls
        with patch("botocore.session.Session") as mock_session:
            mock_client = Mock()
            mock_session_instance = Mock()
            mock_session.return_value = mock_session_instance
            mock_session_instance.create_client.return_value = mock_client

            self.exporter = AwsCloudWatchEmfExporter(
                session=mock_session, namespace="TestNamespace", log_group_name="test-log-group"
            )

    def test_initialization(self):
        """Test exporter initialization."""
        self.assertEqual(self.exporter.namespace, "TestNamespace")
        self.assertEqual(self.exporter.log_group_name, "test-log-group")
        self.assertIsNotNone(self.exporter.log_client)

    @patch("botocore.session.Session")
    def test_initialization_with_custom_params(self, mock_session):
        """Test exporter initialization with custom parameters."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client

        exporter = AwsCloudWatchEmfExporter(
            session=mock_session_instance,
            namespace="CustomNamespace",
            log_group_name="custom-log-group",
            log_stream_name="custom-stream",
            aws_region="us-west-2",
        )
        self.assertEqual(exporter.namespace, "CustomNamespace")
        self.assertEqual(exporter.log_group_name, "custom-log-group")

    def test_get_unit_mapping(self):
        """Test unit mapping functionality."""
        # Test known units from UNIT_MAPPING
        self.assertEqual(
            self.exporter._get_unit(self.exporter._create_metric_record("test", "ms", "test")), "Milliseconds"
        )
        self.assertEqual(self.exporter._get_unit(self.exporter._create_metric_record("test", "s", "test")), "Seconds")
        self.assertEqual(
            self.exporter._get_unit(self.exporter._create_metric_record("test", "us", "test")), "Microseconds"
        )
        self.assertEqual(self.exporter._get_unit(self.exporter._create_metric_record("test", "By", "test")), "Bytes")
        self.assertEqual(self.exporter._get_unit(self.exporter._create_metric_record("test", "bit", "test")), "Bits")

        # Test units that map to empty string (should return empty string from mapping)
        self.assertEqual(self.exporter._get_unit(self.exporter._create_metric_record("test", "1", "test")), "")
        self.assertEqual(self.exporter._get_unit(self.exporter._create_metric_record("test", "ns", "test")), "")

        # Test EMF supported units directly (should return as-is)
        self.assertEqual(self.exporter._get_unit(self.exporter._create_metric_record("test", "Count", "test")), "Count")
        self.assertEqual(
            self.exporter._get_unit(self.exporter._create_metric_record("test", "Percent", "test")), "Percent"
        )
        self.assertEqual(
            self.exporter._get_unit(self.exporter._create_metric_record("test", "Kilobytes", "test")), "Kilobytes"
        )

        # Test unknown unit (not in mapping and not in supported units, returns None)
        self.assertIsNone(self.exporter._get_unit(self.exporter._create_metric_record("test", "unknown", "test")))

        # Test empty unit (should return None due to falsy check)
        self.assertIsNone(self.exporter._get_unit(self.exporter._create_metric_record("test", "", "test")))

        # Test None unit
        self.assertIsNone(self.exporter._get_unit(self.exporter._create_metric_record("test", None, "test")))

    def test_get_metric_name(self):
        """Test metric name extraction."""
        # Test with record that has name attribute
        record = Mock()
        record.name = "test_metric"

        result = self.exporter._get_metric_name(record)
        self.assertEqual(result, "test_metric")

        # Test with record that has empty name (should return None)
        record_empty = Mock()
        record_empty.name = ""

        result_empty = self.exporter._get_metric_name(record_empty)
        self.assertIsNone(result_empty)

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
        record.timestamp = 1234567890

        result = self.exporter._group_by_attributes_and_timestamp(record)

        # Should return a tuple with attributes key and timestamp
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1], 1234567890)

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
        self.assertEqual(record.name, "test_metric")
        self.assertEqual(record.unit, "Count")
        self.assertEqual(record.description, "Test description")

    def test_convert_gauge(self):
        """Test gauge conversion."""
        metric = MockMetric("gauge_metric", "Count", "Gauge description")
        dp = MockDataPoint(value=42.5, attributes={"key": "value"})

        record = self.exporter._convert_gauge_and_sum(metric, dp)

        self.assertIsNotNone(record)
        self.assertEqual(record.name, "gauge_metric")
        self.assertEqual(record.value, 42.5)
        self.assertEqual(record.attributes, {"key": "value"})
        self.assertIsInstance(record.timestamp, int)

    def test_convert_sum(self):
        """Test sum conversion."""
        metric = MockMetric("sum_metric", "Count", "Sum description")
        dp = MockDataPoint(value=100.0, attributes={"env": "test"})

        record = self.exporter._convert_gauge_and_sum(metric, dp)

        self.assertIsNotNone(record)
        self.assertEqual(record.name, "sum_metric")
        self.assertEqual(record.value, 100.0)
        self.assertEqual(record.attributes, {"env": "test"})
        self.assertIsInstance(record.timestamp, int)

    def test_convert_histogram(self):
        """Test histogram conversion."""
        metric = MockMetric("histogram_metric", "ms", "Histogram description")
        dp = MockHistogramDataPoint(
            count=10, sum_val=150.0, min_val=5.0, max_val=25.0, attributes={"region": "us-east-1"}
        )

        record = self.exporter._convert_histogram(metric, dp)

        self.assertIsNotNone(record)
        self.assertEqual(record.name, "histogram_metric")
        self.assertTrue(hasattr(record, "histogram_data"))

        expected_value = {"Count": 10, "Sum": 150.0, "Min": 5.0, "Max": 25.0}
        self.assertEqual(record.histogram_data, expected_value)
        self.assertEqual(record.attributes, {"region": "us-east-1"})
        self.assertIsInstance(record.timestamp, int)

    def test_convert_exp_histogram(self):
        """Test exponential histogram conversion."""
        metric = MockMetric("exp_histogram_metric", "s", "Exponential histogram description")
        dp = MockExpHistogramDataPoint(count=8, sum_val=64.0, min_val=2.0, max_val=32.0, attributes={"service": "api"})

        record = self.exporter._convert_exp_histogram(metric, dp)

        self.assertIsNotNone(record)
        self.assertEqual(record.name, "exp_histogram_metric")
        self.assertTrue(hasattr(record, "exp_histogram_data"))

        exp_data = record.exp_histogram_data
        self.assertIn("Values", exp_data)
        self.assertIn("Counts", exp_data)
        self.assertEqual(exp_data["Count"], 8)
        self.assertEqual(exp_data["Sum"], 64.0)
        self.assertEqual(exp_data["Min"], 2.0)
        self.assertEqual(exp_data["Max"], 32.0)
        self.assertEqual(record.attributes, {"service": "api"})
        self.assertIsInstance(record.timestamp, int)

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

    def test_create_emf_log_with_resource(self):
        """Test EMF log creation with resource attributes."""
        # Create test records
        gauge_record = self.exporter._create_metric_record("gauge_metric", "Count", "Gauge")
        gauge_record.value = 50.0
        gauge_record.timestamp = int(time.time() * 1000)
        gauge_record.attributes = {"env": "test", "service": "api"}

        records = [gauge_record]
        resource = Resource.create({"service.name": "test-service", "service.version": "1.0.0"})

        result = self.exporter._create_emf_log(records, resource, 1234567890)

        # Verify EMF log structure
        self.assertIn("_aws", result)
        self.assertIn("CloudWatchMetrics", result["_aws"])
        self.assertEqual(result["_aws"]["Timestamp"], 1234567890)
        self.assertEqual(result["Version"], "1")

        # Check resource attributes are prefixed
        self.assertEqual(result["otel.resource.service.name"], "test-service")
        self.assertEqual(result["otel.resource.service.version"], "1.0.0")

        # Check metric attributes
        self.assertEqual(result["env"], "test")
        self.assertEqual(result["service"], "api")

        # Check metric value
        self.assertEqual(result["gauge_metric"], 50.0)

        # Check CloudWatch metrics structure
        cw_metrics = result["_aws"]["CloudWatchMetrics"][0]
        self.assertEqual(cw_metrics["Namespace"], "TestNamespace")
        self.assertIn("Dimensions", cw_metrics)
        self.assertEqual(set(cw_metrics["Dimensions"][0]), {"env", "service"})
        self.assertEqual(cw_metrics["Metrics"][0]["Name"], "gauge_metric")

    def test_create_emf_log_without_dimensions(self):
        """Test EMF log creation with metrics but no dimensions."""
        # Create test record without attributes (no dimensions)
        gauge_record = self.exporter._create_metric_record("gauge_metric", "Count", "Gauge")
        gauge_record.value = 75.0
        gauge_record.timestamp = int(time.time() * 1000)
        gauge_record.attributes = {}  # No attributes = no dimensions

        records = [gauge_record]
        resource = Resource.create({"service.name": "test-service"})

        result = self.exporter._create_emf_log(records, resource, 1234567890)

        # Verify EMF log structure
        self.assertIn("_aws", result)
        self.assertIn("CloudWatchMetrics", result["_aws"])
        self.assertEqual(result["_aws"]["Timestamp"], 1234567890)
        self.assertEqual(result["Version"], "1")

        # Check metric value
        self.assertEqual(result["gauge_metric"], 75.0)

        # Check CloudWatch metrics structure - should have metrics but no dimensions
        cw_metrics = result["_aws"]["CloudWatchMetrics"][0]
        self.assertEqual(cw_metrics["Namespace"], "TestNamespace")
        self.assertNotIn("Dimensions", cw_metrics)  # No dimensions should be present
        self.assertEqual(cw_metrics["Metrics"][0]["Name"], "gauge_metric")

    def test_create_emf_log_skips_empty_metric_names(self):
        """Test that EMF log creation skips records with empty metric names."""
        # Create a record with no metric name
        record_without_name = Mock()
        record_without_name.attributes = {"key": "value"}
        record_without_name.value = 10.0
        record_without_name.name = None  # No valid name

        # Create a record with valid metric name
        valid_record = self.exporter._create_metric_record("valid_metric", "Count", "Valid metric")
        valid_record.value = 20.0
        valid_record.attributes = {"key": "value"}

        records = [record_without_name, valid_record]
        resource = Resource.create({"service.name": "test-service"})

        result = self.exporter._create_emf_log(records, resource, 1234567890)

        # Only the valid record should be processed
        self.assertIn("valid_metric", result)
        self.assertEqual(result["valid_metric"], 20.0)

        # Check that only the valid metric is in the definitions (empty names are skipped)
        cw_metrics = result["_aws"]["CloudWatchMetrics"][0]
        self.assertEqual(len(cw_metrics["Metrics"]), 1)
        # Ensure our valid metric is present
        metric_names = [m["Name"] for m in cw_metrics["Metrics"]]
        self.assertIn("valid_metric", metric_names)

    @patch("botocore.session.Session")
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

    def test_export_with_gauge_metrics(self):
        """Test exporting actual gauge metrics."""
        # Create mock metrics data
        resource = Resource.create({"service.name": "test-service"})

        # Create gauge data
        gauge_data = Gauge(data_points=[MockDataPoint(value=42.0, attributes={"key": "value"})])

        metric = MockMetricWithData(name="test_gauge", data=gauge_data)

        scope_metrics = MockScopeMetrics(metrics=[metric])
        resource_metrics = MockResourceMetrics(resource=resource, scope_metrics=[scope_metrics])

        metrics_data = Mock()
        metrics_data.resource_metrics = [resource_metrics]

        result = self.exporter.export(metrics_data)

        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_export_with_sum_metrics(self):
        """Test export with Sum metrics."""
        # Create mock metrics data with Sum type
        resource = Resource.create({"service.name": "test-service"})

        sum_data = MockSumData([MockDataPoint(value=25.0, attributes={"env": "test"})])
        # Create a mock that will pass the type() check for Sum
        sum_data.__class__ = Sum
        metric = MockMetricWithData(name="test_sum", data=sum_data)

        scope_metrics = MockScopeMetrics(metrics=[metric])
        resource_metrics = MockResourceMetrics(resource=resource, scope_metrics=[scope_metrics])

        metrics_data = Mock()
        metrics_data.resource_metrics = [resource_metrics]

        result = self.exporter.export(metrics_data)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_export_with_histogram_metrics(self):
        """Test export with Histogram metrics."""
        # Create mock metrics data with Histogram type
        resource = Resource.create({"service.name": "test-service"})

        hist_dp = MockHistogramDataPoint(count=5, sum_val=25.0, min_val=1.0, max_val=10.0, attributes={"env": "test"})
        hist_data = MockHistogramData([hist_dp])
        # Create a mock that will pass the type() check for Histogram
        hist_data.__class__ = Histogram
        metric = MockMetricWithData(name="test_histogram", data=hist_data)

        scope_metrics = MockScopeMetrics(metrics=[metric])
        resource_metrics = MockResourceMetrics(resource=resource, scope_metrics=[scope_metrics])

        metrics_data = Mock()
        metrics_data.resource_metrics = [resource_metrics]

        result = self.exporter.export(metrics_data)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_export_with_unsupported_metric_type(self):
        """Test export with unsupported metric types."""
        # Create mock metrics data with unsupported metric type
        resource = Resource.create({"service.name": "test-service"})

        # Create non-gauge data
        unsupported_data = Mock()
        unsupported_data.data_points = [MockDataPoint(value=42.0)]

        metric = MockMetricWithData(name="test_counter", data=unsupported_data)

        scope_metrics = MockScopeMetrics(metrics=[metric])
        resource_metrics = MockResourceMetrics(resource=resource, scope_metrics=[scope_metrics])

        metrics_data = Mock()
        metrics_data.resource_metrics = [resource_metrics]

        # Should still return success even with unsupported metrics
        result = self.exporter.export(metrics_data)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_export_with_metric_without_data(self):
        """Test export with metrics that don't have data attribute."""
        # Create mock metrics data
        resource = Resource.create({"service.name": "test-service"})

        # Create metric without data attribute
        metric = Mock(spec=[])

        scope_metrics = MockScopeMetrics(metrics=[metric])
        resource_metrics = MockResourceMetrics(resource=resource, scope_metrics=[scope_metrics])

        metrics_data = Mock()
        metrics_data.resource_metrics = [resource_metrics]

        # Should still return success
        result = self.exporter.export(metrics_data)
        self.assertEqual(result, MetricExportResult.SUCCESS)

    def test_get_metric_name_fallback(self):
        """Test metric name extraction fallback."""
        # Test with record that has no instrument attribute
        record = Mock(spec=[])

        result = self.exporter._get_metric_name(record)
        self.assertIsNone(result)

    def test_get_metric_name_empty_name(self):
        """Test metric name extraction with empty name."""
        # Test with record that has empty name
        record = Mock()
        record.name = ""

        result = self.exporter._get_metric_name(record)
        self.assertIsNone(result)

    @patch("os.environ.get")
    @patch("botocore.session.Session")
    def test_initialization_with_env_region(self, mock_session, mock_env_get):
        """Test initialization with AWS region from environment."""
        # Mock environment variable
        mock_env_get.side_effect = lambda key: "us-west-1" if key == "AWS_REGION" else None

        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client

        exporter = AwsCloudWatchEmfExporter(
            session=mock_session, namespace="TestNamespace", log_group_name="test-log-group"
        )

        # Just verify the exporter was created successfully with region handling
        self.assertIsNotNone(exporter)
        self.assertEqual(exporter.namespace, "TestNamespace")

    def test_force_flush_no_pending_events(self):
        """Test force flush functionality with no pending events."""
        result = self.exporter.force_flush()

        self.assertTrue(result)

    @patch.object(AwsCloudWatchEmfExporter, "force_flush")
    def test_shutdown(self, mock_force_flush):
        """Test shutdown functionality."""
        mock_force_flush.return_value = True

        result = self.exporter.shutdown(timeout_millis=5000)

        self.assertTrue(result)
        mock_force_flush.assert_called_once_with(5000)

    # pylint: disable=broad-exception-caught
    def test_export_method_exists(self):
        """Test that _export method exists and can be called."""
        # Just test that the method exists and doesn't crash with basic input
        log_event = {"message": "test message", "timestamp": 1234567890}

        # Mock the log client to avoid actual AWS calls
        with patch.object(self.exporter.log_client, "send_log_event") as mock_send:
            # Should not raise an exception
            try:
                self.exporter._export(log_event)
                mock_send.assert_called_once_with(log_event)
            except Exception as error:
                self.fail(f"_export raised an exception: {error}")


if __name__ == "__main__":
    unittest.main()
