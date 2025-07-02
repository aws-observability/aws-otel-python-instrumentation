# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import json
import logging
import math
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics import Histogram as HistogramInstr
from opentelemetry.sdk.metrics import ObservableCounter, ObservableGauge, ObservableUpDownCounter, UpDownCounter
from opentelemetry.sdk.metrics._internal.point import Metric
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    ExponentialHistogram,
    Gauge,
    Histogram,
    MetricExporter,
    MetricExportResult,
    MetricsData,
    NumberDataPoint,
    Sum,
)
from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation
from opentelemetry.sdk.resources import Resource
from opentelemetry.util.types import Attributes

from ._cloudwatch_log_client import CloudWatchLogClient

logger = logging.getLogger(__name__)


class MetricRecord:
    """The metric data unified representation of all OTel metrics for OTel to CW EMF conversion."""

    def __init__(self, metric_name: str, metric_unit: str, metric_description: str):
        """
        Initialize metric record.

        Args:
            metric_name: Name of the metric
            metric_unit: Unit of the metric
            metric_description: Description of the metric
        """
        # Instrument metadata
        self.name = metric_name
        self.unit = metric_unit
        self.description = metric_description

        # Will be set by conversion methods
        self.timestamp: Optional[int] = None
        self.attributes: Attributes = {}

        # Different metric type data - only one will be set per record
        self.value: Optional[float] = None
        self.sum_data: Optional[Any] = None
        self.histogram_data: Optional[Any] = None
        self.exp_histogram_data: Optional[Any] = None


class AwsCloudWatchEmfExporter(MetricExporter):
    """
    OpenTelemetry metrics exporter for CloudWatch EMF format.

    This exporter converts OTel metrics into CloudWatch EMF logs which are then
    sent to CloudWatch Logs. CloudWatch Logs automatically extracts the metrics
    from the EMF logs.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html

    """

    # CloudWatch EMF supported units
    # Ref: https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
    EMF_SUPPORTED_UNITS = {
        "Seconds",
        "Microseconds",
        "Milliseconds",
        "Bytes",
        "Kilobytes",
        "Megabytes",
        "Gigabytes",
        "Terabytes",
        "Bits",
        "Kilobits",
        "Megabits",
        "Gigabits",
        "Terabits",
        "Percent",
        "Count",
        "Bytes/Second",
        "Kilobytes/Second",
        "Megabytes/Second",
        "Gigabytes/Second",
        "Terabytes/Second",
        "Bits/Second",
        "Kilobits/Second",
        "Megabits/Second",
        "Gigabits/Second",
        "Terabits/Second",
        "Count/Second",
        "None",
    }

    # OTel to CloudWatch unit mapping
    # Ref: opentelemetry-collector-contrib/blob/main/exporter/awsemfexporter/grouped_metric.go#L188
    UNIT_MAPPING = {
        "1": "",
        "ns": "",
        "ms": "Milliseconds",
        "s": "Seconds",
        "us": "Microseconds",
        "By": "Bytes",
        "bit": "Bits",
    }

    def __init__(
        self,
        namespace: str = "default",
        log_group_name: str = None,
        log_stream_name: Optional[str] = None,
        aws_region: Optional[str] = None,
        preferred_temporality: Optional[Dict[type, AggregationTemporality]] = None,
        preferred_aggregation: Optional[Dict[type, Any]] = None,
        **kwargs,
    ):
        """
        Initialize the CloudWatch EMF exporter.

        Args:
            namespace: CloudWatch namespace for metrics
            log_group_name: CloudWatch log group name
            log_stream_name: CloudWatch log stream name (auto-generated if None)
            aws_region: AWS region (auto-detected if None)
            preferred_temporality: Optional dictionary mapping instrument types to aggregation temporality
            preferred_aggregation: Optional dictionary mapping instrument types to preferred aggregation
            **kwargs: Additional arguments passed to botocore client
        """
        # Set up temporality preference default to DELTA if customers not set
        if preferred_temporality is None:
            preferred_temporality = {
                Counter: AggregationTemporality.DELTA,
                HistogramInstr: AggregationTemporality.DELTA,
                ObservableCounter: AggregationTemporality.DELTA,
                ObservableGauge: AggregationTemporality.DELTA,
                ObservableUpDownCounter: AggregationTemporality.DELTA,
                UpDownCounter: AggregationTemporality.DELTA,
            }

        # Set up aggregation preference default to exponential histogram for histogram metrics
        if preferred_aggregation is None:
            preferred_aggregation = {
                HistogramInstr: ExponentialBucketHistogramAggregation(),
            }

        super().__init__(preferred_temporality, preferred_aggregation)

        self.namespace = namespace
        self.log_group_name = log_group_name

        # Initialize CloudWatch Logs client
        self.log_client = CloudWatchLogClient(
            log_group_name=log_group_name, log_stream_name=log_stream_name, aws_region=aws_region, **kwargs
        )

    def _get_metric_name(self, record: MetricRecord) -> Optional[str]:
        """Get the metric name from the metric record or data point."""

        try:
            if record.name:
                return record.name
        except AttributeError:
            pass
        # Return None if no valid metric name found
        return None

    def _get_unit(self, record: MetricRecord) -> Optional[str]:
        """Get CloudWatch unit from MetricRecord unit."""
        unit = record.unit

        if not unit:
            return None

        # First check if unit is already a supported EMF unit
        if unit in self.EMF_SUPPORTED_UNITS:
            return unit

        # Map from OTel unit to CloudWatch unit
        mapped_unit = self.UNIT_MAPPING.get(unit)

        return mapped_unit

    def _get_dimension_names(self, attributes: Attributes) -> List[str]:
        """Extract dimension names from attributes."""
        # Implement dimension selection logic
        # For now, use all attributes as dimensions
        return list(attributes.keys())

    def _get_attributes_key(self, attributes: Attributes) -> str:
        """
        Create a hashable key from attributes for grouping metrics.

        Args:
            attributes: The attributes dictionary

        Returns:
            A string representation of sorted attributes key-value pairs
        """
        # Sort the attributes to ensure consistent keys
        sorted_attrs = sorted(attributes.items())
        # Create a string representation of the attributes
        return str(sorted_attrs)

    def _normalize_timestamp(self, timestamp_ns: int) -> int:
        """
        Normalize a nanosecond timestamp to milliseconds for CloudWatch.

        Args:
            timestamp_ns: Timestamp in nanoseconds

        Returns:
            Timestamp in milliseconds
        """
        # Convert from nanoseconds to milliseconds
        return timestamp_ns // 1_000_000

    def _create_metric_record(self, metric_name: str, metric_unit: str, metric_description: str) -> MetricRecord:
        """
        Creates the intermediate metric data structure that standardizes different otel metric representation
        and will be used to generate EMF events. The base record
        establishes the instrument schema (name/unit/description) that will be populated
        with dimensions, timestamps, and values during metric processing.

        Args:
            metric_name: Name of the metric
            metric_unit: Unit of the metric
            metric_description: Description of the metric

        Returns:
            A MetricRecord object
        """
        return MetricRecord(metric_name, metric_unit, metric_description)

    def _convert_gauge_and_sum(self, metric: Metric, data_point: NumberDataPoint) -> MetricRecord:
        """Convert a Gauge or Sum metric datapoint to a metric record.

        Args:
            metric: The metric object
            data_point: The datapoint to convert

        Returns:
            MetricRecord with populated timestamp, attributes, and value
        """
        # Create base record
        record = self._create_metric_record(metric.name, metric.unit, metric.description)

        # Set timestamp
        timestamp_ms = (
            self._normalize_timestamp(data_point.time_unix_nano)
            if data_point.time_unix_nano is not None
            else int(time.time() * 1000)
        )
        record.timestamp = timestamp_ms

        # Set attributes
        record.attributes = data_point.attributes

        # Set the value directly for both Gauge and Sum
        record.value = data_point.value

        return record

    def _convert_histogram(self, metric: Metric, data_point: Any) -> MetricRecord:
        """Convert a Histogram metric datapoint to a metric record.

        https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/awsemfexporter/datapoint.go#L87

        Args:
            metric: The metric object
            data_point: The datapoint to convert

        Returns:
            MetricRecord with populated timestamp, attributes, and histogram_data
        """
        # Create base record
        record = self._create_metric_record(metric.name, metric.unit, metric.description)

        # Set timestamp
        timestamp_ms = (
            self._normalize_timestamp(data_point.time_unix_nano)
            if data_point.time_unix_nano is not None
            else int(time.time() * 1000)
        )
        record.timestamp = timestamp_ms

        # Set attributes
        record.attributes = data_point.attributes

        # For Histogram, set the histogram_data
        record.histogram_data = {
            "Count": data_point.count,
            "Sum": data_point.sum,
            "Min": data_point.min,
            "Max": data_point.max,
        }
        return record

    # pylint: disable=too-many-locals
    def _convert_exp_histogram(self, metric: Metric, data_point: Any) -> MetricRecord:
        """
        Convert an ExponentialHistogram metric datapoint to a metric record.

        This function follows the logic of CalculateDeltaDatapoints in the Go implementation,
        converting exponential buckets to their midpoint values.

        Ref:
            https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/22626

        Args:
            metric: The metric object
            data_point: The datapoint to convert

        Returns:
            MetricRecord with populated timestamp, attributes, and exp_histogram_data
        """

        # Create base record
        record = self._create_metric_record(metric.name, metric.unit, metric.description)

        # Set timestamp
        timestamp_ms = (
            self._normalize_timestamp(data_point.time_unix_nano)
            if data_point.time_unix_nano is not None
            else int(time.time() * 1000)
        )
        record.timestamp = timestamp_ms

        # Set attributes
        record.attributes = data_point.attributes

        # Initialize arrays for values and counts
        array_values = []
        array_counts = []

        # Get scale
        scale = data_point.scale
        # Calculate base using the formula: 2^(2^(-scale))
        base = math.pow(2, math.pow(2, float(-scale)))

        # Process positive buckets
        if data_point.positive and data_point.positive.bucket_counts:
            positive_offset = getattr(data_point.positive, "offset", 0)
            positive_bucket_counts = data_point.positive.bucket_counts

            bucket_begin = 0
            bucket_end = 0

            for bucket_index, count in enumerate(positive_bucket_counts):
                index = bucket_index + positive_offset

                if bucket_begin == 0:
                    bucket_begin = math.pow(base, float(index))
                else:
                    bucket_begin = bucket_end

                bucket_end = math.pow(base, float(index + 1))

                # Calculate midpoint value of the bucket
                metric_val = (bucket_begin + bucket_end) / 2

                # Only include buckets with positive counts
                if count > 0:
                    array_values.append(metric_val)
                    array_counts.append(float(count))

        # Process zero bucket
        zero_count = getattr(data_point, "zero_count", 0)
        if zero_count > 0:
            array_values.append(0)
            array_counts.append(float(zero_count))

        # Process negative buckets
        if data_point.negative and data_point.negative.bucket_counts:
            negative_offset = getattr(data_point.negative, "offset", 0)
            negative_bucket_counts = data_point.negative.bucket_counts

            bucket_begin = 0
            bucket_end = 0

            for bucket_index, count in enumerate(negative_bucket_counts):
                index = bucket_index + negative_offset

                if bucket_end == 0:
                    bucket_end = -math.pow(base, float(index))
                else:
                    bucket_end = bucket_begin

                bucket_begin = -math.pow(base, float(index + 1))

                # Calculate midpoint value of the bucket
                metric_val = (bucket_begin + bucket_end) / 2

                # Only include buckets with positive counts
                if count > 0:
                    array_values.append(metric_val)
                    array_counts.append(float(count))

        # Set the histogram data in the format expected by CloudWatch EMF
        record.exp_histogram_data = {
            "Values": array_values,
            "Counts": array_counts,
            "Count": data_point.count,
            "Sum": data_point.sum,
            "Max": data_point.max,
            "Min": data_point.min,
        }

        return record

    def _group_by_attributes_and_timestamp(self, record: MetricRecord) -> Tuple[str, int]:
        """Group metric record by attributes and timestamp.

        Args:
            record: The metric record

        Returns:
            A tuple key for grouping
        """
        # Create a key for grouping based on attributes
        attrs_key = self._get_attributes_key(record.attributes)
        return (attrs_key, record.timestamp)

    def _create_emf_log(
        self, metric_records: List[MetricRecord], resource: Resource, timestamp: Optional[int] = None
    ) -> Dict:
        """
        Create EMF log dictionary from metric records.

        Since metric_records is already grouped by attributes, this function
        creates a single EMF log for all records.
        """
        # Start with base structure
        emf_log = {"_aws": {"Timestamp": timestamp or int(time.time() * 1000), "CloudWatchMetrics": []}}

        # Set with latest EMF version schema
        # opentelemetry-collector-contrib/blob/main/exporter/awsemfexporter/metric_translator.go#L414
        emf_log["Version"] = "1"

        # Add resource attributes to EMF log but not as dimensions
        # OTel collector EMF Exporter has a resource_to_telemetry_conversion flag that will convert resource attributes
        # as regular metric attributes(potential dimensions). However, for this SDK EMF implementation,
        # we align with the OpenTelemetry concept that all metric attributes are treated as dimensions.
        # And have resource attributes as just additional metadata in EMF, added otel.resource as prefix to distinguish.
        if resource and resource.attributes:
            for key, value in resource.attributes.items():
                emf_log[f"otel.resource.{key}"] = str(value)

        # Initialize collections for dimensions and metrics
        metric_definitions = []
        # Collect attributes from all records (they should be the same for all records in the group)
        # Only collect once from the first record and apply to all records
        all_attributes = (
            metric_records[0].attributes
            if metric_records and len(metric_records) > 0 and metric_records[0].attributes
            else {}
        )

        # Process each metric record
        for record in metric_records:

            metric_name = self._get_metric_name(record)

            # Skip processing if metric name is None or empty
            if not metric_name:
                continue

            # Create metric data dict
            metric_data = {"Name": metric_name}

            unit = self._get_unit(record)
            if unit:
                metric_data["Unit"] = unit

            # Process different types of aggregations
            if record.exp_histogram_data:
                # Base2 Exponential Histogram
                emf_log[metric_name] = record.exp_histogram_data
            elif record.histogram_data:
                # Regular Histogram metrics
                emf_log[metric_name] = record.histogram_data
            elif record.value is not None:
                # Gauge, Sum, and other aggregations
                emf_log[metric_name] = record.value
            else:
                logger.debug("Skipping metric %s as it does not have valid metric value", metric_name)
                continue

            # Add to metric definitions list
            metric_definitions.append(metric_data)

        # Get dimension names from collected attributes
        dimension_names = self._get_dimension_names(all_attributes)

        # Add attribute values to the root of the EMF log
        for name, value in all_attributes.items():
            emf_log[name] = str(value)

        # Add the single dimension set to CloudWatch Metrics if we have dimensions and metrics
        if dimension_names and metric_definitions:
            emf_log["_aws"]["CloudWatchMetrics"].append(
                {"Namespace": self.namespace, "Dimensions": [dimension_names], "Metrics": metric_definitions}
            )

        return emf_log

    def _send_log_event(self, log_event: Dict[str, Any]):
        """
        Send a log event to CloudWatch Logs using the log client.

        Args:
            log_event: The log event to send
        """
        self.log_client.send_log_event(log_event)

    # pylint: disable=too-many-nested-blocks,unused-argument,too-many-branches
    def export(
        self, metrics_data: MetricsData, timeout_millis: Optional[int] = None, **_kwargs: Any
    ) -> MetricExportResult:
        """
        Export metrics as EMF logs to CloudWatch.

        Groups metrics by attributes and timestamp before creating EMF logs.

        Args:
            metrics_data: MetricsData containing resource metrics and scope metrics
            timeout_millis: Optional timeout in milliseconds
            **kwargs: Additional keyword arguments

        Returns:
            MetricExportResult indicating success or failure
        """
        try:
            if not metrics_data.resource_metrics:
                return MetricExportResult.SUCCESS

            # Process all metrics from all resource metrics and scope metrics
            for resource_metrics in metrics_data.resource_metrics:
                for scope_metrics in resource_metrics.scope_metrics:
                    # Dictionary to group metrics by attributes and timestamp
                    grouped_metrics = defaultdict(list)

                    # Process all metrics in this scope
                    for metric in scope_metrics.metrics:
                        # Skip if metric.data is None or no data_points exists
                        try:
                            if not (metric.data and metric.data.data_points):
                                continue
                        except AttributeError:
                            # Metric doesn't have data or data_points attribute
                            continue

                        # Process metrics based on type
                        metric_type = type(metric.data)
                        if metric_type in (Gauge, Sum):
                            for dp in metric.data.data_points:
                                record = self._convert_gauge_and_sum(metric, dp)
                                grouped_metrics[self._group_by_attributes_and_timestamp(record)].append(record)
                        elif metric_type == Histogram:
                            for dp in metric.data.data_points:
                                record = self._convert_histogram(metric, dp)
                                grouped_metrics[self._group_by_attributes_and_timestamp(record)].append(record)
                        elif metric_type == ExponentialHistogram:
                            for dp in metric.data.data_points:
                                record = self._convert_exp_histogram(metric, dp)
                                grouped_metrics[self._group_by_attributes_and_timestamp(record)].append(record)
                        else:
                            logger.debug("Unsupported Metric Type: %s", metric_type)

                    # Now process each group separately to create one EMF log per group
                    for (_, timestamp_ms), metric_records in grouped_metrics.items():
                        if not metric_records:
                            continue

                        # Create and send EMF log for this batch of metrics
                        self._send_log_event(
                            {
                                "message": json.dumps(
                                    self._create_emf_log(metric_records, resource_metrics.resource, timestamp_ms)
                                ),
                                "timestamp": timestamp_ms,
                            }
                        )

            return MetricExportResult.SUCCESS
        # pylint: disable=broad-exception-caught
        # capture all types of exceptions to not interrupt the instrumented services
        except Exception as error:
            logger.error("Failed to export metrics: %s", error)
            return MetricExportResult.FAILURE

    def force_flush(self, timeout_millis: int = 10000) -> bool:  # pylint: disable=unused-argument
        """
        Force flush any pending metrics.

        Args:
            timeout_millis: Timeout in milliseconds

        Returns:
            True if successful, False otherwise
        """
        self.log_client.flush_pending_events()
        logger.debug("AwsCloudWatchEmfExporter force flushes the buffered metrics")
        return True

    def shutdown(self, timeout_millis: Optional[int] = None, **_kwargs: Any) -> bool:
        """
        Shutdown the exporter.
        Override to handle timeout and other keyword arguments, but do nothing.

        Args:
            timeout_millis: Ignored timeout in milliseconds
            **kwargs: Ignored additional keyword arguments
        """
        # Force flush any remaining batched events
        self.force_flush(timeout_millis)
        logger.debug("AwsCloudWatchEmfExporter shutdown called with timeout_millis=%s", timeout_millis)
        return True
