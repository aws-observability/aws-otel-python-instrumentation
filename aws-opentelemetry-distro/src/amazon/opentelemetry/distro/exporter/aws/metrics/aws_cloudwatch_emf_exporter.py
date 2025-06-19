# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import json
import logging
import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import botocore.session
from botocore.exceptions import ClientError

from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics._internal.point import Metric
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    Gauge,
    MetricExporter,
    MetricExportResult,
    MetricsData,
    NumberDataPoint,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.util.types import Attributes

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
            **kwargs: Additional arguments passed to botocore client
        """
        # Set up temporality preference default to DELTA if customers not set
        if preferred_temporality is None:
            preferred_temporality = {
                Counter: AggregationTemporality.DELTA,
                Histogram: AggregationTemporality.DELTA,
                ObservableCounter: AggregationTemporality.DELTA,
                ObservableGauge: AggregationTemporality.DELTA,
                ObservableUpDownCounter: AggregationTemporality.DELTA,
                UpDownCounter: AggregationTemporality.DELTA,
            }

        super().__init__(preferred_temporality)

        self.namespace = namespace
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name or self._generate_log_stream_name()

        session = botocore.session.Session()
        self.logs_client = session.create_client("logs", region_name=aws_region, **kwargs)

        # Ensure log group exists
        self._ensure_log_group_exists()

        # Ensure log stream exists
        self._ensure_log_stream_exists()

    # Default to unique log stream name matching OTel Collector
    # EMF Exporter behavior with language for source identification
    def _generate_log_stream_name(self) -> str:
        """Generate a unique log stream name."""

        unique_id = str(uuid.uuid4())[:8]
        return f"otel-python-{unique_id}"

    def _ensure_log_group_exists(self):
        """Ensure the log group exists, create if it doesn't."""
        try:
            self.logs_client.create_log_group(logGroupName=self.log_group_name)
            logger.info("Created log group: %s", self.log_group_name)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ResourceAlreadyExistsException":
                logger.debug("Log group %s already exists", self.log_group_name)
            else:
                logger.error("Failed to create log group %s : %s", self.log_group_name, error)
                raise

    def _ensure_log_stream_exists(self):
        try:
            self.logs_client.create_log_stream(logGroupName=self.log_group_name, logStreamName=self.log_stream_name)
            logger.info("Created log stream: %s", self.log_stream_name)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ResourceAlreadyExistsException":
                logger.debug("Log stream %s already exists", self.log_stream_name)
            else:
                logger.error("Failed to create log stream %s : %s", self.log_group_name, error)
                raise

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

    def _convert_gauge(self, metric: Metric, data_point: NumberDataPoint) -> MetricRecord:
        """Convert a Gauge metric datapoint to a metric record.

        Args:
            metric: The metric object
            data_point: The datapoint to convert

        Returns:
            MetricRecord with populated timestamp, attributes, and value
        """
        # Create base record
        record = self._create_metric_record(metric.name, metric.unit, metric.description)

        # Set timestamp
        try:
            timestamp_ms = (
                self._normalize_timestamp(data_point.time_unix_nano)
                if data_point.time_unix_nano is not None
                else int(time.time() * 1000)
            )
        except AttributeError:
            # data_point doesn't have time_unix_nano attribute
            timestamp_ms = int(time.time() * 1000)
        record.timestamp = timestamp_ms

        # Set attributes
        try:
            record.attributes = data_point.attributes
        except AttributeError:
            # data_point doesn't have attributes
            record.attributes = {}

        # For Gauge, set the value directly
        try:
            record.value = data_point.value
        except AttributeError:
            # data_point doesn't have value
            record.value = None

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
        all_attributes = metric_records[0].attributes if metric_records and metric_records[0].attributes else {}

        # Process each metric record
        for record in metric_records:

            metric_name = self._get_metric_name(record)

            # Skip processing if metric name is None or empty
            if not metric_name:
                continue

            # Skip processing if metric value is None or empty
            if record.value is None:
                logger.debug("Skipping metric %s as it does not have valid metric value", metric_name)
                continue

            # Create metric data dict
            metric_data = {"Name": metric_name}

            unit = self._get_unit(record)
            if unit:
                metric_data["Unit"] = unit

            # Add to metric definitions list
            metric_definitions.append(metric_data)

            emf_log[metric_name] = record.value

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

    # pylint: disable=no-member
    def _send_log_event(self, log_event: Dict[str, Any]):
        """
        Send a log event to CloudWatch Logs.

        Basic implementation for PR 1 - sends individual events directly.

        TODO: Batching event and follow CloudWatch Logs quato constraints - number of events & size limit per payload
        """
        try:
            # Send the log event
            response = self.logs_client.put_log_events(
                logGroupName=self.log_group_name, logStreamName=self.log_stream_name, logEvents=[log_event]
            )

            logger.debug("Successfully sent log event")
            return response

        except ClientError as error:
            logger.debug("Failed to send log event: %s", error)
            raise

    # pylint: disable=too-many-nested-blocks
    def export(
        self, metrics_data: MetricsData, timeout_millis: Optional[int] = None, **kwargs: Any
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
                        if metric_type == Gauge:
                            for dp in metric.data.data_points:
                                record = self._convert_gauge(metric, dp)
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

    def force_flush(self, timeout_millis: int = 10000) -> bool:
        """
        Force flush any pending metrics.

        TODO: will add logic to handle gracefule shutdown

        Args:
            timeout_millis: Timeout in milliseconds

        Returns:
            True if successful, False otherwise
        """
        logger.debug("AwsCloudWatchEmfExporter force flushes the buffered metrics")
        return True

    def shutdown(self, timeout_millis: Optional[int] = None, **kwargs: Any) -> bool:
        """
        Shutdown the exporter.
        Override to handle timeout and other keyword arguments, but do nothing.

        TODO: will add logic to handle gracefule shutdown

        Args:
            timeout_millis: Ignored timeout in milliseconds
            **kwargs: Ignored additional keyword arguments
        """
        # Intentionally do nothing
        self.force_flush(timeout_millis)
        logger.debug("AwsCloudWatchEmfExporter shutdown called with timeout_millis=%s", timeout_millis)
        return True
