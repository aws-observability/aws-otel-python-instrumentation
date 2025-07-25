# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import logging
from typing import Any, Dict, Optional

from opentelemetry.sdk.metrics.export import AggregationTemporality

from ._cloudwatch_log_client import CloudWatchLogClient
from .base_emf_exporter import BaseEmfExporter

logger = logging.getLogger(__name__)


class AwsCloudWatchEmfExporter(BaseEmfExporter):
    """
    OpenTelemetry metrics exporter for CloudWatch EMF format.

    This exporter converts OTel metrics into CloudWatch EMF logs which are then
    sent to CloudWatch Logs. CloudWatch Logs automatically extracts the metrics
    from the EMF logs.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html

    """

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
        super().__init__(namespace, preferred_temporality, preferred_aggregation)

        self.log_group_name = log_group_name

        # Initialize CloudWatch Logs client
        self.log_client = CloudWatchLogClient(
            log_group_name=log_group_name, log_stream_name=log_stream_name, aws_region=aws_region, **kwargs
        )

    def _export(self, log_event: Dict[str, Any]):
        """
        Send a log event to CloudWatch Logs using the log client.

        Args:
            log_event: The log event to send
        """
        self.log_client.send_log_event(log_event)

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
