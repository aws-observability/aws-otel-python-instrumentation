# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import logging
from typing import Any, Dict, Optional

from opentelemetry.sdk.metrics.export import AggregationTemporality

from .base_emf_exporter import BaseEmfExporter

logger = logging.getLogger(__name__)


class ConsoleEmfExporter(BaseEmfExporter):
    """
    OpenTelemetry metrics exporter for CloudWatch EMF format to console output.

    This exporter converts OTel metrics into CloudWatch EMF logs and writes them
    to standard output instead of sending to CloudWatch Logs. This is useful for
    debugging, testing, or when you want to process EMF logs with other tools.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html

    """

    def __init__(
        self,
        namespace: str = "default",
        preferred_temporality: Optional[Dict[type, AggregationTemporality]] = None,
        preferred_aggregation: Optional[Dict[type, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Console EMF exporter.

        Args:
            namespace: CloudWatch namespace for metrics (defaults to "default")
            preferred_temporality: Optional dictionary mapping instrument types to aggregation temporality
            preferred_aggregation: Optional dictionary mapping instrument types to preferred aggregation
            **kwargs: Additional arguments (unused, kept for compatibility)
        """
        # No need to check for None since namespace has a default value
        if namespace is None:
            namespace = "default"
        super().__init__(namespace, preferred_temporality, preferred_aggregation)

    def _export(self, log_event: Dict[str, Any]) -> None:
        """
        Send a log event message to stdout for console output.

        This method writes the EMF log message to stdout, making it easy to
        capture and redirect the output for processing or debugging purposes.

        Args:
            log_event: The log event dictionary containing 'message' and 'timestamp'
                      keys, where 'message' is the JSON-serialized EMF log

        Raises:
            No exceptions are raised - errors are logged and handled gracefully
        """
        try:
            # Write the EMF log message to stdout for easy redirection/capture
            message = log_event.get("message", "")
            if message:
                print(message, flush=True)
            else:
                logger.warning("Empty message in log event: %s", log_event)
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error("Failed to write EMF log to console. Log event: %s. Error: %s", log_event, error)

    def force_flush(self, timeout_millis: int = 10000) -> bool:
        """
        Force flush any pending metrics.

        For console output, there's no buffering since we use print() with
        flush=True, so this is effectively a no-op that always succeeds.

        Args:
            timeout_millis: Timeout in milliseconds (unused for console output)

        Returns:
            Always returns True as console output is immediately flushed
        """
        logger.debug("ConsoleEmfExporter force_flush called - no buffering to flush for console output")
        return True

    def shutdown(self, timeout_millis: Optional[int] = None, **kwargs: Any) -> bool:
        """
        Shutdown the exporter.

        For console output, there are no resources to clean up or connections
        to close, so this is effectively a no-op that always succeeds.

        Args:
            timeout_millis: Timeout in milliseconds (unused for console output)
            **kwargs: Additional keyword arguments (unused for console output)

        Returns:
            Always returns True as there's no cleanup required for console output
        """
        logger.debug("ConsoleEmfExporter shutdown called with timeout_millis=%s", timeout_millis)
        return True
