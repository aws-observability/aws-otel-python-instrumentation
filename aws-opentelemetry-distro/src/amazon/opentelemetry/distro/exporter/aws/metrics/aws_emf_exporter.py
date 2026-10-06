# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Optional

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.aws.metrics.base_emf_exporter import BaseEmfExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.logs._log_header_config import _fetch_logs_header
from opentelemetry.sdk.metrics.export import MetricExportResult, MetricsData

if TYPE_CHECKING:
    from amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client import (
        CloudWatchLogClient as _CloudWatchLogClient,
    )

_logger = getLogger(__name__)


class AwsEmfExporter(BaseEmfExporter):
    """Export EMF metrics to Lambda stdout or the configured CloudWatch Logs destination."""

    def __init__(self) -> None:
        self._console = False
        self._log_client: Optional["_CloudWatchLogClient"] = None
        namespace = None
        try:
            headers = _fetch_logs_header()
            namespace = headers.namespace
            if "AWS_LAMBDA_FUNCTION_NAME" in os.environ and not headers.is_valid():
                namespace = namespace if namespace is not None else "default"
                self._console = True
                _logger.info(
                    "Using the console EMF metrics exporter; destination=standard output; "
                    "authentication=none because the exporter makes no network request."
                )
            else:
                session = get_aws_session()
                if not session:
                    _logger.warning("botocore is not installed. EMF exporter requires botocore")
                elif headers.is_valid():
                    # pylint: disable=import-outside-toplevel
                    from amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client import (
                        CloudWatchLogClient,
                    )

                    self._log_client = CloudWatchLogClient(
                        session=session,
                        log_group_name=headers.log_group,
                        log_stream_name=headers.log_stream,
                    )
                    _logger.info(
                        "Using the CloudWatch EMF metrics exporter; destination=CloudWatch Logs; "
                        "authentication=AWS SDK SigV4."
                    )
        except Exception as error:  # pylint: disable=broad-exception-caught
            _logger.error("Failed to create EMF exporter: %s", error)
        super().__init__(namespace=namespace)

    @property
    def enabled(self) -> bool:
        """Return whether a destination could be configured."""
        return self._console or self._log_client is not None

    def export(self, metrics_data: MetricsData, timeout_millis: float = 10000, **kwargs: Any) -> MetricExportResult:
        if not self.enabled:
            return MetricExportResult.FAILURE
        return super().export(metrics_data, timeout_millis=timeout_millis, **kwargs)

    def _export(self, log_event: Dict[str, Any]) -> None:
        if self._log_client is not None:
            self._log_client.send_log_event(log_event)
        elif self._console:
            try:
                message = log_event.get("message", "")
                if message:
                    print(message, flush=True)
                else:
                    _logger.warning("Empty message in log event: %s", log_event)
            except Exception as error:  # pylint: disable=broad-exception-caught
                _logger.error("Failed to write EMF log to console. Log event: %s. Error: %s", log_event, error)

    def force_flush(self, timeout_millis: float = 10000) -> bool:
        if self._log_client is not None:
            self._log_client.flush_pending_events()
        return True

    def shutdown(self, timeout_millis: float = 30000, **kwargs: Any) -> bool:
        return self.force_flush(timeout_millis=timeout_millis)
