# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from logging import getLogger
from typing import Any, Optional

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.logs._log_header_config import _fetch_logs_header
from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult, MetricsData

_logger = getLogger(__name__)


def _create_emf_exporter() -> Optional[MetricExporter]:
    """Select the EMF destination without requiring botocore for Lambda stdout."""
    try:
        headers = _fetch_logs_header()
        if "AWS_LAMBDA_FUNCTION_NAME" in os.environ and not headers.is_valid():
            # pylint: disable=import-outside-toplevel
            from amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter import ConsoleEmfExporter

            _logger.info(
                "Using the console EMF metrics exporter; destination=standard output; "
                "authentication=none because the exporter makes no network request."
            )
            return ConsoleEmfExporter(namespace=headers.namespace)

        session = get_aws_session()
        if not session:
            _logger.warning("botocore is not installed. EMF exporter requires botocore")
            return None

        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter import (
            AwsCloudWatchEmfExporter,
        )

        if not headers.is_valid():
            return None

        _logger.info(
            "Using the CloudWatch EMF metrics exporter; destination=CloudWatch Logs; authentication=AWS SDK SigV4."
        )
        return AwsCloudWatchEmfExporter(
            session=session,
            namespace=headers.namespace,
            log_group_name=headers.log_group,
            log_stream_name=headers.log_stream,
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        _logger.error("Failed to create EMF exporter: %s", error)
        return None


class AwsEmfExporter(MetricExporter):
    """Environment-configured EMF exporter for the OpenTelemetry entry point."""

    def __init__(self) -> None:
        self._exporter = _create_emf_exporter()
        # pylint: disable=protected-access
        super().__init__(
            preferred_temporality=self._exporter._preferred_temporality if self.enabled else None,
            preferred_aggregation=self._exporter._preferred_aggregation if self.enabled else None,
        )

    @property
    def enabled(self) -> bool:
        """Return whether a destination could be configured."""
        return self._exporter is not None

    def export(self, metrics_data: MetricsData, timeout_millis: float = 10000, **kwargs: Any) -> MetricExportResult:
        if not self.enabled:
            return MetricExportResult.FAILURE
        return self._exporter.export(metrics_data, timeout_millis=timeout_millis, **kwargs)

    def force_flush(self, timeout_millis: float = 10000) -> bool:
        if not self.enabled:
            return True
        return self._exporter.force_flush(timeout_millis=timeout_millis)

    def shutdown(self, timeout_millis: float = 30000, **kwargs: Any) -> bool:
        if not self.enabled:
            return True
        return self._exporter.shutdown(timeout_millis=timeout_millis, **kwargs)
