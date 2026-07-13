# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
ServiceEvents exporters.

- ``ServiceEventsOtlpEmitter``: OTLP LogRecord + metric emitter (primary signal path).
- ``ServiceEventsCloudWatchLogFileExporter`` / ``ServiceEventsCloudWatchMetricFileExporter``:
  local-testing file exporter that writes CloudWatch-faithful NDJSON instead of
  hitting an OTLP endpoint. Enabled via ``OTEL_AWS_SERVICE_EVENTS_OUTPUT_FILE``.
"""

from amazon.opentelemetry.serviceevents.exporter.cloudwatch_file_exporter import (
    ServiceEventsCloudWatchLogFileExporter,
    ServiceEventsCloudWatchMetricFileExporter,
)

__all__ = [
    "ServiceEventsCloudWatchLogFileExporter",
    "ServiceEventsCloudWatchMetricFileExporter",
]
