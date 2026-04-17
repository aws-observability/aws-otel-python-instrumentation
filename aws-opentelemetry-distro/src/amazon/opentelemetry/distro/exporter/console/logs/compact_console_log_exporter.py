# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import re
import sys
from typing import IO, Sequence

try:
    from opentelemetry.sdk._logs.export import LogRecordExportResult as LogExportResult
except ImportError:
    from opentelemetry.sdk._logs.export import LogExportResult

# Support both old (LogData/LogExporter) and new (ReadableLogRecord/LogRecordExporter) APIs
try:
    from opentelemetry.sdk._logs.export import LogRecordExporter

    _BASE_CLASS = LogRecordExporter
except ImportError:
    from opentelemetry.sdk._logs.export import LogExporter

    _BASE_CLASS = LogExporter

_logger = logging.getLogger(__name__)


def _preserve_attrs(attributes) -> dict:
    """Preserve attribute value types (int, float, bool, str, list)."""
    if not attributes:
        return {}
    return dict(attributes)


def _get_dropped_attrs(data, record) -> int:
    """Extract dropped attributes count from whichever object has it."""
    if hasattr(data, "dropped_attributes"):
        return data.dropped_attributes or 0
    if hasattr(record, "dropped_attributes"):
        return record.dropped_attributes or 0
    return 0


class CompactConsoleLogRecordExporter(_BASE_CLASS):
    """Exports log records as compact JSON to stdout.

    Produces a single-line JSON object per log record aligned with the
    CloudWatch OTLP backend's flattened JSON format.

    If the standardized serialization fails for any reason, falls back to
    the upstream SDK's to_json() format to avoid breaking existing infrastructure.
    """

    def __init__(self, out: IO = None):
        self._out = out or sys.stdout
        self._shutdown = False

    def export(self, batch: Sequence) -> LogExportResult:
        if self._shutdown:
            return LogExportResult.FAILURE

        for data in batch:
            try:
                line = self._to_compact_json(data)
            except Exception:  # pylint: disable=broad-exception-caught
                _logger.debug(
                    "Failed to serialize log record, falling back",
                    exc_info=True,
                )
                try:
                    line = self._fallback_format(data)
                except Exception:  # pylint: disable=broad-exception-caught
                    _logger.debug("Fallback also failed", exc_info=True)
                    continue

            self._out.write(line + "\n")
            self._out.flush()

        return LogExportResult.SUCCESS

    def shutdown(self):
        self._shutdown = True

    @staticmethod
    def _to_compact_json(data) -> str:
        # Support both ReadableLogRecord (1.39+) and LogData (older) APIs.
        record = data.log_record
        resource = getattr(data, "resource", None) or getattr(record, "resource", None)
        scope = getattr(data, "instrumentation_scope", None)

        trace_id = getattr(record, "trace_id", None)
        span_id = getattr(record, "span_id", None)
        is_valid = trace_id is not None and span_id is not None and trace_id != 0 and span_id != 0

        return json.dumps(
            {
                "resource": {
                    "attributes": _preserve_attrs(resource.attributes if resource else None),
                    "schemaUrl": getattr(resource, "schema_url", "") or "" if resource else "",
                },
                "scope": {
                    "name": getattr(scope, "name", "") or "" if scope else "",
                    "version": getattr(scope, "version", "") or "" if scope else "",
                    "schemaUrl": getattr(scope, "schema_url", "") or "" if scope else "",
                },
                "body": record.body if record.body is not None else None,
                "severityNumber": (record.severity_number.value if record.severity_number is not None else 0),
                "severityText": (record.severity_number.name if record.severity_number is not None else "UNSPECIFIED"),
                "attributes": _preserve_attrs(record.attributes),
                "droppedAttributes": _get_dropped_attrs(data, record),
                "timeUnixNano": record.timestamp or 0,
                "observedTimeUnixNano": (record.observed_timestamp or 0),
                "traceId": format(trace_id, "032x") if is_valid else "",
                "spanId": format(span_id, "016x") if is_valid else "",
                "flags": int(record.trace_flags) if record.trace_flags is not None else 0,
                "exportPath": "console",
            },
            separators=(",", ":"),
        )

    @staticmethod
    def _fallback_format(data) -> str:
        """Fall back to upstream SDK's to_json() with whitespace stripped."""
        # ReadableLogRecord has to_json() directly; LogData has it on .log_record
        obj = data if hasattr(data, "to_json") else data.log_record
        formatted_json = obj.to_json()
        return re.sub(r"\s*([{}[\]:,])\s*", r"\1", formatted_json)
