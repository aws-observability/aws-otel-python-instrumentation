# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import IO, Optional, Sequence

from opentelemetry.sdk._logs.export import LogExportResult

# Support both old (LogData/LogExporter) and new (ReadableLogRecord/LogRecordExporter) APIs
try:
    from opentelemetry.sdk._logs.export import LogRecordExporter

    _BASE_CLASS = LogRecordExporter
except ImportError:
    from opentelemetry.sdk._logs.export import LogExporter

    _BASE_CLASS = LogExporter

_logger = logging.getLogger(__name__)


class CompactConsoleLogRecordExporter(_BASE_CLASS):
    """Exports log records as compact JSON to stdout.

    Produces a single-line JSON object per log record matching the canonical
    schema shared across all ADOT language implementations. This exporter is
    used in AWS Lambda environments when OTEL_LOGS_EXPORTER=console.

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
            except Exception:
                _logger.debug(
                    "Failed to serialize log record with standardized format, falling back to upstream SDK",
                    exc_info=True,
                )
                line = self._fallback_format(data)

            self._out.write(line + "\n")
            self._out.flush()

        return LogExportResult.SUCCESS

    def shutdown(self):
        self._shutdown = True

    def _to_compact_json(self, data) -> str:
        # Support both ReadableLogRecord (1.40+) and LogData (older) APIs.
        # ReadableLogRecord: .log_record, .resource, .instrumentation_scope
        # LogData: .log_record, .instrumentation_scope (resource on log_record)
        record = data.log_record
        resource = getattr(data, "resource", None) or getattr(record, "resource", None)
        scope = getattr(data, "instrumentation_scope", None)

        # Resource
        resource_attrs = {}
        if resource and resource.attributes:
            for k, v in resource.attributes.items():
                resource_attrs[k] = str(v)
        resource_schema_url = ""
        if resource and hasattr(resource, "schema_url") and resource.schema_url:
            resource_schema_url = resource.schema_url

        # Span context validity: both trace_id and span_id must be non-zero
        trace_id = getattr(record, "trace_id", None)
        span_id = getattr(record, "span_id", None)
        trace_id_valid = trace_id is not None and span_id is not None and trace_id != 0 and span_id != 0

        # Attributes — coerce all values to strings
        attrs = {}
        if record.attributes:
            for k, v in record.attributes.items():
                attrs[k] = str(v)

        # Severity text from severity number enum name (matches OTel spec names)
        severity_text = record.severity_number.name if record.severity_number is not None else "UNSPECIFIED"
        severity_number = record.severity_number.value if record.severity_number is not None else 0

        # Instrumentation scope
        scope_name = ""
        scope_version = ""
        scope_schema_url = ""
        if scope:
            scope_name = getattr(scope, "name", "") or ""
            scope_version = getattr(scope, "version", "") or ""
            scope_schema_url = getattr(scope, "schema_url", "") or ""

        # Dropped attributes
        dropped = 0
        if hasattr(data, "dropped_attributes"):
            dropped = data.dropped_attributes
        elif hasattr(record, "dropped_attributes"):
            dropped = record.dropped_attributes

        output = {
            "resource": {
                "attributes": resource_attrs,
                "schemaUrl": resource_schema_url,
            },
            "body": record.body if record.body is not None else None,
            "severityNumber": severity_number,
            "severityText": severity_text,
            "attributes": attrs,
            "droppedAttributes": dropped,
            "timestamp": _format_nanos(record.timestamp),
            "observedTimestamp": _format_nanos(record.observed_timestamp),
            "traceId": format(trace_id, "032x") if trace_id_valid else "",
            "spanId": format(span_id, "016x") if trace_id_valid else "",
            "traceFlags": int(record.trace_flags) if record.trace_flags is not None else 0,
            "instrumentationScope": {
                "name": scope_name,
                "version": scope_version,
                "schemaUrl": scope_schema_url,
            },
        }

        return json.dumps(output, separators=(",", ":"))

    @staticmethod
    def _fallback_format(data) -> str:
        """Fall back to upstream SDK's to_json() with whitespace stripped."""
        # ReadableLogRecord has to_json() directly; LogData has it on .log_record
        obj = data if hasattr(data, "to_json") else data.log_record
        formatted_json = obj.to_json()
        return re.sub(r"\s*([{}[\]:,])\s*", r"\1", formatted_json)


def _format_nanos(nanos) -> Optional[str]:
    """Convert epoch nanoseconds to ISO-8601 UTC string with trailing zero truncation.

    Matches Java's DateTimeFormatter.ISO_INSTANT behavior:
    - 2001-09-09T01:46:40Z (no fractional seconds when millis == 0)
    - 2001-09-09T01:46:40.1Z (truncated trailing zeros)
    - 2001-09-09T01:46:40.12Z
    - 2001-09-09T01:46:40.123Z
    """
    if nanos is None or nanos == 0:
        return None
    millis = nanos // 1_000_000
    dt = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    frac_millis = millis % 1000
    if frac_millis == 0:
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    frac = f".{frac_millis:03d}".rstrip("0")
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + frac + "Z"
