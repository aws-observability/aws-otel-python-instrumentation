# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import io
import json
import unittest

from amazon.opentelemetry.distro.exporter.console.logs.compact_console_log_exporter import (
    CompactConsoleLogRecordExporter,
)
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs.export import LogExportResult
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags

# Support both old (LogData) and new (ReadableLogRecord) SDK APIs
try:
    from opentelemetry.sdk._logs import ReadableLogRecord as _ReadableLogRecord

    _HAS_READABLE_LOG_RECORD = True
except ImportError:
    _HAS_READABLE_LOG_RECORD = False

from opentelemetry.sdk._logs import LogRecord

try:
    from opentelemetry.sdk._logs import LogData
except ImportError:
    LogData = None


def _make_log_data(
    body="Test log message",
    severity_number=SeverityNumber.INFO,
    trace_id=int("12345678901234567890123456789012", 16),
    span_id=int("1234567890123456", 16),
    trace_flags=TraceFlags(TraceFlags.SAMPLED),
    timestamp=1000000000 * 1_000_000_000,
    observed_timestamp=1000000000 * 1_000_000_000,
    attributes=None,
    resource=None,
    scope=None,
):
    if attributes is None:
        attributes = {"key": "value"}
    if resource is None:
        resource = Resource(
            attributes={"service.name": "test-service"},
            schema_url="https://opentelemetry.io/schemas/1.0.0",
        )
    if scope is None:
        scope = InstrumentationScope(
            name="test-scope",
            version="1.0.0",
            schema_url="https://opentelemetry.io/schemas/1.0.0",
        )

    log_record = LogRecord(
        timestamp=timestamp,
        observed_timestamp=observed_timestamp,
        trace_id=trace_id,
        span_id=span_id,
        trace_flags=trace_flags,
        severity_number=severity_number,
        body=body,
        resource=resource,
        attributes=attributes if attributes else None,
    )

    # Use ReadableLogRecord (1.40+) if available, otherwise LogData
    if _HAS_READABLE_LOG_RECORD:
        return _ReadableLogRecord(
            log_record=log_record,
            resource=resource,
            instrumentation_scope=scope,
        )
    return LogData(log_record=log_record, instrumentation_scope=scope)


class TestCompactConsoleLogRecordExporter(unittest.TestCase):
    def setUp(self):
        self.buf = io.StringIO()
        self.exporter = CompactConsoleLogRecordExporter(out=self.buf)

    def _get_output(self):
        return self.buf.getvalue().strip()

    def _get_parsed(self):
        return json.loads(self._get_output())

    def test_export_with_all_fields_set(self):
        data = _make_log_data()
        result = self.exporter.export([data])

        self.assertEqual(result, LogExportResult.SUCCESS)
        parsed = self._get_parsed()

        # Validate all top-level fields are present
        expected_keys = [
            "resource", "body", "severityNumber", "severityText",
            "attributes", "droppedAttributes", "timestamp", "observedTimestamp",
            "traceId", "spanId", "traceFlags", "instrumentationScope",
        ]
        for key in expected_keys:
            self.assertIn(key, parsed, f"Missing key: {key}")

        # Validate values
        self.assertEqual(parsed["body"], "Test log message")
        self.assertEqual(parsed["severityNumber"], 9)
        self.assertEqual(parsed["severityText"], "INFO")
        self.assertEqual(parsed["attributes"], {"key": "value"})
        self.assertEqual(parsed["droppedAttributes"], 0)
        self.assertEqual(parsed["timestamp"], "2001-09-09T01:46:40Z")
        self.assertEqual(parsed["observedTimestamp"], "2001-09-09T01:46:40Z")
        self.assertEqual(parsed["traceId"], "12345678901234567890123456789012")
        self.assertEqual(parsed["spanId"], "1234567890123456")
        self.assertEqual(parsed["traceFlags"], 1)

        # Validate nested objects
        self.assertIn("attributes", parsed["resource"])
        self.assertEqual(parsed["resource"]["attributes"]["service.name"], "test-service")
        self.assertEqual(parsed["resource"]["schemaUrl"], "https://opentelemetry.io/schemas/1.0.0")

        scope = parsed["instrumentationScope"]
        self.assertEqual(scope["name"], "test-scope")
        self.assertEqual(scope["version"], "1.0.0")
        self.assertEqual(scope["schemaUrl"], "https://opentelemetry.io/schemas/1.0.0")

    def test_null_body(self):
        data = _make_log_data(body=None)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertIsNone(parsed["body"])

    def test_zero_timestamps(self):
        data = _make_log_data(timestamp=0, observed_timestamp=0)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertIsNone(parsed["timestamp"])
        self.assertIsNone(parsed["observedTimestamp"])

    def test_invalid_span_context_all_zeros(self):
        data = _make_log_data(trace_id=0, span_id=0)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["traceId"], "")
        self.assertEqual(parsed["spanId"], "")

    def test_invalid_trace_id_only(self):
        data = _make_log_data(trace_id=0, span_id=int("1234567890123456", 16))
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["traceId"], "")
        self.assertEqual(parsed["spanId"], "")

    def test_invalid_span_id_only(self):
        data = _make_log_data(trace_id=int("12345678901234567890123456789012", 16), span_id=0)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["traceId"], "")
        self.assertEqual(parsed["spanId"], "")

    def test_no_span_context(self):
        data = _make_log_data(trace_id=None, span_id=None, trace_flags=None)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["traceId"], "")
        self.assertEqual(parsed["spanId"], "")
        self.assertEqual(parsed["traceFlags"], 0)

    def test_empty_attributes(self):
        data = _make_log_data(attributes={})
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["attributes"], {})

    def test_numeric_attribute_values_coerced_to_string(self):
        data = _make_log_data(attributes={"count": 42, "enabled": True})
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["attributes"]["count"], "42")
        self.assertEqual(parsed["attributes"]["enabled"], "True")

    def test_multiple_log_records(self):
        data1 = _make_log_data(body="first")
        data2 = _make_log_data(body="second")
        self.exporter.export([data1, data2])
        lines = self.buf.getvalue().strip().split("\n")
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["body"], "first")
        self.assertEqual(json.loads(lines[1])["body"], "second")

    def test_export_empty_batch(self):
        result = self.exporter.export([])
        self.assertEqual(result, LogExportResult.SUCCESS)
        self.assertEqual(self.buf.getvalue(), "")

    def test_shutdown_prevents_export(self):
        self.exporter.shutdown()
        data = _make_log_data()
        result = self.exporter.export([data])
        self.assertEqual(result, LogExportResult.FAILURE)
        self.assertEqual(self.buf.getvalue(), "")

    def test_empty_resource_and_scope(self):
        data = _make_log_data(
            resource=Resource(attributes={}),
            scope=InstrumentationScope(name=""),
        )
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["resource"]["attributes"], {})
        self.assertEqual(parsed["resource"]["schemaUrl"], "")
        self.assertEqual(parsed["instrumentationScope"]["name"], "")
        self.assertEqual(parsed["instrumentationScope"]["version"], "")
        self.assertEqual(parsed["instrumentationScope"]["schemaUrl"], "")

    def test_timestamp_with_milliseconds(self):
        # 1000000000 seconds + 123 milliseconds
        nanos = 1000000000 * 1_000_000_000 + 123 * 1_000_000
        data = _make_log_data(timestamp=nanos)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["timestamp"], "2001-09-09T01:46:40.123Z")

    def test_timestamp_trailing_zero_truncation(self):
        # 1000000000 seconds + 100 milliseconds -> .1Z not .100Z
        nanos = 1000000000 * 1_000_000_000 + 100 * 1_000_000
        data = _make_log_data(timestamp=nanos)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["timestamp"], "2001-09-09T01:46:40.1Z")

    def test_severity_text_uses_enum_name(self):
        for sev, expected_name in [
            (SeverityNumber.TRACE, "TRACE"),
            (SeverityNumber.DEBUG, "DEBUG"),
            (SeverityNumber.INFO, "INFO"),
            (SeverityNumber.WARN, "WARN"),
            (SeverityNumber.ERROR, "ERROR"),
            (SeverityNumber.FATAL, "FATAL"),
        ]:
            self.buf = io.StringIO()
            self.exporter = CompactConsoleLogRecordExporter(out=self.buf)
            data = _make_log_data(severity_number=sev)
            self.exporter.export([data])
            parsed = self._get_parsed()
            self.assertEqual(parsed["severityText"], expected_name)
            self.assertEqual(parsed["severityNumber"], sev.value)

    def test_output_is_compact_single_line(self):
        data = _make_log_data()
        self.exporter.export([data])
        output = self._get_output()
        self.assertNotIn("\n", output)
        self.assertNotIn("  ", output)
