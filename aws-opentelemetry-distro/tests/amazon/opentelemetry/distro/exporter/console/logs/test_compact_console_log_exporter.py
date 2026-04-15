# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import inspect
import io
import json
import unittest
import unittest.mock

from amazon.opentelemetry.distro.exporter.console.logs.compact_console_log_exporter import (
    CompactConsoleLogRecordExporter,
)
from opentelemetry._logs import SeverityNumber

try:
    from opentelemetry.sdk._logs.export import LogRecordExportResult as LogExportResult
except ImportError:
    from opentelemetry.sdk._logs.export import LogExportResult

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import TraceFlags

# SDK 1.40+ moved LogRecord to _internal and removed 'resource' from its constructor.
# ReadableLogRecord wraps LogRecord with resource + scope on 1.40+.
# Older SDKs use LogData for the same purpose and LogRecord accepts 'resource'.
try:
    from opentelemetry.sdk._logs import LogRecord
except ImportError:
    from opentelemetry.sdk._logs._internal import LogRecord

try:
    from opentelemetry.sdk._logs import ReadableLogRecord
except ImportError:
    ReadableLogRecord = None

try:
    from opentelemetry.sdk._logs import LogData
except ImportError:
    LogData = None

_LOG_RECORD_ACCEPTS_RESOURCE = "resource" in inspect.signature(LogRecord.__init__).parameters


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

    log_record_kwargs = dict(
        timestamp=timestamp,
        observed_timestamp=observed_timestamp,
        trace_id=trace_id,
        span_id=span_id,
        trace_flags=trace_flags,
        severity_number=severity_number,
        body=body,
        attributes=attributes if attributes else None,
    )
    if _LOG_RECORD_ACCEPTS_RESOURCE:
        log_record_kwargs["resource"] = resource

    log_record = LogRecord(**log_record_kwargs)

    if ReadableLogRecord is not None:
        return ReadableLogRecord(
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
            "resource",
            "scope",
            "body",
            "severityNumber",
            "severityText",
            "attributes",
            "droppedAttributes",
            "timeUnixNano",
            "observedTimeUnixNano",
            "traceId",
            "spanId",
            "flags",
        ]
        for key in expected_keys:
            self.assertIn(key, parsed, f"Missing key: {key}")

        # Validate values
        self.assertEqual(parsed["body"], "Test log message")
        self.assertEqual(parsed["severityNumber"], 9)
        self.assertEqual(parsed["severityText"], "INFO")
        self.assertEqual(parsed["attributes"], {"key": "value"})
        self.assertEqual(parsed["droppedAttributes"], 0)
        self.assertEqual(parsed["timeUnixNano"], 1000000000 * 1_000_000_000)
        self.assertEqual(parsed["observedTimeUnixNano"], 1000000000 * 1_000_000_000)
        self.assertEqual(parsed["traceId"], "12345678901234567890123456789012")
        self.assertEqual(parsed["spanId"], "1234567890123456")
        self.assertEqual(parsed["flags"], 1)

        # Validate nested objects
        self.assertIn("attributes", parsed["resource"])
        self.assertEqual(parsed["resource"]["attributes"]["service.name"], "test-service")
        self.assertEqual(
            parsed["resource"]["schemaUrl"],
            "https://opentelemetry.io/schemas/1.0.0",
        )

        scope = parsed["scope"]
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
        self.assertEqual(parsed["timeUnixNano"], 0)
        self.assertEqual(parsed["observedTimeUnixNano"], 0)

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
        self.assertEqual(parsed["flags"], 0)

    def test_empty_attributes(self):
        data = _make_log_data(attributes={})
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["attributes"], {})

    def test_attribute_values_preserve_types(self):
        data = _make_log_data(attributes={"count": 42, "enabled": True, "rate": 3.14, "name": "test"})
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["attributes"]["count"], 42)
        self.assertEqual(parsed["attributes"]["enabled"], True)
        self.assertEqual(parsed["attributes"]["rate"], 3.14)
        self.assertEqual(parsed["attributes"]["name"], "test")

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
        self.assertEqual(parsed["scope"]["name"], "")
        self.assertEqual(parsed["scope"]["version"], "")
        self.assertEqual(parsed["scope"]["schemaUrl"], "")

    def test_timestamp_raw_nanos(self):
        nanos = 1000000000 * 1_000_000_000 + 123 * 1_000_000
        data = _make_log_data(timestamp=nanos)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["timeUnixNano"], nanos)

    def test_timestamp_preserves_full_precision(self):
        nanos = 1000000000 * 1_000_000_000 + 100 * 1_000_000
        data = _make_log_data(timestamp=nanos)
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["timeUnixNano"], nanos)

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

    def test_fallback_on_serialization_error(self):
        """If _to_compact_json fails, fallback should produce output."""
        data = _make_log_data()
        with unittest.mock.patch.object(
            CompactConsoleLogRecordExporter,
            "_to_compact_json",
            side_effect=ValueError("forced"),
        ):
            result = self.exporter.export([data])
            self.assertEqual(result, LogExportResult.SUCCESS)
            output = self._get_output()
            self.assertTrue(len(output) > 0)

    def test_fallback_also_fails_continues(self):
        """If both _to_compact_json and _fallback_format fail, skip record."""
        data = _make_log_data()
        with unittest.mock.patch.object(
            CompactConsoleLogRecordExporter,
            "_to_compact_json",
            side_effect=ValueError("forced"),
        ), unittest.mock.patch.object(
            CompactConsoleLogRecordExporter,
            "_fallback_format",
            side_effect=ValueError("fallback forced"),
        ):
            result = self.exporter.export([data])
            self.assertEqual(result, LogExportResult.SUCCESS)
            self.assertEqual(self.buf.getvalue(), "")

    def test_dropped_attributes_from_record(self):
        """Test dropped_attributes extracted from record when data lacks it."""
        data = _make_log_data()
        # Remove dropped_attributes from data if present, add to record
        if hasattr(data, "log_record"):
            data.log_record.dropped_attributes = 5
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertIsInstance(parsed["droppedAttributes"], int)

    def test_dropped_attributes_defaults_to_zero(self):
        """Test droppedAttributes is always an integer."""
        data = _make_log_data()
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertIsInstance(parsed["droppedAttributes"], int)
        self.assertEqual(parsed["droppedAttributes"], 0)

    def test_export_path_field(self):
        """Console exporter includes exportPath:console."""
        data = _make_log_data()
        self.exporter.export([data])
        parsed = self._get_parsed()
        self.assertEqual(parsed["exportPath"], "console")
