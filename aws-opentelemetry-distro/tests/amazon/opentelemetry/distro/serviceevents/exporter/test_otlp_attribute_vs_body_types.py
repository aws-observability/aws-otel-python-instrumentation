# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests validating OTLP LogRecord type constraints for Attributes vs Body.

These tests prove WHY the hybrid approach (flat attributes + structured body)
is necessary for the ServiceEvents OTLP log data model:

Key findings:
- Span/Resource attributes: only accept primitives (bool, str, bytes, int, float)
  and homogeneous sequences. Dicts are REJECTED by SDK validation.
- LogRecord attributes (Python): technically accept dicts via AnyValue protobuf,
  BUT Java's AttributeKey<T> has no mapKey — so dicts in attributes break
  cross-SDK parity.
- Body (AnyValue): accepts primitives + Mapping (dict) + Sequence with full
  nesting. This is where structured data belongs for cross-SDK compatibility.
- Mixed-type sequences: stored as None (data lost) — avoid in attributes.
"""

import unittest

from opentelemetry._events import Event
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import TraceFlags


class TestOtlpAttributeTypeConstraints(unittest.TestCase):
    """Test OTLP LogRecord type behavior for attributes and body."""

    def setUp(self):
        self.exporter = InMemoryLogExporter()
        self.provider = LoggerProvider(resource=Resource.create({"service.name": "test-service"}))
        self.provider.add_log_record_processor(SimpleLogRecordProcessor(self.exporter))
        self.event_logger_provider = EventLoggerProvider(logger_provider=self.provider)
        self.event_logger = self.event_logger_provider.get_event_logger("test.scope")

    def tearDown(self):
        self.provider.shutdown()

    def _emit_and_get(self, body, attributes):
        """Emit an event and return the exported log record."""
        self.exporter.clear()
        event = Event(
            name="test.event",
            body=body,
            attributes=attributes,
            severity_number=SeverityNumber.INFO,
        )
        self.event_logger.emit(event)
        records = self.exporter.get_finished_logs()
        self.assertEqual(len(records), 1)
        return records[0]

    # ── Attribute type tests ──────────────────────────────────────────

    def test_attributes_accept_string(self):
        """String values work in attributes."""
        record = self._emit_and_get(body="test", attributes={"key": "value"})
        self.assertEqual(record.log_record.attributes["key"], "value")

    def test_attributes_accept_int(self):
        """Int values work in attributes."""
        record = self._emit_and_get(body="test", attributes={"key": 42})
        self.assertEqual(record.log_record.attributes["key"], 42)

    def test_attributes_accept_float(self):
        """Float values work in attributes."""
        record = self._emit_and_get(body="test", attributes={"key": 3.14})
        self.assertAlmostEqual(record.log_record.attributes["key"], 3.14)

    def test_attributes_accept_bool(self):
        """Bool values work in attributes."""
        record = self._emit_and_get(body="test", attributes={"key": True})
        self.assertEqual(record.log_record.attributes["key"], True)

    def test_attributes_accept_string_sequence(self):
        """Homogeneous string sequences work in attributes."""
        record = self._emit_and_get(body="test", attributes={"key": ["a", "b", "c"]})
        self.assertEqual(record.log_record.attributes["key"], ("a", "b", "c"))

    def test_attributes_accept_int_sequence(self):
        """Homogeneous int sequences work in attributes."""
        record = self._emit_and_get(body="test", attributes={"key": [1, 2, 3]})
        self.assertEqual(record.log_record.attributes["key"], (1, 2, 3))

    def test_attributes_dict_passes_through_in_python_log_records(self):
        """Python LogRecord attributes accept dicts (unlike Span attributes).

        This is because LogRecord uses AnyValue in protobuf, which supports maps.
        However, Java's AttributeKey<T> has no mapKey — so for cross-SDK parity,
        we must NOT rely on this behavior. Use Body for nested structures instead.
        """
        record = self._emit_and_get(
            body="test",
            attributes={
                "flat_key": "survives",
                "nested_key": {"inner": "value"},  # passes through in Python
            },
        )
        self.assertEqual(record.log_record.attributes["flat_key"], "survives")
        # Python SDK accepts dicts in LogRecord attributes (unlike Span attributes)
        self.assertIn("nested_key", record.log_record.attributes)

    def test_attributes_nested_dict_passes_through_in_python_log_records(self):
        """Python LogRecord attributes accept nested dicts (unlike Span attributes).

        Java SDK cannot do this — AttributeKey has no map type.
        For cross-SDK parity, nested structures belong in Body.
        """
        record = self._emit_and_get(
            body="test",
            attributes={
                "keep": "yes",
                "duration": {"Values": [1.0, 2.0], "Max": 5.0},
            },
        )
        self.assertEqual(record.log_record.attributes["keep"], "yes")
        # Python passes dicts through, but Java cannot
        self.assertIn("duration", record.log_record.attributes)

    def test_attributes_list_of_dicts_passes_through_in_python_log_records(self):
        """Python LogRecord attributes accept lists of dicts.

        Java SDK cannot represent this — no mapKey or nested array types.
        For cross-SDK parity, lists of dicts belong in Body.
        """
        record = self._emit_and_get(
            body="test",
            attributes={
                "keep": "yes",
                "errors": [{"type": "RuntimeError", "count": 3}],
            },
        )
        self.assertEqual(record.log_record.attributes["keep"], "yes")
        # Python passes through, converted to tuple
        self.assertIn("errors", record.log_record.attributes)

    def test_attributes_mixed_type_sequence_stored_as_none(self):
        """Mixed-type sequences are stored as None in attributes (value lost).

        The SDK detects mixed types and sets the value to None.
        This proves mixed-type sequences are not safely representable in attributes.
        """
        record = self._emit_and_get(
            body="test",
            attributes={
                "keep": "yes",
                "mixed": [1, "two", 3.0],  # mixed types → stored as None
            },
        )
        self.assertEqual(record.log_record.attributes["keep"], "yes")
        # Key exists but value is None — data is lost
        self.assertIn("mixed", record.log_record.attributes)
        self.assertIsNone(record.log_record.attributes["mixed"])

    # ── Body type tests ───────────────────────────────────────────────

    def test_body_accepts_string(self):
        """String body works."""
        record = self._emit_and_get(body="hello", attributes={})
        self.assertEqual(record.log_record.body, "hello")

    def test_body_accepts_dict(self):
        """Dict body works — this is what attributes CANNOT do."""
        body = {"endpoint_id": "abc-123", "count": 42}
        record = self._emit_and_get(body=body, attributes={})
        self.assertEqual(record.log_record.body["endpoint_id"], "abc-123")
        self.assertEqual(record.log_record.body["count"], 42)

    def test_body_accepts_nested_dict(self):
        """Nested dicts work in body — full structure preserved."""
        body = {
            "duration": {
                "Values": [1234.5, 2345.6],
                "Counts": [10, 5],
                "Max": 5000.0,
                "Min": 100.0,
                "Count": 15,
                "Sum": 25000.0,
            }
        }
        record = self._emit_and_get(body=body, attributes={})
        self.assertEqual(record.log_record.body["duration"]["Max"], 5000.0)
        self.assertEqual(record.log_record.body["duration"]["Values"], [1234.5, 2345.6])

    def test_body_accepts_list_of_dicts(self):
        """Lists of dicts work in body — error_breakdown structure preserved."""
        body = {
            "error_breakdown": [
                {
                    "failure_type": "500",
                    "count": 3,
                    "errors": [{"error_type": "RuntimeError", "function_id": "abc"}],
                }
            ]
        }
        record = self._emit_and_get(body=body, attributes={})
        breakdown = record.log_record.body["error_breakdown"]
        self.assertEqual(len(breakdown), 1)
        self.assertEqual(breakdown[0]["failure_type"], "500")
        self.assertEqual(breakdown[0]["errors"][0]["error_type"], "RuntimeError")

    def test_body_accepts_deeply_nested_incident_snapshot(self):
        """Full IncidentSnapshot body with 3+ levels of nesting works."""
        body = {
            "snapshot_id": "snap_abc123",
            "severity": "critical",
            "exception_info": [
                {
                    "exception_type": "RuntimeError",
                    "exception_message": "fail",
                    "stack_trace": "Traceback...",
                    "call_path": [
                        {
                            "function_id": "func-1",
                            "caller_function_id": "func-2",
                            "duration_ns": 1958,
                            "error": False,
                        },
                        {
                            "function_id": "func-2",
                            "caller_function_id": None,
                            "duration_ns": 189666,
                            "error": True,
                        },
                    ],
                }
            ],
            "request_context": {
                "type": "http",
                "status_code": 500,
                "request_body": {"user_id": "test-123", "password": "***REDACTED***"},
                "query_params": {"debug": "true"},
            },
        }
        record = self._emit_and_get(body=body, attributes={})

        # 3 levels deep: body → exception_info[0] → call_path[1] → error
        self.assertTrue(record.log_record.body["exception_info"][0]["call_path"][1]["error"])

        # 3 levels deep: body → request_context → request_body → user_id
        self.assertEqual(
            record.log_record.body["request_context"]["request_body"]["user_id"],
            "test-123",
        )

    # ── Hybrid approach tests ─────────────────────────────────────────

    def test_hybrid_endpoint_summary(self):
        """EndpointSummary: flat query fields in attributes, structured data in body."""
        attributes = {
            "serviceevents.signal_type": "EndpointSummary",
            "service.name": "my-api",
            "deployment.environment": "prod",
            "http.request.method": "POST",
            "url.route": "/api/users",
            "serviceevents.endpoint_id": "2775225a-cb8e-5c0a",
            "serviceevents.request.count": 9,
            "serviceevents.request.faults": 9,
            "serviceevents.request.errors": 0,
        }
        body = {
            "endpoint_id": "2775225a-cb8e-5c0a",
            "method": "POST",
            "route": "/api/users",
            "operation": "POST /api/users",
            "count": 9,
            "faults": 9,
            "errors": 0,
            "duration": {"Values": [9433.38], "Counts": [6], "Max": 19335.5, "Min": 9127.6, "Count": 9, "Sum": 97462.6},
            "error_breakdown": [
                {"failure_type": "500", "count": 7, "errors": [{"error_type": "RuntimeError", "function_id": "964a"}]}
            ],
        }

        record = self._emit_and_get(body=body, attributes=attributes)

        # Attributes: all flat, all queryable
        self.assertEqual(record.log_record.attributes["serviceevents.signal_type"], "EndpointSummary")
        self.assertEqual(record.log_record.attributes["http.request.method"], "POST")
        self.assertEqual(record.log_record.attributes["serviceevents.request.faults"], 9)

        # Body: nested structures preserved
        self.assertEqual(record.log_record.body["duration"]["Max"], 19335.5)
        self.assertEqual(record.log_record.body["error_breakdown"][0]["errors"][0]["error_type"], "RuntimeError")

    def test_hybrid_incident_snapshot_with_trace_context(self):
        """IncidentSnapshot: severity mapping + trace context + nested body."""
        attributes = {
            "serviceevents.signal_type": "IncidentSnapshot",
            "serviceevents.severity": "critical",
            "serviceevents.trigger_type": "exception",
            "http.response.status_code": 500,
        }
        body = {
            "snapshot_id": "snap_c361cdc6",
            "severity": "critical",
            "exception_info": [
                {
                    "exception_type": "RuntimeError",
                    "stack_trace": "Traceback...\nRuntimeError: fail",
                    "call_path": [{"function_id": "f1", "duration_ns": 1958, "error": True}],
                }
            ],
            "telemetry_correlation": {
                "trace_id": "0x699e34c662d55e013e833341a5d9f079",
                "span_id": "0x85b7839f6afbae05",
            },
        }

        # Parse trace context (as the emitter would)
        trace_id = int("699e34c662d55e013e833341a5d9f079", 16)
        span_id = int("85b7839f6afbae05", 16)

        event = Event(
            name="serviceevents.incident",
            body=body,
            attributes=attributes,
            severity_number=SeverityNumber.FATAL,  # critical → FATAL
            trace_id=trace_id,
            span_id=span_id,
            trace_flags=TraceFlags.SAMPLED,
        )
        self.exporter.clear()
        self.event_logger.emit(event)
        records = self.exporter.get_finished_logs()
        self.assertEqual(len(records), 1)
        record = records[0].log_record

        # Severity mapped correctly
        self.assertEqual(record.severity_number, SeverityNumber.FATAL)

        # Trace context propagated
        self.assertEqual(record.trace_id, trace_id)
        self.assertEqual(record.span_id, span_id)
        self.assertEqual(record.trace_flags, TraceFlags.SAMPLED)

        # Attributes: flat and queryable
        self.assertEqual(record.attributes["serviceevents.severity"], "critical")
        self.assertEqual(record.attributes["http.response.status_code"], 500)

        # Body: deeply nested structure preserved
        self.assertEqual(record.body["exception_info"][0]["call_path"][0]["function_id"], "f1")
        self.assertTrue(record.body["exception_info"][0]["call_path"][0]["error"])


class TestAttributeTypeValidation(unittest.TestCase):
    """Direct validation of OTel SDK type constants."""

    def test_valid_attr_types_are_primitives_only(self):
        """_VALID_ATTR_VALUE_TYPES contains only primitives — no dict, no list."""
        import opentelemetry.attributes as attrs

        valid = attrs._VALID_ATTR_VALUE_TYPES
        self.assertIn(bool, valid)
        self.assertIn(str, valid)
        self.assertIn(int, valid)
        self.assertIn(float, valid)
        self.assertIn(bytes, valid)
        self.assertNotIn(dict, valid)
        self.assertNotIn(list, valid)

    def test_valid_anyvalue_types_include_mapping(self):
        """_VALID_ANY_VALUE_TYPES includes Mapping (dict) — Body can hold dicts."""
        import collections.abc

        import opentelemetry.attributes as attrs

        valid = attrs._VALID_ANY_VALUE_TYPES
        self.assertIn(collections.abc.Mapping, valid)
        self.assertIn(collections.abc.Sequence, valid)
        self.assertIn(str, valid)
        self.assertIn(int, valid)

    def test_attr_types_are_strict_subset_of_anyvalue_types(self):
        """Attribute types are a strict subset of AnyValue types.
        This proves Body supports more types than Attributes."""
        import opentelemetry.attributes as attrs

        attr_types = set(attrs._VALID_ATTR_VALUE_TYPES)
        any_types = set(attrs._VALID_ANY_VALUE_TYPES)

        # Every attr type is also an anyvalue type
        for t in attr_types:
            self.assertIn(t, any_types, f"{t} is in attr types but not anyvalue types")

        # AnyValue has MORE types than attributes
        self.assertGreater(len(any_types), len(attr_types))


if __name__ == "__main__":
    unittest.main()
