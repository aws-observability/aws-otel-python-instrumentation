# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for SnapshotOtlpEmitter."""

import unittest

from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    CapturedValue,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    StackFrame,
    ThreadInfo,
    TraceContext,
)
from amazon.opentelemetry.distro.debugger._snapshot_otlp_emitter import SnapshotOtlpEmitter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource


class TestSnapshotOtlpEmitter(unittest.TestCase):
    """Tests for SnapshotOtlpEmitter."""

    def setUp(self):
        """Set up test fixtures with in-memory exporter."""
        self.log_exporter = InMemoryLogExporter()
        resource = Resource.create({"service.name": "test-service"})

        self.logger_provider = LoggerProvider(resource=resource)
        self.logger_provider.add_log_record_processor(SimpleLogRecordProcessor(self.log_exporter))

        # Create emitter with pre-built provider (bypasses lazy init)
        self.emitter = SnapshotOtlpEmitter()
        # Directly set the internals for testing
        self.emitter._logger_provider = self.logger_provider
        from opentelemetry.sdk._events import EventLoggerProvider

        event_logger_provider = EventLoggerProvider(logger_provider=self.logger_provider)
        self.emitter._event_logger = event_logger_provider.get_event_logger("aws.dynamic_instrumentation", "1.0")

    def tearDown(self):
        self.logger_provider.shutdown()

    def _make_method_snapshot(self, trace_id=None, span_id=None):
        """Create a method-level snapshot for testing."""
        args = {
            "arg0": CapturedValue(type="int", value="10"),
            "arg1": CapturedValue(type="int", value="5"),
        }
        captures = Captures(
            entry=CapturedContext(arguments=args),
            return_context=CapturedContext(
                return_value=CapturedValue(type="int", value="15"),
            ),
        )

        trace = None
        if trace_id and span_id:
            trace = TraceContext(trace_id=trace_id, span_id=span_id)

        return Snapshot(
            timestamp=1772082470861,
            duration=5,
            service="test-service",
            environment="test-env",
            location_hash="hash123",
            instrumentation=InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit="myapp.services",
                    class_name="myapp.services",
                    method_name="compute",
                    line_number=0,
                    file_path="services.py",
                )
            ),
            trace=trace,
            thread=ThreadInfo(id=12345, name="MainThread"),
            stack=[
                StackFrame(file_name="/app/services.py", function="compute", line_number=42),
                StackFrame(file_name="/app/main.py", function="handle", line_number=10),
            ],
            captures=captures,
        )

    def _make_line_snapshot(self):
        """Create a line-level snapshot for testing."""
        locals_map = {
            "x": CapturedValue(type="int", value="42"),
            "result": CapturedValue(type="int", value="84"),
        }
        captures = Captures(
            lines={98: CapturedContext(locals=locals_map)},
        )

        return Snapshot(
            timestamp=1772082470861,
            location_hash="hash456",
            instrumentation=InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit="myapp.services",
                    class_name="myapp.services",
                    method_name="compute",
                    line_number=98,
                    file_path="services.py",
                )
            ),
            thread=ThreadInfo(id=12345, name="MainThread"),
            captures=captures,
        )

    def test_method_level_snapshot_attributes(self):
        """Test that method-level snapshots have correct attributes."""
        snapshot = self._make_method_snapshot(
            trace_id="aabb00112233445566778899aabbccdd",
            span_id="1122334455667788",
        )

        self.emitter.emit_snapshot(snapshot)

        log_data_list = self.log_exporter.get_finished_logs()
        self.assertEqual(len(log_data_list), 1)

        record = log_data_list[0].log_record
        attrs = dict(record.attributes)

        self.assertEqual(attrs["event.name"], "aws.dynamic_instrumentation.snapshot")
        self.assertIn("aws.di.snapshot_id", attrs)
        self.assertEqual(attrs["aws.di.location_hash"], "hash123")
        self.assertEqual(attrs["aws.di.instrumentation_level"], "method")
        self.assertEqual(attrs["aws.di.duration_ms"], 5)
        self.assertEqual(attrs["aws.di.code_unit"], "myapp.services")
        self.assertEqual(attrs["aws.di.class_name"], "myapp.services")
        self.assertEqual(attrs["aws.di.method_name"], "compute")
        self.assertEqual(attrs["aws.di.file_path"], "services.py")

        # line_number should NOT be present for method-level
        self.assertNotIn("aws.di.line_number", attrs)

    def test_method_level_snapshot_trace_context(self):
        """Test that trace context is propagated."""
        snapshot = self._make_method_snapshot(
            trace_id="aabb00112233445566778899aabbccdd",
            span_id="1122334455667788",
        )

        self.emitter.emit_snapshot(snapshot)

        log_data_list = self.log_exporter.get_finished_logs()
        self.assertEqual(len(log_data_list), 1)

        record = log_data_list[0].log_record
        # OTel SDK stores trace_id and span_id as integers
        self.assertNotEqual(record.trace_id, 0)
        self.assertNotEqual(record.span_id, 0)

    def test_line_level_snapshot_attributes(self):
        """Test that line-level snapshots have line_number and no duration."""
        snapshot = self._make_line_snapshot()

        self.emitter.emit_snapshot(snapshot)

        log_data_list = self.log_exporter.get_finished_logs()
        self.assertEqual(len(log_data_list), 1)

        attrs = dict(log_data_list[0].log_record.attributes)

        self.assertEqual(attrs["aws.di.instrumentation_level"], "line")
        self.assertEqual(attrs["aws.di.line_number"], 98)

        # duration should NOT be present for line-level
        self.assertNotIn("aws.di.duration_ms", attrs)

    def test_snapshot_body_has_captures(self):
        """Test that the body contains captures dict."""
        snapshot = self._make_method_snapshot()

        self.emitter.emit_snapshot(snapshot)

        log_data_list = self.log_exporter.get_finished_logs()
        self.assertEqual(len(log_data_list), 1)

        body = log_data_list[0].log_record.body
        self.assertIsNotNone(body)
        # Body should be a dict with 'captures' key
        self.assertIn("captures", body)
        self.assertIn("stack", body)

    def test_snapshot_body_stack_uses_file_path(self):
        """Test that stack frames use file_path key (not fileName)."""
        snapshot = self._make_method_snapshot()

        self.emitter.emit_snapshot(snapshot)

        log_data_list = self.log_exporter.get_finished_logs()
        body = log_data_list[0].log_record.body
        stack = body["stack"]
        self.assertEqual(len(stack), 2)
        self.assertEqual(stack[0]["file_path"], "/app/services.py")
        self.assertEqual(stack[0]["function"], "compute")
        self.assertEqual(stack[0]["line_number"], 42)

    def test_snapshot_without_trace_context(self):
        """Test that snapshots without trace context don't crash."""
        snapshot = self._make_method_snapshot()  # no trace

        self.emitter.emit_snapshot(snapshot)

        log_data_list = self.log_exporter.get_finished_logs()
        self.assertEqual(len(log_data_list), 1)
        # trace_id should be 0 (invalid)
        self.assertEqual(log_data_list[0].log_record.trace_id, 0)

    def test_null_config_does_not_crash(self):
        """Test that passing None config works fine."""
        snapshot = self._make_method_snapshot()

        self.emitter.emit_snapshot(snapshot, config=None)

        log_data_list = self.log_exporter.get_finished_logs()
        self.assertEqual(len(log_data_list), 1)
        attrs = dict(log_data_list[0].log_record.attributes)
        self.assertNotIn("aws.di.instrumentation_type", attrs)

    def test_reset_clears_state(self):
        """Test that reset() clears internal state for post-fork."""
        self.emitter.reset()

        self.assertIsNone(self.emitter._logger_provider)
        self.assertIsNone(self.emitter._event_logger)
        self.assertFalse(self.emitter._init_failed)

    def test_emit_after_init_failure_is_silent(self):
        """Test that emit is silently skipped when init failed."""
        emitter = SnapshotOtlpEmitter()
        emitter._init_failed = True

        snapshot = self._make_method_snapshot()
        # Should not raise
        emitter.emit_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
