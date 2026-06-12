# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
OTLP LogRecord emitter for DI snapshots.

Converts Snapshot objects into structured OTLP LogRecords with:
- Flat attributes (queryable in CloudWatch Logs Insights)
- Structured body (stack + captures as nested dicts, auto-encoded as AnyValue)
- Trace context correlation (TraceId/SpanId)

Uses a dedicated, isolated LoggerProvider — DI snapshots do not mix with
application logs or Application Signals.
"""

import logging
import os
import threading

from opentelemetry._events import Event
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import TraceFlags

logger = logging.getLogger(__name__)

_INSTRUMENTATION_SCOPE = "aws.dynamic_instrumentation"
_INSTRUMENTATION_VERSION = "1.0"
_EVENT_NAME = "aws.dynamic_instrumentation.snapshot"
_DEFAULT_LOGS_ENDPOINT = "http://localhost:4316/v1/logs"
_LOGS_ENDPOINT_ENV_VAR = "OTEL_AWS_OTLP_LOGS_ENDPOINT"


class SnapshotOtlpEmitter:
    """
    Emits DI snapshots as OTLP LogRecords via a dedicated LoggerProvider.

    Thread-safe with lazy initialization. If provider creation fails,
    the failure is permanent and subsequent emit calls are silently skipped.
    """

    def __init__(self, resource=None, logs_endpoint=None):
        """
        Initialize the emitter.

        Args:
            resource: OTel Resource for the LoggerProvider. If None, uses default.
            logs_endpoint: OTLP logs endpoint URL. If None, reads from env var or uses default.
        """
        self._resource = resource
        endpoint = logs_endpoint or os.environ.get(_LOGS_ENDPOINT_ENV_VAR, "")
        self._logs_endpoint = endpoint.strip() if endpoint.strip() else _DEFAULT_LOGS_ENDPOINT
        self._event_logger = None
        self._logger_provider = None
        self._init_failed = False
        self._lock = threading.Lock()

    def initialize(self) -> bool:
        """
        Eagerly initialize the LoggerProvider and EventLogger.

        Should be called once at SDK startup (from a normal user thread), not
        lazily from a callback. ``BatchLogRecordProcessor`` spawns a daemon
        worker thread on construction, and ``Resource.create()`` does a chain
        of imports / resource detection that can hit
        ``RuntimeError("cannot schedule new futures after interpreter
        shutdown")`` when invoked from a ``sys.monitoring`` callback thread.

        Returns:
            True on success (or already initialized), False if initialization
            failed (subsequent ``emit_snapshot`` calls will then no-op).
        """
        return self._ensure_initialized()

    def _ensure_initialized(self):
        """Initialize the LoggerProvider and EventLogger.

        Idempotent and thread-safe. Uses double-checked locking to avoid
        contention on the hot path. Called eagerly from ``initialize()`` and
        defensively from ``emit_snapshot`` so callers that forgot to init
        explicitly still get a working emitter as long as they're on a
        well-behaved thread.
        """
        if self._event_logger is not None:
            return True
        if self._init_failed:
            return False
        with self._lock:
            if self._event_logger is not None:
                return True
            if self._init_failed:
                return False

            try:
                exporter = OTLPLogExporter(endpoint=self._logs_endpoint)
                resource = self._resource if self._resource else Resource.create()

                self._logger_provider = LoggerProvider(resource=resource)
                self._logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

                event_logger_provider = EventLoggerProvider(logger_provider=self._logger_provider)
                self._event_logger = event_logger_provider.get_event_logger(
                    _INSTRUMENTATION_SCOPE, _INSTRUMENTATION_VERSION
                )

                logger.debug("DI OTLP emitter initialized (endpoint: %s)", self._logs_endpoint)
                return True

            except Exception:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "Failed to initialize DI OTLP LoggerProvider, snapshots will not be exported",
                    exc_info=True,
                )
                self._init_failed = True
                return False

    def emit_snapshot(self, snapshot, config=None):
        """
        Emit a DI snapshot as an OTLP LogRecord.

        Args:
            snapshot: Snapshot object with captured data
            config: Optional BreakpointConfiguration for instrumentation_type attribute
        """
        if not self._ensure_initialized():
            return

        try:
            self._emit_snapshot_internal(snapshot, config)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.warning("Error emitting snapshot as OTLP LogRecord", exc_info=True)

    def _emit_snapshot_internal(self, snapshot, config):
        """Internal emit logic — separated for clean error handling."""
        location = snapshot.instrumentation.location if snapshot.instrumentation else None

        # Build attributes
        attributes = {"event.name": _EVENT_NAME}
        attributes["aws.di.snapshot_id"] = snapshot.id
        attributes["aws.di.location_hash"] = snapshot.location_hash or ""

        is_line_level = location and location.line_number > 0
        attributes["aws.di.instrumentation_level"] = "line" if is_line_level else "method"

        if snapshot.duration is not None:
            attributes["aws.di.duration_ms"] = snapshot.duration

        if location:
            if location.code_unit:
                attributes["aws.di.code_unit"] = location.code_unit
            if location.class_name:
                attributes["aws.di.class_name"] = location.class_name
            if location.method_name:
                attributes["aws.di.method_name"] = location.method_name
            if location.file_path:
                attributes["aws.di.file_path"] = location.file_path
            if is_line_level:
                attributes["aws.di.line_number"] = location.line_number

        if snapshot.instrumentation_type:
            attributes["aws.di.instrumentation_type"] = snapshot.instrumentation_type

        # Build body — structured dict, OTel SDK converts to AnyValue automatically
        body = {}

        # Stack frames and captures use snake_case keys throughout the body.
        # StackFrame.to_dict() produces {file_path, function, line_number}.
        # Captures.to_dict() produces snake_case keys (return_value, is_null, not_captured_reason, etc.).
        if snapshot.stack:
            body["stack"] = [frame.to_dict() for frame in snapshot.stack]

        if snapshot.captures:
            body["captures"] = snapshot.captures.to_dict()

        # Build trace context
        trace_id = None
        span_id = None
        trace_flags = None
        if snapshot.trace:
            trace_id = int(snapshot.trace.trace_id, 16) if snapshot.trace.trace_id else None
            span_id = int(snapshot.trace.span_id, 16) if snapshot.trace.span_id else None
            if trace_id and span_id:
                trace_flags = TraceFlags(0x01)  # SAMPLED

        # Emit as Event
        event = Event(
            name=_EVENT_NAME,
            timestamp=snapshot.timestamp * 1_000_000,  # ms to ns
            body=body if body else None,
            attributes=attributes,
            trace_id=trace_id,
            span_id=span_id,
            trace_flags=trace_flags,
        )

        self._event_logger.emit(event)

    def shutdown(self):
        """Flush and shutdown the owned LoggerProvider."""
        if self._logger_provider:
            try:
                self._logger_provider.force_flush()
                self._logger_provider.shutdown()
                logger.debug("DI OTLP LoggerProvider shut down")
            except Exception:  # pylint: disable=broad-exception-caught
                logger.warning("Error shutting down DI OTLP LoggerProvider", exc_info=True)
            self._logger_provider = None
            self._event_logger = None

    def reset(self):
        """Reset state for post-fork cleanup. Does not flush — parent's threads are dead."""
        self._logger_provider = None
        self._event_logger = None
        self._init_failed = False
