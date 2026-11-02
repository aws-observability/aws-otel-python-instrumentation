# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Local-testing file exporter producing CloudWatch-faithful NDJSON.

When ``OTEL_AWS_SERVICE_EVENTS_OUTPUT_FILE`` is set, ``ServiceEventsInstrumentation``
wires these exporters into the ServiceEvents ``LoggerProvider`` +
``MeterProvider`` in place of the OTLP HTTP exporters. The output shape
matches what users see in CloudWatch Logs Insights:

* Logs: one NDJSON line per LogRecord with top-level
  ``eventName``/``timeUnixNano``/``attributes``/``body`` plus nested
  ``resource``.
* Metrics: one NDJSON line per export batch as a canonical OTLP/JSON
  ``ExportMetricsServiceRequest`` — byte-identical to the OTLP wire,
  covering both ``count`` (Sum) and ``service.function.duration``
  (ExponentialHistogram).

Both exporters append to the same file via a writer singleton keyed on
absolute path — one ``open(..., "a")`` + ``threading.Lock`` per path, so
log + metric lines don't interleave.
"""

import json
import logging
import os
import threading
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional, Sequence

from google.protobuf.json_format import MessageToJson

from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult
from opentelemetry.sdk.metrics import Counter, Histogram
from opentelemetry.sdk.metrics.export import AggregationTemporality, MetricExporter, MetricExportResult, MetricsData
from opentelemetry.sdk.metrics.view import Aggregation

logger = logging.getLogger(__name__)


# Output-file rotation policy. When the active file reaches MAX_BYTES it is
# renamed to <file>.1, existing backups shift one slot, and <file>.{BACKUP_COUNT}
# is dropped. Bounds total disk footprint per output path at
# (BACKUP_COUNT + 1) * MAX_BYTES.
MAX_BYTES = 50 * 1024 * 1024
BACKUP_COUNT = 5


class _Utf8RotatingFileHandler(RotatingFileHandler):
    """``RotatingFileHandler`` that compares against UTF-8 byte length.

    The stdlib version's ``shouldRollover`` uses ``len(msg)`` which returns
    code points, not bytes. With non-ASCII content (multi-byte UTF-8) the
    on-disk file can grow well past ``maxBytes`` before rotation triggers.
    Override to count encoded bytes so the threshold matches what's actually
    written to disk.

    Preserves the two stdlib guards: never rotate an empty file (gh-116263)
    and never rotate a non-regular file like a FIFO (bpo-45401).
    """

    def shouldRollover(self, record):  # noqa: N802 — stdlib API name
        if self.stream is None:
            self.stream = self._open()
        if self.maxBytes <= 0:
            return False
        # Preserve stdlib guard: don't rotate non-regular files (FIFOs, /dev/stdout, …).
        if not os.path.isfile(self.baseFilename):
            return False
        try:
            self.stream.seek(0, 2)  # SEEK_END
            pos = self.stream.tell()
        except OSError:
            return False
        # Preserve stdlib guard: never rotate an empty file even if the next
        # message itself exceeds maxBytes — a useless empty backup helps no one.
        if pos == 0:
            return False
        msg = self.format(record) + self.terminator
        try:
            msg_bytes = len(msg.encode("utf-8"))
        except (UnicodeError, AttributeError):
            msg_bytes = len(msg)
        return pos + msg_bytes >= self.maxBytes


# ─── writer singleton ────────────────────────────────────────────────


@dataclass
class _Writer:
    handler: RotatingFileHandler
    lock: threading.Lock
    ref_count: int


_writers: Dict[str, _Writer] = {}
_writers_mutex = threading.Lock()


def _acquire_writer(abs_path: str) -> Optional[_Writer]:
    """Open or share the writer for ``abs_path``.

    Returns ``None`` if the path can't be opened (permission denied, disk
    full, etc.). Callers must tolerate a ``None`` return — telemetry-side
    code MUST NOT propagate I/O failures into the customer application.
    """
    try:
        with _writers_mutex:
            entry = _writers.get(abs_path)
            if entry is not None:
                entry.ref_count += 1
                return entry
            os.makedirs(os.path.dirname(abs_path) or ".", exist_ok=True)
            # RotatingFileHandler enforces the 50MB / 5-backup policy for the
            # local-testing output file. The handler is shared across
            # log + metric exporters via this registry; threading.Lock guards
            # cross-exporter writes so log + metric lines don't interleave.
            handler = _Utf8RotatingFileHandler(
                abs_path,
                mode="a",
                maxBytes=MAX_BYTES,
                backupCount=BACKUP_COUNT,
                encoding="utf-8",
                delay=False,
            )
            # Each emitted record's msg already ends with "\n" — suppress the
            # default "\n" terminator so the on-disk format is unchanged.
            handler.terminator = ""
            handler.setFormatter(logging.Formatter("%(message)s"))
            entry = _Writer(handler=handler, lock=threading.Lock(), ref_count=1)
            _writers[abs_path] = entry
            return entry
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("ServiceEvents: failed to open output file %s", abs_path)
        return None


def _release_writer(abs_path: str) -> None:
    with _writers_mutex:
        entry = _writers.get(abs_path)
        if entry is None:
            return
        entry.ref_count -= 1
        if entry.ref_count > 0:
            return
        del _writers[abs_path]
        try:
            entry.handler.close()
        except Exception:  # pylint: disable=broad-exception-caught
            pass


def _reset_file_writers() -> None:
    """Test-only: drop all cached writers and close their files."""
    with _writers_mutex:
        for entry in _writers.values():
            try:
                entry.handler.close()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        _writers.clear()


def _safe_acquire_writer(output_path: str) -> "tuple[str, Optional[_Writer]]":
    """Resolve ``output_path`` and acquire its writer without raising.

    Telemetry SDK code MUST NOT propagate failures into the customer
    application. This wraps both ``os.path.abspath`` (which raises
    ``TypeError`` on non-string input) and ``_acquire_writer`` (which
    can fail on permission/disk errors) so exporter constructors are
    never an exception source.
    """
    try:
        abs_path = os.path.abspath(output_path)
    except (TypeError, ValueError, OSError):
        logger.exception("ServiceEvents: invalid output file path %r", output_path)
        return "", None
    return abs_path, _acquire_writer(abs_path)


def _emit_lines(writer: _Writer, lines: Sequence[str]) -> None:
    """Write each pre-serialized NDJSON line through the rotating handler.

    Building one logging.LogRecord per line lets RotatingFileHandler.emit()
    run its size check + rollover between records, so the file is bounded
    even within a single export batch.
    """
    handler = writer.handler
    for line in lines:
        record = logging.LogRecord(
            name="serviceevents",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=line,
            args=(),
            exc_info=None,
        )
        handler.emit(record)


# ─── serialization ───────────────────────────────────────────────────


def _unwrap_body(value: Any) -> Any:
    """Recursively unwrap nested dicts/lists into JSON-serializable form.

    Python's LogRecord.body is set by the emitter directly to a plain dict
    or string; this pass is a safety net for anything that slipped through
    as an ``AnyValue`` wrapper.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _unwrap_body(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_unwrap_body(v) for v in value]
    return str(value)


def serialize_log_record(readable: ReadableLogRecord) -> Dict[str, Any]:
    """Build the flat CloudWatch-shape JSON for one LogRecord."""
    record = readable.log_record

    attributes: Dict[str, Any] = {}
    if record.attributes:
        for attr_key, attr_value in record.attributes.items():
            attributes[str(attr_key)] = attr_value

    resource: Dict[str, Any] = {}
    if readable.resource is not None:
        for attr_key, attr_value in readable.resource.attributes.items():
            resource[str(attr_key)] = attr_value

    out: Dict[str, Any] = {
        "eventName": record.event_name or "",
        "timeUnixNano": record.timestamp if record.timestamp is not None else 0,
        "attributes": attributes,
        "body": _unwrap_body(record.body) or {},
        "resource": resource,
    }

    # trace/span context — present only when the LogRecord carries one
    # (e.g. IncidentSnapshot correlated to an active trace).
    trace_id = record.trace_id
    span_id = record.span_id
    flags = record.trace_flags
    if trace_id:
        out["traceId"] = f"{trace_id:032x}" if isinstance(trace_id, int) else str(trace_id)
    if span_id:
        out["spanId"] = f"{span_id:016x}" if isinstance(span_id, int) else str(span_id)
    if flags is not None and trace_id:
        out["flags"] = int(flags)

    return out


# ─── exporters ───────────────────────────────────────────────────────


class ServiceEventsCloudWatchLogFileExporter(LogRecordExporter):
    """LogRecordExporter subclass that writes CloudWatch-faithful NDJSON to a file."""

    def __init__(self, output_path: str):
        self._abs_path, self._writer = _safe_acquire_writer(output_path)
        self._shutdown_called = False

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        if self._shutdown_called or self._writer is None:
            return LogRecordExportResult.FAILURE
        try:
            lines = [
                json.dumps(serialize_log_record(record), separators=(",", ":"), default=str) + "\n" for record in batch
            ]
            if lines:
                with self._writer.lock:
                    _emit_lines(self._writer, lines)
            return LogRecordExportResult.SUCCESS
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("ServiceEvents: failed to write log records to file")
            return LogRecordExportResult.FAILURE

    def force_flush(self, timeout_millis: Optional[int] = 30000) -> bool:  # pylint: disable=unused-argument
        if self._writer is None:
            return False
        try:
            with self._writer.lock:
                self._writer.handler.flush()
            return True
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    def shutdown(self) -> None:
        if self._shutdown_called:
            return
        self._shutdown_called = True
        if self._writer is None:
            return
        _release_writer(self._abs_path)


class ServiceEventsCloudWatchMetricFileExporter(MetricExporter):
    """MetricExporter subclass that writes canonical OTLP metrics JSON to a file.

    Each ``export()`` batch is written as ONE NDJSON line containing a full
    OTLP ``ExportMetricsServiceRequest`` (``resourceMetrics[].scopeMetrics[].metrics[]``),
    byte-identical to what the CloudWatch OTLP metrics endpoint accepts. This
    mirrors the OTLP HTTP exporter exactly — the file exporter is a pure
    transport swap, so both ``count`` (Sum) and ``service.function.duration``
    (ExponentialHistogram) serialize natively with no per-type special-casing.
    """

    def __init__(self, output_path: str):
        # Delta temporality preference MUST match the OTLP HTTP exporter's config in
        # serviceevents_instrumentation.py (Counter+Histogram → DELTA) so the file
        # mirror is a true transport swap. The MetricReader reads this off the
        # exporter's `_preferred_temporality`; an empty dict would default to
        # CUMULATIVE and diverge from the network wire.
        preferred_temporality = {
            Counter: AggregationTemporality.DELTA,
            Histogram: AggregationTemporality.DELTA,
        }
        preferred_aggregation: Dict[type, Aggregation] = {}
        super().__init__(
            preferred_temporality=preferred_temporality,
            preferred_aggregation=preferred_aggregation,
        )
        self._abs_path, self._writer = _safe_acquire_writer(output_path)
        self._shutdown_called = False

    def export(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs: Any,
    ) -> MetricExportResult:
        if self._shutdown_called or self._writer is None:
            return MetricExportResult.FAILURE
        try:
            if not metrics_data.resource_metrics:
                return MetricExportResult.SUCCESS
            # Encode the whole batch to a single OTLP/JSON ExportMetricsServiceRequest,
            # exactly as the OTLP HTTP exporter does on the wire. One NDJSON line per batch.
            request = encode_metrics(metrics_data)
            line = MessageToJson(request, indent=None) + "\n"
            with self._writer.lock:
                _emit_lines(self._writer, [line])
            return MetricExportResult.SUCCESS
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("ServiceEvents: failed to write metrics to file")
            return MetricExportResult.FAILURE

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        if self._writer is None:
            return False
        try:
            with self._writer.lock:
                self._writer.handler.flush()
            return True
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        if self._shutdown_called:
            return
        self._shutdown_called = True
        if self._writer is None:
            return
        _release_writer(self._abs_path)
