# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import glob
import json
import os
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.serviceevents.exporter import cloudwatch_file_exporter
from amazon.opentelemetry.serviceevents.exporter.cloudwatch_file_exporter import (
    ServiceEventsCloudWatchLogFileExporter,
    ServiceEventsCloudWatchMetricFileExporter,
    _acquire_writer,
    _release_writer,
    _reset_file_writers,
    _Utf8RotatingFileHandler,
    serialize_log_record,
)
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs._internal import LogRecord
from opentelemetry.sdk._logs.export import LogRecordExportResult
from opentelemetry.sdk.metrics.export import MetricExportResult
from opentelemetry.sdk.resources import Resource


def _make_readable(
    event_name="aws.service_events.function_call",
    attributes=None,
    body=None,
    timestamp=1744137998974205000,
    trace_id=None,
    span_id=None,
    trace_flags=None,
):
    record = LogRecord(
        timestamp=timestamp,
        observed_timestamp=timestamp,
        trace_id=trace_id,
        span_id=span_id,
        trace_flags=trace_flags,
        severity_text=None,
        severity_number=None,
        body=body if body is not None else {"exceptions": {"RuntimeError": 3}},
        attributes=(
            attributes
            if attributes is not None
            else {
                "event.name": "aws.service_events.function_call",
                "aws.service_events.function_name": "process_order",
            }
        ),
        event_name=event_name,
    )
    resource = Resource.create(
        {
            "service.name": "shoppingcart",
            "deployment.environment": "prod",
            "telemetry.sdk.language": "python",
        }
    )
    return ReadableLogRecord(log_record=record, resource=resource)


class TestSerializeLogRecord(TestCase):
    def test_flat_shape(self):
        out = serialize_log_record(_make_readable())
        self.assertEqual(out["eventName"], "aws.service_events.function_call")
        self.assertEqual(out["timeUnixNano"], 1744137998974205000)
        self.assertEqual(out["attributes"]["aws.service_events.function_name"], "process_order")
        self.assertEqual(out["body"], {"exceptions": {"RuntimeError": 3}})
        self.assertEqual(out["resource"]["service.name"], "shoppingcart")
        self.assertEqual(out["resource"]["deployment.environment"], "prod")
        self.assertNotIn("traceId", out)
        self.assertNotIn("spanId", out)

    def test_trace_context_included_when_set(self):
        out = serialize_log_record(
            _make_readable(
                trace_id=0xAABBCCDDEEFF00112233445566778899,
                span_id=0x1122334455667788,
                trace_flags=1,
            )
        )
        self.assertEqual(out["traceId"], "aabbccddeeff00112233445566778899")
        self.assertEqual(out["spanId"], "1122334455667788")
        self.assertEqual(out["flags"], 1)

    def test_empty_body_becomes_empty_dict(self):
        # Explicit empty dict → serializer preserves as {}
        out = serialize_log_record(_make_readable(body={}))
        self.assertEqual(out["body"], {})

    def test_list_body_is_unwrapped_recursively(self):
        # _unwrap_body recurses through lists/tuples preserving primitives.
        out = serialize_log_record(_make_readable(body=[1, "a", {"k": 2}]))
        self.assertEqual(out["body"], [1, "a", {"k": 2}])

    def test_unserializable_body_falls_back_to_str(self):
        # A value that is not a primitive/dict/list is coerced via str().
        sentinel = object()
        out = serialize_log_record(_make_readable(body={"obj": sentinel}))
        self.assertEqual(out["body"]["obj"], str(sentinel))


class TestLogExporter(TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "serviceevents.ndjson")

    def tearDown(self):
        _reset_file_writers()
        self.tmpdir.cleanup()

    def _lines(self):
        if not os.path.exists(self.path):
            return []
        with open(self.path) as f:
            return [json.loads(line) for line in f if line.strip()]

    def test_writes_one_ndjson_line_per_record(self):
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        try:
            result = exporter.export(
                [
                    _make_readable(event_name="aws.service_events.function_call"),
                    _make_readable(event_name="aws.service_events.endpoint_summary"),
                ]
            )
        finally:
            exporter.shutdown()
        self.assertEqual(result, LogRecordExportResult.SUCCESS)
        lines = self._lines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0]["eventName"], "aws.service_events.function_call")
        self.assertEqual(lines[1]["eventName"], "aws.service_events.endpoint_summary")

    def test_appends_across_exports(self):
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        try:
            exporter.export([_make_readable()])
            exporter.export([_make_readable()])
        finally:
            exporter.shutdown()
        self.assertEqual(len(self._lines()), 2)

    def test_empty_batch_no_error(self):
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        try:
            result = exporter.export([])
        finally:
            exporter.shutdown()
        self.assertEqual(result, LogRecordExportResult.SUCCESS)
        self.assertEqual(self._lines(), [])

    def test_rejects_after_shutdown(self):
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        exporter.shutdown()
        result = exporter.export([_make_readable()])
        self.assertEqual(result, LogRecordExportResult.FAILURE)

    def test_force_flush_returns_true_on_success(self):
        # force_flush flushes the underlying handler and reports success.
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        try:
            exporter.export([_make_readable()])
            self.assertTrue(exporter.force_flush())
        finally:
            exporter.shutdown()

    def test_force_flush_returns_false_when_handler_raises(self):
        # An I/O error inside flush() is swallowed; force_flush returns False.
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        try:
            exporter.export([_make_readable()])
            with patch.object(exporter._writer.handler, "flush", side_effect=OSError("disk full")):
                self.assertFalse(exporter.force_flush())
        finally:
            exporter.shutdown()

    def test_export_failure_is_swallowed(self):
        # A write failure inside export() is caught and reported as FAILURE,
        # never propagated into the customer application.
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        try:
            with patch.object(exporter._writer.handler, "emit", side_effect=OSError("disk full")):
                result = exporter.export([_make_readable()])
            self.assertEqual(result, LogRecordExportResult.FAILURE)
        finally:
            exporter.shutdown()

    def test_double_shutdown_is_idempotent(self):
        # Second shutdown() hits the early-return guard and does not raise.
        exporter = ServiceEventsCloudWatchLogFileExporter(self.path)
        exporter.shutdown()
        exporter.shutdown()  # idempotent no-op


class TestMetricExporter(TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "serviceevents.ndjson")

    def tearDown(self):
        _reset_file_writers()
        self.tmpdir.cleanup()

    def _lines(self):
        if not os.path.exists(self.path):
            return []
        with open(self.path) as f:
            return [json.loads(line) for line in f if line.strip()]

    def _make_metrics_data(self):
        # Build a real MetricsData with one Sum metric (count) and one data point.
        from opentelemetry.sdk.metrics.export import (
            AggregationTemporality,
            Metric,
            MetricsData,
            NumberDataPoint,
            ResourceMetrics,
            ScopeMetrics,
            Sum,
        )
        from opentelemetry.sdk.util.instrumentation import InstrumentationScope

        dp = NumberDataPoint(
            attributes={
                "Telemetry.Source": "ServiceEvents",
                "service_name": "shoppingcart",
                "operation": "POST /api/checkout",
                "exception": "RuntimeError",
            },
            start_time_unix_nano=1744137900_000_000_000,
            time_unix_nano=1744137960_000_000_000,
            value=5,
        )
        sum_data = Sum(
            data_points=[dp],
            aggregation_temporality=AggregationTemporality.DELTA,
            is_monotonic=True,
        )
        metric = Metric(name="count", description="", unit="Count", data=sum_data)
        scope = InstrumentationScope(name="serviceevents", version="1.0")
        sm = ScopeMetrics(scope=scope, metrics=[metric], schema_url="")
        rm = ResourceMetrics(
            resource=Resource.create({"service.name": "shoppingcart"}),
            scope_metrics=[sm],
            schema_url="",
        )
        return MetricsData(resource_metrics=[rm])

    def _make_histogram_metrics_data(self):
        # Build a real MetricsData with one ExponentialHistogram (service.function.duration).
        from opentelemetry.sdk.metrics.export import (
            AggregationTemporality,
            Buckets,
            ExponentialHistogram,
            ExponentialHistogramDataPoint,
            Metric,
            MetricsData,
            ResourceMetrics,
            ScopeMetrics,
        )
        from opentelemetry.sdk.util.instrumentation import InstrumentationScope

        dp = ExponentialHistogramDataPoint(
            attributes={"Telemetry.Source": "ServiceEvents", "function.name": "app.handle", "status": "success"},
            start_time_unix_nano=1744137900_000_000_000,
            time_unix_nano=1744137960_000_000_000,
            count=3,
            sum=16166.0,
            scale=4,
            zero_count=0,
            positive=Buckets(offset=47, bucket_counts=[1, 2]),
            negative=Buckets(offset=0, bucket_counts=[]),
            flags=0,
            min=1500.0,
            max=3875.0,
        )
        histogram = ExponentialHistogram(
            data_points=[dp],
            aggregation_temporality=AggregationTemporality.DELTA,
        )
        metric = Metric(
            name="service.function.duration",
            description="Function call duration",
            unit="Microseconds",
            data=histogram,
        )
        scope = InstrumentationScope(name="serviceevents", version="1.0")
        sm = ScopeMetrics(scope=scope, metrics=[metric], schema_url="")
        rm = ResourceMetrics(
            resource=Resource.create({"service.name": "shoppingcart"}),
            scope_metrics=[sm],
            schema_url="",
        )
        return MetricsData(resource_metrics=[rm])

    def test_writes_otlp_json_per_batch(self):
        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            result = exporter.export(self._make_metrics_data())
        finally:
            exporter.shutdown()
        self.assertEqual(result, MetricExportResult.SUCCESS)
        lines = self._lines()
        # One line per export batch (an ExportMetricsServiceRequest), not per data point.
        self.assertEqual(len(lines), 1)
        req = lines[0]
        rm = req["resourceMetrics"][0]
        self.assertEqual(rm["scopeMetrics"][0]["scope"]["name"], "serviceevents")
        metric = rm["scopeMetrics"][0]["metrics"][0]
        self.assertEqual(metric["name"], "count")
        self.assertEqual(metric["unit"], "Count")
        sum_dp = metric["sum"]["dataPoints"][0]
        self.assertEqual(sum_dp["asInt"], "5")
        # No EMF envelope; metric name stays lowercase (no count->Count capitalization).
        self.assertNotIn("_aws", req)

    def test_writes_exponential_histogram_as_otlp_json(self):
        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            result = exporter.export(self._make_histogram_metrics_data())
        finally:
            exporter.shutdown()
        self.assertEqual(result, MetricExportResult.SUCCESS)
        lines = self._lines()
        self.assertEqual(len(lines), 1)
        metric = lines[0]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
        self.assertEqual(metric["name"], "service.function.duration")
        self.assertEqual(metric["unit"], "Microseconds")
        # The histogram serializes natively as exponentialHistogram (not dropped, not EMF).
        self.assertIn("exponentialHistogram", metric)
        hist_dp = metric["exponentialHistogram"]["dataPoints"][0]
        self.assertEqual(hist_dp["count"], "3")
        self.assertEqual(hist_dp["scale"], 4)

    def test_empty_batch_no_error(self):
        from opentelemetry.sdk.metrics.export import MetricsData

        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            result = exporter.export(MetricsData(resource_metrics=[]))
        finally:
            exporter.shutdown()
        self.assertEqual(result, MetricExportResult.SUCCESS)
        self.assertEqual(self._lines(), [])

    def test_temporality_is_delta_through_real_reader(self):
        # Drive a real counter through a MeterProvider + PeriodicExportingMetricReader
        # using this exporter, so the exporter's CONFIGURED temporality is exercised
        # (not a hand-built MetricsData). The reader reads `_preferred_temporality`
        # off the exporter; an empty dict would default to CUMULATIVE and diverge
        # from the OTLP network exporter, which configures DELTA. Regression guard.
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=3_600_000)
        provider = MeterProvider(metric_readers=[reader])
        try:
            counter = provider.get_meter("serviceevents", "1.0").create_counter("count", unit="Count")
            counter.add(5, {"Telemetry.Source": "ServiceEvents", "operation": "POST /x"})
            # Force a collection+export cycle through the reader (not a direct export()).
            reader.collect()
        finally:
            provider.shutdown()
        lines = self._lines()
        self.assertTrue(lines, "reader.collect() should have produced one OTLP-JSON line")
        metric = lines[0]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
        self.assertEqual(
            metric["sum"]["aggregationTemporality"],
            "AGGREGATION_TEMPORALITY_DELTA",
            "file exporter must export Delta to match the OTLP network exporter",
        )

    def test_force_flush_returns_true_on_success(self):
        # force_flush flushes the underlying handler and reports success.
        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            exporter.export(self._make_metrics_data())
            self.assertTrue(exporter.force_flush())
        finally:
            exporter.shutdown()

    def test_force_flush_returns_false_when_handler_raises(self):
        # An I/O error inside flush() is swallowed; force_flush returns False.
        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            exporter.export(self._make_metrics_data())
            with patch.object(exporter._writer.handler, "flush", side_effect=OSError("disk full")):
                self.assertFalse(exporter.force_flush())
        finally:
            exporter.shutdown()

    def test_export_failure_is_swallowed(self):
        # A write failure inside export() is caught and reported as FAILURE,
        # never propagated into the customer application.
        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            with patch.object(exporter._writer.handler, "emit", side_effect=OSError("disk full")):
                result = exporter.export(self._make_metrics_data())
            self.assertEqual(result, MetricExportResult.FAILURE)
        finally:
            exporter.shutdown()

    def test_double_shutdown_is_idempotent(self):
        # Second shutdown() hits the early-return guard and does not raise.
        exporter = ServiceEventsCloudWatchMetricFileExporter(self.path)
        exporter.shutdown()
        exporter.shutdown()  # idempotent no-op

    def test_log_and_metric_exporters_share_one_file(self):
        log_exp = ServiceEventsCloudWatchLogFileExporter(self.path)
        metric_exp = ServiceEventsCloudWatchMetricFileExporter(self.path)
        try:
            log_exp.export([_make_readable()])
            metric_exp.export(self._make_metrics_data())
        finally:
            log_exp.shutdown()
            metric_exp.shutdown()
        lines = self._lines()
        self.assertEqual(len(lines), 2)
        # Logs keep the flat CloudWatch-Insights shape (eventName); metrics are OTLP JSON.
        self.assertTrue(any(line.get("eventName") == "aws.service_events.function_call" for line in lines))
        self.assertTrue(any(line.get("resourceMetrics") for line in lines))


class TestRotation(TestCase):
    """Verify size-based rotation enforces the declared 50MB / 5-backup policy.

    Tests use a tiny MAX_BYTES (1 KiB) so a handful of records cross the
    threshold; the rotation mechanism is the same at any threshold.
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "serviceevents.ndjson")

    def tearDown(self):
        _reset_file_writers()
        self.tmpdir.cleanup()

    def _backup_paths(self):
        return sorted(glob.glob(self.path + ".*"))

    def _exporter_with_threshold(self, max_bytes: int):
        # Patch module constant so the exporter's _acquire_writer picks up the
        # smaller threshold via its default arg.
        patcher = patch.object(cloudwatch_file_exporter, "MAX_BYTES", max_bytes)
        patcher.start()
        self.addCleanup(patcher.stop)
        return ServiceEventsCloudWatchLogFileExporter(self.path)

    def test_below_threshold_no_rollover(self):
        # 10 KiB threshold; 5 records × ~418 bytes ≈ 2 KiB — well below.
        exporter = self._exporter_with_threshold(max_bytes=10 * 1024)
        try:
            exporter.export([_make_readable() for _ in range(5)])
        finally:
            exporter.shutdown()
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(self._backup_paths(), [])

    def test_at_threshold_single_rollover(self):
        # 1 KiB threshold; 4 records × ~418 bytes ≈ 1.6 KiB — crosses once.
        exporter = self._exporter_with_threshold(max_bytes=1024)
        try:
            exporter.export([_make_readable() for _ in range(4)])
        finally:
            exporter.shutdown()
        self.assertTrue(os.path.exists(self.path))
        backups = self._backup_paths()
        self.assertGreaterEqual(len(backups), 1, "expected at least one rollover")
        self.assertIn(self.path + ".1", backups)

    def test_backup_cap_drops_oldest(self):
        # 512-byte threshold + many records → far more than 5 rotations.
        # Backup cap must hold at 5 regardless.
        exporter = self._exporter_with_threshold(max_bytes=512)
        try:
            for _ in range(10):
                exporter.export([_make_readable() for _ in range(20)])
        finally:
            exporter.shutdown()
        backups = self._backup_paths()
        # Cap at 5 backups: <file>.1 .. <file>.5, no <file>.6.
        self.assertLessEqual(len(backups), 5, f"expected ≤5 backups, got {backups}")
        self.assertFalse(os.path.exists(self.path + ".6"), "<file>.6 should never exist with backupCount=5")

    def test_utf8_byte_count_drives_rollover(self):
        # 200-byte threshold; each record body holds 100 4-byte CJK code points.
        # If shouldRollover used len(msg) (code points), 100 chars + a small JSON
        # envelope wouldn't trip the threshold even though the on-disk bytes are
        # ~400+. The override must count UTF-8 bytes so rotation fires.
        exporter = self._exporter_with_threshold(max_bytes=200)
        cjk_body = {"msg": "上" * 100}  # each char is 3 UTF-8 bytes
        try:
            for _ in range(3):
                exporter.export([_make_readable(body=cjk_body)])
        finally:
            exporter.shutdown()
        backups = self._backup_paths()
        self.assertGreaterEqual(len(backups), 1, "UTF-8 byte counting must trigger rollover for multi-byte content")


class TestExporterErrorHandling(TestCase):
    """Constructing an exporter on an unopenable path MUST NOT raise — telemetry
    code is forbidden from crashing the customer application."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        _reset_file_writers()
        self.tmpdir.cleanup()

    def _bogus_path(self) -> str:
        # Create a regular file then try to write *under* it — mkdir on a
        # path whose parent is a file fails with NotADirectoryError.
        plain_file = os.path.join(self.tmpdir.name, "regular-file")
        with open(plain_file, "wb") as f:
            f.write(b"\0")
        return os.path.join(plain_file, "nested", "svc.ndjson")

    def test_log_exporter_unopenable_path_does_not_raise(self):
        exporter = ServiceEventsCloudWatchLogFileExporter(self._bogus_path())
        try:
            result = exporter.export([_make_readable()])
            self.assertEqual(result, LogRecordExportResult.FAILURE)
            self.assertFalse(exporter.force_flush())
        finally:
            # shutdown must be a safe no-op when no writer was acquired.
            exporter.shutdown()

    def test_metric_exporter_unopenable_path_does_not_raise(self):
        exporter = ServiceEventsCloudWatchMetricFileExporter(self._bogus_path())
        try:
            metrics = MagicMock()
            metrics.resource_metrics = []
            result = exporter.export(metrics)
            self.assertEqual(result, MetricExportResult.FAILURE)
            self.assertFalse(exporter.force_flush())
        finally:
            exporter.shutdown()

    def test_log_exporter_invalid_path_type_does_not_raise(self):
        # os.path.abspath raises TypeError on non-string input. Constructor MUST
        # NOT propagate this — exporter is constructed at SDK init, before the
        # customer app has any chance to recover.
        try:
            exporter = ServiceEventsCloudWatchLogFileExporter(None)  # type: ignore[arg-type]
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.fail(f"constructor must not raise on invalid path type: {exc!r}")
        try:
            self.assertEqual(exporter.export([_make_readable()]), LogRecordExportResult.FAILURE)
        finally:
            exporter.shutdown()

    def test_metric_exporter_invalid_path_type_does_not_raise(self):
        try:
            exporter = ServiceEventsCloudWatchMetricFileExporter(None)  # type: ignore[arg-type]
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.fail(f"constructor must not raise on invalid path type: {exc!r}")
        try:
            metrics = MagicMock()
            metrics.resource_metrics = []
            self.assertEqual(exporter.export(metrics), MetricExportResult.FAILURE)
        finally:
            exporter.shutdown()


class TestWriterSingleton(TestCase):
    """Cover the shared-writer registry's release/reset edge cases."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "serviceevents.ndjson")

    def tearDown(self):
        _reset_file_writers()
        self.tmpdir.cleanup()

    def test_release_unknown_path_is_noop(self):
        # Releasing a path that was never acquired hits the entry-is-None guard
        # and returns without raising.
        _release_writer(os.path.join(self.tmpdir.name, "never-acquired.ndjson"))

    def test_release_swallows_handler_close_error(self):
        # A failure while closing the handler on final release is swallowed.
        abs_path = os.path.abspath(self.path)
        entry = _acquire_writer(abs_path)
        self.assertIsNotNone(entry)
        with patch.object(entry.handler, "close", side_effect=OSError("close failed")):
            _release_writer(abs_path)  # must not raise

    def test_reset_swallows_handler_close_error(self):
        # _reset_file_writers swallows per-handler close errors and still clears.
        abs_path = os.path.abspath(self.path)
        entry = _acquire_writer(abs_path)
        self.assertIsNotNone(entry)
        with patch.object(entry.handler, "close", side_effect=OSError("close failed")):
            _reset_file_writers()  # must not raise
        # Registry is cleared even though close() raised.
        self.assertEqual(cloudwatch_file_exporter._writers, {})


class TestUtf8RotatingFileHandler(TestCase):
    """Exercise the shouldRollover guards in the UTF-8 byte-counting handler."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmpdir.name, "serviceevents.ndjson")

    def tearDown(self):
        _reset_file_writers()
        self.tmpdir.cleanup()

    def _record(self, msg="hello\n"):
        import logging

        return logging.LogRecord(
            name="serviceevents",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def _handler(self, max_bytes):
        import logging

        handler = _Utf8RotatingFileHandler(
            self.path, mode="a", maxBytes=max_bytes, backupCount=1, encoding="utf-8", delay=True
        )
        handler.terminator = ""
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.addCleanup(handler.close)
        return handler

    def test_opens_stream_when_none(self):
        # delay=True leaves the stream unopened; shouldRollover must open it
        # (and report False on the resulting empty file).
        handler = self._handler(max_bytes=1024)
        self.assertIsNone(handler.stream)
        self.assertFalse(handler.shouldRollover(self._record()))
        self.assertIsNotNone(handler.stream)

    def test_disabled_when_max_bytes_non_positive(self):
        # maxBytes <= 0 disables rotation entirely.
        handler = self._handler(max_bytes=0)
        self.assertFalse(handler.shouldRollover(self._record()))

    def test_no_rollover_for_non_regular_base_file(self):
        # If the base path is not a regular file, the guard returns False even
        # past the byte threshold.
        handler = self._handler(max_bytes=1)
        handler.stream = handler._open()
        with patch.object(os.path, "isfile", return_value=False):
            self.assertFalse(handler.shouldRollover(self._record()))

    def test_no_rollover_when_seek_raises_oserror(self):
        # A seek/tell OSError is treated as "cannot determine size" → no rollover.
        handler = self._handler(max_bytes=1)
        handler.stream = handler._open()
        with patch.object(handler.stream, "seek", side_effect=OSError("seek failed")):
            self.assertFalse(handler.shouldRollover(self._record()))

    def test_rollover_uses_codepoint_len_on_encode_failure(self):
        # If encoding the formatted message fails, fall back to len(msg) for the
        # size check rather than crashing the rotation path.
        handler = self._handler(max_bytes=4)
        handler.stream = handler._open()
        handler.stream.write("seed")  # non-empty so the empty-file guard passes
        handler.stream.flush()

        class _BadStr(str):
            def encode(self, *args, **kwargs):
                raise UnicodeError("boom")

            def __add__(self, other):
                # format(record) + terminator → still a _BadStr so .encode() raises.
                return _BadStr(str(self) + str(other))

        with patch.object(handler, "format", return_value=_BadStr("abcdef")):
            record = self._record()
            # Fallback len("abcdef")==6 + current size (4) >= 4 → rollover.
            self.assertTrue(handler.shouldRollover(record))
