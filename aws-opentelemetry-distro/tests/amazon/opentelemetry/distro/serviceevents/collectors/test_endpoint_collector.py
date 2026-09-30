# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.serviceevents.collectors.endpoint_collector import EndpointMetricCollector
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState


class TestEndpointMetricCollector(TestCase):
    """Test the EndpointMetricCollector class."""

    def setUp(self):
        """Reset monitor state before each test."""
        _ServiceEventsMonitorState._instance = None

    def test_init_with_explicit_params(self):
        """Test collector initialization with explicit parameters."""
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-service",
            sdk_version="1.0.0",
        )

        self.assertEqual(collector.flush_interval_ms, 10000)
        self.assertEqual(collector.environment, "testing")
        self.assertEqual(collector.service_name, "test-service")
        self.assertEqual(collector.sdk_version, "1.0.0")
        self.assertEqual(collector.name, "EndpointMetricCollector")

    @patch.dict("os.environ", {"OTEL_SERVICE_NAME": "api-svc"})
    def test_init_with_defaults(self):
        """Test collector initialization with default values."""
        collector = EndpointMetricCollector(flush_interval_ms=5000, environment="production")

        self.assertEqual(collector.environment, "production")
        self.assertEqual(collector.service_name, "api-svc")
        # sdk_version defaults to "" (must be provided by the caller, which always passes
        # ServiceEventsConfig.sdk_version = ADOT_VERSION); the old "0.14.2" default was stale.
        self.assertEqual(collector.sdk_version, "")

    def test_record_request_creates_aggregation_entry(self):
        """Test that record_request creates an aggregation entry."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(
            route="/api/users",
            method="GET",
            status_code=200,
            duration_ns=50_000_000,  # 50ms
        )

        self.assertEqual(len(collector._aggregations), 1)
        # Get the single entry
        operation = list(collector._aggregations.keys())[0]
        agg = collector._aggregations[operation]
        # route and method are no longer stored in aggregation; operation is the key
        self.assertEqual(agg["count"], 1)
        self.assertEqual(agg["faults"], 0)
        self.assertEqual(agg["errors"], 0)
        self.assertIsNotNone(agg["seh_histogram"])
        self.assertEqual(agg["incidents_exemplar"], [])

    def test_record_request_increments_count(self):
        """Test that multiple record_request calls increment the count."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        for _ in range(5):
            collector.record_request(
                route="/api/users",
                method="GET",
                status_code=200,
                duration_ns=50_000_000,
            )

        operation = list(collector._aggregations.keys())[0]
        self.assertEqual(collector._aggregations[operation]["count"], 5)

    def test_record_request_with_error_info(self):
        """Test that error recording populates error_breakdown."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(
            route="/api/users",
            method="POST",
            status_code=500,
            duration_ns=100_000_000,
            error_info={"error_type": "ValueError", "function_name": "func_123"},
        )

        operation = list(collector._aggregations.keys())[0]
        agg = collector._aggregations[operation]
        self.assertEqual(agg["count"], 1)
        # Check error_breakdown
        self.assertIn("500", agg["error_breakdown"])
        error_key = "ValueError:func_123"
        self.assertIn(error_key, agg["error_breakdown"]["500"])
        self.assertEqual(agg["error_breakdown"]["500"][error_key]["count"], 1)

    def test_collect_with_empty_data(self):
        """Test that collect with no data doesn't crash."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)
        # Should not raise any exception
        collector.collect()

    def test_collect_swaps_aggregations(self):
        """Test that collect swaps aggregations - old data returned, internal state cleared."""
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=MagicMock(),
        )

        collector.record_request(
            route="/api/items",
            method="GET",
            status_code=200,
            duration_ns=10_000_000,
        )

        # Verify there's data before collect
        self.assertEqual(len(collector._aggregations), 1)

        collector.collect()

        # After collect, aggregations should be empty
        self.assertEqual(len(collector._aggregations), 0)

    def test_emit_endpoint_summary_event_fields(self):
        """The EndpointMetricEvent handed to the emitter carries operation/route/method/count."""
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )

        collector.record_request(
            route="/health",
            method="GET",
            status_code=200,
            duration_ns=5_000_000,
        )

        collector.collect()

        emitter.emit_endpoint_summary.assert_called_once()
        event = emitter.emit_endpoint_summary.call_args[0][0]
        self.assertEqual(event.operation, "GET /health")
        self.assertEqual(event.route, "/health")
        self.assertEqual(event.method, "GET")
        self.assertEqual(event.count, 1)
        self.assertEqual(event.telemetry_type, "EndpointSummary")

    def test_start_stop_lifecycle(self):
        """Test starting and stopping the collector."""
        collector = EndpointMetricCollector(flush_interval_ms=100)

        collector.start()
        self.assertTrue(collector._running)
        self.assertIsNotNone(collector._thread)
        self.assertTrue(collector._thread.is_alive())

        time.sleep(0.05)

        collector.stop()
        self.assertFalse(collector._running)

        time.sleep(0.2)
        self.assertFalse(collector._thread.is_alive())

    def test_concurrent_record_request(self):
        """Test concurrent record_request from multiple threads."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)
        errors = []

        def record_requests(thread_id):
            try:
                for i in range(50):
                    collector.record_request(
                        route="/api/items",
                        method="GET",
                        status_code=200,
                        duration_ns=10_000_000,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_requests, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors during concurrent access: {errors}")
        # All 200 requests should be counted
        operation = list(collector._aggregations.keys())[0]
        self.assertEqual(collector._aggregations[operation]["count"], 200)

    def test_different_routes_create_separate_entries(self):
        """Test that different route/method combos create separate aggregation entries."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(route="/api/users", method="GET", status_code=200, duration_ns=10_000_000)
        collector.record_request(route="/api/users", method="POST", status_code=201, duration_ns=20_000_000)
        collector.record_request(route="/api/items", method="GET", status_code=200, duration_ns=15_000_000)

        self.assertEqual(len(collector._aggregations), 3)

    def test_error_breakdown_not_recorded_below_500(self):
        """error_breakdown is only populated for faults (5xx), matching Java.

        Neither a 2xx nor a 4xx with error_info produces a breakdown entry; the
        gate is status_code >= 500. (A 4xx still bumps the aggregate ``errors``
        counter, asserted separately in test_4xx_increments_errors.)
        """
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(
            route="/api/users",
            method="GET",
            status_code=200,
            duration_ns=10_000_000,
            error_info={"error_type": "ValueError", "function_name": "func_123"},
        )
        collector.record_request(
            route="/api/users",
            method="GET",
            status_code=404,
            duration_ns=10_000_000,
            error_info={"error_type": "NotFoundError", "function_name": "func_456"},
        )

        operation = list(collector._aggregations.keys())[0]
        agg = collector._aggregations[operation]
        # error_breakdown should be empty since no request was a 5xx fault.
        self.assertEqual(len(agg["error_breakdown"]), 0)

    def test_error_breakdown_not_recorded_for_5xx_without_error_info(self):
        """A 5xx with error_info=None produces no breakdown entry, matching Java.

        The framework hook's _extract_error_from_call_path returns None when no real
        error type was captured (e.g. a handler that returns a 500 status without
        raising), so record_request is called with error_info=None. The request is
        still counted as a fault, but no per-error breakdown is recorded — matching
        Java's `statusCode >= 500 && errorType != null` gate.
        """
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ns=10_000_000,
            error_info=None,
        )

        operation = list(collector._aggregations.keys())[0]
        agg = collector._aggregations[operation]
        self.assertEqual(agg["faults"], 1)
        self.assertEqual(len(agg["error_breakdown"]), 0)

    def test_5xx_increments_faults(self):
        """Test that 5xx status codes increment faults counter."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(route="/api/users", method="GET", status_code=500, duration_ns=10_000_000)
        collector.record_request(route="/api/users", method="GET", status_code=503, duration_ns=10_000_000)
        collector.record_request(route="/api/users", method="GET", status_code=200, duration_ns=10_000_000)

        operation = list(collector._aggregations.keys())[0]
        agg = collector._aggregations[operation]
        self.assertEqual(agg["faults"], 2)
        self.assertEqual(agg["errors"], 0)
        self.assertEqual(agg["count"], 3)

    def test_4xx_increments_errors(self):
        """Test that 4xx status codes increment errors counter, not faults."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        collector.record_request(route="/api/users", method="GET", status_code=400, duration_ns=10_000_000)
        collector.record_request(route="/api/users", method="GET", status_code=404, duration_ns=10_000_000)
        collector.record_request(route="/api/users", method="GET", status_code=429, duration_ns=10_000_000)

        operation = list(collector._aggregations.keys())[0]
        agg = collector._aggregations[operation]
        self.assertEqual(agg["errors"], 3)
        self.assertEqual(agg["faults"], 0)
        self.assertEqual(agg["count"], 3)

    def test_emitted_event_includes_fault_error_duration_fields(self):
        """The emitted EndpointMetricEvent carries faults, errors, count, and a duration histogram."""
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )

        collector.record_request(route="/api/users", method="GET", status_code=200, duration_ns=50_000_000)
        collector.record_request(route="/api/users", method="GET", status_code=500, duration_ns=100_000_000)
        collector.record_request(route="/api/users", method="GET", status_code=404, duration_ns=30_000_000)

        collector.collect()

        emitter.emit_endpoint_summary.assert_called_once()
        event = emitter.emit_endpoint_summary.call_args[0][0]
        self.assertEqual(event.faults, 1)
        self.assertEqual(event.errors, 1)
        self.assertEqual(event.count, 3)

        # Duration is a populated DurationMetrics histogram.
        self.assertIsNotNone(event.duration)
        self.assertEqual(event.duration.count, 3)
        self.assertTrue(event.duration.values)
        self.assertTrue(event.duration.counts)

    def test_format_converts_duration_to_microseconds(self):
        """Test that duration values are converted from nanoseconds to microseconds."""
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )

        # Record a single request: 50ms = 50_000_000 ns = 50_000 us
        collector.record_request(route="/api/test", method="GET", status_code=200, duration_ns=50_000_000)

        collector.collect()

        event = emitter.emit_endpoint_summary.call_args[0][0]
        # Sum should be 50_000 microseconds (50ms)
        self.assertAlmostEqual(event.duration.sum, 50_000.0, places=1)

    def test_record_incident_exemplar(self):
        """Test that record_incident_exemplar stores exemplar in aggregation."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        # First record a request to create the aggregation entry
        collector.record_request(route="/api/users", method="GET", status_code=500, duration_ns=10_000_000)

        operation = list(collector._aggregations.keys())[0]

        # Now record an incident exemplar
        exemplar = {
            "snapshot_id": "snap_abc123",
            "trigger_type": "exception",
            "severity": "critical",
            "operation": operation,
            "timestamp": 1706745600000,
        }
        collector.record_incident_exemplar(operation, exemplar)

        agg = collector._aggregations[operation]
        self.assertEqual(len(agg["incidents_exemplar"]), 1)
        self.assertEqual(agg["incidents_exemplar"][0]["snapshot_id"], "snap_abc123")

    def test_record_incident_exemplar_no_aggregation(self):
        """Test that record_incident_exemplar is safe when no aggregation exists."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)

        # Record exemplar for non-existent endpoint — should not raise
        collector.record_incident_exemplar(
            "nonexistent-id",
            {
                "snapshot_id": "snap_xyz",
                "trigger_type": "exception",
                "severity": "high",
                "timestamp": 1706745600000,
            },
        )

        # No aggregation created
        self.assertEqual(len(collector._aggregations), 0)

    def test_incidents_exemplar_in_emitted_event(self):
        """The emitted event includes incident_count and the recorded incident exemplars."""
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )

        collector.record_request(route="/api/users", method="GET", status_code=500, duration_ns=10_000_000)
        operation = list(collector._aggregations.keys())[0]

        collector.record_incident_exemplar(
            operation,
            {
                "snapshot_id": "snap_001",
                "trigger_type": "exception",
                "severity": "critical",
                "operation": operation,
                "timestamp": 1706745600000,
            },
        )
        collector.record_incident_exemplar(
            operation,
            {
                "snapshot_id": "snap_002",
                "trigger_type": "exception",
                "severity": "high",
                "operation": operation,
                "timestamp": 1706745601000,
            },
        )

        collector.collect()

        event = emitter.emit_endpoint_summary.call_args[0][0]
        self.assertEqual(event.incident_count, 2)
        self.assertEqual(len(event.incidents_exemplar), 2)
        self.assertEqual(event.incidents_exemplar[0].snapshot_id, "snap_001")
        self.assertEqual(event.incidents_exemplar[1].snapshot_id, "snap_002")

    def test_no_incidents_defaults(self):
        """Test that endpoints with no incidents have zero count and empty exemplar."""
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )

        collector.record_request(route="/api/healthy", method="GET", status_code=200, duration_ns=5_000_000)

        collector.collect()

        event = emitter.emit_endpoint_summary.call_args[0][0]
        self.assertEqual(event.incident_count, 0)
        self.assertEqual(event.incidents_exemplar, [])

    def test_error_metrics_emitted_only_for_faults(self):
        """collect() emits one EndpointErrorMetric per 5xx error type; 4xx is excluded.

        Matches Java: the breakdown that feeds EndpointErrorMetric is gated on
        status_code >= 500. A 4xx with error_info still increments the aggregate
        ``errors`` counter on the EndpointSummary, but does not produce a breakdown
        data point.
        """
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )

        for _ in range(3):
            collector.record_request(
                route="/api/users",
                method="POST",
                status_code=500,
                duration_ns=10_000_000,
                error_info={"error_type": "RuntimeError", "function_name": "func_a"},
            )
        for _ in range(2):
            collector.record_request(
                route="/api/users",
                method="POST",
                status_code=400,
                duration_ns=10_000_000,
                error_info={"error_type": "ValueError", "function_name": "func_b"},
            )

        collector.collect()

        # EndpointSummary emitted once.
        emitter.emit_endpoint_summary.assert_called_once()

        # Only the 5xx fault produces an EndpointErrorMetric; the 4xx is excluded.
        emitter.emit_endpoint_error_metrics.assert_called_once()
        metrics = emitter.emit_endpoint_error_metrics.call_args[0][0]
        by_exception = {m.exception: m for m in metrics}
        self.assertEqual(set(by_exception), {"RuntimeError"})
        self.assertEqual(by_exception["RuntimeError"].count, 3)
        for metric in metrics:
            self.assertEqual(metric.telemetry_type, "EndpointErrorMetric")

    def test_no_error_metrics_emitted_without_errors(self):
        """No EndpointErrorMetric is emitted when there are no errors (summary only)."""
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )
        collector.record_request(route="/api/healthy", method="GET", status_code=200, duration_ns=5_000_000)

        collector.collect()

        emitter.emit_endpoint_summary.assert_called_once()
        emitter.emit_endpoint_error_metrics.assert_not_called()


class TestEndpointCollectorAppSignalsSuppression(TestCase):
    """EndpointSummary suppression when Application Signals is enabled.

    App Signals carries equivalent per-endpoint duration + error metrics, so emitting
    EndpointSummary in bundled mode duplicates data on the backend. The collector
    still runs (latency histograms feed IncidentSnapshot triggers), but stops emitting
    the aws.service_events.endpoint_summary LogRecord.

    EndpointErrorMetric (per-exception-type breakdown) is ServiceEvents-specific and always
    emits regardless — App Signals doesn't carry the same shape.
    """

    def setUp(self) -> None:
        _ServiceEventsMonitorState._instance = None

    def _build_collector_with_emitter(self, suppress: bool) -> tuple:
        emitter = MagicMock()
        collector = EndpointMetricCollector(
            flush_interval_ms=10000,
            environment="testing",
            service_name="svc",
            otlp_emitter=emitter,
            suppress_endpoint_summary=suppress,
        )
        return collector, emitter

    def test_suppress_true_skips_endpoint_summary_emit(self) -> None:
        collector, emitter = self._build_collector_with_emitter(suppress=True)
        collector.record_request(route="/api/users", method="GET", status_code=200, duration_ns=10_000_000)

        collector.collect()

        emitter.emit_endpoint_summary.assert_not_called()

    def test_suppress_true_still_emits_error_metrics(self) -> None:
        """EndpointErrorMetric carries per-exception-type breakdown App Signals doesn't have."""
        collector, emitter = self._build_collector_with_emitter(suppress=True)
        collector.record_request(
            route="/api/users",
            method="POST",
            status_code=500,
            duration_ns=10_000_000,
            error_info={"error_type": "RuntimeError", "function_name": "handler"},
        )

        collector.collect()

        emitter.emit_endpoint_summary.assert_not_called()
        emitter.emit_endpoint_error_metrics.assert_called()

    def test_suppress_false_emits_both(self) -> None:
        """Default bundled-off path: both signals emit as they always have."""
        collector, emitter = self._build_collector_with_emitter(suppress=False)
        collector.record_request(
            route="/api/users",
            method="POST",
            status_code=500,
            duration_ns=10_000_000,
            error_info={"error_type": "RuntimeError", "function_name": "handler"},
        )

        collector.collect()

        emitter.emit_endpoint_summary.assert_called_once()
        emitter.emit_endpoint_error_metrics.assert_called_once()

    def test_suppress_true_does_not_clear_aggregations_via_shortcut(self) -> None:
        """Suppression is emit-path only: internal aggregation swap still happens."""
        collector, _emitter = self._build_collector_with_emitter(suppress=True)
        collector.record_request(route="/api/users", method="GET", status_code=200, duration_ns=10_000_000)
        self.assertEqual(len(collector._aggregations), 1)

        collector.collect()

        # Aggregations swapped out on collect() regardless of suppression.
        self.assertEqual(len(collector._aggregations), 0)

    def test_suppress_default_is_false(self) -> None:
        """Keyword-only arg with default False preserves backward compatibility."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)
        self.assertFalse(collector.suppress_endpoint_summary)


class TestEndpointCollectorForkReset(TestCase):
    """Fork-safety: child must not inherit the parent's accumulated aggregations."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_reset_for_fork_clears_aggregations(self):
        """Inherited aggregations are dropped so the child doesn't re-emit parent metrics."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)
        collector.record_request(route="/api/users", method="GET", status_code=200, duration_ns=1_000_000)
        self.assertEqual(len(collector._aggregations), 1)

        collector._reset_for_fork()

        self.assertEqual(len(collector._aggregations), 0)

    def test_reset_for_fork_recreates_lock_and_refreshes_pid(self):
        """A fresh lock (parent thread may have held the old one) and the child's pid."""
        collector = EndpointMetricCollector(flush_interval_ms=10000)
        old_lock = collector._aggregations_lock

        collector._reset_for_fork()

        self.assertIsNot(collector._aggregations_lock, old_lock)
        # New lock is unlocked and usable.
        self.assertTrue(collector._aggregations_lock.acquire(blocking=False))
        collector._aggregations_lock.release()
