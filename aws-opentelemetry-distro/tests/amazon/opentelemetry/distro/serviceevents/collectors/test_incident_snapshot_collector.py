# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.serviceevents.collectors.incident_snapshot_collector import IncidentSnapshotCollector
from amazon.opentelemetry.distro.serviceevents.models import (
    CallPathEntry,
    ExceptionInfo,
    IncidentSnapshot,
    RequestContext,
    ResourceAttributes,
    TelemetryCorrelation,
)
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState


class TestDetermineTriggerType(TestCase):
    """Test the _determine_trigger_type method."""

    def setUp(self):
        """Reset monitor state before each test."""
        _ServiceEventsMonitorState._instance = None

    def test_exception_trigger(self):
        """Test that exception returns 'exception' trigger type."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._determine_trigger_type(
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("test"),
        )
        self.assertEqual(result, "exception")

    def test_error_status_trigger(self):
        """Test that status >= 500 without exception returns 'exception'."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._determine_trigger_type(
            status_code=500,
            duration_ms=50.0,
            exception=None,
        )
        self.assertEqual(result, "exception")

    def test_latency_trigger(self):
        """Test that slow request triggers 'latency'."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=100,
            max_per_period=100,
        )

        result = collector._determine_trigger_type(
            status_code=200,
            duration_ms=150.0,
            exception=None,
        )
        self.assertEqual(result, "latency")

    def test_no_trigger(self):
        """Test that normal request (200 OK, fast) returns None."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._determine_trigger_type(
            status_code=200,
            duration_ms=50.0,
            exception=None,
        )
        self.assertIsNone(result)

    def test_exception_with_error_status(self):
        """Test that exception + status >= 500 returns 'exception'."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._determine_trigger_type(
            status_code=500,
            duration_ms=50.0,
            exception=RuntimeError("server error"),
        )
        self.assertEqual(result, "exception")

    def test_latency_with_per_operation_threshold(self):
        """Test latency trigger with per-operation threshold (exact match)."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,  # default high
            max_per_period=100,
        )

        # Set a low threshold for this specific operation
        collector.set_latency_threshold("GET /api/users", 50.0)

        result = collector._determine_trigger_type(
            status_code=200,
            duration_ms=75.0,
            exception=None,
            operation="GET /api/users",
        )
        self.assertEqual(result, "latency")


class TestDetermineSeverity(TestCase):
    """Test the _determine_severity method."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self):
        return IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

    def test_critical_500(self):
        """Test that status 500 returns 'critical'."""
        collector = self._make_collector()
        self.assertEqual(collector._determine_severity(500, "exception"), "critical")

    def test_critical_503(self):
        """Test that status 503 returns 'critical'."""
        collector = self._make_collector()
        self.assertEqual(collector._determine_severity(503, "exception"), "critical")

    def test_high_504(self):
        """Test that status 504 returns 'high'."""
        collector = self._make_collector()
        self.assertEqual(collector._determine_severity(504, "exception"), "high")

    def test_high_exception(self):
        """Test that exception trigger returns 'high'."""
        collector = self._make_collector()
        self.assertEqual(collector._determine_severity(200, "exception"), "high")

    def test_medium_latency(self):
        """Test that latency trigger returns 'medium'."""
        collector = self._make_collector()
        self.assertEqual(collector._determine_severity(200, "latency"), "medium")

    def test_low_default(self):
        """Test default severity is 'low'."""
        collector = self._make_collector()
        self.assertEqual(collector._determine_severity(200, "some_other"), "low")


class TestCheckRateLimit(TestCase):
    """Test the atomic _try_reserve_rate_limit_slot (check-and-reserve under one lock hold)."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_allows_within_limit(self):
        """Test that requests within limit are allowed, each reservation consuming one slot."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=5,
        )

        for _ in range(5):
            self.assertTrue(collector._try_reserve_rate_limit_slot())

    def test_denies_over_limit(self):
        """Test that requests over limit are denied."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=3,
        )

        # Fill up the limit — each successful reservation consumes a slot.
        for _ in range(3):
            self.assertTrue(collector._try_reserve_rate_limit_slot())

        # Next should be denied
        self.assertFalse(collector._try_reserve_rate_limit_slot())

    def test_rejected_reservation_consumes_no_slot(self):
        """A rejected reservation must NOT consume a slot: once full, repeated calls stay False and
        the count never grows past max_per_period."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=1,
        )

        # One reservation fills the single slot.
        self.assertTrue(collector._try_reserve_rate_limit_slot())
        # Further attempts are rejected and do not append phantom slots.
        for _ in range(5):
            self.assertFalse(collector._try_reserve_rate_limit_slot())
        self.assertEqual(len(collector._snapshot_timestamps), 1)

    def test_reservation_is_atomic_check_and_append(self):
        """A successful reservation appends exactly one timestamp in the same call (no separate
        commit step) — the property that prevents concurrent distinct-hash overshoot."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=5,
        )

        self.assertEqual(len(collector._snapshot_timestamps), 0)
        self.assertTrue(collector._try_reserve_rate_limit_slot())
        self.assertEqual(len(collector._snapshot_timestamps), 1)

    def test_old_entries_expire(self):
        """Test that old entries expire and free up capacity."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=2,
        )

        # Fill up the limit
        self.assertTrue(collector._try_reserve_rate_limit_slot())
        self.assertTrue(collector._try_reserve_rate_limit_slot())
        self.assertFalse(collector._try_reserve_rate_limit_slot())

        # Manually age out old entries by modifying timestamps
        with collector._timestamps_lock:
            old_time = time.time() - 7200  # well past the fixed 60s window
            collector._snapshot_timestamps.clear()
            collector._snapshot_timestamps.append(old_time)
            collector._snapshot_timestamps.append(old_time)

        # Now should be allowed (old entries expired)
        self.assertTrue(collector._try_reserve_rate_limit_slot())


class TestGenerateErrorHash(TestCase):
    """Test the _generate_error_hash method."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_with_exception(self):
        """Test hash with exception type and message."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        exc = ValueError("test error")
        result = collector._generate_error_hash("/api/users", exc)

        expected_input = "route:/api/users|exc:ValueError:test error"
        expected = hashlib.md5(expected_input.encode("utf-8")).hexdigest()
        self.assertEqual(result, expected)

    def test_without_exception(self):
        """Test hash without exception (slow request)."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._generate_error_hash("/api/users", None)

        expected_input = "route:/api/users"
        expected = hashlib.md5(expected_input.encode("utf-8")).hexdigest()
        self.assertEqual(result, expected)

    def test_different_routes_different_hashes(self):
        """Test that different routes produce different hashes."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        hash1 = collector._generate_error_hash("/api/users", None)
        hash2 = collector._generate_error_hash("/api/items", None)
        self.assertNotEqual(hash1, hash2)


class TestCheckDeduplication(TestCase):
    """Test the _is_within_dedup_limit (pure check) + _record_error_hash (commit) pair."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_first_occurrence_allowed(self):
        """Test that first occurrence of an error hash is allowed."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=2,
        )

        self.assertTrue(collector._is_within_dedup_limit("error_hash_1"))

    def test_pure_check_does_not_consume_slot(self):
        """The pure check is idempotent — repeated calls without record() never dedup."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=1,
        )

        # Many checks without a commit stay allowed.
        for _ in range(5):
            self.assertTrue(collector._is_within_dedup_limit("error_hash_1"))
        # A single commit fills the max_same_error=1 slot.
        collector._record_error_hash("error_hash_1")
        self.assertFalse(collector._is_within_dedup_limit("error_hash_1"))

    def test_blocks_after_max_same_error(self):
        """Test that error is blocked after max_same_error occurrences."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=2,
        )

        # Emit twice (check-then-record for each).
        self.assertTrue(collector._is_within_dedup_limit("error_hash_1"))
        collector._record_error_hash("error_hash_1")
        self.assertTrue(collector._is_within_dedup_limit("error_hash_1"))
        collector._record_error_hash("error_hash_1")
        # Third would exceed max_same_error=2.
        self.assertFalse(collector._is_within_dedup_limit("error_hash_1"))

    def test_batch_deduplication(self):
        """Test batch-level deduplication (one per error type per batch)."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=10,  # High limit so period dedup doesn't interfere
        )

        # Add to current batch hashes
        with collector._error_hashes_lock:
            collector._current_batch_hashes.add("batch_error_1")

        # It's already in the batch, so process_potential_incident would skip it
        # We test this directly on the set
        self.assertIn("batch_error_1", collector._current_batch_hashes)

    def test_different_hashes_independent(self):
        """Test that different error hashes are tracked independently."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=1,
        )

        self.assertTrue(collector._is_within_dedup_limit("hash_a"))
        collector._record_error_hash("hash_a")
        self.assertTrue(collector._is_within_dedup_limit("hash_b"))
        collector._record_error_hash("hash_b")
        # hash_a is at limit
        self.assertFalse(collector._is_within_dedup_limit("hash_a"))
        # hash_b is at limit
        self.assertFalse(collector._is_within_dedup_limit("hash_b"))
        # New hash is fine
        self.assertTrue(collector._is_within_dedup_limit("hash_c"))


class TestSetAndGetLatencyThreshold(TestCase):
    """Test latency threshold setting and retrieval."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_set_exact_operation(self):
        """Test setting latency threshold by exact operation."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=500,
            max_per_period=100,
        )

        collector.set_latency_threshold("GET /api/users", 200.0)

        result = collector.get_latency_threshold(operation="GET /api/users")
        self.assertEqual(result, 200.0)

    def test_set_by_route_method_combo(self):
        """Test setting latency threshold by route/method combination."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=500,
            max_per_period=100,
        )

        op = collector.set_latency_threshold_by_route("/api/users", "GET", 100.0)
        self.assertIsNotNone(op)
        self.assertEqual(op, "GET /api/users")

        result = collector.get_latency_threshold(operation=op)
        self.assertEqual(result, 100.0)

    def test_pattern_matching(self):
        """Test pattern-based threshold matching."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=500,
            max_per_period=100,
        )

        collector.set_latency_threshold_patterns(
            [
                ("* /server_request", 50.0),
                ("GET /api/*", 100.0),
            ]
        )

        # Matches first pattern
        result = collector.get_latency_threshold(route="/server_request", method="GET")
        self.assertEqual(result, 50.0)

        # Matches second pattern
        result = collector.get_latency_threshold(route="/api/users", method="GET")
        self.assertEqual(result, 100.0)

    def test_fallback_to_default(self):
        """Test that unknown endpoints fall back to default threshold."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=500,
            max_per_period=100,
        )

        result = collector.get_latency_threshold(operation="nonexistent")
        self.assertEqual(result, 500.0)

    def test_get_all_latency_thresholds(self):
        """Test retrieving all configured exact-match thresholds."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=500,
            max_per_period=100,
        )

        collector.set_latency_threshold("GET /api/users", 100.0)
        collector.set_latency_threshold("POST /api/orders", 200.0)

        result = collector.get_all_latency_thresholds()
        self.assertEqual(result, {"GET /api/users": 100.0, "POST /api/orders": 200.0})

    def test_get_all_latency_threshold_patterns(self):
        """Test retrieving all configured patterns."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=500,
            max_per_period=100,
        )

        collector.set_latency_threshold_patterns(
            [
                ("* /health", 1000.0),
                ("POST /api/*", 200.0),
            ]
        )

        result = collector.get_all_latency_threshold_patterns()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ("* /health", 1000.0))
        self.assertEqual(result[1], ("POST /api/*", 200.0))


class TestProcessPotentialIncident(TestCase):
    """Test the process_potential_incident method."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_creates_snapshot_for_exception(self, mock_collect):
        """Test that process_potential_incident creates a snapshot for exceptions."""
        mock_snapshot = MagicMock()
        mock_collect.return_value = mock_snapshot

        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        exc = ValueError("test error")
        collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=exc,
            request_data={"headers": {}, "args": {}},
        )

        mock_collect.assert_called_once()
        self.assertEqual(len(collector._pending_snapshots), 1)

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_failed_collection_rolls_back_reservation(self, mock_collect):
        """If collection raises, the dedup/batch/rate-limit slots are released so a later
        identical error can still produce a snapshot."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=5,
        )

        # First attempt: collection blows up → reservation must be rolled back.
        mock_collect.side_effect = RuntimeError("boom")
        result = collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("same error"),
            request_data={"headers": {}, "args": {}},
        )
        self.assertIsNone(result)
        # Slots released: no lingering batch hash, dedup entry, or rate-limit timestamp.
        self.assertEqual(collector._current_batch_hashes, set())
        self.assertEqual(collector._error_hashes, {})
        self.assertEqual(len(collector._snapshot_timestamps), 0)

        # Second attempt with the SAME error now succeeds (was not suppressed by the
        # failed first attempt's stale reservation).
        mock_collect.side_effect = None
        mock_collect.return_value = MagicMock()
        result = collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("same error"),
            request_data={"headers": {}, "args": {}},
        )
        self.assertIsNotNone(result)
        self.assertEqual(len(collector._pending_snapshots), 1)

    def test_no_snapshot_for_normal_request(self):
        """Test that no snapshot is created for normal requests."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=200,
            duration_ms=50.0,
            exception=None,
            request_data={"headers": {}, "args": {}},
        )

        self.assertEqual(len(collector._pending_snapshots), 0)

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_rate_limited_skip(self, mock_collect):
        """Test that rate-limited incidents are skipped."""
        mock_snapshot = MagicMock()
        mock_collect.return_value = mock_snapshot

        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=1,  # Only 1 allowed
        )

        # First should succeed
        collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("error1"),
            request_data={"headers": {}, "args": {}},
        )

        # Second should be rate limited
        collector.process_potential_incident(
            route="/api/items",
            method="POST",
            status_code=500,
            duration_ms=60.0,
            exception=RuntimeError("error2"),
            request_data={"headers": {}, "args": {}},
        )

        # Only 1 snapshot should have been created
        self.assertEqual(mock_collect.call_count, 1)

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_dedup_blocked_requests_dont_consume_rate_limit(self, mock_collect):
        """Dedup-blocked requests should NOT consume rate limit slots.

        Regression test: Previously _check_rate_limit() ran before dedup checks,
        so every error attempt consumed a rate limit slot even if dedup rejected it.
        Under high error rates, this exhausted the rate limit with phantom slots.
        """
        mock_snapshot = MagicMock()
        mock_collect.return_value = mock_snapshot

        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=2,
        )

        exc = ValueError("same error")
        # Trigger same error 50 times — only 2 should pass dedup (max_same_error=2)
        for _ in range(50):
            collector.process_potential_incident(
                route="/api/users",
                method="GET",
                status_code=500,
                duration_ms=50.0,
                exception=exc,
                request_data={"headers": {}, "args": {}},
            )
            # Clear batch hashes between calls to allow period dedup to handle it
            # (batch dedup would block all after the first within same collect cycle)
            with collector._error_hashes_lock:
                collector._current_batch_hashes.clear()

        # Only 2 snapshots should have been created (max_same_error=2)
        self.assertEqual(mock_collect.call_count, 2)
        # Rate limit should show only 2 consumed, not 50
        self.assertEqual(len(collector._snapshot_timestamps), 2)

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_rate_limit_reserves_capacity_for_different_errors(self, mock_collect):
        """After dedup blocks one error type, rate limit capacity remains for other errors."""
        mock_snapshot = MagicMock()
        mock_collect.return_value = mock_snapshot

        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=5,
            max_same_error=1,
        )

        # Send same error 20 times — only 1 passes dedup
        for _ in range(20):
            collector.process_potential_incident(
                route="/api/users",
                method="GET",
                status_code=500,
                duration_ms=50.0,
                exception=ValueError("error A"),
                request_data={"headers": {}, "args": {}},
            )
            with collector._error_hashes_lock:
                collector._current_batch_hashes.clear()

        # 1 snapshot created, 1 rate limit slot used
        self.assertEqual(mock_collect.call_count, 1)
        self.assertEqual(len(collector._snapshot_timestamps), 1)

        # Now send 4 different errors — all should succeed (4 remaining slots)
        for i in range(4):
            collector.process_potential_incident(
                route=f"/api/endpoint_{i}",
                method="GET",
                status_code=500,
                duration_ms=50.0,
                exception=ValueError(f"error {i}"),
                request_data={"headers": {}, "args": {}},
            )
            with collector._error_hashes_lock:
                collector._current_batch_hashes.clear()

        # Total: 1 + 4 = 5 snapshots, 5 rate limit slots
        self.assertEqual(mock_collect.call_count, 5)
        self.assertEqual(len(collector._snapshot_timestamps), 5)

        # 6th different error should be rate-limited
        result = collector.process_potential_incident(
            route="/api/overflow",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("error overflow"),
            request_data={"headers": {}, "args": {}},
        )
        self.assertIsNone(result)
        self.assertEqual(mock_collect.call_count, 5)  # No additional snapshot


class TestInBatchSampledUpgrade(TestCase):
    """Point #2: within a collection cycle, a sampled occurrence upgrades a pending UNSAMPLED
    snapshot's correlation (whole-snapshot swap, snapshot_id preserved)."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self):
        # max_same_error high so period dedup never interferes; batch dedup is what we exercise.
        return IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=100,
        )

    @staticmethod
    def _args(trace_id=None, span_id=None):
        rd = {"headers": {}, "args": {}}
        if trace_id is not None:
            rd["trace_id"] = trace_id
            rd["span_id"] = span_id
        return dict(
            route="/boom",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("same error"),
            request_data=rd,
        )

    def test_sampled_occurrence_upgrades_unsampled_pending(self):
        collector = self._make_collector()
        # First occurrence: unsampled (no trace_id) → pending snapshot has no correlation.
        collector.process_potential_incident(**self._args())
        self.assertEqual(len(collector._pending_snapshots), 1)
        original_id = collector._pending_snapshots[0].snapshot_id
        self.assertIsNone(collector._pending_snapshots[0].telemetry_correlation.trace_id)

        # Second occurrence same error, sampled → upgrades the pending snapshot in place.
        collector.process_potential_incident(**self._args(trace_id=0xAA, span_id=0xBB))
        self.assertEqual(len(collector._pending_snapshots), 1)  # still ONE snapshot (batch dedup)
        upgraded = collector._pending_snapshots[0]
        self.assertEqual(upgraded.snapshot_id, original_id)  # identity preserved for the exemplar
        # Exact values (not just non-None): proves request_data["trace_id"]/["span_id"] flow through
        # to the correlation as 0x-prefixed, zero-padded hex — the format the backend joins on.
        self.assertEqual(upgraded.telemetry_correlation.trace_id, "0x" + "0" * 30 + "aa")
        self.assertEqual(upgraded.telemetry_correlation.span_id, "0x" + "0" * 14 + "bb")

    def test_unsampled_occurrence_does_not_downgrade_sampled_pending(self):
        collector = self._make_collector()
        # First occurrence sampled → pending snapshot correlated.
        collector.process_potential_incident(**self._args(trace_id=0xAA, span_id=0xBB))
        correlated_tid = collector._pending_snapshots[0].telemetry_correlation.trace_id
        self.assertIsNotNone(correlated_tid)

        # Second occurrence unsampled → must NOT clear the existing correlation.
        collector.process_potential_incident(**self._args())
        self.assertEqual(len(collector._pending_snapshots), 1)
        self.assertEqual(collector._pending_snapshots[0].telemetry_correlation.trace_id, correlated_tid)

    def test_first_sampled_occurrence_wins(self):
        collector = self._make_collector()
        collector.process_potential_incident(**self._args(trace_id=0xAA, span_id=0xBB))
        first_tid = collector._pending_snapshots[0].telemetry_correlation.trace_id

        # A later sampled occurrence with different ids must not churn the already-correlated one.
        collector.process_potential_incident(**self._args(trace_id=0xCC, span_id=0xDD))
        self.assertEqual(len(collector._pending_snapshots), 1)
        self.assertEqual(collector._pending_snapshots[0].telemetry_correlation.trace_id, first_tid)

    def test_collect_clears_upgrade_index(self):
        collector = self._make_collector()
        collector.process_potential_incident(**self._args())
        self.assertTrue(collector._pending_by_hash)
        collector.collect()
        # After flush the per-hash index is cleared (upgrade window is one cycle).
        self.assertEqual(collector._pending_by_hash, {})

    def test_unsampled_occurrence_with_header_does_not_upgrade(self):
        # Correlation comes solely from the SAMPLED-gated request_data["trace_id"] (fix #1); inbound
        # headers are never consulted. A RECORD_ONLY occurrence carries no trace_id but may still
        # arrive with a traceparent header — that header must NOT resurrect a correlation, either at
        # the upgrade gate or when the snapshot body is built. Both stay uncorrelated.
        collector = self._make_collector()
        collector.process_potential_incident(**self._args())
        self.assertIsNone(collector._pending_snapshots[0].telemetry_correlation.trace_id)

        # Second occurrence: still unsampled (no request_data["trace_id"]) but with a traceparent
        # header a header-based extractor would have honored. Must NOT upgrade.
        unsampled_with_header = self._args()
        unsampled_with_header["request_data"]["headers"] = {
            "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00"
        }
        collector.process_potential_incident(**unsampled_with_header)
        self.assertEqual(len(collector._pending_snapshots), 1)
        self.assertIsNone(collector._pending_snapshots[0].telemetry_correlation.trace_id)

    def _seed_investigation(self, collector, func_name):
        # Seed the per-request investigation data that _collect_incident_snapshot consumes to build
        # the snapshot body (call_path). Read-and-cleared by get_investigation_data(), so it must be
        # set immediately before each process_potential_incident call that should observe it.
        collector._monitor_state._investigation_data.set(
            {
                "call_path": [{"function_name": func_name, "caller_function_name": "root", "duration_ns": 100}],
                "exception": None,
            }
        )

    def test_upgrade_swaps_whole_snapshot_body_coherent_with_trace(self):
        # The upgrade is a WHOLE-snapshot swap, not a correlation-only patch: the replacement body
        # (call_path/stack trace) must come from the sampled occurrence whose trace it now links to,
        # so the snapshot stays coherent with that trace. A correlation-only patch would leave the
        # first (unsampled) occurrence's body attached to the second occurrence's trace.
        collector = self._make_collector()

        # Occurrence 1 — unsampled, body captured from "func_unsampled".
        self._seed_investigation(collector, "func_unsampled")
        collector.process_potential_incident(**self._args())
        original = collector._pending_snapshots[0]
        original_names = [e.function_name for exc in original.exception_info for e in exc.call_path]
        self.assertIn("func_unsampled", original_names)
        self.assertIsNone(original.telemetry_correlation.trace_id)

        # Occurrence 2 — sampled, body captured from "func_sampled" → upgrades in place.
        self._seed_investigation(collector, "func_sampled")
        collector.process_potential_incident(**self._args(trace_id=0xAA, span_id=0xBB))

        self.assertEqual(len(collector._pending_snapshots), 1)  # still one (batch dedup)
        upgraded = collector._pending_snapshots[0]
        self.assertEqual(upgraded.snapshot_id, original.snapshot_id)  # identity preserved
        self.assertIsNotNone(upgraded.telemetry_correlation.trace_id)  # now correlated
        # Body reflects the SAMPLED occurrence (coherent with the linked trace), not the stale first.
        upgraded_names = [e.function_name for exc in upgraded.exception_info for e in exc.call_path]
        self.assertIn("func_sampled", upgraded_names)
        self.assertNotIn("func_unsampled", upgraded_names)


class TestDedupKeysOnRecoveredErrorIdentity(TestCase):
    """Regression: the span processor passes exception=None and defers exception detail to the
    collector. The dedup hash must recover the error type+message from investigation data so two
    DISTINCT errors on the same route do NOT collapse to one snapshot under max_same_error=1."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self):
        emitter = MagicMock()
        collector = IncidentSnapshotCollector(
            flush_interval_ms=600000,
            duration_threshold_ms=5000,
            max_per_period=100,
            max_same_error=1,
            otlp_emitter=emitter,
        )
        return collector, emitter

    @staticmethod
    def _seed_exception(collector, name, message):
        # Mirror the processor seeding the span's exception event into per-request investigation
        # data for an uninstrumented 5xx (exception=None reaches the collector). _recover_error_identity
        # PEEKs this to key the dedup hash on type+message rather than route-only.
        collector._monitor_state._investigation_data.set(
            {
                "call_path": [],
                "exception": {"name": name, "message": message, "function_name": "app.handler"},
            }
        )

    @staticmethod
    def _rd():
        return {"headers": {}, "args": {}}

    def test_two_distinct_error_types_same_route_both_emit(self):
        collector, emitter = self._make_collector()
        self._seed_exception(collector, "TypeError", "x")
        self.assertIsNotNone(collector.process_potential_incident("/orders", "POST", 500, 10.0, None, self._rd()))
        collector.collect()  # clears the per-batch set so the next call reaches period dedup
        self._seed_exception(collector, "RangeError", "y")
        self.assertIsNotNone(collector.process_potential_incident("/orders", "POST", 500, 10.0, None, self._rd()))
        collector.collect()
        self.assertEqual(emitter.emit_incident_snapshot.call_count, 2)

    def test_two_distinct_messages_same_type_same_route_both_emit(self):
        collector, emitter = self._make_collector()
        self._seed_exception(collector, "DbError", "timeout on shard A")
        self.assertIsNotNone(collector.process_potential_incident("/orders", "POST", 500, 10.0, None, self._rd()))
        collector.collect()
        self._seed_exception(collector, "DbError", "timeout on shard B")
        self.assertIsNotNone(collector.process_potential_incident("/orders", "POST", 500, 10.0, None, self._rd()))
        collector.collect()
        self.assertEqual(emitter.emit_incident_snapshot.call_count, 2)

    def test_same_error_type_and_message_same_route_deduplicates(self):
        collector, emitter = self._make_collector()
        self._seed_exception(collector, "DbError", "timeout")
        self.assertIsNotNone(collector.process_potential_incident("/orders", "POST", 500, 10.0, None, self._rd()))
        collector.collect()
        self._seed_exception(collector, "DbError", "timeout")
        # Same recovered identity → same hash → period-deduplicated (max_same_error=1).
        self.assertIsNone(collector.process_potential_incident("/orders", "POST", 500, 10.0, None, self._rd()))
        collector.collect()
        self.assertEqual(emitter.emit_incident_snapshot.call_count, 1)

    def test_latency_incident_no_exception_keys_route_only(self):
        collector, emitter = self._make_collector()
        # No investigation exception; two slow 2xx on the same route dedup together (route-only).
        self.assertIsNotNone(collector.process_potential_incident("/slow", "GET", 200, 6000.0, None, self._rd()))
        collector.collect()
        self.assertIsNone(collector.process_potential_incident("/slow", "GET", 200, 6000.0, None, self._rd()))
        collector.collect()
        self.assertEqual(emitter.emit_incident_snapshot.call_count, 1)


class TestRateLimitedRequestDoesNotPoisonDedup(TestCase):
    """Pure-check/commit ordering: a request rejected by the rate limit must NOT record a dedup
    occurrence, or a later retry of that same error (after the rate window frees) is wrongly dropped
    as a duplicate even though it never produced a snapshot."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_rate_limited_error_does_not_consume_dedup_slot(self, mock_collect):
        mock_collect.return_value = MagicMock()
        # max_per_period=1, max_same_error=1. Two DISTINCT errors: the first emits (consuming the
        # single rate slot); the second is rate-limited.
        collector = IncidentSnapshotCollector(
            flush_interval_ms=600000,
            duration_threshold_ms=5000,
            max_per_period=1,
            max_same_error=1,
        )
        err_a = ValueError("A")
        err_b = ValueError("B")
        rd = {"headers": {}, "args": {}}
        self.assertIsNotNone(collector.process_potential_incident("/x", "GET", 500, 10.0, err_a, rd))
        # Second distinct error: passes dedup but the rate window (1) is full → rejected.
        self.assertIsNone(collector.process_potential_incident("/x", "GET", 500, 10.0, err_b, rd))
        # err_b left NO dedup timestamp and NO batch entry. Free the rate window and retry it in a
        # fresh cycle: it must now emit (it was never actually recorded).
        collector._snapshot_timestamps.clear()
        collector.collect()
        self.assertIsNotNone(collector.process_potential_incident("/x", "GET", 500, 10.0, err_b, rd))


class TestEndpointExemplarTracksUpgrade(TestCase):
    """Point #2: the exemplar dict returned to the endpoint collector is the SAME object indexed by
    error hash, so a later whole-snapshot upgrade must mutate it in place to stay coherent."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self):
        return IncidentSnapshotCollector(
            flush_interval_ms=600000,
            duration_threshold_ms=5000,
            max_per_period=100,
            max_same_error=100,
            otlp_emitter=MagicMock(),
        )

    @patch("amazon.opentelemetry.distro.serviceevents.collectors.incident_snapshot_collector.time")
    def test_returned_exemplar_resyncs_timestamp_and_id_on_upgrade(self, mock_time):
        # Drive time so the two occurrences get DIFFERENT timestamps. This is what makes the in-place
        # exemplar["timestamp"] sync observable: if the mutation were dropped, the exemplar would keep
        # the first occurrence's timestamp while the emitted snapshot carries the later one.
        mock_time.time.return_value = 1_000.0
        collector = self._make_collector()
        err = ValueError("boom")
        # First (unsampled) occurrence. This is the exemplar the endpoint collector records.
        exemplar = collector.process_potential_incident("/api/x", "POST", 500, 50.0, err, {"headers": {}, "args": {}})
        self.assertIsNotNone(exemplar)
        first_timestamp = exemplar["timestamp"]
        # Advance 5s, then a SAMPLED occurrence of the same error upgrades the pending snapshot
        # wholesale. The already-returned exemplar dict (held by reference by the endpoint collector)
        # must track the swap.
        mock_time.time.return_value = 1_005.0
        collector.process_potential_incident(
            "/api/x", "POST", 500, 50.0, err, {"headers": {}, "args": {}, "trace_id": 0xAA, "span_id": 0xBB}
        )
        self.assertEqual(len(collector._pending_snapshots), 1)  # still ONE snapshot (batch dedup)
        upgraded = collector._pending_snapshots[0]
        # The emitter serializes snapshot_id/trigger_type/timestamp for the exemplar (severity is NOT
        # on the wire), so those must stay coherent with the emitted (upgraded) snapshot.
        self.assertEqual(exemplar["snapshot_id"], upgraded.snapshot_id)
        self.assertEqual(exemplar["trigger_type"], upgraded.trigger_type)
        # The exemplar timestamp moved to the upgraded snapshot's (later) timestamp, not the first.
        self.assertGreater(upgraded.timestamp, first_timestamp)
        self.assertEqual(exemplar["timestamp"], upgraded.timestamp)
        # The upgraded snapshot now carries the sampled trace correlation.
        self.assertIsNotNone(upgraded.telemetry_correlation.trace_id)

    def test_upgrade_preserves_first_occurrence_operation(self):
        # The dedup hash keys on route (not method), so GET /api/x and POST /api/x with the same error
        # share a hash. The exemplar is filed under the FIRST occurrence's operation, so the swapped
        # snapshot must keep the first occurrence's operation, not adopt the upgrader's method.
        collector = self._make_collector()
        err = ValueError("boom")
        exemplar = collector.process_potential_incident("/api/x", "GET", 500, 50.0, err, {"headers": {}, "args": {}})
        self.assertIsNotNone(exemplar)
        self.assertEqual(exemplar["operation"], "GET /api/x")
        # Sampled POST occurrence of the same error upgrades the pending (unsampled) GET snapshot.
        collector.process_potential_incident(
            "/api/x", "POST", 500, 50.0, err, {"headers": {}, "args": {}, "trace_id": 0xAA, "span_id": 0xBB}
        )
        self.assertEqual(len(collector._pending_snapshots), 1)
        upgraded = collector._pending_snapshots[0]
        # operation stays 'GET /api/x' (the exemplar's filed operation), NOT 'POST /api/x'.
        self.assertEqual(upgraded.operation, "GET /api/x")
        self.assertEqual(upgraded.snapshot_id, exemplar["snapshot_id"])


class TestBuildCallPath(TestCase):
    """Test the _build_call_path method."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    @patch("amazon.opentelemetry.distro.serviceevents.collectors.incident_snapshot_collector.get_function_info")
    def test_from_dict_entries(self, mock_get_func_info):
        """Test building call path from dict entries, including is_async lookup."""
        # func_a is sync, func_b is async
        mock_get_func_info.side_effect = lambda fid: ({"is_async": True} if fid == "func_b" else {"is_async": False})

        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        inv_data = {
            "call_path": [
                {"function_name": "func_a", "caller_function_name": None, "duration_ns": 1000},
                {"function_name": "func_b", "caller_function_name": "func_a", "duration_ns": 500},
            ]
        }

        result = collector._build_call_path(inv_data, error_function_name="func_b")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].function_name, "func_a")
        self.assertFalse(result[0].error)
        self.assertFalse(result[0].is_async)
        self.assertEqual(result[1].function_name, "func_b")
        self.assertTrue(result[1].error)
        self.assertTrue(result[1].is_async)

    @patch("amazon.opentelemetry.distro.serviceevents.collectors.incident_snapshot_collector.get_function_info")
    def test_from_tuple_entries(self, mock_get_func_info):
        """Test building call path from tuple entries (caller, callee)."""
        # func_a is async, func_b is unknown (returns None)
        mock_get_func_info.side_effect = lambda fid: ({"is_async": True} if fid == "func_a" else None)

        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        inv_data = {
            "call_path": [
                (None, "func_a"),
                ("func_a", "func_b"),
            ]
        }

        result = collector._build_call_path(inv_data, error_function_name=None)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].function_name, "func_a")
        self.assertEqual(result[0].caller_function_name, None)
        self.assertFalse(result[0].error)
        self.assertTrue(result[0].is_async)
        self.assertEqual(result[1].function_name, "func_b")
        self.assertEqual(result[1].caller_function_name, "func_a")
        self.assertFalse(result[1].is_async)  # Unknown function defaults to False

    def test_empty_inv_data(self):
        """Test building call path from None investigation data."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._build_call_path(None)
        self.assertEqual(result, [])

    def test_no_call_path_key(self):
        """Test building call path when call_path key is missing."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        result = collector._build_call_path({"other_key": "value"})
        self.assertEqual(result, [])


class TestCollect(TestCase):
    """Test the collect method."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_collect_clears_batch_hashes(self):
        """Test that collect clears batch-level hashes."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        # Add some batch hashes
        with collector._error_hashes_lock:
            collector._current_batch_hashes.add("hash1")
            collector._current_batch_hashes.add("hash2")

        collector.collect()

        self.assertEqual(len(collector._current_batch_hashes), 0)

    def test_collect_exports_pending_snapshots(self):
        """Test that collect exports pending snapshots and clears them."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        mock_snapshot = MagicMock()
        mock_snapshot.to_dict.return_value = {"snapshot_id": "test"}

        with collector._pending_lock:
            collector._pending_snapshots.append(mock_snapshot)

        with patch("builtins.print"):
            collector.collect()

        self.assertEqual(len(collector._pending_snapshots), 0)


class TestIncidentSnapshotToDict(TestCase):
    """Test IncidentSnapshot.to_dict() sparse serialization of is_async."""

    def _make_snapshot(self, call_path_entries):
        """Helper to create an IncidentSnapshot with given call_path entries."""
        return IncidentSnapshot(
            snapshot_id="snap_test",
            timestamp=1000,
            severity="high",
            trigger_type="exception",
            service="test-service",
            environment="test",
            instance_id="host-1",
            operation="POST /api/test",
            sdk_version="0.14.2",
            pid=1234,
            duration_ms=100.0,
            exception_info=[
                ExceptionInfo(
                    exception_type="ValueError",
                    exception_message="test",
                    stack_trace="traceback...",
                    call_path=call_path_entries,
                )
            ],
            request_context=RequestContext(type="http", timestamp=1000, status_code=500),
            telemetry_correlation=TelemetryCorrelation(),
        )

    def test_is_async_stripped_when_false(self):
        """Test that is_async is omitted from call_path entries when False."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=1000, is_async=False),
            ]
        )

        result = snapshot.to_dict()
        entry = result["exception_info"][0]["call_path"][0]
        self.assertNotIn("is_async", entry)

    def test_is_async_present_when_true(self):
        """Test that is_async is present in call_path entries when True."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=1000, is_async=True),
            ]
        )

        result = snapshot.to_dict()
        entry = result["exception_info"][0]["call_path"][0]
        self.assertIn("is_async", entry)
        self.assertTrue(entry["is_async"])

    def test_mixed_async_sync_entries(self):
        """Test sparse pattern with mixed async and sync call_path entries."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=1000, is_async=False),
                CallPathEntry(function_name="f2", caller_function_name="f1", duration_ns=500, is_async=True),
                CallPathEntry(function_name="f3", caller_function_name="f2", duration_ns=200, is_async=False),
            ]
        )

        result = snapshot.to_dict()
        entries = result["exception_info"][0]["call_path"]

        self.assertNotIn("is_async", entries[0])  # sync — stripped
        self.assertTrue(entries[1]["is_async"])  # async — present
        self.assertNotIn("is_async", entries[2])  # sync — stripped

    def test_is_partial_false_when_complete(self):
        """Test that is_partial=false is always present in JSON (complete data)."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=1000),
            ]
        )
        result = snapshot.to_dict()
        self.assertIn("is_partial", result)
        self.assertFalse(result["is_partial"])

    def test_is_partial_true_strips_only_zero_duration_ns(self):
        """is_partial=true strips only the misleading zero durations from unsampled frames."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=0),
                CallPathEntry(function_name="f2", caller_function_name="f1", duration_ns=0),
            ]
        )
        snapshot.is_partial = True
        result = snapshot.to_dict()
        self.assertIn("is_partial", result)
        self.assertTrue(result["is_partial"])
        # All-zero durations are stripped (nothing meaningful to keep).
        for entry in result["exception_info"][0]["call_path"]:
            self.assertNotIn("duration_ns", entry)

    def test_is_partial_true_preserves_real_durations(self):
        """A partial snapshot keeps genuine per-frame timings, dropping only the zeros."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=5000),
                CallPathEntry(function_name="f2", caller_function_name="f1", duration_ns=0),
                CallPathEntry(function_name="f3", caller_function_name="f2", duration_ns=3000),
            ]
        )
        snapshot.is_partial = True
        result = snapshot.to_dict()
        entries = result["exception_info"][0]["call_path"]
        # Sampled frames keep their real durations...
        self.assertEqual(entries[0]["duration_ns"], 5000)
        self.assertEqual(entries[2]["duration_ns"], 3000)
        # ...and only the unsampled zero is dropped.
        self.assertNotIn("duration_ns", entries[1])

    def test_non_partial_keeps_duration_ns(self):
        """Test that is_partial=false retains duration_ns in call_path entries."""
        snapshot = self._make_snapshot(
            [
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=5000),
                CallPathEntry(function_name="f2", caller_function_name="f1", duration_ns=3000),
            ]
        )
        result = snapshot.to_dict()
        self.assertFalse(result["is_partial"])
        # duration_ns should be present
        entries = result["exception_info"][0]["call_path"]
        self.assertEqual(entries[0]["duration_ns"], 5000)
        self.assertEqual(entries[1]["duration_ns"], 3000)


class TestIncidentCollectorForkReset(TestCase):
    """Fork-safety: child must not inherit pending snapshots or stale dedup/rate-limit state."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _build(self):
        return IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

    def test_reset_for_fork_clears_pending_and_dedup_state(self):
        """Pending snapshots and dedup/rate-limit bookkeeping are cleared in the child."""
        collector = self._build()
        # Simulate parent-accumulated state.
        collector._pending_snapshots.append(MagicMock())
        collector._error_hashes["abc"] = [time.time()]
        collector._current_batch_hashes.add("abc")
        collector._snapshot_timestamps.append(time.time())

        collector._reset_for_fork()

        self.assertEqual(collector._pending_snapshots, [])
        self.assertEqual(collector._error_hashes, {})
        self.assertEqual(collector._current_batch_hashes, set())
        self.assertEqual(len(collector._snapshot_timestamps), 0)

    def test_reset_for_fork_recreates_locks_and_refreshes_pid(self):
        """All collector locks are fresh (parent threads may have held them) post-fork."""
        collector = self._build()
        old_locks = (
            collector._pending_lock,
            collector._timestamps_lock,
            collector._error_hashes_lock,
            collector._latency_thresholds_lock,
            collector._latency_patterns_lock,
        )

        collector._reset_for_fork()

        new_locks = (
            collector._pending_lock,
            collector._timestamps_lock,
            collector._error_hashes_lock,
            collector._latency_thresholds_lock,
            collector._latency_patterns_lock,
        )
        for old, new in zip(old_locks, new_locks):
            self.assertIsNot(new, old)
            self.assertTrue(new.acquire(blocking=False))
            new.release()

    def test_reset_for_fork_preserves_latency_thresholds(self):
        """Latency thresholds are config, not per-request state — preserved across fork."""
        collector = self._build()
        collector.set_latency_threshold("GET /api/slow", 250.0)
        collector.set_latency_threshold_patterns([("GET /api/*", 500.0)])

        collector._reset_for_fork()

        self.assertEqual(collector.get_latency_threshold(operation="GET /api/slow"), 250.0)
        self.assertEqual(collector.get_latency_threshold(route="/api/other", method="GET"), 500.0)


class TestConstructorHostId(TestCase):
    """Constructor enhances instance_id from resource attributes."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_host_id_overrides_instance_id(self):
        """host.id from resource attributes is preferred as the instance_id."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            resource_attributes=ResourceAttributes(host_id="i-0abc123def456"),
        )
        self.assertEqual(collector.instance_id, "i-0abc123def456")


class TestUpdateIncidentConfig(TestCase):
    """Test the update_incident_config live-setter."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self, max_per_period=5):
        return IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=max_per_period,
        )

    def test_updates_flags_without_recreating_deque(self):
        """Updating only flags leaves the timestamps deque object unchanged."""
        collector = self._make_collector(max_per_period=5)
        original_deque = collector._snapshot_timestamps

        collector.update_incident_config(max_per_period=5, max_same_error=3)

        self.assertEqual(collector._max_same_error, 3)
        # max_per_period unchanged -> same deque instance
        self.assertIs(collector._snapshot_timestamps, original_deque)

    def test_changing_max_per_period_recreates_deque_preserving_entries(self):
        """Changing max_per_period rebuilds the deque with the new maxlen, keeping entries."""
        collector = self._make_collector(max_per_period=2)
        with collector._timestamps_lock:
            collector._snapshot_timestamps.append(111.0)
            collector._snapshot_timestamps.append(222.0)
        original_deque = collector._snapshot_timestamps

        collector.update_incident_config(max_per_period=10, max_same_error=1)

        self.assertEqual(collector.max_per_period, 10)
        self.assertIsNot(collector._snapshot_timestamps, original_deque)
        self.assertEqual(collector._snapshot_timestamps.maxlen, 20)
        self.assertEqual(list(collector._snapshot_timestamps), [111.0, 222.0])


class TestProcessPotentialIncidentBranches(TestCase):
    """Branch coverage for process_potential_incident dedup/stack-trace paths."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self, **kwargs):
        return IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            **kwargs,
        )

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_fallback_stack_trace_when_no_active_traceback(self, mock_collect):
        """When sys.exc_info has no traceback, the stack trace is formatted from the exception object."""
        mock_collect.return_value = MagicMock()
        collector = self._make_collector()

        # An exception built outside an active handler has no exc_tb via sys.exc_info().
        exc = ValueError("orphan error")
        result = collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=exc,
            request_data={"headers": {}, "args": {}},
        )

        self.assertIsNotNone(result)
        mock_collect.assert_called_once()
        # The fallback path passes a captured_stack_trace string built from the exception object.
        self.assertIsNotNone(mock_collect.call_args.kwargs["captured_stack_trace"])

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_active_traceback_is_captured(self, mock_collect):
        """Inside an active handler, the live traceback is formatted via sys.exc_info()."""
        mock_collect.return_value = MagicMock()
        collector = self._make_collector()

        try:
            raise ValueError("live error")
        except ValueError as exc:
            result = collector.process_potential_incident(
                route="/api/users",
                method="GET",
                status_code=500,
                duration_ms=50.0,
                exception=exc,
                request_data={"headers": {}, "args": {}},
            )

        self.assertIsNotNone(result)
        stack_trace = mock_collect.call_args.kwargs["captured_stack_trace"]
        self.assertIn("ValueError", stack_trace)
        self.assertIn("Traceback", stack_trace)

    @patch.object(IncidentSnapshotCollector, "_collect_incident_snapshot")
    def test_batch_deduplicated_returns_none(self, mock_collect):
        """An error hash already in the current batch is batch-deduplicated and skipped."""
        mock_collect.return_value = MagicMock()
        collector = self._make_collector()

        error_hash = collector._generate_error_hash("/api/users", ValueError("dup"))
        with collector._error_hashes_lock:
            collector._current_batch_hashes.add(error_hash)

        result = collector.process_potential_incident(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=50.0,
            exception=ValueError("dup"),
            request_data={"headers": {}, "args": {}},
        )

        self.assertIsNone(result)
        mock_collect.assert_not_called()


class TestRollbackReservationErrorPath(TestCase):
    """The rollback helper must never raise, even on internal errors."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_rollback_swallows_exceptions(self):
        """_rollback_reservation logs and swallows any exception raised internally."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )

        # Force an internal failure by making the batch-hash set raise on discard.
        broken = MagicMock()
        broken.discard.side_effect = RuntimeError("boom")
        collector._current_batch_hashes = broken

        # Should not raise.
        collector._rollback_reservation("some_hash")


class TestCheckDeduplicationCleanup(TestCase):
    """Cleanup of fully-expired hash entries during dedup."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_expired_hash_entry_is_deleted(self):
        """A hash whose timestamps are all older than the window is removed during cleanup."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            max_same_error=5,
        )

        # Seed a stale hash whose only timestamp predates the 60s window.
        with collector._error_hashes_lock:
            collector._error_hashes["stale_hash"] = [time.time() - 7200]

        # A different hash triggers the cleanup loop, which deletes the stale entry.
        self.assertTrue(collector._is_within_dedup_limit("fresh_hash"))
        self.assertNotIn("stale_hash", collector._error_hashes)


class TestCollectExports(TestCase):
    """Collect path that emits to the OTLP emitter."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_no_pending_snapshots_returns_early(self):
        """collect() returns early when there are no pending snapshots."""
        emitter = MagicMock()
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            otlp_emitter=emitter,
        )

        collector.collect()

        emitter.emit_incident_snapshot.assert_not_called()

    def test_no_emitter_drops_snapshots(self):
        """collect() with snapshots but no emitter clears pending without emitting."""
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            otlp_emitter=None,
        )
        with collector._pending_lock:
            collector._pending_snapshots.append(MagicMock())

        collector.collect()

        self.assertEqual(len(collector._pending_snapshots), 0)

    def test_emits_each_pending_snapshot(self):
        """collect() emits every pending snapshot via the OTLP emitter and clears them."""
        emitter = MagicMock()
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            otlp_emitter=emitter,
        )

        snap1 = MagicMock()
        snap1.to_dict.return_value = {"snapshot_id": "s1"}
        snap2 = MagicMock()
        snap2.to_dict.return_value = {"snapshot_id": "s2"}
        with collector._pending_lock:
            collector._pending_snapshots.extend([snap1, snap2])

        collector.collect()

        self.assertEqual(emitter.emit_incident_snapshot.call_count, 2)
        emitter.emit_incident_snapshot.assert_any_call({"snapshot_id": "s1"})
        emitter.emit_incident_snapshot.assert_any_call({"snapshot_id": "s2"})
        self.assertEqual(len(collector._pending_snapshots), 0)


class TestCollectIncidentSnapshot(TestCase):
    """Test the _collect_incident_snapshot assembly method."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self, **kwargs):
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
            service_name="svc",
            environment="prod",
            **kwargs,
        )
        # Deterministic investigation data — no real monitor singleton involved.
        collector._monitor_state = MagicMock()
        collector._monitor_state.get_investigation_data.return_value = None
        return collector

    def test_exception_snapshot_basic_fields(self):
        """Assembles a snapshot for an exception incident with core metadata populated."""
        collector = self._make_collector()

        snapshot = collector._collect_incident_snapshot(
            route="/api/users",
            method="GET",
            status_code=500,
            duration_ms=42.0,
            exception=ValueError("boom"),
            request_data={"headers": {}, "args": {}},
            trigger_type="exception",
            captured_stack_trace="TRACE",
        )

        self.assertTrue(snapshot.snapshot_id.startswith("snap_"))
        self.assertEqual(snapshot.severity, "critical")
        self.assertEqual(snapshot.trigger_type, "exception")
        self.assertEqual(snapshot.operation, "GET /api/users")
        self.assertEqual(snapshot.service, "svc")
        self.assertEqual(snapshot.environment, "prod")
        self.assertEqual(snapshot.duration_ms, 42.0)
        self.assertEqual(snapshot.exception_info[0].stack_trace, "TRACE")
        # Request-payload capture is permanently disabled: the four payload fields are always null
        # and custom_context is always empty (see SERVICE_EVENTS_OTLP_SIGNALS_SPEC.md §5).
        self.assertEqual(snapshot.request_context.custom_context, {})
        self.assertIsNone(snapshot.request_context.request_body)
        self.assertIsNone(snapshot.request_context.query_params)
        self.assertIsNone(snapshot.request_context.path_params)
        self.assertIsNone(snapshot.request_context.request_headers)

    def test_partial_flag_when_call_path_durations_zero(self):
        """is_partial is True when any call_path entry has a zero duration."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {
            "call_path": [(None, "func_a")],
        }

        with patch(
            "amazon.opentelemetry.distro.serviceevents.collectors." "incident_snapshot_collector.get_function_info",
            return_value={"is_async": False},
        ):
            snapshot = collector._collect_incident_snapshot(
                route="/slow",
                method="GET",
                status_code=200,
                duration_ms=999.0,
                exception=None,
                request_data={"headers": {}, "args": {}},
                trigger_type="latency",
            )

        self.assertTrue(snapshot.is_partial)
        self.assertEqual(snapshot.severity, "medium")

    def test_request_payload_fields_always_null(self):
        """Request-payload capture is permanently off: even when the request_data carries a body,
        query args, path params, and headers, none of them reach the emitted snapshot."""
        collector = self._make_collector()

        snapshot = collector._collect_incident_snapshot(
            route="/api/users",
            method="POST",
            status_code=500,
            duration_ms=10.0,
            exception=ValueError("x"),
            request_data={
                "headers": {"Content-Type": "application/json"},
                "args": {"user_id": "42"},
                "view_args": {"id": "7"},
                "cached_body": "raw-payload",
            },
            trigger_type="exception",
        )

        self.assertIsNone(snapshot.request_context.request_body)
        self.assertEqual(snapshot.request_context.custom_context, {})
        self.assertIsNone(snapshot.request_context.query_params)
        self.assertIsNone(snapshot.request_context.path_params)
        self.assertIsNone(snapshot.request_context.request_headers)


class TestCollectExceptionInfo(TestCase):
    """Test the _collect_exception_info branches."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def _make_collector(self):
        collector = IncidentSnapshotCollector(
            flush_interval_ms=10000,
            duration_threshold_ms=1000,
            max_per_period=100,
        )
        collector._monitor_state = MagicMock()
        return collector

    def test_no_exception_no_inv_data_returns_empty(self):
        """No explicit exception and no investigation data yields an empty list."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = None

        self.assertEqual(collector._collect_exception_info(None), [])

    def test_latency_returns_call_path_without_exception(self):
        """A latency incident with call_path but no captured exception returns a call-path-only entry."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {
            "call_path": [(None, "func_a")],
        }

        with patch(
            "amazon.opentelemetry.distro.serviceevents.collectors." "incident_snapshot_collector.get_function_info",
            return_value={"is_async": False},
        ):
            result = collector._collect_exception_info(None)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].exception_type, "")
        self.assertEqual(result[0].exception_message, "")
        self.assertEqual(len(result[0].call_path), 1)

    def test_exc_data_none_with_call_path(self):
        """inv_data has an 'exception' key set to None -> treated as latency call-path-only."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {
            "exception": None,
            "call_path": [(None, "func_a")],
        }

        with patch(
            "amazon.opentelemetry.distro.serviceevents.collectors." "incident_snapshot_collector.get_function_info",
            return_value=None,
        ):
            result = collector._collect_exception_info(None)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].exception_type, "")
        self.assertEqual(len(result[0].call_path), 1)

    def test_exc_data_none_without_call_path_returns_empty(self):
        """inv_data 'exception' is None and there is no call_path -> empty list."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {"exception": None}

        self.assertEqual(collector._collect_exception_info(None), [])

    def test_no_exception_no_call_path_returns_empty(self):
        """No explicit exception, inv_data present but lacking exception/call_path -> empty list."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {"other": "value"}

        self.assertEqual(collector._collect_exception_info(None), [])

    def test_monitor_captured_exception_with_traceback_info(self):
        """A monitor-captured exception with traceback_info formats a full stack trace."""
        collector = self._make_collector()
        try:
            raise ValueError("captured")
        except ValueError as exc:
            tb_info = (type(exc), exc, exc.__traceback__)
        collector._monitor_state.get_investigation_data.return_value = {
            "exception": {
                "function_name": "func_a",
                "name": "ValueError",
                "message": "captured",
                "traceback_info": tb_info,
            },
            "call_path": [{"function_name": "func_a", "caller_function_name": None, "duration_ns": 5}],
        }

        with patch(
            "amazon.opentelemetry.distro.serviceevents.collectors." "incident_snapshot_collector.get_function_info",
            return_value={"is_async": False},
        ):
            result = collector._collect_exception_info(None)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].exception_type, "ValueError")
        self.assertEqual(result[0].exception_message, "captured")
        self.assertIn("ValueError", result[0].stack_trace)
        self.assertTrue(result[0].call_path[0].error)

    def test_monitor_captured_exception_with_preformatted_traceback_string(self):
        """The monitor now stores traceback_info as a pre-formatted string; use it as-is."""
        collector = self._make_collector()
        preformatted = "Traceback (most recent call last):\n  ...\nValueError: captured\n"
        collector._monitor_state.get_investigation_data.return_value = {
            "exception": {
                "function_name": "func_a",
                "name": "ValueError",
                "message": "captured",
                "traceback_info": preformatted,
            },
            "call_path": [{"function_name": "func_a", "caller_function_name": None, "duration_ns": 5}],
        }

        with patch(
            "amazon.opentelemetry.distro.serviceevents.collectors." "incident_snapshot_collector.get_function_info",
            return_value={"is_async": False},
        ):
            result = collector._collect_exception_info(None)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].exception_type, "ValueError")
        # The pre-formatted string is used verbatim, not re-formatted.
        self.assertEqual(result[0].stack_trace, preformatted)
        self.assertTrue(result[0].call_path[0].error)

    def test_monitor_captured_exception_traceback_info_format_fails(self):
        """When formatting traceback_info raises, fall back to name+message string."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {
            "exception": {
                "function_name": "func_a",
                "name": "BadError",
                "message": "oops",
                # Invalid traceback_info triggers the except branch in format_exception.
                "traceback_info": ("not", "a", "valid", "tuple"),
            },
            "call_path": [],
        }

        result = collector._collect_exception_info(None)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].stack_trace, "BadError: oops")

    def test_monitor_captured_exception_with_traceback_strings(self):
        """A captured exception with a 'traceback' string list joins them as the stack trace."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {
            "exception": {
                "name": "KeyError",
                "message": "missing",
                "traceback": ["line1\n", "line2\n"],
            },
            "call_path": [],
        }

        result = collector._collect_exception_info(None)

        self.assertEqual(result[0].exception_type, "KeyError")
        self.assertEqual(result[0].stack_trace, "line1\nline2\n")

    def test_explicit_exception_uses_captured_stack_trace(self):
        """An explicit exception uses the pre-captured stack trace and marks the error frame."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = {
            "exception": {"function_name": "func_b"},
            "call_path": [{"function_name": "func_b", "caller_function_name": None, "duration_ns": 9}],
        }

        with patch(
            "amazon.opentelemetry.distro.serviceevents.collectors." "incident_snapshot_collector.get_function_info",
            return_value={"is_async": False},
        ):
            result = collector._collect_exception_info(ValueError("explicit"), captured_stack_trace="MY TRACE")

        self.assertEqual(result[0].exception_type, "ValueError")
        self.assertEqual(result[0].exception_message, "explicit")
        self.assertEqual(result[0].stack_trace, "MY TRACE")
        self.assertTrue(result[0].call_path[0].error)

    def test_explicit_exception_falls_back_to_str(self):
        """Without a captured stack trace, an explicit exception falls back to str(exception)."""
        collector = self._make_collector()
        collector._monitor_state.get_investigation_data.return_value = None

        result = collector._collect_exception_info(ValueError("plain"))

        self.assertEqual(result[0].stack_trace, "plain")
        self.assertEqual(result[0].call_path, [])


class TestFormatIds(TestCase):
    """Test the _format_trace_id and _format_span_id static helpers."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_format_trace_id_none(self):
        """A None trace id formats to None."""
        self.assertIsNone(IncidentSnapshotCollector._format_trace_id(None))

    def test_format_trace_id_int(self):
        """An int trace id formats to 0x-prefixed 32-char hex."""
        self.assertEqual(IncidentSnapshotCollector._format_trace_id(1), "0x" + "0" * 31 + "1")

    def test_format_trace_id_str_passthrough(self):
        """A string trace id is passed through as-is."""
        self.assertEqual(IncidentSnapshotCollector._format_trace_id("0xabc"), "0xabc")

    def test_format_span_id_none(self):
        """A None span id formats to None."""
        self.assertIsNone(IncidentSnapshotCollector._format_span_id(None))

    def test_format_span_id_int(self):
        """An int span id formats to 0x-prefixed 16-char hex."""
        self.assertEqual(IncidentSnapshotCollector._format_span_id(255), "0x" + "0" * 14 + "ff")

    def test_format_span_id_str_passthrough(self):
        """A string span id is passed through as-is."""
        self.assertEqual(IncidentSnapshotCollector._format_span_id("span-1"), "span-1")


class TestGenerateSessionAndRequestId(TestCase):
    """Test the _generate_session_id and _generate_request_id helpers."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_session_id_with_user_id(self):
        """A session id is built from the user_id when present in args."""
        result = IncidentSnapshotCollector._generate_session_id({"args": {"user_id": "u1"}})
        self.assertIsNotNone(result)
        self.assertTrue(result.startswith("session_u1_"))

    def test_session_id_without_user_id(self):
        """No user_id yields no session id."""
        result = IncidentSnapshotCollector._generate_session_id({"args": {}})
        self.assertIsNone(result)

    def test_request_id_format(self):
        """A request id is a uuid prefixed with 'req_'."""
        result = IncidentSnapshotCollector._generate_request_id()
        self.assertTrue(result.startswith("req_"))
