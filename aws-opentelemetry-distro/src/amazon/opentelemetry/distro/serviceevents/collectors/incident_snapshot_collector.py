# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
IncidentSnapshotCollector - Triggers and collects deep incident snapshots.

Incident snapshots are triggered when:
- HTTP status code >= 500 or unhandled exception -> trigger_type: "exception"
- Request duration > threshold (slow requests) -> trigger_type: "latency"

Latency thresholds can be configured per-endpoint for fine-grained control.
Rate limiting and deduplication prevent snapshot spam.
"""

import fnmatch
import hashlib
import logging
import os
import re
import sys
import threading
import time
import traceback
import uuid
from collections import deque
from typing import Dict, List, Optional, Pattern, Set, Tuple

from amazon.opentelemetry.distro.serviceevents.ast_transformation import get_function_info
from amazon.opentelemetry.distro.serviceevents.collectors.base_collector import BaseCollector
from amazon.opentelemetry.distro.serviceevents.models import (
    CallPathEntry,
    ExceptionInfo,
    IncidentSnapshot,
    RequestContext,
    ResourceAttributes,
    TelemetryCorrelation,
)
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState
from amazon.opentelemetry.distro.serviceevents.utils import get_instance_id

logger = logging.getLogger(__name__)

# too-many-lines: this collector owns the full incident pipeline (trigger detection,
# per-endpoint latency thresholds, dedup + rate limiting, fork-safe state reset, trace
# correlation, and snapshot assembly). Splitting it would scatter tightly-coupled state
# across modules for no readability gain; the content is all live.
# pylint: disable=too-many-lines


class IncidentSnapshotCollector(BaseCollector):
    """
    Collector for incident snapshot events.

    Triggers snapshots based on errors or latency thresholds,
    applies rate limiting and deduplication, and collects detailed context.
    """

    def __init__(
        self,
        flush_interval_ms: int,
        duration_threshold_ms: int,
        max_per_period: int,
        environment: Optional[str] = None,
        service_name: Optional[str] = None,
        sdk_version: str = "",
        max_same_error: int = 1,
        resource_attributes: Optional[ResourceAttributes] = None,
        otlp_emitter=None,
    ):
        """
        Initialize the incident snapshot collector.

        Args:
            flush_interval_ms: How often to collect and export data (milliseconds)
            duration_threshold_ms: Default duration threshold for triggering (milliseconds)
            max_per_period: Maximum snapshots per rate-limit window (window fixed at 60s)
            environment: Deployment environment
            service_name: Service name
            sdk_version: SDK version
            max_same_error: Maximum occurrences of same error pattern
            resource_attributes: AWS platform resource attributes from OTel Resource detectors
            otlp_emitter: Optional ServiceEventsOtlpEmitter for OTLP export
        """
        super().__init__(flush_interval_ms, "IncidentSnapshotCollector", otlp_emitter)

        # Default latency threshold (used when no per-endpoint threshold is set)
        self.default_latency_threshold_ms = duration_threshold_ms
        # Per-operation latency thresholds: operation -> threshold_ms (for exact matches)
        self._latency_thresholds: Dict[str, float] = {}
        self._latency_thresholds_lock = threading.Lock()
        # Pattern-based latency thresholds: list of (compiled_regex, threshold_ms, original_pattern) tuples
        # Supports glob patterns like "* /server_request:50" or "GET /api/*:100"
        # Patterns are pre-compiled to regex at startup for faster per-request matching
        self._latency_threshold_patterns: List[Tuple[Pattern, float, str]] = []
        self._latency_patterns_lock = threading.Lock()
        # `max_per_period` is the max snapshots allowed per rate-limit window. The window
        # length is now fixed at 60 seconds (no longer configurable); the field/param name
        # is kept as-is to avoid churning callers and tests.
        self.max_per_period = max_per_period
        self.period_seconds = 60

        # Environment and service metadata. None/empty when unset — omitted from the
        # snapshot rather than emitted as a sentinel.
        self.environment = environment
        self.service_name = service_name or os.getenv("OTEL_SERVICE_NAME", "UnknownService")
        self.sdk_version = sdk_version
        self.git_commit_sha = os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA")
        self.deployment_id = os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID")
        self.pid = os.getpid()
        self.resource_attributes = resource_attributes or ResourceAttributes()
        self.instance_id = get_instance_id()

        # Enhance instance_id: prefer host.id from resource attributes (e.g., EC2 instance ID)
        if self.resource_attributes.host_id:
            self.instance_id = self.resource_attributes.host_id

        # Rate limiting: track snapshot timestamps
        self._snapshot_timestamps: deque = deque(maxlen=max_per_period * 2)
        self._timestamps_lock = threading.Lock()

        # Period-level deduplication: track error hashes with TTL (limits same error over the fixed 60s window)
        self._error_hashes: Dict[str, List[float]] = {}  # hash -> [timestamp1, timestamp2, ...]
        self._error_hashes_lock = threading.Lock()
        self._max_same_error = max_same_error

        # Batch-level deduplication: one snapshot per error type per collection interval
        # Cleared after each collect() call
        self._current_batch_hashes: Set[str] = set()

        # Pending snapshots to export
        self._pending_snapshots: List[IncidentSnapshot] = []
        self._pending_lock = threading.Lock()
        # Within a collection cycle, the single pending snapshot per error hash (batch dedup keeps
        # exactly one). Lets a later SAMPLED occurrence of the same error upgrade an earlier
        # UNSAMPLED pending snapshot's correlation before it flushes — see
        # _maybe_upgrade_pending_correlation. Guarded by _pending_lock; cleared each collect().
        self._pending_by_hash: Dict[str, IncidentSnapshot] = {}

        # Monitor state for getting execution flow
        self._monitor_state = _ServiceEventsMonitorState.get_instance()

    def update_incident_config(
        self,
        max_per_period: int,
        max_same_error: int,
    ) -> None:
        """Live-update incident config (max-per-window, max-same-error).

        Recreates the snapshot_timestamps deque when max_per_period changes
        since deque maxlen is immutable after construction. The rate-limit window
        stays fixed at 60s and is not adjustable here.

        NOTE: no longer watcher-driven — the DI watcher syncer was removed. Retained as
        a public live-setter for callers that mutate the collector directly.
        """
        self._max_same_error = max_same_error
        if max_per_period != self.max_per_period:
            self.max_per_period = max_per_period
            with self._timestamps_lock:
                old = list(self._snapshot_timestamps)
                self._snapshot_timestamps = deque(old, maxlen=max_per_period * 2)

    def _reset_for_fork(self):
        """Reset collector state after fork.

        The child inherits the parent's pending snapshots and dedup/rate-limit bookkeeping.
        Left in place the child would re-emit the parent's pending snapshots (double export)
        and start life with stale rate-limit/dedup windows that suppress its own early
        incidents. Locks are recreated because a parent daemon thread may have held one at
        fork time (those threads do not survive fork), which would otherwise deadlock the
        child. Safe to mutate without holding the old locks: os.register_at_fork's
        after_in_child hook runs single-threaded in the child.

        Latency thresholds (exact + pattern) are deliberately preserved — they are
        configuration, not per-request state, and the child needs the same triggers.
        """
        super()._reset_for_fork()
        self.pid = os.getpid()
        self._pending_snapshots = []
        self._pending_by_hash = {}
        self._pending_lock = threading.Lock()
        self._snapshot_timestamps = deque(maxlen=self.max_per_period * 2)
        self._timestamps_lock = threading.Lock()
        self._error_hashes = {}
        self._error_hashes_lock = threading.Lock()
        self._current_batch_hashes = set()
        self._latency_thresholds_lock = threading.Lock()
        self._latency_patterns_lock = threading.Lock()

    # Live-setter overrides below (set_latency_threshold*, update_incident_config) are no
    # longer watcher-driven — the DI watcher syncer was removed. They remain as public
    # methods for callers that configure the collector directly (e.g. at startup).

    def set_latency_threshold(self, operation: str, threshold_ms: float) -> None:
        """
        Set latency threshold for a specific operation (exact match).

        Args:
            operation: Operation string (e.g., "GET /api/users")
            threshold_ms: Latency threshold in milliseconds
        """
        with self._latency_thresholds_lock:
            self._latency_thresholds[operation] = threshold_ms
            logger.info("Set latency threshold for operation %s: %sms", operation, threshold_ms)

    def set_latency_threshold_by_route(self, route: str, method: str, threshold_ms: float) -> str:
        """
        Set latency threshold for a specific route/method combination (exact match).

        Args:
            route: Route pattern (e.g., "/api/users")
            method: HTTP method (e.g., "GET", "POST")
            threshold_ms: Latency threshold in milliseconds

        Returns:
            The operation string that was configured
        """
        operation = f"{method} {route}"
        self.set_latency_threshold(operation, threshold_ms)
        return operation

    def set_latency_threshold_patterns(self, patterns: List[Tuple[str, float]]) -> None:
        """
        Set latency threshold patterns with glob support.

        Patterns are pre-compiled to regex at startup for faster per-request matching.
        Patterns are matched in order - first match wins.

        Args:
            patterns: List of (pattern, threshold_ms) tuples.
                      Pattern format: "METHOD /route" (e.g., "* /server_request", "GET /api/*")
        """
        compiled_patterns: List[Tuple[Pattern, float, str]] = []
        for pattern, threshold_ms in patterns:
            # Convert glob pattern to regex and compile for faster matching
            regex = re.compile(fnmatch.translate(pattern))
            compiled_patterns.append((regex, threshold_ms, pattern))
            logger.info("Set latency threshold pattern '%s': %sms", pattern, threshold_ms)

        with self._latency_patterns_lock:
            self._latency_threshold_patterns = compiled_patterns

    def get_latency_threshold(
        self, operation: Optional[str] = None, route: Optional[str] = None, method: Optional[str] = None
    ) -> float:
        """
        Get latency threshold for an endpoint.

        Lookup order:
        1. Pattern matching (if route and method provided) - first match wins
        2. Exact operation match
        3. Default threshold

        Args:
            operation: Operation string (e.g., "GET /api/users") for exact match
            route: Route pattern (e.g., "/server_request") - for pattern matching
            method: HTTP method (e.g., "GET") - for pattern matching

        Returns:
            Latency threshold in milliseconds
        """
        # Try pattern matching first (if route and method provided)
        if route is not None and method is not None:
            endpoint_str = f"{method.upper()} {route}"
            with self._latency_patterns_lock:
                for regex, threshold_ms, _ in self._latency_threshold_patterns:
                    if regex.match(endpoint_str):
                        return threshold_ms

        # Fall back to exact operation match
        if operation is not None:
            with self._latency_thresholds_lock:
                if operation in self._latency_thresholds:
                    return self._latency_thresholds[operation]

        return self.default_latency_threshold_ms

    def get_all_latency_thresholds(self) -> Dict[str, float]:
        """
        Get all configured per-operation latency thresholds (exact matches).

        Returns:
            Dictionary of operation -> threshold_ms
        """
        with self._latency_thresholds_lock:
            return dict(self._latency_thresholds)

    def get_all_latency_threshold_patterns(self) -> List[Tuple[str, float]]:
        """
        Get all configured latency threshold patterns (original patterns, not compiled).

        Returns:
            List of (pattern, threshold_ms) tuples
        """
        with self._latency_patterns_lock:
            # Return original patterns (third element of tuple), not compiled regex
            return [(pattern, threshold_ms) for _, threshold_ms, pattern in self._latency_threshold_patterns]

    def process_potential_incident(  # pylint: disable=too-many-locals
        self,
        route: str,
        method: str,
        status_code: int,
        duration_ms: float,
        exception: Optional[Exception],
        request_data: Dict,
    ) -> Optional[Dict]:
        """
        Process a potential incident snapshot trigger.

        Args:
            route: Route pattern
            method: HTTP method
            status_code: HTTP status code
            duration_ms: Request duration
            exception: Exception object if any
            request_data: Request metadata (headers, args, etc.)

        Returns:
            Exemplar dict with snapshot_id, trigger_type, severity, operation,
            and timestamp when a snapshot is created; None otherwise.
        """
        # Capture stack trace IMMEDIATELY while we're still in exception context
        # sys.exc_info() only works inside an active exception handler
        captured_stack_trace = None
        if exception is not None:
            exc_type, exc_value, exc_tb = sys.exc_info()
            if exc_tb is not None:
                captured_stack_trace = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            else:
                # Fallback: try to format from exception object directly
                captured_stack_trace = "".join(
                    traceback.format_exception(type(exception), exception, exception.__traceback__)
                )

        # Compute operation for threshold lookup and exemplar
        operation = f"{method} {route}"

        # Check if snapshot should be triggered
        # Pass route and method for pattern-based threshold matching, and operation for exact match
        trigger_type = self._determine_trigger_type(status_code, duration_ms, exception, operation, route, method)
        if trigger_type is None:
            return None

        # Generate error hash for deduplication
        error_hash = self._generate_error_hash(route, exception)

        # Check batch-level deduplication FIRST (one per error type per collection interval).
        with self._error_hashes_lock:
            already_in_batch = error_hash in self._current_batch_hashes
            if not already_in_batch:
                # Add to current batch (will be cleared after collect())
                self._current_batch_hashes.add(error_hash)

        if already_in_batch:
            # A snapshot for this error is already pending this cycle. Batch dedup keeps that
            # single snapshot, but if it was built from an UNSAMPLED occurrence (no resolvable
            # trace link, see fix #1) and THIS occurrence is sampled, upgrade the pending snapshot
            # in place so the one snapshot we emit per cycle carries a resolvable trace link.
            self._maybe_upgrade_pending_correlation(
                error_hash,
                route,
                method,
                status_code,
                duration_ms,
                exception,
                request_data,
                trigger_type,
                captured_stack_trace,
            )
            logger.debug("Incident snapshot batch-deduplicated (hash: %s)", error_hash)
            return None

        # Check period-level deduplication (limits same error over the fixed 60s window)
        if not self._check_deduplication(error_hash):
            logger.debug("Incident snapshot period-deduplicated (hash: %s)", error_hash)
            return None

        # Check rate limit AFTER dedup — only non-deduplicated requests consume slots.
        # Previously this ran before dedup, causing phantom slot consumption:
        # dedup-blocked requests incremented the counter without producing snapshots.
        if not self._check_rate_limit():
            logger.debug("Incident snapshot rate limit exceeded, skipping")
            return None

        # Collect incident snapshot data
        try:
            snapshot = self._collect_incident_snapshot(
                route=route,
                method=method,
                status_code=status_code,
                duration_ms=duration_ms,
                exception=exception,
                request_data=request_data,
                trigger_type=trigger_type,
                captured_stack_trace=captured_stack_trace,
            )

            # Add to pending snapshots, and index by error hash so a later sampled occurrence of
            # the same error can upgrade this snapshot's correlation in place before it flushes.
            with self._pending_lock:
                self._pending_snapshots.append(snapshot)
                self._pending_by_hash[error_hash] = snapshot

            logger.info(
                "Incident snapshot triggered: %s %s (status=%s, trigger=%s)",
                route,
                method,
                status_code,
                trigger_type,
            )

            # Return exemplar for endpoint telemetry correlation
            return {
                "snapshot_id": snapshot.snapshot_id,
                "trigger_type": snapshot.trigger_type,
                "severity": snapshot.severity,
                "operation": operation,
                "timestamp": snapshot.timestamp,
            }

        except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
            logger.error("Error collecting incident snapshot data: %s", exc, exc_info=True)
            # Roll back the slots this attempt consumed. The batch/dedup/rate-limit slots
            # are claimed before collection (claim-then-check is required for the
            # concurrent-request race protection in _check_deduplication). If collection
            # then fails, leaving them claimed would suppress a *later* identical error
            # that could have produced a snapshot — for up to the 60s dedup/rate windows.
            self._rollback_reservation(error_hash)
            return None

    def _maybe_upgrade_pending_correlation(
        self,
        error_hash: str,
        route: str,
        method: str,
        status_code: int,
        duration_ms: float,
        exception: Optional[Exception],
        request_data: Dict,
        trigger_type: str,
        captured_stack_trace: Optional[str],
    ) -> None:
        """Upgrade a pending UNSAMPLED snapshot to this SAMPLED occurrence (whole-snapshot swap).

        Trace correlation is sampling-conditional (fix #1): an unsampled request emits a snapshot
        with no trace_id. Because batch dedup keeps exactly one snapshot per error hash per cycle,
        that single snapshot inherits the FIRST occurrence's sampling state — so under reduced
        sampling it usually carries no resolvable trace link even if a sampled occurrence of the
        same error happens moments later in the same cycle.

        When this occurrence IS sampled (request_data carries a trace_id) and the pending snapshot
        is NOT (its trace_id is None), replace the pending snapshot WHOLESALE with a freshly
        collected one for this occurrence, preserving the original snapshot_id so the endpoint
        exemplar pointer stays valid. The replacement is whole-snapshot (not correlation-only) so
        the body — stack trace, call path, duration, timestamp — stays coherent with the trace it
        links to. First sampled occurrence wins; once upgraded, later occurrences are left alone.
        No-op (so the original is preserved) on any failure — telemetry must never crash the host.
        """
        # Only sampled occurrences can upgrade — an unsampled one has nothing better to offer.
        # request_data["trace_id"] is the span processor's SAMPLED-gated correlation (fix #1):
        # present iff the trace was sampled, so it is the authoritative "is this sampled?" signal.
        if not request_data.get("trace_id"):
            return
        try:
            with self._pending_lock:
                pending = self._pending_by_hash.get(error_hash)
                # Upgrade only an uncorrelated pending snapshot; if it already has a trace_id, the
                # first sampled occurrence already won.
                if pending is None or pending.telemetry_correlation.trace_id is not None:
                    return

            # Collect outside the lock (it can do non-trivial work); the snapshot_id is stamped
            # after so the replacement keeps the original's identity.
            replacement = self._collect_incident_snapshot(
                route=route,
                method=method,
                status_code=status_code,
                duration_ms=duration_ms,
                exception=exception,
                request_data=request_data,
                trigger_type=trigger_type,
                captured_stack_trace=captured_stack_trace,
            )

            with self._pending_lock:
                # Re-fetch under the lock: collect() may have drained the cycle, or another thread
                # may have upgraded it meanwhile. Only swap if the same uncorrelated snapshot is
                # still pending.
                pending = self._pending_by_hash.get(error_hash)
                if pending is None or pending.telemetry_correlation.trace_id is not None:
                    return
                replacement.snapshot_id = pending.snapshot_id
                try:
                    idx = self._pending_snapshots.index(pending)
                except ValueError:
                    return
                self._pending_snapshots[idx] = replacement
                self._pending_by_hash[error_hash] = replacement
            logger.debug("Upgraded pending incident snapshot to a sampled occurrence (hash: %s)", error_hash)
        except Exception:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
            logger.debug("Failed to upgrade pending incident correlation", exc_info=True)

    def _rollback_reservation(self, error_hash: str) -> None:
        """Undo the batch/period-dedup/rate-limit slots claimed for a failed collection.

        Best-effort and guarded: this runs on an error path, so it must not raise. Removes
        the batch hash, the most-recent period-dedup timestamp for this hash, and the
        most-recent rate-limit timestamp — the three slots claimed earlier in
        process_potential_incident for this attempt.
        """
        try:
            with self._error_hashes_lock:
                self._current_batch_hashes.discard(error_hash)
                timestamps = self._error_hashes.get(error_hash)
                if timestamps:
                    timestamps.pop()  # drop the timestamp this attempt added
                    if not timestamps:
                        del self._error_hashes[error_hash]
            with self._timestamps_lock:
                if self._snapshot_timestamps:
                    self._snapshot_timestamps.pop()  # drop the slot this attempt added
        except Exception:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
            logger.debug("Failed to roll back incident reservation", exc_info=True)

    def collect(self):
        """Collect pending snapshots and export to console."""
        # Clear batch-level hashes for new collection cycle
        with self._error_hashes_lock:
            self._current_batch_hashes.clear()

        # Get pending snapshots
        with self._pending_lock:
            snapshots = self._pending_snapshots
            self._pending_snapshots = []
            # Drop the per-hash index for the drained cycle; the upgrade window is one cycle.
            self._pending_by_hash = {}

        if not snapshots:
            logger.debug("No incident snapshots to export")
            return

        if not self.otlp_emitter:
            return

        for snapshot in snapshots:
            self.otlp_emitter.emit_incident_snapshot(snapshot.to_dict())
        logger.info("Exported %d incident snapshots", len(snapshots))

    def _determine_trigger_type(
        self,
        status_code: int,
        duration_ms: float,
        exception: Optional[Exception],
        operation: Optional[str] = None,
        route: Optional[str] = None,
        method: Optional[str] = None,
    ) -> Optional[str]:
        """
        Determine trigger type for incident snapshot.

        Args:
            status_code: HTTP status code
            duration_ms: Request duration
            exception: Exception object if any
            operation: Operation string for per-operation latency threshold lookup
            route: Route pattern for pattern-based threshold matching
            method: HTTP method for pattern-based threshold matching

        Returns:
            Trigger type string or None if no trigger
            - "exception": Server error (status >= 500 or unhandled exception)
            - "latency": Request duration exceeded threshold
        """
        # Priority order: exception > latency
        if exception is not None:
            return "exception"

        if status_code >= 500:
            return "exception"

        # Get latency threshold (pattern matching first, then operation, then default)
        latency_threshold = self.get_latency_threshold(operation, route, method)
        if duration_ms > latency_threshold:
            return "latency"

        return None

    @staticmethod
    def _determine_severity(status_code: int, trigger_type: str) -> str:
        """
        Determine severity level based on status code and trigger type.

        Args:
            status_code: HTTP status code
            trigger_type: Trigger type

        Returns:
            Severity level: "critical", "high", "medium", "low"
        """
        # Critical: 500-503 (server errors)
        if 500 <= status_code <= 503:
            return "critical"

        # High: 504+ (timeouts), exceptions
        if status_code >= 504 or trigger_type == "exception":
            return "high"

        # Medium: latency trigger (slow requests)
        if trigger_type == "latency":
            return "medium"

        return "low"

    def _check_rate_limit(self) -> bool:
        """
        Check if rate limit allows new snapshot.

        Returns:
            True if snapshot allowed, False if rate limited
        """
        current_time = time.time()
        cutoff_time = current_time - self.period_seconds

        with self._timestamps_lock:
            # Remove old timestamps outside the window
            while self._snapshot_timestamps and self._snapshot_timestamps[0] < cutoff_time:
                self._snapshot_timestamps.popleft()

            # Check if we're at the limit
            if len(self._snapshot_timestamps) >= self.max_per_period:
                return False

            # Add current timestamp
            self._snapshot_timestamps.append(current_time)
            return True

    @staticmethod
    def _generate_error_hash(route: str, exception: Optional[Exception]) -> str:
        """
        Generate hash for error deduplication.

        Args:
            route: Route pattern
            exception: Exception object

        Returns:
            Hash string
        """
        if exception is None:
            # For non-exception incidents (slow requests), use route only
            hash_input = f"route:{route}"
        else:
            # Include exception type and message
            exc_type = type(exception).__name__
            exc_message = str(exception)
            hash_input = f"route:{route}|exc:{exc_type}:{exc_message}"

        return hashlib.md5(hash_input.encode("utf-8")).hexdigest()

    def _check_deduplication(self, error_hash: str) -> bool:
        """
        Check if error should be deduplicated.

        Uses atomic add-then-check to handle concurrent requests correctly.

        Args:
            error_hash: Error hash

        Returns:
            True if snapshot allowed, False if deduplicated
        """
        current_time = time.time()
        cutoff_time = current_time - self.period_seconds

        with self._error_hashes_lock:
            # Clean up old hashes
            for hash_key in list(self._error_hashes.keys()):
                timestamps = self._error_hashes[hash_key]
                # Remove old timestamps
                timestamps = [ts for ts in timestamps if ts >= cutoff_time]
                if timestamps:
                    self._error_hashes[hash_key] = timestamps
                else:
                    del self._error_hashes[hash_key]

            # Add timestamp FIRST (atomic add-then-check pattern)
            # This prevents race conditions where concurrent requests both pass the check
            if error_hash in self._error_hashes:
                self._error_hashes[error_hash].append(current_time)
            else:
                self._error_hashes[error_hash] = [current_time]

            # Now check if we've exceeded the limit (use > because we already added)
            if len(self._error_hashes[error_hash]) > self._max_same_error:
                return False  # Deduplicate

            return True

    def _collect_incident_snapshot(  # pylint: disable=too-many-locals
        self,
        route: str,
        method: str,
        status_code: int,
        duration_ms: float,
        exception: Optional[Exception],
        request_data: Dict,
        trigger_type: str,
        captured_stack_trace: Optional[str] = None,
    ) -> IncidentSnapshot:
        """
        Collect detailed incident snapshot data.

        Args:
            route: Route pattern
            method: HTTP method
            status_code: HTTP status code
            duration_ms: Request duration
            exception: Exception object
            request_data: Request metadata
            trigger_type: Trigger type
            captured_stack_trace: Pre-captured stack trace (captured at exception time)

        Returns:
            IncidentSnapshot object
        """
        # Generate snapshot ID
        snapshot_id = f"snap_{uuid.uuid4()}"

        # Determine severity
        severity = self._determine_severity(status_code, trigger_type)

        # Compute operation string
        operation = f"{method} {route}"

        # Collect exception info (pass pre-captured stack trace)
        exception_info = self._collect_exception_info(exception, captured_stack_trace)

        # Detect if call_path timing data is missing (first incident, sampling was off)
        is_partial = (
            any(entry.duration_ns == 0 for exc in exception_info for entry in exc.call_path)
            if exception_info
            else False
        )

        # Build request context. Actual request-payload capture is permanently disabled and is no
        # longer customer-configurable, so the four payload fields (request_body/query_params/
        # path_params/request_headers) are always null and custom_context is always empty — see
        # SERVICE_EVENTS_OTLP_SIGNALS_SPEC.md §5 (the keys stay on the wire as null for consumer
        # compatibility). Only the non-payload fields (type/timestamp/status_code) carry data.
        request_context = RequestContext(
            type="http",
            timestamp=int(time.time() * 1000),
            status_code=status_code,
        )

        # Build telemetry correlation. trace_id/span_id come straight from request_data, where the
        # span processor already gated them on the real SAMPLED flag (fix #1): present iff the trace
        # was sampled, else None (an unsampled request emits a complete, self-contained snapshot with
        # empty correlation). They are NOT re-derived from the current span or inbound headers — those
        # sources are not sampling-gated and would resurrect a link to a trace the backend never
        # exported. The span processor is the single, sampling-gated source of correlation truth.
        telemetry_correlation = TelemetryCorrelation(
            trace_id=self._format_trace_id(request_data.get("trace_id")),
            session_id=self._generate_session_id(request_data),
            span_id=self._format_span_id(request_data.get("span_id")),
            request_id=self._generate_request_id(),
        )

        # Create IncidentSnapshot. Trace correlation (trace_id/span_id) is carried on
        # the emitted LogRecord so the backend can join it to the request's spans.
        snapshot = IncidentSnapshot(
            snapshot_id=snapshot_id,
            timestamp=int(time.time() * 1000),
            severity=severity,
            trigger_type=trigger_type,
            service=self.service_name,
            environment=self.environment,
            instance_id=self.instance_id,
            operation=operation,
            sdk_version=self.sdk_version,
            pid=self.pid,
            duration_ms=duration_ms,
            exception_info=exception_info,
            request_context=request_context,
            telemetry_correlation=telemetry_correlation,
            git_commit_sha=self.git_commit_sha,
            deployment_id=self.deployment_id,
            is_partial=is_partial,
            resource_attributes=self.resource_attributes,
        )

        return snapshot

    def _collect_exception_info(
        self,
        exception: Optional[Exception],
        captured_stack_trace: Optional[str] = None,
    ) -> List[ExceptionInfo]:
        """
        Collect exception information with call path.

        Args:
            exception: Exception object
            captured_stack_trace: Pre-captured stack trace (captured at exception time)

        Returns:
            List of ExceptionInfo objects
        """
        # Get investigation data from monitor state (contains call_path and possibly exception)
        inv_data = self._monitor_state.get_investigation_data()

        # If no explicit exception, check if the per-function monitor captured one
        if exception is None:
            if not inv_data or "exception" not in inv_data:
                # For latency incidents: return call_path even without exception
                # This allows latency snapshots to show which functions took time
                call_path = self._build_call_path(inv_data, None)
                if call_path:
                    return [
                        ExceptionInfo(
                            exception_type="",
                            exception_message="",
                            stack_trace="",
                            call_path=call_path,
                        )
                    ]
                return []
            # Use exception details captured by per-function monitor
            exc_data = inv_data["exception"]
            # Check if exc_data is valid (not None)
            if exc_data is None:
                # For latency incidents: return call_path even without exception
                call_path = self._build_call_path(inv_data, None)
                if call_path:
                    return [
                        ExceptionInfo(
                            exception_type="",
                            exception_message="",
                            stack_trace="",
                            call_path=call_path,
                        )
                    ]
                return []
            # Get the function_name that threw the exception (if captured)
            error_function_name = exc_data.get("function_name")
            call_path = self._build_call_path(inv_data, error_function_name)
            traceback_info = exc_data.get("traceback_info")
            if isinstance(traceback_info, str):
                # The monitor formats the traceback to a string eagerly (so it does not pin the
                # frame chain alive in the ContextVar); use it as-is.
                stack_trace = traceback_info
            elif traceback_info:
                # Backward-compatible path: an (exc_type, exc_value, exc_traceback) tuple.
                try:
                    stack_trace = "".join(traceback.format_exception(*traceback_info))
                except Exception:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
                    stack_trace = f"{exc_data.get('name', 'Unknown')}: {exc_data.get('message', '')}"
            else:
                stack_trace = "".join(exc_data.get("traceback", []))
            exception_info = ExceptionInfo(
                exception_type=exc_data.get("name", "Unknown"),
                exception_message=exc_data.get("message", ""),
                stack_trace=stack_trace,
                call_path=call_path,
            )
            return [exception_info]

        # Get the function_name that threw the exception from investigation data
        error_function_name = None
        if inv_data and inv_data.get("exception"):
            error_function_name = inv_data["exception"].get("function_name")

        # Build call_path entries with error marking
        call_path = self._build_call_path(inv_data, error_function_name)

        # Use pre-captured stack trace if available, otherwise fall back to str(exception)
        # Note: captured_stack_trace should be captured in process_potential_incident()
        # while still in the exception handler context
        stack_trace = captured_stack_trace if captured_stack_trace else str(exception)

        exception_info = ExceptionInfo(
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            stack_trace=stack_trace,
            call_path=call_path,
        )

        return [exception_info]

    @staticmethod
    def _build_call_path(
        inv_data: Optional[dict],
        error_function_name: Optional[str] = None,
    ) -> List[CallPathEntry]:
        """Build call path entries from investigation data.

        Args:
            inv_data: Investigation data containing call_path
            error_function_name: Function name that threw the exception (to mark with error=True)

        Returns:
            List of CallPathEntry objects with error flag set appropriately
        """
        call_path = []
        if inv_data and "call_path" in inv_data:
            for entry in inv_data["call_path"]:
                if isinstance(entry, dict):
                    func_name = entry["function_name"]
                    func_info = get_function_info(func_name)
                    is_async = func_info.get("is_async", False) if func_info else False
                    call_path.append(
                        CallPathEntry(
                            function_name=func_name,
                            caller_function_name=entry["caller_function_name"],
                            duration_ns=entry["duration_ns"],
                            error=(func_name == error_function_name) if error_function_name else False,
                            is_async=is_async,
                        )
                    )
                else:
                    caller, callee = entry
                    func_info = get_function_info(callee)
                    is_async = func_info.get("is_async", False) if func_info else False
                    call_path.append(
                        CallPathEntry(
                            function_name=callee,
                            caller_function_name=caller,
                            duration_ns=0,
                            error=(callee == error_function_name) if error_function_name else False,
                            is_async=is_async,
                        )
                    )
        return call_path

    @staticmethod
    def _format_trace_id(trace_id) -> Optional[str]:
        """Format a trace ID as a 0x-prefixed 32-char hex string."""
        if trace_id is None:
            return None
        if isinstance(trace_id, int):
            return f"0x{trace_id:032x}"
        return str(trace_id)

    @staticmethod
    def _format_span_id(span_id) -> Optional[str]:
        """Format a span ID as a 0x-prefixed 16-char hex string."""
        if span_id is None:
            return None
        if isinstance(span_id, int):
            return f"0x{span_id:016x}"
        return str(span_id)

    @staticmethod
    def _generate_session_id(request_data: Dict) -> Optional[str]:
        """Generate session ID from request data."""
        args = request_data.get("args", {})
        user_id = args.get("user_id")

        if user_id:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            return f"session_{user_id}_{timestamp}"

        return None

    @staticmethod
    def _generate_request_id() -> str:
        """Generate unique request ID."""
        return f"req_{uuid.uuid4()}"
