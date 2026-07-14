# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Status reporter for instrumentation configurations.
"""

import logging
import queue
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set

try:
    from opentelemetry.instrumentation.utils import suppress_http_instrumentation
except ImportError:
    from contextlib import nullcontext as suppress_http_instrumentation

logger = logging.getLogger(__name__)


class ConfigurationStatus(Enum):
    """Status of an instrumentation configuration."""

    READY = "READY"
    ERROR = "ERROR"
    DISABLED = "DISABLED"
    ACTIVE = "ACTIVE"


class ErrorCause(Enum):
    """Error causes for failed configurations."""

    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    METHOD_NOT_FOUND = "METHOD_NOT_FOUND"
    LINE_NOT_EXECUTABLE = "LINE_NOT_EXECUTABLE"
    OVERLOADED_METHODS = "OVERLOADED_METHODS"
    LANGUAGE_MISMATCH = "LANGUAGE_MISMATCH"
    RUNTIME_ERROR = "RUNTIME_ERROR"


class StatusReporter:
    """Reports instrumentation configuration status to backend."""

    def __init__(self, client, manager, report_interval: int = 60):
        """
        Initialize status reporter.

        Args:
            client: DebuggerClient instance (for lazy service/environment access)
            manager: InstrumentationManager instance (to pull breakpoint states)
            report_interval: Interval in seconds for reporting active configurations
        """
        self._client = client
        self._manager = manager
        self.report_interval = report_interval

        # Track reported configurations to avoid duplicate reports
        self._reported_configs: Set[str] = set()

        # Lock for thread safety
        self._lock = threading.Lock()

        # Background thread for continuous reporting
        self._periodic_status_reporter_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self._background_status_reporter = BackgroundStatusReporter(self._send_report, self._stop_event)

        logger.debug("StatusReporter initialized")

    def start(self):
        """Start background threads for continuous + immediate reporting."""
        if self._periodic_status_reporter_thread is None or not self._periodic_status_reporter_thread.is_alive():
            self._stop_event.clear()
            self._periodic_status_reporter_thread = threading.Thread(target=self._report_loop, daemon=True)
            self._periodic_status_reporter_thread.start()
            logger.debug("Status reporter background thread started")
        self._background_status_reporter.start()

    def stop(self):
        """Stop background threads."""
        self._stop_event.set()
        self._background_status_reporter.stop()
        if self._periodic_status_reporter_thread:
            self._periodic_status_reporter_thread.join(timeout=5)
        logger.debug("Status reporter stopped")

    def report_now(self):
        """Trigger immediate status report (called when new configurations are applied)."""
        try:
            self._pull_and_report_statuses(is_initial_report=True)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error in immediate status report: %s", exception, exc_info=True)

    def report_status_immediately(
        self,
        location_hash: str,
        instrumentation_type: str,
        status: ConfigurationStatus,
        error_cause: Optional[ErrorCause] = None,
    ):
        """Report a single status change immediately (push-based).

        Called at the exact moment a state transition happens:
        - nothing -> READY (config applied successfully)
        - nothing -> ERROR (config failed to apply)
        - READY -> ACTIVE (first hit)
        - ACTIVE -> DISABLED (max hits reached)

        This complements the periodic pull-based loop which continues
        to run as a backup/heartbeat.

        Args:
            location_hash: Config identifier
            instrumentation_type: "BREAKPOINT" or "PROBE"
            status: The new status
            error_cause: Optional error cause (for ERROR status)
        """
        try:
            entry = StatusReporter._build_status_entry(
                instrumentation_type, "SNAPSHOT", location_hash, status, error_cause
            )
            self._background_status_reporter.submit(entry)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to enqueue immediate status report: %s", exception)

    def _report_loop(self):
        """Background loop for pulling and reporting breakpoint statuses."""
        logger.debug("Status reporter loop started, will report every %ds", self.report_interval)
        while not self._stop_event.wait(self.report_interval):
            try:
                logger.debug("Status reporter loop tick")
                self._pull_and_report_statuses(is_initial_report=False)
            except Exception as exception:  # pylint: disable=broad-exception-caught
                logger.error("Error in status report loop: %s", exception, exc_info=True)
        logger.debug("Status reporter loop stopped")

    def _pull_and_report_statuses(self, is_initial_report: bool = False):
        """Pull breakpoint states from manager and report statuses.

        Args:
            is_initial_report: True for out-of-band reports when configs are applied,
                             False for periodic 60s reports
        """
        with self._manager._lock:
            bp_sets_snapshot = list(self._manager._active_functions.values())

        with self._lock:
            ready_entries = []
            active_entries = []
            disabled_entries = []
            error_entries = []

            for bp_set in bp_sets_snapshot:
                for _, state in bp_set.states.items():
                    # Determine status based on state
                    # Report Active if hit in last period (skip if disabled — DISABLED
                    # is reported immediately by the manager, so a stale periodic ACTIVE
                    # would overwrite it)
                    if state.hit_in_last_period and not is_initial_report and not state.is_disabled:
                        active_entries.append(
                            StatusReporter._build_status_entry(
                                state.instrumentation_type,
                                "SNAPSHOT",
                                state.location_hash,
                                ConfigurationStatus.ACTIVE,
                            )
                        )
                        # Reset flag after reporting
                        state.hit_in_last_period = False

                    # Report Disabled once in periodic report (after final Active status)
                    if state.is_disabled and not is_initial_report:
                        config_key = StatusReporter._get_config_key(
                            "SNAPSHOT", state.location_hash, ConfigurationStatus.DISABLED
                        )
                        if config_key not in self._reported_configs:
                            self._reported_configs.add(config_key)
                            disabled_entries.append(
                                StatusReporter._build_status_entry(
                                    state.instrumentation_type,
                                    "SNAPSHOT",
                                    state.location_hash,
                                    ConfigurationStatus.DISABLED,
                                )
                            )

                    # Report Ready once (only in initial reports, only if never hit)
                    if state.hit_count == 0 and is_initial_report:
                        config_key = StatusReporter._get_config_key(
                            "SNAPSHOT", state.location_hash, ConfigurationStatus.READY
                        )
                        if config_key not in self._reported_configs:
                            self._reported_configs.add(config_key)
                            ready_entries.append(
                                StatusReporter._build_status_entry(
                                    state.instrumentation_type,
                                    "SNAPSHOT",
                                    state.location_hash,
                                    ConfigurationStatus.READY,
                                )
                            )

            # Send reports (batch up to 100 per request)
            all_entries = ready_entries + active_entries + disabled_entries + error_entries
            if all_entries:
                for index in range(0, len(all_entries), 100):
                    batch = all_entries[index : index + 100]
                    self._send_report(batch)

    def _send_report(self, configurations: List[Dict]):
        """
        Send status report to backend.

        Args:
            configurations: List of configuration status entries
        """
        payload = {
            "Service": self._client.service_name,
            "Environment": self._client.environment,
            "Configurations": configurations,
        }

        logger.debug("Payload: %s", payload)

        try:
            url = self._client.proxy_url + "/report-instrumentation-configuration-status"
            with suppress_http_instrumentation():
                response = self._client._session.post(url, json=payload, timeout=self._client.timeout)

            if response.status_code == 200:
                logger.debug("Status report sent successfully: %s", payload)
                logger.debug("Status report sent successfully: %d configurations", len(configurations))
            else:
                logger.debug("Status report failed with status %d: %s", response.status_code, response.text)
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.debug("Error sending status report: %s", exception)

    @staticmethod
    def _build_status_entry(
        instrumentation_type: str,
        signal_type: str,
        location_hash: str,
        status: ConfigurationStatus,
        error_cause: Optional[ErrorCause] = None,
        timestamp: Optional[datetime] = None,
    ) -> Dict:
        """Build a status entry for reporting."""
        entry = {
            "InstrumentationType": instrumentation_type,
            "SignalType": signal_type,
            "LocationHash": location_hash,
            "Status": status.value,
            # datetime.timestamp() treats naive datetimes as local time, so ensure UTC.
            "Time": StatusReporter._to_epoch_seconds(timestamp),
        }

        if error_cause:
            entry["ErrorCause"] = error_cause.value

        return entry

    @staticmethod
    def _to_epoch_seconds(timestamp: Optional[datetime]) -> int:
        """Convert datetime to epoch seconds, assuming UTC for naive datetimes."""
        if timestamp is None:
            return int(time.time())
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp())

    @staticmethod
    def _get_config_key(signal_type: str, location_hash: str, status: ConfigurationStatus = None) -> str:
        """Generate unique key for a configuration.

        For Ready/Error/Disabled statuses, include status in key to allow reporting different statuses.
        For Active status (continuous reporting), don't include status in key.
        """
        base_key = f"{signal_type}:{location_hash}"
        if status and status in (ConfigurationStatus.READY, ConfigurationStatus.ERROR, ConfigurationStatus.DISABLED):
            return f"{base_key}:{status.value}"
        return base_key


class BackgroundStatusReporter:
    _MAX_QUEUE_SIZE = 256

    def __init__(self, send_callback, stop_event):
        self._send = send_callback
        self._stop_event = stop_event
        self._queue: "queue.Queue" = queue.Queue(maxsize=self._MAX_QUEUE_SIZE)
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name="BackgroundStatusReporter",
            )
            self._thread.start()

    def stop(self):
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        if self._thread:
            self._thread.join(timeout=5)

    def submit(self, entry):
        try:
            self._queue.put_nowait(entry)
        except queue.Full:
            logger.debug("Background status reporter queue full; dropping entry")

    def _run(self):
        while not self._stop_event.is_set():
            try:
                first = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if first is None:
                return
            batch = [first]
            try:
                while True:
                    batch.append(self._queue.get_nowait())
            except queue.Empty:
                pass
            try:
                batch = [e for e in batch if e is not None]
                if batch:
                    self._send(batch)
                    logger.debug("Background status reporter flushed %d entries", len(batch))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.debug("Background status reporter send failed: %s", exc)
