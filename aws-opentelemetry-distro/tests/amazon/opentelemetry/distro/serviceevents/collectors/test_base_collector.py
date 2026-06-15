# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.serviceevents.collectors.base_collector import BaseCollector


class _CountingCollector(BaseCollector):
    """Concrete BaseCollector that records each collect() call and can signal completion."""

    def __init__(self, flush_interval_ms=10, name="CountingCollector", raise_on_collect=False):
        super().__init__(flush_interval_ms=flush_interval_ms, name=name)
        self.collect_count = 0
        self.raise_on_collect = raise_on_collect
        self.collected = threading.Event()

    def collect(self):
        self.collect_count += 1
        self.collected.set()
        if self.raise_on_collect:
            raise ValueError("boom")


class TestBaseCollector(TestCase):
    """Cover BaseCollector start/stop/_run_loop/_reset_for_fork threading paths."""

    def test_start_launches_loop_thread_and_stop_joins_cleanly(self):
        """start() runs the loop thread; stop() sets state false and joins it."""
        collector = _CountingCollector(flush_interval_ms=10)
        try:
            collector.start()
            self.assertTrue(collector._running)
            self.assertIsNotNone(collector._thread)
            # Wait deterministically for at least one collection rather than sleeping.
            self.assertTrue(collector.collected.wait(timeout=5.0))
        finally:
            collector.stop()

        self.assertFalse(collector._running)
        collector._thread.join(timeout=5.0)
        self.assertFalse(collector._thread.is_alive())

    def test_start_twice_logs_already_running_and_returns(self):
        """A second start() hits the _running guard and does not spawn a new thread."""
        collector = _CountingCollector(flush_interval_ms=10)
        try:
            collector.start()
            self.assertTrue(collector.collected.wait(timeout=5.0))
            first_thread = collector._thread

            collector.start()

            # Guard returned early: same thread object, still running.
            self.assertIs(collector._thread, first_thread)
            self.assertTrue(collector._running)
        finally:
            collector.stop()
        collector._thread.join(timeout=5.0)

    def test_stop_when_not_running_returns_early(self):
        """stop() on a never-started collector returns without touching the (None) thread."""
        collector = _CountingCollector(flush_interval_ms=10)

        collector.stop()

        self.assertFalse(collector._running)
        self.assertIsNone(collector._thread)

    def test_collect_exception_is_caught_by_run_loop(self):
        """collect() raising is swallowed by _run_loop so telemetry never crashes the host."""
        collector = _CountingCollector(flush_interval_ms=10, raise_on_collect=True)
        try:
            collector.start()
            # First periodic collect() raises but the loop survives.
            self.assertTrue(collector.collected.wait(timeout=5.0))
            self.assertTrue(collector._thread.is_alive())
        finally:
            # stop() triggers the final collect() too; both raise and are caught.
            collector.stop()

        self.assertFalse(collector._running)
        collector._thread.join(timeout=5.0)
        self.assertFalse(collector._thread.is_alive())
        self.assertGreaterEqual(collector.collect_count, 1)

    def test_stop_warns_when_thread_does_not_stop_cleanly(self):
        """stop() logs a warning when the worker thread is still alive after join timeout."""
        collector = _CountingCollector(flush_interval_ms=10)
        # Simulate a thread that refuses to die within the join timeout.
        fake_thread = MagicMock()
        fake_thread.is_alive.return_value = True
        collector._thread = fake_thread
        collector._running = True

        with self.assertLogs(
            "amazon.opentelemetry.distro.serviceevents.collectors.base_collector", level="WARNING"
        ) as captured:
            collector.stop()

        fake_thread.join.assert_called_once_with(timeout=5.0)
        self.assertTrue(any("did not stop cleanly" in line for line in captured.output))
        self.assertFalse(collector._running)

    def test_flush_interval_sec_derived_from_ms(self):
        """flush_interval_sec is the millisecond value divided by 1000."""
        collector = _CountingCollector(flush_interval_ms=2500)
        self.assertEqual(collector.flush_interval_sec, 2.5)

    def test_reset_for_fork_resets_thread_state(self):
        """_reset_for_fork clears the dead thread and running flag and swaps the stop event."""
        collector = _CountingCollector(flush_interval_ms=10)
        old_event = collector._stop_event
        collector._thread = MagicMock()
        collector._running = True

        collector._reset_for_fork()

        self.assertIsNone(collector._thread)
        self.assertFalse(collector._running)
        self.assertIsNot(collector._stop_event, old_event)
        self.assertFalse(collector._stop_event.is_set())
