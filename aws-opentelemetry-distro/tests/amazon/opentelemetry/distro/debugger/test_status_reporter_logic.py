# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the StatusReporter lifecycle, queuing, batching, and HTTP send.

These tests deliberately avoid starting the real reporting thread or issuing real
HTTP. The background thread is mocked (threading.Thread -> MagicMock) and the
HTTP session is a mock whose post() return value / side effects are controlled.

The helpers _build_status_entry, _get_config_key, and _to_epoch_seconds are
covered by test_status_reporter.py / test_status_reporter_snapshot.py and are
intentionally not re-tested here.
"""

import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from amazon.opentelemetry.distro.debugger import _status_reporter as sr_module
from amazon.opentelemetry.distro.debugger._data_models import BreakpointState, FunctionBreakpointSet
from amazon.opentelemetry.distro.debugger._status_reporter import ConfigurationStatus, ErrorCause, StatusReporter


def _make_client(status_code=200):
    """Build a fake DebuggerClient exposing only what _send_report reads."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.status_code = status_code
    response.text = "body"
    session.post.return_value = response
    return SimpleNamespace(
        service_name="my-service",
        environment="prod",
        proxy_url="http://localhost:2000",
        timeout=5,
        _session=session,
    )


def _make_manager(states_by_func=None):
    """Build a fake manager exposing _active_functions of FunctionBreakpointSet."""
    active = {}
    for func_key, states in (states_by_func or {}).items():
        bp_set = FunctionBreakpointSet(function_key=func_key, module="m", function_name="f")
        bp_set.states = states
        active[func_key] = bp_set
    return SimpleNamespace(_active_functions=active, _lock=threading.RLock())


def _state(location_hash, *, hit_count=0, is_disabled=False, hit_in_last_period=False, instr_type="BREAKPOINT"):
    return BreakpointState(
        breakpoint_key=f"{location_hash}:0",
        location_hash=location_hash,
        instrumentation_type=instr_type,
        hit_count=hit_count,
        is_disabled=is_disabled,
        hit_in_last_period=hit_in_last_period,
    )


class TestInit(unittest.TestCase):
    def test_init_defaults(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        self.assertEqual(reporter.report_interval, 60)
        self.assertEqual(reporter._reported_configs, set())
        self.assertIsNone(reporter._periodic_status_reporter_thread)
        self.assertFalse(reporter._stop_event.is_set())

    def test_init_custom_interval(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager(), report_interval=15)
        self.assertEqual(reporter.report_interval, 15)


class TestSendReport(unittest.TestCase):
    def test_send_report_posts_expected_payload(self):
        client = _make_client(status_code=200)
        reporter = StatusReporter(client=client, manager=_make_manager())
        entries = [{"LocationHash": "h1"}, {"LocationHash": "h2"}]

        reporter._send_report(entries)

        client._session.post.assert_called_once()
        args, kwargs = client._session.post.call_args
        self.assertEqual(args[0], "http://localhost:2000/report-instrumentation-configuration-status")
        self.assertEqual(kwargs["json"]["Service"], "my-service")
        self.assertEqual(kwargs["json"]["Environment"], "prod")
        self.assertEqual(kwargs["json"]["Configurations"], entries)
        self.assertEqual(kwargs["timeout"], 5)

    def test_send_report_non_200_does_not_raise(self):
        client = _make_client(status_code=500)
        reporter = StatusReporter(client=client, manager=_make_manager())
        # Should not raise even on error status.
        reporter._send_report([{"LocationHash": "h"}])
        client._session.post.assert_called_once()

    def test_send_report_swallows_post_exception(self):
        client = _make_client()
        client._session.post.side_effect = RuntimeError("network down")
        reporter = StatusReporter(client=client, manager=_make_manager())
        # Should not raise.
        reporter._send_report([{"LocationHash": "h"}])


class TestReportStatusImmediately(unittest.TestCase):
    def test_report_status_immediately_enqueues_entry(self):
        client = _make_client()
        reporter = StatusReporter(client=client, manager=_make_manager())

        reporter.report_status_immediately("hash-1", "BREAKPOINT", ConfigurationStatus.READY)

        client._session.post.assert_not_called()
        self.assertEqual(reporter._background_status_reporter._queue.qsize(), 1)
        entry = reporter._background_status_reporter._queue.get_nowait()
        self.assertEqual(entry["LocationHash"], "hash-1")
        self.assertEqual(entry["InstrumentationType"], "BREAKPOINT")
        self.assertEqual(entry["SignalType"], "SNAPSHOT")
        self.assertEqual(entry["Status"], "READY")

    def test_report_status_immediately_includes_error_cause(self):
        client = _make_client()
        reporter = StatusReporter(client=client, manager=_make_manager())

        reporter.report_status_immediately(
            "hash-err", "PROBE", ConfigurationStatus.ERROR, error_cause=ErrorCause.METHOD_NOT_FOUND
        )

        entry = reporter._background_status_reporter._queue.get_nowait()
        self.assertEqual(entry["Status"], "ERROR")
        self.assertEqual(entry["ErrorCause"], "METHOD_NOT_FOUND")

    def test_report_status_immediately_swallows_build_error(self):
        client = _make_client()
        reporter = StatusReporter(client=client, manager=_make_manager())
        # status without a .value attribute triggers the internal build to raise; swallowed.
        with mock.patch.object(StatusReporter, "_send_report", side_effect=RuntimeError("boom")):
            # Should not raise.
            reporter.report_status_immediately("h", "BREAKPOINT", ConfigurationStatus.READY)


class TestReportNow(unittest.TestCase):
    def test_report_now_invokes_pull_with_initial_true(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        with mock.patch.object(reporter, "_pull_and_report_statuses") as mock_pull:
            reporter.report_now()
        mock_pull.assert_called_once_with(is_initial_report=True)

    def test_report_now_swallows_exceptions(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        with mock.patch.object(reporter, "_pull_and_report_statuses", side_effect=RuntimeError("boom")):
            # Should not raise.
            reporter.report_now()


class TestPullAndReportStatuses(unittest.TestCase):
    def _reporter_with_states(self, states):
        manager = _make_manager({"mod.func": states})
        return StatusReporter(client=_make_client(), manager=manager)

    def test_initial_report_emits_ready_for_unhit_state(self):
        states = {"k0": _state("hash-ready", hit_count=0)}
        reporter = self._reporter_with_states(states)
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=True)

        mock_send.assert_called_once()
        batch = mock_send.call_args[0][0]
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0]["Status"], "READY")
        self.assertEqual(batch[0]["LocationHash"], "hash-ready")
        # READY recorded so it is not reported twice.
        self.assertIn(
            StatusReporter._get_config_key("SNAPSHOT", "hash-ready", ConfigurationStatus.READY),
            reporter._reported_configs,
        )

    def test_initial_report_ready_not_repeated(self):
        states = {"k0": _state("hash-ready", hit_count=0)}
        reporter = self._reporter_with_states(states)
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=True)
            reporter._pull_and_report_statuses(is_initial_report=True)
        # Second call finds no new entries => only one send.
        self.assertEqual(mock_send.call_count, 1)

    def test_initial_report_skips_ready_when_already_hit(self):
        states = {"k0": _state("hash-hit", hit_count=3)}
        reporter = self._reporter_with_states(states)
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=True)
        mock_send.assert_not_called()

    def test_initial_report_skips_ready_for_failed_config(self):
        # A config the manager could not bind (in _failed_configs) keeps its ERROR status;
        # the initial sweep must not promote it to READY.
        manager = _make_manager({"mod.func": {"k0": _state("hash-failed", hit_count=0)}})
        manager._failed_configs = {"hash-failed": "LINE_NOT_EXECUTABLE"}
        reporter = StatusReporter(client=_make_client(), manager=manager)
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=True)
        mock_send.assert_not_called()

    def test_periodic_report_emits_active_for_recent_hit(self):
        state = _state("hash-active", hit_count=5, hit_in_last_period=True)
        reporter = self._reporter_with_states({"k0": state})
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=False)

        batch = mock_send.call_args[0][0]
        self.assertEqual(batch[0]["Status"], "ACTIVE")
        self.assertEqual(batch[0]["LocationHash"], "hash-active")
        # hit_in_last_period reset after reporting.
        self.assertFalse(state.hit_in_last_period)

    def test_periodic_report_skips_active_when_disabled(self):
        # A disabled state that was hit should NOT emit ACTIVE (DISABLED is reported separately).
        state = _state("hash-d", hit_count=5, hit_in_last_period=True, is_disabled=True)
        reporter = self._reporter_with_states({"k0": state})
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=False)

        batch = mock_send.call_args[0][0]
        statuses = [entry["Status"] for entry in batch]
        self.assertNotIn("ACTIVE", statuses)
        self.assertIn("DISABLED", statuses)

    def test_periodic_report_emits_disabled_once(self):
        state = _state("hash-d", hit_count=5, is_disabled=True)
        reporter = self._reporter_with_states({"k0": state})
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=False)
            reporter._pull_and_report_statuses(is_initial_report=False)
        # DISABLED only emitted on the first pull.
        self.assertEqual(mock_send.call_count, 1)
        self.assertEqual(mock_send.call_args_list[0][0][0][0]["Status"], "DISABLED")

    def test_no_entries_means_no_send(self):
        # No states at all => nothing to report.
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=False)
        mock_send.assert_not_called()

    def test_batches_entries_in_chunks_of_100(self):
        # 150 unhit states => 150 READY entries => two batches (100 + 50).
        states = {f"k{i}": _state(f"hash-{i}", hit_count=0) for i in range(150)}
        reporter = self._reporter_with_states(states)
        with mock.patch.object(reporter, "_send_report") as mock_send:
            reporter._pull_and_report_statuses(is_initial_report=True)

        self.assertEqual(mock_send.call_count, 2)
        self.assertEqual(len(mock_send.call_args_list[0][0][0]), 100)
        self.assertEqual(len(mock_send.call_args_list[1][0][0]), 50)


class TestLifecycle(unittest.TestCase):
    def test_stop_before_start_is_safe(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        # No thread started yet; stop() must not raise.
        reporter.stop()
        self.assertTrue(reporter._stop_event.is_set())

    def test_start_creates_and_starts_thread(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        with mock.patch.object(sr_module.threading, "Thread") as mock_thread_cls:
            thread_instance = mock.MagicMock()
            mock_thread_cls.return_value = thread_instance
            reporter.start()

        # Two daemon threads start: the periodic report loop and the
        # BackgroundStatusReporter drain.
        self.assertEqual(mock_thread_cls.call_count, 2)
        targets = [c.kwargs["target"] for c in mock_thread_cls.call_args_list]
        self.assertIn(reporter._report_loop, targets)
        self.assertIn(reporter._background_status_reporter._run, targets)
        for c in mock_thread_cls.call_args_list:
            self.assertTrue(c.kwargs["daemon"])
        self.assertEqual(thread_instance.start.call_count, 2)
        self.assertIs(reporter._periodic_status_reporter_thread, thread_instance)
        self.assertFalse(reporter._stop_event.is_set())

    def test_start_is_idempotent_when_thread_alive(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        alive_thread = mock.MagicMock()
        alive_thread.is_alive.return_value = True
        reporter._periodic_status_reporter_thread = alive_thread
        reporter._background_status_reporter._thread = alive_thread

        with mock.patch.object(sr_module.threading, "Thread") as mock_thread_cls:
            reporter.start()
        # Already-alive threads => no new threads created.
        mock_thread_cls.assert_not_called()

    def test_stop_sets_event_and_joins_existing_thread(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        thread_instance = mock.MagicMock()
        reporter._periodic_status_reporter_thread = thread_instance

        reporter.stop()

        self.assertTrue(reporter._stop_event.is_set())
        thread_instance.join.assert_called_once_with(timeout=5)


class TestReportLoop(unittest.TestCase):
    def test_loop_exits_immediately_when_stop_event_set(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        reporter._stop_event.set()  # wait() returns True immediately => loop body skipped
        with mock.patch.object(reporter, "_pull_and_report_statuses") as mock_pull:
            reporter._report_loop()
        mock_pull.assert_not_called()

    def test_loop_runs_one_tick_then_stops(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        # First wait() => False (run body), second wait() => True (exit). No real sleeping.
        with mock.patch.object(reporter._stop_event, "wait", side_effect=[False, True]):
            with mock.patch.object(reporter, "_pull_and_report_statuses") as mock_pull:
                reporter._report_loop()
        mock_pull.assert_called_once_with(is_initial_report=False)

    def test_loop_swallows_pull_exception_and_continues(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        with mock.patch.object(reporter._stop_event, "wait", side_effect=[False, True]):
            with mock.patch.object(reporter, "_pull_and_report_statuses", side_effect=RuntimeError("boom")):
                # Should not raise; the loop catches and proceeds to the next wait().
                reporter._report_loop()


class TestBackgroundStatusReporter(unittest.TestCase):
    def test_submit_does_not_block_when_send_is_slow(self):
        send_started = threading.Event()
        let_send_finish = threading.Event()

        def hang_send(_entries):
            send_started.set()
            let_send_finish.wait(timeout=10)

        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        reporter._background_status_reporter._send = hang_send
        reporter._background_status_reporter.start()
        try:
            reporter.report_status_immediately("hash-warm", "BREAKPOINT", ConfigurationStatus.READY)
            self.assertTrue(send_started.wait(timeout=2))

            t0 = time.perf_counter()
            reporter.report_status_immediately("hash-2", "BREAKPOINT", ConfigurationStatus.READY)
            elapsed = time.perf_counter() - t0

            self.assertLess(elapsed, 0.1)
        finally:
            let_send_finish.set()
            reporter.stop()

    def test_submit_drops_silently_when_queue_full(self):
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        bg = reporter._background_status_reporter
        for idx in range(bg._MAX_QUEUE_SIZE):
            bg.submit({"i": idx})
        self.assertEqual(bg._queue.qsize(), bg._MAX_QUEUE_SIZE)

        t0 = time.perf_counter()
        bg.submit({"i": "overflow"})
        elapsed = time.perf_counter() - t0

        self.assertLess(elapsed, 0.1)
        self.assertEqual(bg._queue.qsize(), bg._MAX_QUEUE_SIZE)

    def test_drain_actually_sends(self):
        sent = []
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        reporter._background_status_reporter._send = sent.append
        reporter._background_status_reporter.start()
        try:
            reporter.report_status_immediately("hash-1", "BREAKPOINT", ConfigurationStatus.READY)
            deadline = time.time() + 5
            while time.time() < deadline and not sent:
                time.sleep(0.05)
            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0][0]["LocationHash"], "hash-1")
        finally:
            reporter.stop()

    def test_drain_batches_multiple_entries_into_one_send(self):
        sent_batches = []
        reporter = StatusReporter(client=_make_client(), manager=_make_manager())
        bg = reporter._background_status_reporter
        bg._send = lambda batch: sent_batches.append(list(batch))
        for idx in range(5):
            bg.submit({"i": idx})
        bg.start()
        try:
            deadline = time.time() + 5
            while time.time() < deadline and not sent_batches:
                time.sleep(0.05)
            self.assertEqual(len(sent_batches), 1)
            self.assertEqual(len(sent_batches[0]), 5)
        finally:
            reporter.stop()


if __name__ == "__main__":
    unittest.main()
