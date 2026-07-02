# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.serviceevents import python_monitor_impl as impl
from amazon.opentelemetry.serviceevents.python_monitor import (
    PythonServiceEventsMonitor,
    _ServiceEventsMonitorState,
)
from amazon.opentelemetry.serviceevents.python_monitor_impl import (
    _CALL_PATH_TRUNCATION_SENTINEL,
    _MAX_CALL_PATH_ENTRIES,
    _call_counters,
    _call_counters_lock,
    _call_stack,
    _current_operation,
    _increment_call_counter,
    _should_sample,
    get_call_stack,
    get_current_operation,
    get_sampling_mode,
    reset_after_fork,
    set_current_operation,
    set_sampling_mode,
    set_sampling_thresholds,
)


class TestServiceEventsMonitorState(TestCase):
    """Test the _ServiceEventsMonitorState singleton class."""

    def setUp(self):
        """Reset the singleton instance before each test."""
        _ServiceEventsMonitorState._instance = None
        # Reset the process-wide active-investigation count so a leaked begin from another test
        # can't disable (or, when high, mask) the hot-path gate exercised below.
        impl._investigation_active_count = 0

    def test_singleton_pattern(self):
        """Test that get_instance returns the same instance."""
        instance1 = _ServiceEventsMonitorState.get_instance()
        instance2 = _ServiceEventsMonitorState.get_instance()

        self.assertIs(instance1, instance2)

    def test_singleton_thread_safe(self):
        """Test that singleton is thread-safe."""
        instances = []

        def get_instance():
            instances.append(_ServiceEventsMonitorState.get_instance())

        threads = [threading.Thread(target=get_instance) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All instances should be the same
        self.assertEqual(len(set(id(i) for i in instances)), 1)

    def test_begin_investigation(self):
        """Test beginning an investigation."""
        state = _ServiceEventsMonitorState.get_instance()

        state.begin_investigation()

        inv_data = state._investigation_data.get()
        self.assertIsNotNone(inv_data)
        self.assertIn("call_path", inv_data)
        self.assertIn("exception", inv_data)
        self.assertIn("start_time", inv_data)

    def test_get_investigation_data(self):
        """Test getting and clearing investigation data."""
        state = _ServiceEventsMonitorState.get_instance()

        state.begin_investigation()
        state.record_execution_flow(None, "func1")
        state.record_execution_flow("func1", "func2")

        inv_data = state.get_investigation_data()

        self.assertIsNotNone(inv_data)
        self.assertEqual(len(inv_data["call_path"]), 2)
        self.assertEqual(inv_data["call_path"][0], (None, "func1"))
        self.assertEqual(inv_data["call_path"][1], ("func1", "func2"))

        # Should be cleared after get
        inv_data2 = state.get_investigation_data()
        self.assertIsNone(inv_data2)

    def test_clear_investigation_data(self):
        """clear_investigation_data drops the dict on the normal (non-incident) path."""
        state = _ServiceEventsMonitorState.get_instance()

        state.begin_investigation()
        self.assertIsNotNone(state.peek_investigation_data())

        state.clear_investigation_data()

        # Dropped without going through get_investigation_data (the incident-only path).
        self.assertIsNone(state.peek_investigation_data())

    def test_clear_investigation_data_is_idempotent(self):
        """Calling clear when there is nothing to clear is a safe no-op."""
        state = _ServiceEventsMonitorState.get_instance()

        # Already empty (or already consumed by the incident path): must not raise.
        state.clear_investigation_data()
        state.clear_investigation_data()
        self.assertIsNone(state.peek_investigation_data())

    def test_call_path_capped_with_truncation_sentinel(self):
        """record_call_path_entry caps the path and appends one sentinel on overflow (keep-first)."""
        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        # Record well past the cap; every real frame has a non-zero duration so is_partial would be
        # False if the cap never fired.
        for i in range(_MAX_CALL_PATH_ENTRIES + 50):
            caller = None if i == 0 else f"func-{i - 1}"
            state.record_call_path_entry(f"func-{i}", caller, 1000)

        inv_data = state.get_investigation_data()
        self.assertIsNotNone(inv_data)
        call_path = inv_data["call_path"]

        # Keep-first: MAX real frames + exactly one sentinel, regardless of overflow size.
        self.assertEqual(len(call_path), _MAX_CALL_PATH_ENTRIES + 1)

        # A below-cap frame is a normal entry.
        self.assertEqual(call_path[0]["function_name"], "func-0")
        self.assertEqual(call_path[_MAX_CALL_PATH_ENTRIES - 1]["function_name"], f"func-{_MAX_CALL_PATH_ENTRIES - 1}")
        self.assertEqual(call_path[_MAX_CALL_PATH_ENTRIES - 1]["duration_ns"], 1000)

        # The overflow frame is the sentinel with duration_ns=0 (trips is_partial downstream).
        sentinel = call_path[_MAX_CALL_PATH_ENTRIES]
        self.assertEqual(sentinel["function_name"], _CALL_PATH_TRUNCATION_SENTINEL)
        self.assertIsNone(sentinel["caller_function_name"])
        self.assertEqual(sentinel["duration_ns"], 0)

        # is_partial parity: at least one zero-duration frame is present.
        self.assertTrue(any(e["duration_ns"] == 0 for e in call_path))

    # ───── active-count gate on record_call_path_entry (JS/Java parity) ──

    def test_record_call_path_entry_noop_when_no_investigation_active(self):
        """With no begin_investigation(), the active-count gate keeps record_call_path_entry from
        touching the ContextVar at all — it's a no-op and creates no data."""
        state = _ServiceEventsMonitorState.get_instance()
        # setUp reset the count to 0; no begin_investigation() here.
        self.assertEqual(impl._investigation_active_count, 0)

        state.record_call_path_entry("func-a", None, 1000)

        self.assertIsNone(state.peek_investigation_data())

    def test_begin_get_balance_active_count(self):
        """begin_investigation increments and get/clear decrements, so the count nets to zero per
        request — and a re-entrant begin in the same context doesn't double-count."""
        state = _ServiceEventsMonitorState.get_instance()

        state.begin_investigation()
        self.assertEqual(impl._investigation_active_count, 1)

        # Re-entrant begin (nested-dispatch analogue) must NOT increment again.
        state.begin_investigation()
        self.assertEqual(impl._investigation_active_count, 1)

        # The single consume decrements back to zero.
        state.get_investigation_data()
        self.assertEqual(impl._investigation_active_count, 0)

        # A redundant clear with nothing present is a no-op (clamped, never negative).
        state.clear_investigation_data()
        self.assertEqual(impl._investigation_active_count, 0)


class TestPythonServiceEventsMonitor(TestCase):
    """Test the PythonServiceEventsMonitor context manager."""

    def setUp(self):
        """Reset state before each test."""
        _ServiceEventsMonitorState._instance = None
        _call_stack.set([])
        set_sampling_mode("always")  # Ensure timing is collected for monitor tests

    @patch("amazon.opentelemetry.serviceevents.python_monitor_impl.time.perf_counter_ns")
    def test_basic_context_manager(self, mock_time):
        """Test basic enter/exit of context manager records a timed call-path entry."""
        mock_time.side_effect = [1000000000, 1100000000]  # 100ms difference

        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        with PythonServiceEventsMonitor("test_func"):
            pass

        inv_data = state.get_investigation_data()
        self.assertIsNotNone(inv_data)
        self.assertEqual(len(inv_data["call_path"]), 1)
        entry = inv_data["call_path"][0]
        self.assertEqual(entry["function_name"], "test_func")
        self.assertIsNone(entry["caller_function_name"])
        # Duration is in nanoseconds (100ms = 100_000_000ns)
        self.assertEqual(entry["duration_ns"], 100_000_000)

    @patch("amazon.opentelemetry.serviceevents.python_monitor_impl.time.perf_counter_ns")
    def test_nested_calls(self, mock_time):
        """Test nested function calls capture the caller relationship in the call path."""
        mock_time.side_effect = [
            1000000000,
            2000000000,  # inner enter
            2500000000,  # inner exit
            3000000000,  # outer exit
        ]

        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        with PythonServiceEventsMonitor("outer_func"):
            with PythonServiceEventsMonitor("inner_func"):
                pass

        inv_data = state.get_investigation_data()
        self.assertIsNotNone(inv_data)

        # Both functions are recorded; the inner one exits (and records) first.
        by_name = {e["function_name"]: e for e in inv_data["call_path"]}
        self.assertIn("outer_func", by_name)
        self.assertIn("inner_func", by_name)

        # Inner function should have outer_func as caller; outer is an entry point.
        self.assertEqual(by_name["inner_func"]["caller_function_name"], "outer_func")
        self.assertIsNone(by_name["outer_func"]["caller_function_name"])

    @patch("amazon.opentelemetry.serviceevents.python_monitor_impl.time.perf_counter_ns")
    def test_exception_handling(self, mock_time):
        """Test that exceptions are captured in investigation data but not suppressed."""
        mock_time.side_effect = [1000000000, 1100000000]

        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        with self.assertRaises(ValueError):
            with PythonServiceEventsMonitor("error_func"):
                raise ValueError("Test error")

        inv_data = state.get_investigation_data()
        self.assertIsNotNone(inv_data)
        # The call is still recorded in the call path...
        self.assertEqual(len(inv_data["call_path"]), 1)
        self.assertEqual(inv_data["call_path"][0]["function_name"], "error_func")
        # ...and the raised exception is captured (without being suppressed).
        self.assertIsNotNone(inv_data["exception"])
        self.assertEqual(inv_data["exception"]["name"], "ValueError")
        self.assertEqual(inv_data["exception"]["function_name"], "error_func")
        # The traceback is formatted to a STRING eagerly (not stored as an
        # (exc_type, exc_value, exc_traceback) tuple) so it cannot pin the frame
        # chain alive in the ContextVar; the formatted text includes the exception.
        traceback_info = inv_data["exception"]["traceback_info"]
        self.assertIsInstance(traceback_info, str)
        self.assertIn("ValueError", traceback_info)
        self.assertIn("Test error", traceback_info)

    def test_exception_does_not_set_dead_exception_info_attr(self):
        """The monitor no longer keeps a self.exception_info attr pinning the traceback."""
        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        monitor = PythonServiceEventsMonitor("error_func")
        with self.assertRaises(ValueError):
            with monitor:
                raise ValueError("boom")

        # The dead instance attribute was removed; exception data flows only through
        # the investigation-data dict, so nothing on the monitor pins the traceback.
        self.assertFalse(hasattr(monitor, "exception_info"))

    @patch("amazon.opentelemetry.serviceevents.python_monitor_impl.time.perf_counter_ns")
    def test_multiple_invocations(self, mock_time):
        """Test multiple invocations of the same function each record a call-path entry."""
        mock_time.side_effect = [
            1000000000,
            1100000000,  # First call: 100ms
            2000000000,
            2300000000,  # Second call: 300ms
            3000000000,
            3150000000,  # Third call: 150ms
        ]

        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        for _ in range(3):
            with PythonServiceEventsMonitor("repeated_func"):
                pass

        inv_data = state.get_investigation_data()
        self.assertIsNotNone(inv_data)
        entries = [e for e in inv_data["call_path"] if e["function_name"] == "repeated_func"]
        self.assertEqual(len(entries), 3)
        # Durations in nanoseconds: 100ms + 300ms + 150ms = 550_000_000ns
        self.assertEqual(sum(e["duration_ns"] for e in entries), 550_000_000)

    def test_call_stack_isolation(self):
        """Test that call stack is properly isolated per context."""
        # Each context should have its own call stack
        _call_stack.set(["initial"])

        with PythonServiceEventsMonitor("func1"):
            stack = _call_stack.get()
            self.assertIn("func1", stack)

        # After exiting, stack should be restored
        stack = _call_stack.get()
        self.assertNotIn("func1", stack)

    @patch("amazon.opentelemetry.serviceevents.python_monitor_impl.time.perf_counter_ns")
    def test_concurrent_monitoring(self, mock_time):
        """Test thread-safe concurrent monitoring of the shared call counters.

        The per-function counter only drives AUTO-mode tiered sampling, so it is only
        maintained in that mode — drive AUTO here to exercise the locked increment path.
        """
        mock_time.return_value = 1000000000

        set_sampling_mode("auto")
        with _call_counters_lock:
            _call_counters.clear()

        def monitor_function(func_id):
            with PythonServiceEventsMonitor(func_id):
                time.sleep(0.001)  # Small delay

        threads = [threading.Thread(target=monitor_function, args=(f"func_{i}",)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All 10 functions should be counted exactly once in the shared counter.
        with _call_counters_lock:
            for i in range(10):
                self.assertEqual(_call_counters.get(f"func_{i}"), 1)

    def test_counter_skipped_outside_auto_mode(self):
        """In non-AUTO modes the call counter is not maintained (hot-path optimization)."""
        with _call_counters_lock:
            _call_counters.clear()

        for mode in ("always", "never"):
            set_sampling_mode(mode)
            with PythonServiceEventsMonitor(f"skip_func_{mode}"):
                pass

        with _call_counters_lock:
            self.assertEqual(_call_counters, {}, "counter must stay empty outside AUTO mode")

    def test_get_instance_class_method(self):
        """Test that get_instance on PythonServiceEventsMonitor returns state."""
        state = PythonServiceEventsMonitor.get_instance()
        self.assertIsInstance(state, _ServiceEventsMonitorState)


class TestCrashSafety(TestCase):
    """The monitor wraps the entire body of every instrumented customer function,
    so __enter__/__exit__ must NEVER propagate a telemetry exception into customer
    control flow (it would crash the function, or worse, mask the customer's own
    in-flight exception)."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        _call_stack.set([])
        set_sampling_mode("always")

    def test_exit_telemetry_failure_does_not_break_successful_call(self):
        """If a recording call raises in __exit__, the customer's normal return is preserved."""
        state = _ServiceEventsMonitorState.get_instance()

        def boom(*args, **kwargs):
            raise RuntimeError("histogram exploded")

        ran = []
        with patch.object(state, "record_function_call_metrics", side_effect=boom):
            with PythonServiceEventsMonitor("ok_func"):
                ran.append(True)

        # The body ran and no telemetry exception escaped the with-block.
        self.assertEqual(ran, [True])

    def test_exit_telemetry_failure_does_not_mask_customer_exception(self):
        """A telemetry failure in __exit__ must not replace the customer's in-flight exception."""
        state = _ServiceEventsMonitorState.get_instance()

        def boom(*args, **kwargs):
            raise RuntimeError("telemetry error that must not surface")

        with patch.object(state, "record_call_path_entry", side_effect=boom):
            with patch.object(state, "record_function_call_metrics", side_effect=boom):
                # The ValueError the customer raised must be what propagates,
                # NOT the telemetry RuntimeError.
                with self.assertRaises(ValueError) as ctx:
                    with PythonServiceEventsMonitor("error_func"):
                        raise ValueError("customer error")
        self.assertEqual(str(ctx.exception), "customer error")

    def test_exit_survives_exception_with_misbehaving_str(self):
        """A customer exception whose __str__ raises must not crash __exit__.

        Requires an active investigation so __exit__ actually reaches the
        ``str(exc_value)`` branch (otherwise the test is vacuous).
        """
        # Activate investigation capture so the str(exc_value) path is exercised.
        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()
        self.assertIsNotNone(state.peek_investigation_data())

        called = {"str": False}

        class EvilError(Exception):
            def __str__(self):
                called["str"] = True
                raise RuntimeError("__str__ exploded")

        # __exit__ calls str(exc_value); the original EvilError must still propagate.
        with self.assertRaises(EvilError):
            with PythonServiceEventsMonitor("evil_func"):
                raise EvilError()

        # Prove the guarded str(exc_value) line was actually hit.
        self.assertTrue(called["str"], "test did not exercise the str(exc_value) branch")

    def test_enter_telemetry_failure_still_runs_customer_body(self):
        """If __enter__ setup raises, the customer body must still execute."""
        ran = []
        # AUTO mode is the only one that calls _increment_call_counter in __enter__, so
        # drive AUTO to make the injected failure actually fire on the setup path.
        set_sampling_mode("auto")
        with patch(
            "amazon.opentelemetry.serviceevents.python_monitor_impl._increment_call_counter",
            side_effect=RuntimeError("counter exploded"),
        ):
            with PythonServiceEventsMonitor("enter_fail_func"):
                ran.append(True)
        self.assertEqual(ran, [True])

    def test_exit_recorder_failure_keeps_call_stack_balanced(self):
        """A recording failure in __exit__ must not leak this frame on the stack."""
        state = _ServiceEventsMonitorState.get_instance()

        def boom(*args, **kwargs):
            raise RuntimeError("recorder exploded")

        _call_stack.set([])
        with patch.object(state, "record_call_path_entry", side_effect=boom):
            with PythonServiceEventsMonitor("solo_func"):
                pass

        # The frame pushed in __enter__ must be popped even though recording failed.
        self.assertEqual(_call_stack.get(), [])

    def test_enter_failure_does_not_pop_parent_frame(self):
        """If a nested __enter__ fails before pushing, its __exit__ must not pop the parent."""
        # AUTO mode is the only one that calls _increment_call_counter in __enter__, which is
        # where this test injects the inner-frame failure.
        set_sampling_mode("auto")
        # Seed a parent frame, then make only the inner monitor's __enter__ fail.
        original = _increment_call_counter

        def selective(function_name):
            if function_name == "inner":
                raise RuntimeError("counter exploded for inner")
            return original(function_name)

        with patch(
            "amazon.opentelemetry.serviceevents.python_monitor_impl._increment_call_counter",
            side_effect=selective,
        ):
            with PythonServiceEventsMonitor("outer"):
                self.assertEqual(_call_stack.get(), ["outer"])
                with PythonServiceEventsMonitor("inner"):
                    # inner.__enter__ failed before pushing, so the stack is unchanged.
                    self.assertEqual(_call_stack.get(), ["outer"])
                # inner.__exit__ must NOT have popped the parent's frame.
                self.assertEqual(_call_stack.get(), ["outer"])
        # After outer exits, the stack is balanced.
        self.assertEqual(_call_stack.get(), [])


class TestSamplingModes(TestCase):
    """Test the always/never/auto sampling modes and validation."""

    def setUp(self):
        """Reset state before each test."""
        _ServiceEventsMonitorState._instance = None
        _call_stack.set([])
        _current_operation.set(None)
        set_sampling_mode("always")

    def tearDown(self):
        """Restore sampling mode after each test."""
        set_sampling_mode("always")
        _current_operation.set(None)

    def test_existing_modes_unchanged(self):
        """Existing sampling modes should work as before."""
        _current_operation.set("endpoint-123")

        set_sampling_mode("always")
        self.assertTrue(_should_sample(1))

        set_sampling_mode("never")
        self.assertFalse(_should_sample(1))

    def test_invalid_sampling_mode_rejected(self):
        """Invalid sampling mode should raise ValueError."""
        with self.assertRaises(ValueError):
            set_sampling_mode("invalid")

    def test_removed_adaptive_mode_rejected(self):
        """The removed 'adaptive' mode is no longer valid and must be rejected."""
        with self.assertRaises(ValueError):
            set_sampling_mode("adaptive")


class TestSamplingThresholds(TestCase):
    """Test configurable sampling thresholds."""

    def setUp(self):
        """Reset thresholds to defaults before each test."""
        set_sampling_thresholds(
            tier1_threshold=100,
            tier2_threshold=1000,
            tier2_rate=10,
            tier3_rate=100,
        )
        set_sampling_mode("auto")

    def tearDown(self):
        """Restore defaults."""
        set_sampling_thresholds(
            tier1_threshold=100,
            tier2_threshold=1000,
            tier2_rate=10,
            tier3_rate=100,
        )
        set_sampling_mode("always")

    def test_set_sampling_thresholds_affects_auto_mode(self):
        """Test that custom thresholds change auto-mode sampling behavior."""
        # With default tier1=100, call 50 should be sampled
        self.assertTrue(_should_sample(50))

        # Set tier1 threshold to 10 — call 50 now falls in tier2
        set_sampling_thresholds(tier1_threshold=10, tier2_threshold=100)
        # Call 50 in tier2 (10 < 50 <= 100): sampled only if 50 % tier2_rate == 0
        self.assertTrue(_should_sample(50))  # 50 % 10 == 0
        self.assertFalse(_should_sample(51))  # 51 % 10 != 0

    def test_set_sampling_thresholds_tier3_rate(self):
        """Test that custom tier3 rate is applied."""
        set_sampling_thresholds(tier1_threshold=10, tier2_threshold=20, tier3_rate=50)
        # Call 100 is in tier3 (> 20): sampled if 100 % 50 == 0
        self.assertTrue(_should_sample(100))
        self.assertFalse(_should_sample(101))

    def test_zero_rate_samples_none_in_tier_without_crashing(self):
        """A non-positive tier rate (only reachable via the unvalidated test-config hook) must
        degrade to 'sample none in this tier' rather than raise ZeroDivisionError. Mirrors Java/JS."""
        set_sampling_thresholds(tier1_threshold=100, tier2_threshold=1000, tier2_rate=0, tier3_rate=0)
        # tier1 unaffected by the rates.
        self.assertTrue(_should_sample(1))
        # tier2 (100 < n <= 1000) and tier3 (n > 1000): zero rate → no crash, sample none.
        self.assertFalse(_should_sample(500))
        self.assertFalse(_should_sample(5000))

    def test_default_thresholds_unchanged_without_call(self):
        """Test that defaults match the original hardcoded values."""
        self.assertEqual(impl._sample_tier1_threshold, 100)
        self.assertEqual(impl._sample_tier2_threshold, 1000)
        self.assertEqual(impl._sample_tier2_rate, 10)
        self.assertEqual(impl._sample_tier3_rate, 100)


class TestHistogramWiringIntegration(TestCase):
    """Integration tests for the full histogram wiring path:

    1. Create a real MeterProvider + InMemoryMetricReader (mirrors production setup).
    2. Create a `service.function.duration` histogram on the meter.
    3. Wire it into _ServiceEventsMonitorState via set_function_duration_histogram().
    4. Drive PythonServiceEventsMonitor.__exit__ via the context manager.
    5. Read the metric data back through the in-memory reader and assert
       attributes, durations, and status values.

    These tests catch wiring regressions that the helper-based contract tests
    cannot — for example, if __exit__ stops calling record_function_call_metrics(),
    or set_function_duration_histogram() forgets to store base_attrs.
    """

    def setUp(self):
        # Reset singleton + sampling state so tests can't leak across each other.
        _ServiceEventsMonitorState._instance = None
        set_sampling_mode("always")
        _call_stack.set([])
        _current_operation.set(None)

        # Build a real MeterProvider with an in-memory reader.
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation, View
        from opentelemetry.sdk.resources import Resource

        self._metric_reader = InMemoryMetricReader()
        view = View(
            instrument_name="service.function.duration",
            aggregation=ExponentialBucketHistogramAggregation(),
        )
        self._meter_provider = MeterProvider(
            resource=Resource.create({"service.name": "wiring-test"}),
            metric_readers=[self._metric_reader],
            views=[view],
        )

        # Mirror the production wiring done by ServiceEventsInstrumentation.initialize():
        # create the histogram once on the meter and store it on monitor state with
        # the same base attrs the runtime would.
        meter = self._meter_provider.get_meter("serviceevents", "1.0")
        histogram = meter.create_histogram(
            "service.function.duration",
            unit="Microseconds",
            description="Function call duration",
        )
        # Service identity (service.name, environment, deployment, vcs.*) lives
        # on the Resource attached to the MeterProvider above. The per-call
        # base_attrs only carry signal-level identifiers.
        base_attrs = {
            "Telemetry.Source": "ServiceEvents",
        }
        self._state = _ServiceEventsMonitorState.get_instance()
        self._state.set_metric_base_attrs(base_attrs)
        self._state.set_function_duration_histogram(histogram)

    def tearDown(self):
        self._meter_provider.shutdown()
        _ServiceEventsMonitorState._instance = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_duration_metric(self):
        """Return (metric, scope) for service.function.duration, or (None, None)."""
        data = self._metric_reader.get_metrics_data()
        if data is None:
            return None, None
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    if metric.name == "service.function.duration":
                        return metric, sm.scope
        return None, None

    def _data_points_by_function(self, metric, function_name):
        return [dp for dp in metric.data.data_points if dict(dp.attributes).get("function.name") == function_name]

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_histogram_wired_into_monitor_state(self):
        """set_function_duration_histogram populates state used by __exit__."""
        self.assertIsNotNone(self._state._function_duration_histogram)
        self.assertEqual(self._state._metric_base_attrs["Telemetry.Source"], "ServiceEvents")
        # Service identity is on the Resource, not the per-call dimension set.
        self.assertNotIn("service.name", self._state._metric_base_attrs)
        self.assertNotIn("environment", self._state._metric_base_attrs)

    def test_exit_records_to_wired_histogram_success(self):
        """A successful with-block records a data point with status=success."""
        with PythonServiceEventsMonitor("mod.func_success"):
            pass

        metric, scope = self._get_duration_metric()
        self.assertIsNotNone(metric, "Expected service.function.duration metric to be present")
        self.assertEqual(scope.name, "serviceevents")

        dps = self._data_points_by_function(metric, "mod.func_success")
        self.assertEqual(len(dps), 1, "Expected exactly one data point for func_success")
        attrs = dict(dps[0].attributes)
        self.assertEqual(attrs.get("status"), "success")
        self.assertEqual(attrs.get("Telemetry.Source"), "ServiceEvents")
        # service.name + environment ride along on the Resource, not per-call attrs.
        self.assertNotIn("service.name", attrs)
        self.assertNotIn("environment", attrs)
        self.assertNotIn("exception.type", attrs)
        # Duration should be > 0 even though the body is empty (perf_counter resolution).
        self.assertGreaterEqual(dps[0].sum, 0)
        self.assertEqual(dps[0].count, 1)

    def test_exit_records_to_wired_histogram_exception(self):
        """An exception inside the context propagates and emits status=error."""
        with self.assertRaises(ValueError):
            with PythonServiceEventsMonitor("mod.func_error"):
                raise ValueError("boom")

        metric, _ = self._get_duration_metric()
        self.assertIsNotNone(metric)
        dps = self._data_points_by_function(metric, "mod.func_error")
        self.assertEqual(len(dps), 1)
        attrs = dict(dps[0].attributes)
        self.assertEqual(attrs.get("status"), "error")
        # Exception class name is intentionally NOT a histogram dimension —
        # it lives on the IncidentSnapshot log signal so cardinality stays bounded.
        self.assertNotIn("exception.type", attrs)

    def test_exit_records_caller_attribute(self):
        """Nested with-blocks emit the outer function as the caller of the inner one."""
        with PythonServiceEventsMonitor("mod.outer"):
            with PythonServiceEventsMonitor("mod.inner"):
                pass

        metric, _ = self._get_duration_metric()
        self.assertIsNotNone(metric)

        inner_dps = self._data_points_by_function(metric, "mod.inner")
        self.assertEqual(len(inner_dps), 1)
        self.assertEqual(dict(inner_dps[0].attributes).get("aws.service_events.caller"), "mod.outer")

        outer_dps = self._data_points_by_function(metric, "mod.outer")
        self.assertEqual(len(outer_dps), 1)
        self.assertNotIn("aws.service_events.caller", dict(outer_dps[0].attributes))

    def test_exit_aggregates_multiple_calls_into_one_data_point(self):
        """Same function_name + same status collapses into one data point with count=N."""
        for _ in range(5):
            with PythonServiceEventsMonitor("mod.repeated"):
                pass

        metric, _ = self._get_duration_metric()
        self.assertIsNotNone(metric)
        dps = self._data_points_by_function(metric, "mod.repeated")
        self.assertEqual(len(dps), 1, "Expected the 5 calls to aggregate into one data point")
        self.assertEqual(dps[0].count, 5)

    def test_exit_no_ops_when_histogram_not_wired(self):
        """When no histogram is wired, __exit__ records nothing and does not crash.

        There is no SEH/EMF fallback: function-call duration is recorded only into
        the OTel histogram, so an un-wired instrument simply no-ops.
        """
        # Reset so no histogram is wired.
        _ServiceEventsMonitorState._instance = None
        state = _ServiceEventsMonitorState.get_instance()
        self.assertIsNone(state._function_duration_histogram)

        # Must not raise even though no instrument is wired.
        with PythonServiceEventsMonitor("mod.no_histogram"):
            pass

        # No metric reader is attached to this un-wired state, and the call simply
        # no-ops — there is no fallback aggregation store to inspect.
        self.assertFalse(hasattr(state, "_aggregations"))


class TestSamplingAwareHistogramWiring(TestCase):
    """Verify the sampling contract for ``service.function.duration``.

    The pre-existing TestHistogramWiringIntegration class only exercises the
    ``always`` sampling mode, so it can't catch a regression where the
    histogram starts recording zero-duration entries for non-sampled calls
    again. These tests close that gap.
    """

    def setUp(self):
        # Reset singleton + sampling state so tests can't leak across each other.
        _ServiceEventsMonitorState._instance = None
        set_sampling_mode("always")
        _call_stack.set([])
        _current_operation.set(None)

        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation, View
        from opentelemetry.sdk.resources import Resource

        self._metric_reader = InMemoryMetricReader()
        view = View(
            instrument_name="service.function.duration",
            aggregation=ExponentialBucketHistogramAggregation(),
        )
        self._meter_provider = MeterProvider(
            resource=Resource.create({"service.name": "wiring-test"}),
            metric_readers=[self._metric_reader],
            views=[view],
        )

        meter = self._meter_provider.get_meter("serviceevents", "1.0")
        histogram = meter.create_histogram(
            "service.function.duration",
            unit="Microseconds",
            description="Function call duration",
        )
        # Service identity (service.name, environment, deployment, vcs.*) lives
        # on the Resource attached to the MeterProvider above. The per-call
        # base_attrs only carry signal-level identifiers.
        base_attrs = {
            "Telemetry.Source": "ServiceEvents",
        }
        self._state = _ServiceEventsMonitorState.get_instance()
        self._state.set_metric_base_attrs(base_attrs)
        self._state.set_function_duration_histogram(histogram)

    def tearDown(self):
        self._meter_provider.shutdown()
        _ServiceEventsMonitorState._instance = None
        set_sampling_mode("always")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_histogram(self):
        """Return (metric, scope) for service.function.duration, or (None, None)."""
        data = self._metric_reader.get_metrics_data()
        if data is None:
            return None, None
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    if metric.name == "service.function.duration":
                        return metric, sm.scope
        return None, None

    def _data_points_by_function(self, metric, function_name):
        return [dp for dp in metric.data.data_points if dict(dp.attributes).get("function.name") == function_name]

    # ------------------------------------------------------------------
    # Sampling-aware recording
    # ------------------------------------------------------------------

    def test_non_sampled_calls_skip_histogram(self):
        """In 'never' mode the histogram must stay silent.

        This is the core regression guard for the duration-pollution bug:
        non-sampled calls must NOT produce zero-duration histogram entries.
        """
        set_sampling_mode("never")

        for _ in range(7):
            with PythonServiceEventsMonitor("mod.never_sampled"):
                pass

        metric, _ = self._get_histogram()
        if metric is not None:
            histogram_dps = self._data_points_by_function(metric, "mod.never_sampled")
            self.assertEqual(
                len(histogram_dps),
                0,
                "Histogram must not record non-sampled calls (would pollute sum/percentiles with zeros)",
            )

    def test_mixed_sampling_histogram_count_matches_sampled_only(self):
        """Histogram count tracks the sampled subset, not total invocations."""
        # 3 sampled calls
        set_sampling_mode("always")
        for _ in range(3):
            with PythonServiceEventsMonitor("mod.mixed"):
                pass

        # 5 non-sampled calls
        set_sampling_mode("never")
        for _ in range(5):
            with PythonServiceEventsMonitor("mod.mixed"):
                pass

        metric, _ = self._get_histogram()
        self.assertIsNotNone(metric)
        histogram_dps = self._data_points_by_function(metric, "mod.mixed")

        # Histogram only sees the 3 sampled calls.
        self.assertEqual(sum(dp.count for dp in histogram_dps), 3)

    def test_non_sampled_call_has_no_aggregation_fallback(self):
        """A non-sampled call must not record anywhere but the (skipped) histogram.

        The legacy SEH/EMF aggregation path has been removed entirely, so there
        is structurally no second sink that could resurrect the old
        ``aws.service_events.function_call`` log signal and double-count
        downstream. This guards that the fallback store stays gone.
        """
        set_sampling_mode("never")

        with PythonServiceEventsMonitor("mod.no_double_path"):
            pass

        # No aggregation store exists, and the histogram skipped the non-sampled call.
        self.assertFalse(hasattr(self._state, "_aggregations"))
        metric, _ = self._get_histogram()
        if metric is not None:
            self.assertEqual(len(self._data_points_by_function(metric, "mod.no_double_path")), 0)

    def test_sampled_error_lands_on_histogram(self):
        """A sampled error adds one histogram entry tagged status=error.
        Exception class name is intentionally not a histogram dimension."""
        with self.assertRaises(ValueError):
            with PythonServiceEventsMonitor("mod.err_sampled"):
                raise ValueError("boom")

        metric, _ = self._get_histogram()
        self.assertIsNotNone(metric)
        histogram_dps = self._data_points_by_function(metric, "mod.err_sampled")

        self.assertEqual(len(histogram_dps), 1)
        attrs = dict(histogram_dps[0].attributes)
        self.assertEqual(attrs.get("status"), "error")
        self.assertNotIn("exception.type", attrs)

    def test_non_sampled_error_skipped_on_histogram(self):
        """An error in 'never' mode must not record a zero-duration error
        entry on the histogram."""
        set_sampling_mode("never")

        with self.assertRaises(RuntimeError):
            with PythonServiceEventsMonitor("mod.err_unsampled"):
                raise RuntimeError("nope")

        metric, _ = self._get_histogram()
        if metric is not None:
            histogram_dps = self._data_points_by_function(metric, "mod.err_unsampled")
            self.assertEqual(len(histogram_dps), 0)


class TestModuleStateFunctions(TestCase):
    """Test the module-level getters and the after-fork state reset."""

    def setUp(self):
        """Reset module + singleton state before each test."""
        _ServiceEventsMonitorState._instance = None
        _call_stack.set([])
        _current_operation.set(None)
        with _call_counters_lock:
            _call_counters.clear()
        set_sampling_mode("always")

    def tearDown(self):
        """Restore defaults so this class can't leak into other suites."""
        set_sampling_mode("always")
        _call_stack.set([])
        _current_operation.set(None)
        with _call_counters_lock:
            _call_counters.clear()

    def test_get_sampling_mode_returns_current_mode(self):
        """get_sampling_mode reflects the mode set via set_sampling_mode."""
        set_sampling_mode("never")
        self.assertEqual(get_sampling_mode(), "never")

    def test_get_current_operation_returns_set_operation(self):
        """get_current_operation returns the operation set on the context."""
        set_current_operation("GET /widgets")
        self.assertEqual(get_current_operation(), "GET /widgets")

    def test_get_call_stack_returns_copy_of_stack(self):
        """get_call_stack returns the current frames as a fresh list."""
        _call_stack.set(["a", "b"])

        stack = get_call_stack()

        self.assertEqual(stack, ["a", "b"])
        # It must be a copy, not the underlying context-var list.
        self.assertIsNot(stack, _call_stack.get())

    def test_get_call_stack_handles_none(self):
        """get_call_stack returns [] when the context var holds None."""
        _call_stack.set(None)
        self.assertEqual(get_call_stack(), [])

    def test_reset_after_fork_clears_mutable_state(self):
        """reset_after_fork restores defaults and clears all mutable state."""
        # Dirty every piece of mutable state the reset is responsible for.
        set_sampling_mode("never")
        with _call_counters_lock:
            _call_counters["fork_func"] = 7
        _call_stack.set(["frame"])
        _current_operation.set("GET /forked")

        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()
        self.assertIsNotNone(state.peek_investigation_data())

        reset_after_fork()

        # Sampling mode falls back to the default "always".
        self.assertEqual(get_sampling_mode(), "always")
        # Counters are emptied.
        with _call_counters_lock:
            self.assertEqual(_call_counters, {})
        # Thread-local state is reset.
        self.assertEqual(_call_stack.get(), [])
        self.assertIsNone(_current_operation.get())
        # The singleton's identity is preserved, only its investigation data cleared.
        self.assertIs(_ServiceEventsMonitorState.get_instance(), state)
        self.assertIsNone(state.peek_investigation_data())

    def test_reset_after_fork_without_singleton_is_safe(self):
        """reset_after_fork no-ops the singleton branch when no instance exists."""
        _ServiceEventsMonitorState._instance = None

        # Must not raise even though no singleton has been created yet.
        reset_after_fork()

        self.assertIsNone(_ServiceEventsMonitorState._instance)
        self.assertEqual(get_sampling_mode(), "always")


class TestMonitorEdgeBranches(TestCase):
    """Cover defensive edge branches in __enter__/__exit__ and metric attribution."""

    def setUp(self):
        """Reset state before each test."""
        _ServiceEventsMonitorState._instance = None
        _call_stack.set([])
        _current_operation.set(None)
        set_sampling_mode("always")

    def tearDown(self):
        """Restore defaults."""
        set_sampling_mode("always")
        _call_stack.set([])

    def test_enter_handles_none_call_stack(self):
        """__enter__ treats a None call stack as empty and still pushes the frame."""
        _call_stack.set(None)

        monitor = PythonServiceEventsMonitor("mod.none_stack")
        monitor.__enter__()
        try:
            # With no prior frames, this call is an entry point (no caller).
            self.assertIsNone(monitor.caller)
            self.assertEqual(_call_stack.get(), ["mod.none_stack"])
        finally:
            monitor.__exit__(None, None, None)

        # Frame popped cleanly on exit.
        self.assertEqual(_call_stack.get(), [])

    def test_exit_swallows_pop_failure(self):
        """If reading the stack during the finally pop raises, __exit__ must not propagate."""
        # Enter normally so _pushed is True and the finally block runs the pop.
        monitor = PythonServiceEventsMonitor("mod.pop_fail")
        monitor.__enter__()
        self.assertTrue(monitor._pushed)

        # Swap the module-level call-stack ContextVar for one whose get() raises.
        # __exit__'s recording body does not touch _call_stack, so only the
        # finally-block stack read hits this fault.
        broken_stack = MagicMock()
        broken_stack.get.side_effect = RuntimeError("context var read exploded")
        with patch(
            "amazon.opentelemetry.serviceevents.python_monitor_impl._call_stack",
            broken_stack,
        ):
            # Must return False (does not suppress) and must not raise the telemetry error.
            result = monitor.__exit__(None, None, None)

        self.assertFalse(result)
        broken_stack.get.assert_called_once()

    def test_metrics_attach_function_info_attributes(self):
        """record_function_call_metrics adds function_at_line + async from the registry."""
        state = _ServiceEventsMonitorState.get_instance()
        state.set_metric_base_attrs({"Telemetry.Source": "ServiceEvents"})

        recorded = {}

        histogram = MagicMock()
        histogram.record.side_effect = lambda value, attrs: recorded.update(attrs)
        state.set_function_duration_histogram(histogram)

        with patch(
            "amazon.opentelemetry.serviceevents.python_monitor_impl.get_function_info_unlocked",
            return_value={"line": 42, "is_async": True},
        ):
            state.record_function_call_metrics(
                function_name="mod.async_func",
                duration_ns=1000,
                caller=None,
                exception_name=None,
                is_sampled=True,
            )

        histogram.record.assert_called_once()
        self.assertEqual(recorded.get("aws.service_events.function_at_line"), 42)
        self.assertTrue(recorded.get("aws.service_events.async"))
        self.assertEqual(recorded.get("status"), "success")

    def test_metrics_skip_function_info_when_fields_absent(self):
        """A registry hit with no line/async info adds neither attribute."""
        state = _ServiceEventsMonitorState.get_instance()
        state.set_metric_base_attrs({"Telemetry.Source": "ServiceEvents"})

        recorded = {}
        histogram = MagicMock()
        histogram.record.side_effect = lambda value, attrs: recorded.update(attrs)
        state.set_function_duration_histogram(histogram)

        with patch(
            "amazon.opentelemetry.serviceevents.python_monitor_impl.get_function_info_unlocked",
            return_value={"line": None, "is_async": False},
        ):
            state.record_function_call_metrics(
                function_name="mod.plain_func",
                duration_ns=1000,
                caller=None,
                exception_name=None,
                is_sampled=True,
            )

        histogram.record.assert_called_once()
        self.assertNotIn("aws.service_events.function_at_line", recorded)
        self.assertNotIn("aws.service_events.async", recorded)
