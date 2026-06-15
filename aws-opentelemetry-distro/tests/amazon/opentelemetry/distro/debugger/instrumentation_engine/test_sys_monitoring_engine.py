# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
from unittest import mock

from test_instrumentation_engine import InstrumentationEngineTestBase

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger.instrumentation_engine import _sys_monitoring_engine
from amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine import _TOOL_NAME


@unittest.skipIf(sys.version_info < (3, 12), "SysMonitoringEngine requires Python 3.12+")
class TestSysMonitoringEngine(InstrumentationEngineTestBase):

    def _setup_engine(self):
        # Ensure tool ID is free before each test (guards against cross-test contamination)
        try:
            if sys.monitoring.get_tool(sys.monitoring.DEBUGGER_ID) is not None:
                sys.monitoring.free_tool_id(sys.monitoring.DEBUGGER_ID)
        except Exception:
            pass
        self.engine = _sys_monitoring_engine.SysMonitoringEngine()

    def tearDown(self):
        super().tearDown()
        # Defensive: always free tool ID after each test
        try:
            if sys.monitoring.get_tool(sys.monitoring.DEBUGGER_ID) is not None:
                sys.monitoring.free_tool_id(sys.monitoring.DEBUGGER_ID)
        except Exception:
            pass

    def test_initialization(self):
        """Test initialization, tool registration, and callback registration."""
        with mock.patch("sys.monitoring.use_tool_id") as mock_use_tool, mock.patch(
            "sys.monitoring.register_callback"
        ) as mock_register, mock.patch("sys.monitoring.set_events") as mock_set_events:

            self.engine.initialize(hit_count_callback=self.callback)

            mock_use_tool.assert_called_once_with(sys.monitoring.DEBUGGER_ID, _TOOL_NAME)
            # Four event callbacks: LINE for line BPs; PY_START / PY_RETURN / PY_UNWIND
            # for function-level (PROBE / function-level BREAKPOINT).
            mock_register.assert_any_call(
                sys.monitoring.DEBUGGER_ID, sys.monitoring.events.LINE, self.engine._line_event_handler
            )
            mock_register.assert_any_call(
                sys.monitoring.DEBUGGER_ID,
                sys.monitoring.events.PY_START,
                self.engine._function_start_event_handler,
            )
            mock_register.assert_any_call(
                sys.monitoring.DEBUGGER_ID,
                sys.monitoring.events.PY_RETURN,
                self.engine._function_return_event_handler,
            )
            mock_register.assert_any_call(
                sys.monitoring.DEBUGGER_ID,
                sys.monitoring.events.PY_UNWIND,
                self.engine._function_unwind_event_handler,
            )
            self.assertEqual(mock_register.call_count, 4)
            # PY_UNWIND is set globally (not local-event-capable on 3.12-3.14).
            mock_set_events.assert_called_once_with(sys.monitoring.DEBUGGER_ID, sys.monitoring.events.PY_UNWIND)

            self.assertTrue(self.engine._initialized)
            self.assertEqual(self.engine._hit_count_callback, self.callback)
            self.assertEqual(self.engine.tool_id, sys.monitoring.DEBUGGER_ID)

    def test_initialization_fails_with_existing_tool(self):
        """Test that initialization SHOULD fail when another tool is already registered."""
        sys.monitoring.use_tool_id(sys.monitoring.DEBUGGER_ID, "ExistingTool")
        try:
            existing_tool = sys.monitoring.get_tool(sys.monitoring.DEBUGGER_ID)
            self.assertEqual(existing_tool, "ExistingTool")

            self.engine.initialize(hit_count_callback=self.callback)

            self.assertFalse(self.engine._initialized)
            self.assertIsNone(self.engine._hit_count_callback)

            current_tool = sys.monitoring.get_tool(sys.monitoring.DEBUGGER_ID)
            self.assertEqual(current_tool, "ExistingTool")
        finally:
            sys.monitoring.free_tool_id(sys.monitoring.DEBUGGER_ID)

    def test_initialization_reuses_our_own_existing_tool(self):
        """Initialization SHOULD succeed when the id is already registered to our own tool name.

        After os.fork() (gunicorn/uWSGI prefork) the child inherits the parent's tool-id
        registration. That is OURS, not a conflict — the fresh engine must reuse it (rebinding
        its callback) rather than bail, otherwise line breakpoints silently die in every worker.
        """
        sys.monitoring.use_tool_id(sys.monitoring.DEBUGGER_ID, _TOOL_NAME)
        try:
            # use_tool_id must NOT be called again (the id is already ours), but the callback
            # MUST be re-registered so it binds to this fresh engine instance.
            with mock.patch("sys.monitoring.use_tool_id") as mock_use_tool, mock.patch(
                "sys.monitoring.register_callback"
            ) as mock_register:
                self.engine.initialize(hit_count_callback=self.callback)

                mock_use_tool.assert_not_called()
                # All four callbacks must rebind to the fresh engine instance after fork.
                mock_register.assert_any_call(
                    sys.monitoring.DEBUGGER_ID, sys.monitoring.events.LINE, self.engine._line_event_handler
                )
                mock_register.assert_any_call(
                    sys.monitoring.DEBUGGER_ID,
                    sys.monitoring.events.PY_START,
                    self.engine._function_start_event_handler,
                )
                mock_register.assert_any_call(
                    sys.monitoring.DEBUGGER_ID,
                    sys.monitoring.events.PY_RETURN,
                    self.engine._function_return_event_handler,
                )
                mock_register.assert_any_call(
                    sys.monitoring.DEBUGGER_ID,
                    sys.monitoring.events.PY_UNWIND,
                    self.engine._function_unwind_event_handler,
                )
                self.assertEqual(mock_register.call_count, 4)

            self.assertTrue(self.engine._initialized)
            self.assertEqual(self.engine._hit_count_callback, self.callback)
        finally:
            sys.monitoring.free_tool_id(sys.monitoring.DEBUGGER_ID)

    def test_initialization_failure(self):
        """Test that cleanup is called when initialization fails."""
        # Only fail for OUR tool id; pass other callers (e.g. coverage.py's own sys.monitoring
        # backend on Python 3.14) through to the real implementation.
        real_register_callback = sys.monitoring.register_callback

        def fail_for_our_tool(tool_id, *args, **kwargs):
            if tool_id == self.engine.tool_id:
                raise RuntimeError("Callback registration failed")
            return real_register_callback(tool_id, *args, **kwargs)

        with mock.patch("sys.monitoring.register_callback", side_effect=fail_for_our_tool):
            self.engine.initialize(hit_count_callback=self.callback)

            self.assertFalse(self.engine._initialized)
            self.assertIsNone(self.engine._hit_count_callback)

    def test_enable_breakpoints_without_initialization(self):
        """Test enabling breakpoints without initialization."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 1}, function_key="test.func"
        )

        # Should not crash, but should not enable breakpoints
        self.assertEqual(len(self.engine._breakpoints), 0)

    def test_enable_breakpoints_empty_line_numbers(self):
        """Test enabling breakpoints with empty line numbers set."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(code=code, func=func, line_numbers=set(), function_key="test.func")

        self.assertEqual(len(self.engine._breakpoints), 0)

    def test_enable_breakpoints_success(self):
        """Test enabling breakpoints succeeds and stores state."""
        func = self._create_test_function()
        original_code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={original_code.co_firstlineno + 2},
            function_key="test.module.test_func",
        )

        code_id = id(original_code)
        self.assertIn(code_id, self.engine._breakpoints)
        self.assertIn(original_code.co_firstlineno + 2, self.engine._breakpoints[code_id])
        self.assertEqual(self.engine._function_keys[code_id], "test.module.test_func")

    def test_enable_breakpoints_triggers_line_handler(self):
        """Test enabling breakpoints triggers custom line event handler on execution."""
        func = self._create_test_function()
        original_code = func.__code__

        with mock.patch.object(
            self.engine, "_line_event_handler", wraps=self.engine._line_event_handler
        ) as mock_handler:
            self.engine.initialize()
            self.engine.enable_breakpoints_for_function(
                code=original_code,
                func=func,
                line_numbers={original_code.co_firstlineno + 2},
                function_key="test.func",
            )
            func(5, 3)
            mock_handler.assert_called()

    def test_enable_breakpoints_exception_handling(self):
        """Test enabling breakpoints handles exceptions gracefully."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize()
        # Only fail for OUR tool id; pass other callers (e.g. coverage.py's own sys.monitoring
        # backend on Python 3.14) through to the real implementation. See
        # test_disable_breakpoints_exception_handling for details.
        real_set_local_events = sys.monitoring.set_local_events

        def fail_for_our_tool(tool_id, *args, **kwargs):
            if tool_id == self.engine.tool_id:
                raise RuntimeError("set_local_events error")
            return real_set_local_events(tool_id, *args, **kwargs)

        with mock.patch("sys.monitoring.set_local_events", side_effect=fail_for_our_tool):
            self.engine.enable_breakpoints_for_function(
                code=code, func=func, line_numbers={code.co_firstlineno + 2}, function_key="test.func"
            )
        # Should not crash, engine still initialized, no partial state
        self.assertTrue(self.engine._initialized)
        self.assertEqual(len(self.engine._breakpoints), 0)

    def test_disable_breakpoints_without_initialization(self):
        """Test disabling breakpoints without initialization."""
        func = self._create_test_function()
        self.engine.disable_breakpoints_for_function(code=func.__code__, func=func)
        self.assertFalse(self.engine._initialized)
        self.assertEqual(len(self.engine._breakpoints), 0)

    def test_disable_breakpoints_no_existing_breakpoints(self):
        """Test disabling breakpoints when none exist."""
        func = self._create_test_function()
        self.engine.initialize()
        self.engine.disable_breakpoints_for_function(code=func.__code__, func=func)
        self.assertEqual(len(self.engine._breakpoints), 0)

    def test_disable_breakpoints_success(self):
        """Test disabling breakpoints clears state."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 2}, function_key="test.func"
        )

        code_id = id(code)
        self.assertIn(code_id, self.engine._breakpoints)

        self.engine.disable_breakpoints_for_function(code=code, func=func)

        self.assertNotIn(code_id, self.engine._breakpoints)
        self.assertNotIn(code_id, self.engine._function_keys)

    def test_disable_breakpoints_exception_handling(self):
        """Test disabling breakpoints handles exceptions gracefully."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 2}, function_key="test.func"
        )

        # Only fail for OUR tool id. On Python 3.14, coverage.py uses sys.monitoring itself, so a
        # blanket side_effect would also break coverage's own set_local_events callbacks; scope the
        # failure to the engine's tool id and pass everything else through to the real implementation.
        real_set_local_events = sys.monitoring.set_local_events

        def fail_for_our_tool(tool_id, *args, **kwargs):
            if tool_id == self.engine.tool_id:
                raise RuntimeError("set_local_events error")
            return real_set_local_events(tool_id, *args, **kwargs)

        with mock.patch("sys.monitoring.set_local_events", side_effect=fail_for_our_tool):
            self.engine.disable_breakpoints_for_function(code=code, func=func)
        # Should not crash, engine still initialized
        self.assertTrue(self.engine._initialized)

    def test_line_event_handler_no_breakpoint_set(self):
        """Test line event handler returns DISABLE when no breakpoint set exists."""
        func = self._create_test_function()
        code = func.__code__
        tracer, exporter = self._create_test_tracer()

        self.engine.initialize()

        with tracer.start_as_current_span("test_span"):
            result = self.engine._line_event_handler(code, 10)

        self.assertEqual(result, sys.monitoring.DISABLE)
        spans = exporter.get_finished_spans()
        self.assertEqual(len(spans[0].events), 0)

    def test_line_event_handler_line_not_in_breakpoints(self):
        """Test line event handler returns DISABLE when line number not in breakpoint set."""
        func = self._create_test_function()
        code = func.__code__
        tracer, exporter = self._create_test_tracer()

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 2}, function_key="test.func"
        )

        with tracer.start_as_current_span("test_span"):
            result = self.engine._line_event_handler(code, 9999)

        self.assertEqual(result, sys.monitoring.DISABLE)
        spans = exporter.get_finished_spans()
        self.assertEqual(len(spans[0].events), 0)

    def test_line_event_handler_exception_no_span_event(self):
        """Test line event handler creates no span event when exception occurs."""
        func = self._create_test_function()
        code = func.__code__
        tracer, exporter = self._create_test_tracer()

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 2}, function_key="test.func"
        )

        with tracer.start_as_current_span("test_span"):
            with mock.patch.object(self.engine, "_handle_breakpoint", side_effect=RuntimeError("test error")):
                result = self.engine._line_event_handler(code, code.co_firstlineno + 2)

        # Handler returns None on error to continue monitoring (not DISABLE)
        self.assertIsNone(result)
        spans = exporter.get_finished_spans()
        self.assertEqual(len(spans[0].events), 0)

    def test_handle_breakpoint_no_active_span(self):
        """Test _handle_breakpoint still produces snapshot when no active span exists."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 2}, function_key="test.func"
        )

        # No span context — snapshot should still be created, hit count called
        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            self.engine._handle_breakpoint(code, code.co_firstlineno + 2)
            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            # trace should be absent when no active span
            self.assertNotIn("trace", snapshot)
        self.callback.assert_called_once()

    def test_line_event_handler_captures_local_variables(self):
        """Test that local variables are captured in snapshot."""
        tracer, _ = self._create_test_tracer()

        def func_with_locals():
            x = 42
            _ = "test"  # name variable not used in test
            result = x + 1
            return result

        code = func_with_locals.__code__
        breakpoint_line = code.co_firstlineno + 3

        capture_config = CaptureConfig(capture_locals=[])  # [] = capture all
        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func_with_locals,
            line_numbers={breakpoint_line},
            function_key="test.func_with_locals",
            line_capture_configs={breakpoint_line: capture_config},
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                func_with_locals()

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            # Verify line-level captures
            self.assertIn("captures", snapshot)
            self.assertIn("lines", snapshot["captures"])
            line_key = str(breakpoint_line)
            self.assertIn(line_key, snapshot["captures"]["lines"])
            locals_dict = snapshot["captures"]["lines"][line_key].get("locals", {})
            self.assertIn("x", locals_dict)
            self.assertEqual(locals_dict["x"]["value"], "42")

    def test_line_event_handler_filters_imports(self):
        """Test that functions, modules, classes, methods, and builtins are filtered from local variables."""
        tracer, _ = self._create_test_tracer()
        import math

        class MyClass:
            def my_method(self):
                pass

        def my_func():
            pass

        obj = MyClass()

        def func_with_imports():
            local_module = math  # noqa: F841
            local_func = my_func  # noqa: F841
            local_class = MyClass  # noqa: F841
            local_method = obj.my_method  # noqa: F841
            local_builtin = len  # noqa: F841
            x = 10
            y = x + 1  # breakpoint here
            return y

        code = func_with_imports.__code__
        breakpoint_line = code.co_firstlineno + 7

        capture_config = CaptureConfig(capture_locals=[])  # [] = capture all
        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func_with_imports,
            line_numbers={breakpoint_line},
            function_key="test.func",
            line_capture_configs={breakpoint_line: capture_config},
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                func_with_imports()

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            line_key = str(breakpoint_line)
            locals_dict = snapshot["captures"]["lines"][line_key].get("locals", {})
            # x should be captured (plain int value)
            self.assertIn("x", locals_dict)
            self.assertEqual(locals_dict["x"]["value"], "10")
            # modules, functions, classes, methods, builtins should be filtered out
            self.assertNotIn("local_module", locals_dict)
            self.assertNotIn("local_func", locals_dict)
            self.assertNotIn("local_class", locals_dict)
            self.assertNotIn("local_method", locals_dict)
            self.assertNotIn("local_builtin", locals_dict)

    def test_hit_count_callback_called_with_correct_key(self):
        """Test that hit count callback is called with correct breakpoint key."""
        tracer, exporter = self._create_test_tracer()

        def simple_func():
            x = 1
            return x

        code = simple_func.__code__
        breakpoint_line = code.co_firstlineno + 1

        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code, func=simple_func, line_numbers={breakpoint_line}, function_key="test.module.simple_func"
        )

        with tracer.start_as_current_span("test_span"):
            simple_func()

        self.callback.assert_called_once_with(f"test.module.simple_func:{breakpoint_line}")

    def test_line_event_handler_returns_none_for_target_line(self):
        """Test line event handler returns None for active breakpoint lines to keep them monitored."""
        func = self._create_test_function()
        code = func.__code__
        breakpoint_line = code.co_firstlineno + 2

        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={breakpoint_line}, function_key="test.func"
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            result = self.engine._line_event_handler(code, breakpoint_line)

        self.assertIsNone(result)

    def test_handler_only_fires_for_target_after_training(self):
        """Test that after training pass, only breakpoint lines trigger the handler."""

        def multi_line_func():
            a = 1
            b = 2
            c = 3
            d = 4
            return a + b + c + d

        code = multi_line_func.__code__
        breakpoint_line = code.co_firstlineno + 3

        handler_calls = []
        original_handler = self.engine._line_event_handler

        def tracking_handler(code_obj, line_number):
            handler_calls.append(line_number)
            return original_handler(code_obj, line_number)

        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code, func=multi_line_func, line_numbers={breakpoint_line}, function_key="test.func"
        )

        sys.monitoring.register_callback(self.engine.tool_id, sys.monitoring.events.LINE, tracking_handler)

        # First call: training pass — all lines fire
        handler_calls.clear()
        multi_line_func()
        training_count = len(handler_calls)

        # Second call: only breakpoint line should fire
        handler_calls.clear()
        multi_line_func()
        steady_state_count = len(handler_calls)

        self.assertGreater(training_count, 1)
        self.assertEqual(steady_state_count, 1)
        self.assertEqual(handler_calls[0], breakpoint_line)
        self.callback.assert_called()

    def test_enable_additional_breakpoints_retrains(self):
        """Test that adding breakpoints to already-monitored function resets DISABLE state."""

        def multi_line_func():
            a = 1
            b = 2
            c = 3
            d = 4
            return a + b + c + d

        code = multi_line_func.__code__
        line_a = code.co_firstlineno + 1
        line_c = code.co_firstlineno + 3

        handler_calls = []
        original_handler = self.engine._line_event_handler

        def tracking_handler(code_obj, line_number):
            handler_calls.append(line_number)
            return original_handler(code_obj, line_number)

        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code, func=multi_line_func, line_numbers={line_a}, function_key="test.func"
        )

        sys.monitoring.register_callback(self.engine.tool_id, sys.monitoring.events.LINE, tracking_handler)

        # Train with first breakpoint
        multi_line_func()
        handler_calls.clear()
        multi_line_func()
        self.assertEqual(len(handler_calls), 1)
        self.assertIn(line_a, handler_calls)

        # Add second breakpoint — should trigger reset
        self.engine.enable_breakpoints_for_function(
            code=code, func=multi_line_func, line_numbers={line_c}, function_key="test.func"
        )

        # Re-training call
        multi_line_func()
        # Steady state: both breakpoints should fire
        handler_calls.clear()
        multi_line_func()
        self.assertEqual(len(handler_calls), 2)
        self.assertIn(line_a, handler_calls)
        self.assertIn(line_c, handler_calls)

    def test_cleanup_clears_state_and_frees_tool(self):
        """Test cleanup clears breakpoints, function keys, location hashes, capture configs, and frees tool ID."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func,
            line_numbers={code.co_firstlineno + 2},
            function_key="test.func",
            line_location_hashes={code.co_firstlineno + 2: "hash123"},
            line_capture_configs={code.co_firstlineno + 2: CaptureConfig(capture_locals=[])},
        )

        self.assertTrue(self.engine._initialized)
        self.assertEqual(len(self.engine._breakpoints), 1)
        self.assertEqual(len(self.engine._location_hashes), 1)
        self.assertEqual(len(self.engine._capture_configs), 1)

        self.engine.cleanup()

        self.assertFalse(self.engine._initialized)
        self.assertIsNone(self.engine._hit_count_callback)
        self.assertEqual(len(self.engine._breakpoints), 0)
        self.assertEqual(len(self.engine._function_keys), 0)
        self.assertEqual(len(self.engine._location_hashes), 0)
        self.assertEqual(len(self.engine._capture_configs), 0)
        self.assertIsNone(sys.monitoring.get_tool(self.engine.tool_id))

    def test_cleanup_without_initialization(self):
        """Test cleanup works when engine was never initialized."""
        self.engine.cleanup()

        self.assertFalse(self.engine._initialized)
        self.assertIsNone(self.engine._hit_count_callback)

    def test_line_event_handler_captures_stack_trace_when_enabled(self):
        """Test that stack trace is included in snapshot when capture_stack_trace=True."""
        tracer, _ = self._create_test_tracer()

        def func_with_stack():
            x = 42
            return x

        code = func_with_stack.__code__
        breakpoint_line = code.co_firstlineno + 1

        capture_config = CaptureConfig(capture_stack_trace=True, capture_locals=[], max_stack_frames=20)
        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func_with_stack,
            line_numbers={breakpoint_line},
            function_key="test.func_with_stack",
            line_capture_configs={breakpoint_line: capture_config},
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                func_with_stack()

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("stack", snapshot)
            self.assertIsInstance(snapshot["stack"], list)
            self.assertGreater(len(snapshot["stack"]), 0)
            frame = snapshot["stack"][0]
            self.assertIn("file_path", frame)
            self.assertIn("function", frame)
            self.assertIn("line_number", frame)

    def test_line_event_handler_no_stack_trace_when_disabled(self):
        """Test that stack trace is NOT included when capture_stack_trace=False."""
        tracer, _ = self._create_test_tracer()

        def func_no_stack():
            x = 42
            return x

        code = func_no_stack.__code__
        breakpoint_line = code.co_firstlineno + 1

        capture_config = CaptureConfig(capture_stack_trace=False, capture_locals=[])
        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func_no_stack,
            line_numbers={breakpoint_line},
            function_key="test.func_no_stack",
            line_capture_configs={breakpoint_line: capture_config},
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                func_no_stack()

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertNotIn("stack", snapshot)

    def test_line_event_handler_stack_trace_respects_max_frames(self):
        """Test that stack trace respects max_stack_frames limit."""
        tracer, _ = self._create_test_tracer()

        def func_max_frames():
            x = 42
            return x

        code = func_max_frames.__code__
        breakpoint_line = code.co_firstlineno + 1

        capture_config = CaptureConfig(capture_stack_trace=True, capture_locals=[], max_stack_frames=2)
        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func_max_frames,
            line_numbers={breakpoint_line},
            function_key="test.func_max_frames",
            line_capture_configs={breakpoint_line: capture_config},
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                func_max_frames()

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("stack", snapshot)
            self.assertGreater(len(snapshot["stack"]), 0)
            self.assertLessEqual(len(snapshot["stack"]), 2)

    def test_line_event_handler_stack_trace_filters_internal_frames(self):
        """Test that internal ADOT/OTel frames are filtered from stack trace."""
        tracer, _ = self._create_test_tracer()

        def func_filtered():
            x = 42
            return x

        code = func_filtered.__code__
        breakpoint_line = code.co_firstlineno + 1

        capture_config = CaptureConfig(capture_stack_trace=True, capture_locals=[], max_stack_frames=20)
        self.engine.initialize(hit_count_callback=self.callback)
        self.engine.enable_breakpoints_for_function(
            code=code,
            func=func_filtered,
            line_numbers={breakpoint_line},
            function_key="test.func_filtered",
            line_capture_configs={breakpoint_line: capture_config},
        )

        with mock.patch(
            "amazon.opentelemetry.distro.debugger.instrumentation_engine._sys_monitoring_engine.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = mock.MagicMock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                func_filtered()

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("stack", snapshot)
            for frame in snapshot["stack"]:
                self.assertNotIn("/amazon/opentelemetry/", frame["file_path"])
                self.assertNotIn("/site-packages/opentelemetry/", frame["file_path"])


@unittest.skipIf(sys.version_info < (3, 12), "SysMonitoringEngine requires Python 3.12+")
class TestSysMonitoringEngineFunctionLevelRecursion(unittest.TestCase):
    """Regression: PROBE on a recursive function on 3.12+ must report a
    correct duration for every frame, not just the innermost.

    The original implementation keyed start_ns by (code_id, thread_id), so
    each recursive call clobbered the outer frame's stamp. Per-thread LIFO
    of (code_id, start_ns) tuples preserves nested-frame durations."""

    def setUp(self):
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.debugger._function_wrapper import set_snapshot_emitter

        self.engine = _sys_monitoring_engine.SysMonitoringEngine()
        self.engine.initialize()
        self.snapshots = []

        class _FakeEmitter:
            def emit_snapshot(_self, snap):  # noqa: N805
                self.snapshots.append(snap)

        set_snapshot_emitter(_FakeEmitter())

    def tearDown(self):
        try:
            self.engine.cleanup()
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    def test_recursive_calls_each_get_their_own_duration(self):
        """fib(5) makes 15 nested calls; every snapshot must carry a duration
        (could be 0ms for a fast call, but the field must be populated, i.e.
        no frame got duration=None which is the symptom of a clobbered stamp)."""

        def fib(n):
            if n < 2:
                return n
            return fib(n - 1) + fib(n - 2)

        ok = self.engine.enable_function_level_instrumentation(
            code=fib.__code__,
            func=fib,
            function_key="m.fib",
            module_name="m",
            qualified_name="fib",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
            instrumentation_type="PROBE",
        )
        self.assertTrue(ok)
        self.assertEqual(fib(5), 5)
        # fib(5) emits 15 snapshots (one per call). Every snapshot must have
        # a non-None duration — None would indicate start_ns was 0, which is
        # the symptom of the recursion clobber bug.
        self.assertEqual(len(self.snapshots), 15)
        for snap in self.snapshots:
            self.assertIsNotNone(snap.duration, "every recursive frame must record duration")

    def test_nested_calls_have_increasing_durations(self):
        """A recursive function with sleep at each level: outer frames must
        report durations ≥ inner frames'. The test is empirical proof the
        per-thread stack pops in LIFO order."""
        import time as _time  # pylint: disable=import-outside-toplevel

        def slow(n):
            _time.sleep(0.01)
            if n > 1:
                return slow(n - 1) + 1
            return 0

        ok = self.engine.enable_function_level_instrumentation(
            code=slow.__code__,
            func=slow,
            function_key="m.slow",
            module_name="m",
            qualified_name="slow",
            instrumentation_type="PROBE",
        )
        self.assertTrue(ok)
        slow(4)
        self.assertEqual(len(self.snapshots), 4)
        # Snapshots emitted in PY_RETURN order: innermost first, outermost last.
        durations = [s.duration for s in self.snapshots]
        for d in durations:
            self.assertIsNotNone(d, "every frame must record duration")
        # Each successive snapshot is for a frame one level outer, so its
        # duration must be at least as long as the previous frame's.
        for inner, outer in zip(durations, durations[1:]):
            self.assertGreaterEqual(outer, inner, f"outer frame duration {outer}ms < inner {inner}ms")


if __name__ == "__main__":
    unittest.main()
