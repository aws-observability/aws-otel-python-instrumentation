# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
import unittest.mock
from unittest.mock import Mock

from test_instrumentation_engine import InstrumentationEngineTestBase

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger.instrumentation_engine import _bytecode_injection_engine


@unittest.skipIf(not (3, 9) <= sys.version_info < (3, 12), "BytecodeInjectionEngine requires Python 3.9-3.11")
class TestBytecodeInjectionEngine(InstrumentationEngineTestBase):
    """Tests for BytecodeInjectionEngine."""

    def _setup_engine(self):
        self.engine = _bytecode_injection_engine.BytecodeInjectionEngine()

    def test_import_error_sets_bytecode_unavailable(self):
        """When bytecode import fails, IS_BYTECODE_INSTALLED=False and engine won't initialize."""
        original_bytecode = _bytecode_injection_engine.IS_BYTECODE_INSTALLED
        original_bytecode_class = _bytecode_injection_engine.Bytecode
        original_instr_class = _bytecode_injection_engine.Instr
        try:
            _bytecode_injection_engine.IS_BYTECODE_INSTALLED = False
            _bytecode_injection_engine.Bytecode = None
            _bytecode_injection_engine.Instr = None

            engine = _bytecode_injection_engine.BytecodeInjectionEngine()
            engine.initialize()

            self.assertFalse(engine._initialized)
            self.assertFalse(engine.supports_runtime())
        finally:
            _bytecode_injection_engine.IS_BYTECODE_INSTALLED = original_bytecode
            _bytecode_injection_engine.Bytecode = original_bytecode_class
            _bytecode_injection_engine.Instr = original_instr_class

    def test_initialization_exception_does_not_set_initialized(self):
        """Test that _initialized stays False when exception occurs during initialize."""
        original_debug = _bytecode_injection_engine.logger.debug
        _bytecode_injection_engine.logger.debug = Mock(side_effect=RuntimeError("test error"))
        try:
            engine = _bytecode_injection_engine.BytecodeInjectionEngine()
            engine.initialize()
            self.assertFalse(engine._initialized)
        finally:
            _bytecode_injection_engine.logger.debug = original_debug

    def test_initialization_without_callback(self):
        """Test engine initialization without callback."""
        self.engine.initialize()
        self.assertTrue(self.engine._initialized)
        self.assertIsNone(self.engine._hit_count_callback)

    def test_initialization_with_callback(self):
        """Test engine initialization with callback."""
        self.engine.initialize(hit_count_callback=self.callback)
        self.assertTrue(self.engine._initialized)
        self.assertEqual(self.engine._hit_count_callback, self.callback)

    def test_enable_without_initialization(self):
        """Test enabling breakpoints without initialization."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.enable_breakpoints_for_function(
            code=code, func=func, line_numbers={code.co_firstlineno + 1}, function_key="test.func"
        )

        # Should not crash, but should not enable breakpoints
        self.assertNotIn(id(func), self.engine._injection_states)

    def test_enable_bytecode_not_available(self):
        """Test enabling breakpoints when bytecode library is not available."""
        original_value = _bytecode_injection_engine.IS_BYTECODE_INSTALLED
        try:
            _bytecode_injection_engine.IS_BYTECODE_INSTALLED = False
            engine = _bytecode_injection_engine.BytecodeInjectionEngine()
            engine.initialize()
            func = self._create_test_function()
            result = engine.enable_breakpoints_for_function(
                code=func.__code__, func=func, line_numbers={func.__code__.co_firstlineno + 1}, function_key="test.func"
            )
            self.assertIsNone(result)
            self.assertNotIn(id(func), engine._injection_states)
        finally:
            _bytecode_injection_engine.IS_BYTECODE_INSTALLED = original_value

    def test_enable_empty_line_numbers(self):
        """Test enabling with empty line numbers set."""
        func = self._create_test_function()
        code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(code=code, func=func, line_numbers=set(), function_key="test.func")

        # Should not crash, but should not enable anything
        self.assertNotIn(id(func), self.engine._injection_states)

    def test_enable_breakpoints_success(self):
        """Test successful breakpoint injection."""
        func = self._create_test_function()
        original_code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={original_code.co_firstlineno + 2},
            function_key="test.module.test_func",
        )

        # Verify bytecode was modified with breakpoint handler call
        self.assertNotEqual(func.__code__, original_code)
        self.assertIn("_breakpoint_handler", func.__code__.co_names)

        # Verify state was created
        self.assertIn(id(func), self.engine._injection_states)
        state = self.engine._injection_states[id(func)]
        self.assertEqual(state.original_code, original_code)
        self.assertEqual(state.function_key, "test.module.test_func")

        # Verify globals were injected
        self.assertIn("_breakpoint_handler", func.__globals__)
        self.assertIn("_breakpoint_locals", func.__globals__)

    def test_enable_breakpoint_should_not_modify_original_function(self):
        """Should not modify function behavior after adding breakpoints."""
        func = self._create_test_function()
        original_code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={original_code.co_firstlineno + 2},
            function_key="test.func",
        )

        result = func(5, 3)
        self.assertEqual(result, 8)

    def test_enable_breakpoints_invalid_line_numbers(self):
        """Test enabling breakpoints at lines that don't exist in function."""
        func = self._create_test_function()
        original_code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={9999},
            function_key="test.func",
        )

        self.assertNotIn(id(func), self.engine._injection_states)

    def test_breakpoint_handler_called_on_execution(self):
        """Verify breakpoint handler is called when function executes."""
        func = self._create_test_function()
        original_code = func.__code__
        self.engine._breakpoint_handler = Mock()

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={original_code.co_firstlineno + 2},
            function_key="test.func",
        )

        func(5, 3)
        self.engine._breakpoint_handler.assert_called_once()

    def test_breakpoint_handler_adds_span_event(self):
        """Verify breakpoint handler creates a snapshot with trace context when span is active."""
        tracer, exporter = self._create_test_tracer()

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_get_writer.return_value = mock_writer

            with tracer.start_as_current_span("test_span"):
                self.engine._breakpoint_handler("test_func", 10, {"x": 5, "y": 3})

            # Verify snapshot was written
            mock_writer.emit_snapshot.assert_called_once()
            snapshot_dict = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("captures", snapshot_dict)
            self.assertIn("instrumentation", snapshot_dict)
            self.assertEqual(snapshot_dict["instrumentation"]["location"]["method_name"], "test_func")
            # Verify trace context was captured
            self.assertIn("trace", snapshot_dict)
            self.assertIsNotNone(snapshot_dict["trace"]["trace_id"])

    def test_breakpoints_for_various_function_types(self):
        """Test breakpoint injection works for various function types."""
        test_cases = [
            ("regular_function", self._create_test_function(), "test.module.test_func"),
            ("class_method", self._create_test_method()[1], "test.module.TestClass.method"),
            ("inner_function", self._create_test_inner_function(), "test.module.outer.<locals>.inner"),
            ("static_method", self._create_test_static_method()[1], "test.module.TestClass.static_method"),
        ]

        for name, func, function_key in test_cases:
            with self.subTest(function_type=name):
                # Reset engine state
                self.engine.cleanup()
                self.engine = _bytecode_injection_engine.BytecodeInjectionEngine()
                self.engine.initialize()

                original_code = func.__code__

                self.engine.enable_breakpoints_for_function(
                    code=original_code,
                    func=func,
                    line_numbers={original_code.co_firstlineno + 2},
                    function_key=function_key,
                )

                # Verify state was created
                self.assertIn(id(func), self.engine._injection_states)
                state = self.engine._injection_states[id(func)]
                self.assertEqual(state.function_key, function_key)

                # Execute and verify function still works correctly
                with unittest.mock.patch(
                    "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
                ) as mock_get_writer:
                    mock_writer = Mock()
                    mock_get_writer.return_value = mock_writer

                    if name == "class_method":
                        TestClass = self._create_test_method()[0]
                        result = func(TestClass(), 5, 3)
                    else:
                        result = func(5, 3)

                self.assertEqual(result, 8)

    def test_breakpoint_handler_calls_hit_count_callback(self):
        """Verify hit count callback is called when breakpoint is hit."""
        tracer, _ = self._create_test_tracer()
        self.engine.initialize(hit_count_callback=self.callback)

        with tracer.start_as_current_span("test_span"):
            self.engine._breakpoint_handler("test_func", 10, {})

        self.callback.assert_called_once_with("test_func:10")

    def test_breakpoint_handler_no_active_span(self):
        """Tests that handler still creates snapshot when no active span (without trace context)."""
        self.engine.initialize(hit_count_callback=self.callback)
        self.callback.return_value = True

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_get_writer.return_value = mock_writer

            self.engine._breakpoint_handler("test_func", 10, {"x": 5})

            self.callback.assert_called_once_with("test_func:10")
            mock_writer.emit_snapshot.assert_called_once()
            snapshot_dict = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            # No active span, so trace should be None
            self.assertIsNone(snapshot_dict.get("trace"))

    def test_disable_without_initialization(self):
        """Test disabling breakpoints without initialization."""
        func = self._create_test_function()

        result = self.engine.disable_breakpoints_for_function(code=func.__code__, func=func)
        self.assertIsNone(result)

    def test_disable_no_injection_state(self):
        """Test disabling when no injection state exists."""
        func = self._create_test_function()

        self.engine.initialize()
        result = self.engine.disable_breakpoints_for_function(code=func.__code__, func=func)
        self.assertIsNone(result)

    def test_disable_breakpoints_success(self):
        """Test successful breakpoint disable and restoration."""
        func = self._create_test_function()
        original_code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={original_code.co_firstlineno + 2},
            function_key="test.module.test_func",
        )

        self.assertIn(id(func), self.engine._injection_states)
        self.assertNotEqual(func.__code__, original_code)

        result = self.engine.disable_breakpoints_for_function(code=func.__code__, func=func)
        self.assertIsNone(result)

        # Verify restored
        self.assertNotIn(id(func), self.engine._injection_states)
        self.assertEqual(func.__code__, original_code)
        self.assertNotIn("_breakpoint_handler", func.__globals__)
        self.assertNotIn("_breakpoint_locals", func.__globals__)

    def test_disable_keeps_handler_for_sibling_in_same_module(self):
        """Disabling one function must not pop the shared handler while a sibling stays instrumented.

        Two functions defined in the same module share one __globals__ dict. If disabling
        func_a removed the injected _breakpoint_handler global, func_b's still-injected
        LOAD_GLOBAL would raise NameError inside user code (a SAFETY violation).
        """
        func_a = self._create_test_function()
        func_b = self._create_test_function()
        # Both functions are defined in this test module, so they share one globals dict.
        self.assertIs(func_a.__globals__, func_b.__globals__)

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=func_a.__code__,
            func=func_a,
            line_numbers={func_a.__code__.co_firstlineno + 2},
            function_key="test.module.func_a",
        )
        self.engine.enable_breakpoints_for_function(
            code=func_b.__code__,
            func=func_b,
            line_numbers={func_b.__code__.co_firstlineno + 2},
            function_key="test.module.func_b",
        )
        self.assertIn("_breakpoint_handler", func_a.__globals__)

        # Disable only func_a — the handler must remain because func_b still uses it.
        self.engine.disable_breakpoints_for_function(code=func_a.__code__, func=func_a)
        self.assertIn("_breakpoint_handler", func_b.__globals__)
        self.assertIn("_breakpoint_locals", func_b.__globals__)

        # func_b is still executable (its injected LOAD_GLOBAL resolves) and returns correctly.
        self.assertEqual(func_b(5), 15)

        # Disabling the last function finally removes the shared handler.
        self.engine.disable_breakpoints_for_function(code=func_b.__code__, func=func_b)
        self.assertNotIn("_breakpoint_handler", func_b.__globals__)
        self.assertNotIn("_breakpoint_locals", func_b.__globals__)

    def test_cleanup_restores_functions_and_clears_state(self):
        """Test cleanup restores functions and clears all state."""
        func = self._create_test_function()
        original_code = func.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code,
            func=func,
            line_numbers={original_code.co_firstlineno + 2},
            function_key="test.func",
        )

        # Verify breakpoint was enabled
        self.assertIn(id(func), self.engine._injection_states)
        self.assertIn("_breakpoint_handler", func.__globals__)

        self.engine.cleanup()

        # Verify cleanup restored everything
        self.assertEqual(func.__code__, original_code)
        self.assertNotIn("_breakpoint_handler", func.__globals__)
        self.assertNotIn("_breakpoint_locals", func.__globals__)
        self.assertEqual(len(self.engine._injection_states), 0)
        self.assertFalse(self.engine._initialized)

    def test_cleanup_continues_on_partial_failure(self):
        """Test cleanup continues restoring other functions when one fails."""
        func1 = self._create_test_function()
        func2 = self._create_test_function()
        original_code1 = func1.__code__
        original_code2 = func2.__code__

        self.engine.initialize()
        self.engine.enable_breakpoints_for_function(
            code=original_code1, func=func1, line_numbers={original_code1.co_firstlineno + 2}, function_key="test.func1"
        )
        self.engine.enable_breakpoints_for_function(
            code=original_code2, func=func2, line_numbers={original_code2.co_firstlineno + 2}, function_key="test.func2"
        )

        # Corrupt func1's state to cause failure during cleanup
        self.engine._injection_states[id(func1)].function_ref = None

        self.engine.cleanup()

        # func2 should still be restored despite func1 failure
        self.assertEqual(func2.__code__, original_code2)
        self.assertNotIn("_breakpoint_handler", func2.__globals__)
        self.assertEqual(len(self.engine._injection_states), 0)
        self.assertFalse(self.engine._initialized)

    def test_breakpoint_handler_captures_stack_trace_when_enabled(self):
        """Test that stack trace is included in snapshot when capture_stack_trace=True."""
        tracer, _ = self._create_test_tracer()
        self.engine.initialize(hit_count_callback=self.callback)

        capture_config = CaptureConfig(capture_stack_trace=True, capture_locals=[], max_stack_frames=20)
        self.engine._capture_configs[("test.func", 10)] = capture_config

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                self.engine._breakpoint_handler("test.func", 10, {"x": 42})

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("stack", snapshot)
            self.assertIsInstance(snapshot["stack"], list)
            self.assertGreater(len(snapshot["stack"]), 0)
            frame = snapshot["stack"][0]
            self.assertIn("file_path", frame)
            self.assertIn("function", frame)
            self.assertIn("line_number", frame)

    def test_breakpoint_handler_no_stack_trace_when_disabled(self):
        """Test that stack trace is NOT included when capture_stack_trace=False."""
        tracer, _ = self._create_test_tracer()
        self.engine.initialize(hit_count_callback=self.callback)

        capture_config = CaptureConfig(capture_stack_trace=False, capture_locals=[])
        self.engine._capture_configs[("test.func", 10)] = capture_config

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                self.engine._breakpoint_handler("test.func", 10, {"x": 42})

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertNotIn("stack", snapshot)

    def test_breakpoint_handler_stack_trace_respects_max_frames(self):
        """Test that stack trace respects max_stack_frames limit."""
        tracer, _ = self._create_test_tracer()
        self.engine.initialize(hit_count_callback=self.callback)

        capture_config = CaptureConfig(capture_stack_trace=True, capture_locals=[], max_stack_frames=2)
        self.engine._capture_configs[("test.func", 10)] = capture_config

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                self.engine._breakpoint_handler("test.func", 10, {"x": 42})

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("stack", snapshot)
            self.assertGreater(len(snapshot["stack"]), 0)
            self.assertLessEqual(len(snapshot["stack"]), 2)

    def test_breakpoint_handler_stack_trace_filters_internal_frames(self):
        """Test that internal ADOT/OTel frames are filtered from stack trace."""
        tracer, _ = self._create_test_tracer()
        self.engine.initialize(hit_count_callback=self.callback)

        capture_config = CaptureConfig(capture_stack_trace=True, capture_locals=[], max_stack_frames=20)
        self.engine._capture_configs[("test.func", 10)] = capture_config

        with unittest.mock.patch(
            "amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter"
        ) as mock_get_writer:
            mock_writer = Mock()
            mock_get_writer.return_value = mock_writer
            with tracer.start_as_current_span("test_span"):
                self.engine._breakpoint_handler("test.func", 10, {"x": 42})

            mock_writer.emit_snapshot.assert_called_once()
            snapshot = mock_writer.emit_snapshot.call_args[0][0].to_dict()
            self.assertIn("stack", snapshot)
            for frame in snapshot["stack"]:
                self.assertNotIn("/amazon/opentelemetry/", frame["file_path"])
                self.assertNotIn("/site-packages/opentelemetry/", frame["file_path"])


if __name__ == "__main__":
    unittest.main()
