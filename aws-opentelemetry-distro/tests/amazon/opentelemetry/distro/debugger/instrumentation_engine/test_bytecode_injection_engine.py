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


@unittest.skipIf(
    not ((3, 9) <= sys.version_info < (3, 12)),
    "Function-level bytecode rewrite supported on Python 3.9-3.11 only",
)
class TestBytecodeInjectionEngineFunctionLevel(unittest.TestCase):
    """End-to-end tests for function-level instrumentation via bytecode rewrite."""

    def setUp(self):
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.debugger._function_wrapper import set_snapshot_emitter
        from amazon.opentelemetry.distro.debugger.instrumentation_engine._bytecode_injection_engine import (
            BytecodeInjectionEngine,
        )

        self._set_emitter = set_snapshot_emitter
        self.engine = BytecodeInjectionEngine()
        self.engine.initialize()
        self.snapshots = []

        class _FakeEmitter:
            def emit_snapshot(_self, snap):  # noqa: N805
                self.snapshots.append(snap)

        self._set_emitter(_FakeEmitter())

    def tearDown(self):
        try:
            self.engine.cleanup()
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    def test_function_level_basic_return(self):
        """Arming a function emits an entry+exit snapshot with serialized return value."""

        def add(x, y):
            return x + y

        ok = self.engine.enable_function_level_instrumentation(
            code=add.__code__,
            func=add,
            function_key="m.add",
            module_name="m",
            qualified_name="add",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
            location_hash="hash-add",
            instrumentation_type="PROBE",
        )
        self.assertTrue(ok)
        self.assertEqual(add(2, 3), 5)
        self.assertEqual(len(self.snapshots), 1)
        snap = self.snapshots[0]
        self.assertEqual(snap.instrumentation_type, "PROBE")
        self.assertIsNotNone(snap.captures.return_context)
        self.assertEqual(snap.captures.return_context.return_value.value, "5")

    def test_function_level_throwable_capture(self):
        """The PR #770 regression-fix: an instrumented function that raises must
        produce a snapshot whose return_context.throwable is fully populated."""

        def boom(x):
            raise ValueError(f"bad: {x}")

        ok = self.engine.enable_function_level_instrumentation(
            code=boom.__code__,
            func=boom,
            function_key="m.boom",
            module_name="m",
            qualified_name="boom",
            capture_config=CaptureConfig(capture_locals=[]),
            location_hash="hash-boom",
            instrumentation_type="PROBE",
        )
        self.assertTrue(ok)
        with self.assertRaises(ValueError) as ctx:
            boom(7)
        self.assertEqual(str(ctx.exception), "bad: 7")  # original exception preserved
        self.assertEqual(len(self.snapshots), 1)
        snap = self.snapshots[0]
        self.assertIsNotNone(snap.captures.return_context)
        throwable = snap.captures.return_context.throwable
        self.assertIsNotNone(throwable)
        self.assertEqual(throwable.type, "ValueError")
        self.assertEqual(throwable.message, "bad: 7")
        self.assertGreater(len(throwable.stacktrace), 0)

    def test_function_level_refuses_generator(self):
        def gen():
            yield 1

        ok = self.engine.enable_function_level_instrumentation(
            code=gen.__code__, func=gen, function_key="m.gen", module_name="m", qualified_name="gen"
        )
        self.assertFalse(ok)

    def test_function_level_refuses_coroutine(self):
        async def coro():
            return 1

        ok = self.engine.enable_function_level_instrumentation(
            code=coro.__code__, func=coro, function_key="m.coro", module_name="m", qualified_name="coro"
        )
        self.assertFalse(ok)

    def test_function_level_refuses_async_generator(self):
        async def agen():
            yield 1

        ok = self.engine.enable_function_level_instrumentation(
            code=agen.__code__, func=agen, function_key="m.agen", module_name="m", qualified_name="agen"
        )
        self.assertFalse(ok)

    def test_function_level_disable_restores_original(self):
        """After disable, the original code object is restored and no snapshots fire."""

        def f(x):
            return x * 10

        original_code = f.__code__
        ok = self.engine.enable_function_level_instrumentation(
            code=f.__code__,
            func=f,
            function_key="m.f",
            module_name="m",
            qualified_name="f",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        f(1)  # generates one snapshot
        self.assertEqual(len(self.snapshots), 1)
        self.snapshots.clear()

        self.engine.disable_function_level_instrumentation(code=original_code, func=f)
        self.assertIs(f.__code__, original_code)
        self.assertEqual(f(2), 20)
        self.assertEqual(len(self.snapshots), 0)

    def test_function_level_kwonly_and_varargs(self):
        """Argument-shape parity: positional + var-args + kwonly + var-kwargs all callable."""

        def f(a, b=10, *args, c, d=20, **kwargs):  # pylint: disable=unused-argument
            return (a, b, args, c, d, sorted(kwargs.items()))

        ok = self.engine.enable_function_level_instrumentation(
            code=f.__code__,
            func=f,
            function_key="m.f",
            module_name="m",
            qualified_name="f",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        result = f(1, 2, 3, 4, c=5, d=6, e=7, fld=8)
        self.assertEqual(result, (1, 2, (3, 4), 5, 6, [("e", 7), ("fld", 8)]))
        self.assertEqual(len(self.snapshots), 1)

    def test_function_level_recursion(self):
        """Recursive call: every entry/exit pair must fire correctly."""

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
        )
        self.assertTrue(ok)
        self.assertEqual(fib(5), 5)
        # fib(5) makes 15 calls (1+2+3+5+...). Each emits 1 snapshot.
        self.assertEqual(len(self.snapshots), 15)

    def test_function_level_throwable_inside_user_try(self):
        """User's own try/except suppresses the exception → unwind hook does NOT
        fire (function returns normally), but exit hook captures the return."""

        def safe(x):
            try:
                if x < 0:
                    raise KeyError("nope")
            except KeyError:
                return -1
            return x

        ok = self.engine.enable_function_level_instrumentation(
            code=safe.__code__,
            func=safe,
            function_key="m.safe",
            module_name="m",
            qualified_name="safe",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        self.assertEqual(safe(-1), -1)
        self.assertEqual(safe(7), 7)
        self.assertEqual(len(self.snapshots), 2)
        # The user's try/except caught their KeyError, so unwind never fired:
        # neither snapshot should have a throwable.
        for snap in self.snapshots:
            self.assertTrue(
                snap.captures.return_context is None or snap.captures.return_context.throwable is None,
                "exit hook fired but throwable was unexpectedly populated",
            )

    def test_function_level_undecorate_resolves_wrapped_function(self):
        """A @functools.wraps decorator hides the real function — engine must
        instrument the underlying user code, not the wrapper."""
        import functools  # pylint: disable=import-outside-toplevel

        def auth_required(view):
            @functools.wraps(view)
            def wrapper(request):
                return view(request)

            return wrapper

        @auth_required
        def my_view(request):
            return f"view: {request}"

        ok = self.engine.enable_function_level_instrumentation(
            code=my_view.__code__,  # this is the wrapper's code
            func=my_view,  # this is the wrapper
            function_key="m.my_view",
            module_name="m",
            qualified_name="my_view",  # but the user function's co_name is "my_view"
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        my_view("hi")
        self.assertEqual(len(self.snapshots), 1)
        # Snapshot reports the underlying function name, not the decorator.
        self.assertEqual(self.snapshots[0].instrumentation.location.method_name, "my_view")

    def test_function_level_disable_after_decorated_enable_restores_inner(self):
        """Regression: enable_function_level_instrumentation redirects through
        @functools.wraps to instrument the inner function. disable must find
        the same state even though the manager passes back the wrapper."""
        import functools  # pylint: disable=import-outside-toplevel

        def auth_required(view):
            @functools.wraps(view)
            def wrapper(request):
                return view(request)

            return wrapper

        @auth_required
        def my_view(request):  # pylint: disable=unused-argument
            return "ok"

        original_inner_code = my_view.__wrapped__.__code__
        ok = self.engine.enable_function_level_instrumentation(
            code=my_view.__code__,
            func=my_view,
            function_key="m.my_view",
            module_name="m",
            qualified_name="my_view",
            capture_config=CaptureConfig(capture_locals=[], capture_return=True),
        )
        self.assertTrue(ok)
        self.assertIsNot(my_view.__wrapped__.__code__, original_inner_code, "inner code should be patched")

        # Manager passes the wrapper back on disable. Engine must resolve
        # through the decorator and find the state keyed by id(my_view).
        self.engine.disable_function_level_instrumentation(code=my_view.__code__, func=my_view)

        self.assertIs(my_view.__wrapped__.__code__, original_inner_code, "inner code must be restored on disable")
        # State must be removed.
        self.assertEqual(len(self.engine._injection_states), 0)


if __name__ == "__main__":
    unittest.main()
