# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests that the function-level capture path honors the per-instrumentation
capture_config limits (MaxStringLength / MaxCollectionWidth / MaxFieldsPerObject)
and applies the cross-SDK parity defaults (255/20/20) when limits are omitted.

Regression coverage for the bug where FunctionWrapper serialized arguments and
return values with a fixed, config-ignoring SnapshotSerializer, so user-supplied
limits were silently dropped on the function-level path.
"""

import sys
import threading
import types
import unittest

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger._function_wrapper import FunctionWrapper, set_snapshot_emitter


def _make_module(name, **attrs):
    """Create a fake module with given attributes and register it in sys.modules."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _remove_module(name):
    sys.modules.pop(name, None)


def _echo(text):
    """Module-level function so its qualified name is stable (no <locals> qualifier)."""
    return text


class _ManyFields:
    """An object with more attributes than a small max_fields_per_object limit."""

    def __init__(self, count):
        for i in range(count):
            setattr(self, f"attr_{i}", i)


class TestEntryContextRespectsLimits(unittest.TestCase):
    """_capture_entry_context must serialize arguments using capture_config limits."""

    def setUp(self):
        self.wrapper = FunctionWrapper()

    def test_string_argument_truncated_at_configured_limit(self):
        def func(text):
            return text

        config = CaptureConfig(capture_arguments=[], max_string_length=10)
        long_text = "x" * 50

        ctx = self.wrapper._capture_entry_context(func, (long_text,), {}, config)

        self.assertIsNotNone(ctx)
        captured = ctx.arguments["text"]
        self.assertEqual(captured.type, "str")
        self.assertEqual(len(captured.value), 10)
        self.assertTrue(captured.truncated)
        self.assertEqual(captured.size, 50)

    def test_collection_argument_capped_at_configured_width(self):
        def func(items):
            return items

        config = CaptureConfig(capture_arguments=[], max_collection_width=2)
        big_list = list(range(5))

        ctx = self.wrapper._capture_entry_context(func, (big_list,), {}, config)

        captured = ctx.arguments["items"]
        self.assertEqual(captured.type, "list")
        self.assertEqual(len(captured.elements), 2)
        self.assertTrue(captured.truncated)
        self.assertEqual(captured.size, 5)

    def test_object_argument_capped_at_configured_fields(self):
        def func(obj):
            return obj

        config = CaptureConfig(capture_arguments=[], max_fields_per_object=2)
        obj = _ManyFields(6)

        ctx = self.wrapper._capture_entry_context(func, (obj,), {}, config)

        captured = ctx.arguments["obj"]
        self.assertEqual(len(captured.fields), 2)
        self.assertEqual(captured.not_captured_reason, "fieldCount")
        self.assertEqual(captured.size, 6)


class TestReturnContextRespectsLimits(unittest.TestCase):
    """_capture_return_context must serialize the return value using capture_config limits."""

    def setUp(self):
        self.wrapper = FunctionWrapper()

    def test_return_string_truncated_at_configured_limit(self):
        config = CaptureConfig(capture_return=True, max_string_length=8)
        result = "y" * 40

        ctx = self.wrapper._capture_return_context(result, None, config)

        self.assertIsNotNone(ctx)
        self.assertEqual(len(ctx.return_value.value), 8)
        self.assertTrue(ctx.return_value.truncated)
        self.assertEqual(ctx.return_value.size, 40)


class TestParityDefaultsOnFunctionPath(unittest.TestCase):
    """With default limits, the function path must use the parity defaults (255/20/20)."""

    def setUp(self):
        self.wrapper = FunctionWrapper()

    def test_string_below_default_not_truncated(self):
        def func(text):
            return text

        config = CaptureConfig(capture_arguments=[])  # default max_string_length == 255
        text = "a" * 200

        ctx = self.wrapper._capture_entry_context(func, (text,), {}, config)

        captured = ctx.arguments["text"]
        self.assertFalse(captured.truncated)
        self.assertEqual(captured.value, text)

    def test_string_above_default_truncated_at_255(self):
        def func(text):
            return text

        config = CaptureConfig(capture_arguments=[])  # default max_string_length == 255
        text = "a" * 300

        ctx = self.wrapper._capture_entry_context(func, (text,), {}, config)

        captured = ctx.arguments["text"]
        self.assertTrue(captured.truncated)
        self.assertEqual(len(captured.value), 255)
        self.assertEqual(captured.size, 300)


class _RecordingEmitter:
    """Captures the last emitted snapshot for assertions."""

    def __init__(self):
        self.snapshots = []

    def emit_snapshot(self, snapshot):
        self.snapshots.append(snapshot)


class _FakeBreakpoint:
    def __init__(self):
        self.instrumentation_type = "BREAKPOINT"


class _FakeBreakpointSet:
    """Minimal stand-in for FunctionBreakpointSet with a function-level (line 0) breakpoint."""

    def __init__(self):
        self.breakpoints = {0: _FakeBreakpoint()}
        self.states = {}


class _FakeManager:
    """Minimal manager exposing the surface sync_wrapper touches for a function-level breakpoint."""

    def __init__(self, func_key):
        self._lock = threading.RLock()
        self._active_functions = {func_key: _FakeBreakpointSet()}

    def increment_hit_count(self, _breakpoint_key):
        return True


class TestEndToEndFunctionCaptureRespectsLimits(unittest.TestCase):
    """Instrument a real function and assert the emitted snapshot honors capture_config limits."""

    def setUp(self):
        self.module_name = "_test_capture_limits_module"
        _remove_module(self.module_name)
        self.emitter = _RecordingEmitter()
        set_snapshot_emitter(self.emitter)

    def tearDown(self):
        _remove_module(self.module_name)
        set_snapshot_emitter(None)

    def test_instrumented_function_truncates_return_at_configured_limit(self):
        module = _make_module(self.module_name, echo=_echo)
        wrapper = FunctionWrapper()
        # The wrapper keys breakpoints by "<module>.<qualified_name>"; use the same key
        # the wrapper computes so the fake manager's function-level breakpoint is found.
        func_key = f"{self.module_name}.{FunctionWrapper._get_qualified_name(_echo)}"
        manager = _FakeManager(func_key)

        config = CaptureConfig(capture_arguments=[], capture_return=True, max_string_length=10)

        _original, instrumented = wrapper.instrument_function(
            module_name=self.module_name,
            function_name="echo",
            capture_config=config,
            location_hash="test-hash",
            manager=manager,
        )
        module.echo = instrumented

        long_text = "z" * 50
        self.assertEqual(module.echo(long_text), long_text)  # user output unaffected

        self.assertEqual(len(self.emitter.snapshots), 1)
        captures = self.emitter.snapshots[0].captures
        return_value = captures.return_context.return_value
        self.assertEqual(len(return_value.value), 10)
        self.assertTrue(return_value.truncated)
        self.assertEqual(return_value.size, 50)


if __name__ == "__main__":
    unittest.main()
