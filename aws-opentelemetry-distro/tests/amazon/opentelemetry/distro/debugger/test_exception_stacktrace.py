# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for exception stacktrace capture in function-level DI snapshots."""

import traceback
import unittest

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger._function_wrapper import FunctionWrapper


class TestCaptureReturnContextExceptionStack(unittest.TestCase):
    """Tests for _capture_return_context with exception stack stitching."""

    def setUp(self):
        self.wrapper = FunctionWrapper()

    def _make_config(self, max_stack_frames=20):
        return CaptureConfig(
            capture_return=True,
            capture_stack_trace=True,
            max_stack_frames=max_stack_frames,
        )

    def test_throwable_with_caller_stack_combines_frames(self):
        """Combined stack has frames from both tb_frames and caller_stack."""
        try:
            raise ValueError("test error")
        except ValueError as e:
            thrown = e
            caller_stack = traceback.extract_stack()

        ctx = self.wrapper._capture_return_context(None, thrown, self._make_config(), caller_stack)

        self.assertIsNotNone(ctx)
        self.assertIsNotNone(ctx.throwable)
        self.assertEqual(ctx.throwable.type, "ValueError")
        self.assertEqual(ctx.throwable.message, "test error")
        # Should have frames from both tb (the raise site) and caller_stack
        self.assertGreater(len(ctx.throwable.stacktrace), 1)

    def test_throwable_without_caller_stack_uses_tb_only(self):
        """caller_stack=None falls back to tb_frames only (backwards compat)."""
        try:
            raise RuntimeError("fallback")
        except RuntimeError as e:
            thrown = e

        ctx = self.wrapper._capture_return_context(None, thrown, self._make_config(), caller_stack=None)

        self.assertIsNotNone(ctx)
        self.assertIsNotNone(ctx.throwable)
        self.assertEqual(ctx.throwable.type, "RuntimeError")
        self.assertEqual(ctx.throwable.message, "fallback")
        # tb_frames only — may be empty if all frames are internal (test paths)
        # but throwable itself is always populated
        self.assertIsNotNone(ctx.throwable.stacktrace)

    def test_throw_site_is_at_index_zero(self):
        """Frame ordering: throw site is at index 0 (deepest frame first)."""
        from unittest.mock import patch

        # Create a real exception with a traceback
        try:
            raise ValueError("ordering test")
        except ValueError as e:
            thrown = e

        # Simulate tb_frames in extract_tb order: outermost -> innermost (raise site last)
        fake_tb = traceback.StackSummary.from_list(
            [
                ("/app/service.py", 50, "process_order", ""),
                ("/app/service.py", 30, "lookup_order", ""),
            ]
        )
        # Simulate caller_stack in extract_stack order: outermost -> innermost
        fake_caller = traceback.StackSummary.from_list(
            [
                ("/usr/lib/python/threading.py", 100, "_bootstrap", ""),
                ("/app/server.py", 167, "route_handler", ""),
            ]
        )

        with patch("traceback.extract_tb", return_value=fake_tb):
            ctx = self.wrapper._capture_return_context(None, thrown, self._make_config(), fake_caller)

        self.assertIsNotNone(ctx.throwable)
        functions = [f.function for f in ctx.throwable.stacktrace]
        # After reversal: throw site (lookup_order) first, then callers innermost-first
        self.assertEqual(functions[0], "lookup_order")
        self.assertEqual(functions[1], "process_order")
        self.assertEqual(functions[2], "route_handler")
        self.assertEqual(functions[3], "_bootstrap")

    def test_max_stack_frames_truncates_combined_frames(self):
        """max_stack_frames correctly truncates the combined frames."""
        try:
            raise ValueError("truncate test")
        except ValueError as e:
            thrown = e
            caller_stack = traceback.extract_stack()

        ctx = self.wrapper._capture_return_context(None, thrown, self._make_config(max_stack_frames=3), caller_stack)

        self.assertIsNotNone(ctx.throwable)
        self.assertLessEqual(len(ctx.throwable.stacktrace), 3)

    def test_internal_frames_filtered_from_combined_stack(self):
        """Internal frames (containing /amazon/opentelemetry/) are filtered out."""
        try:
            raise TypeError("filter test")
        except TypeError as e:
            thrown = e

        fake_stack = traceback.StackSummary.from_list(
            [
                ("/path/to/amazon/opentelemetry/distro/debugger/_function_wrapper.py", 100, "sync_wrapper", ""),
                ("/path/to/site-packages/flask/app.py", 902, "dispatch_request", ""),
                ("/path/to/myapp/server.py", 42, "my_handler", ""),
            ]
        )

        ctx = self.wrapper._capture_return_context(None, thrown, self._make_config(), fake_stack)

        self.assertIsNotNone(ctx.throwable)
        functions = [f.function for f in ctx.throwable.stacktrace]
        # Internal frame should be filtered out
        self.assertNotIn("sync_wrapper", functions)
        # Non-internal frames from caller_stack should be present (reversed order)
        self.assertIn("my_handler", functions)
        self.assertIn("dispatch_request", functions)
        # my_handler (innermost after reversal) should come before dispatch_request
        self.assertLess(functions.index("my_handler"), functions.index("dispatch_request"))

    def test_no_exception_returns_context_without_throwable(self):
        """When thrown is None, no throwable is set."""
        ctx = self.wrapper._capture_return_context("result_value", None, self._make_config())

        self.assertIsNotNone(ctx)
        self.assertIsNone(ctx.throwable)


if __name__ == "__main__":
    unittest.main()
