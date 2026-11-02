# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for capture_stack_frames in _stack_utils.

is_internal_frame is covered separately in test_internal_frame_filter.py;
this file focuses on capture_stack_frames behavior.
"""

import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger import _stack_utils
from amazon.opentelemetry.distro.debugger._snapshot_models import StackFrame
from amazon.opentelemetry.distro.debugger._stack_utils import capture_stack_frames


class TestCaptureStackFrames(unittest.TestCase):
    """Tests for capture_stack_frames."""

    # NOTE: This test file physically lives under a path containing
    # "/amazon/opentelemetry/", which is_internal_frame treats as internal.
    # Tests that need real frames captured therefore force is_internal_frame
    # to return False so the capture path runs deterministically regardless
    # of where the test file sits on disk.

    def test_returns_stack_frames(self):
        with mock.patch.object(_stack_utils, "is_internal_frame", return_value=False):
            frames = capture_stack_frames()
        self.assertIsInstance(frames, list)
        self.assertGreater(len(frames), 0)
        for frame in frames:
            self.assertIsInstance(frame, StackFrame)

    def test_zero_max_frames_returns_empty(self):
        self.assertEqual(capture_stack_frames(max_frames=0), [])

    def test_negative_max_frames_returns_empty(self):
        self.assertEqual(capture_stack_frames(max_frames=-1), [])

    def test_includes_calling_frame(self):
        with mock.patch.object(_stack_utils, "is_internal_frame", return_value=False):
            frames = capture_stack_frames()
        functions = [frame.function for frame in frames]
        self.assertIn("test_includes_calling_frame", functions)

    def test_respects_max_frames_limit(self):
        def level_three():
            return capture_stack_frames(max_frames=2)

        def level_two():
            return level_three()

        def level_one():
            return level_two()

        with mock.patch.object(_stack_utils, "is_internal_frame", return_value=False):
            frames = level_one()
        self.assertEqual(len(frames), 2)

    def test_filters_internal_frames(self):
        # Force every frame to look internal — result should be empty.
        with mock.patch.object(_stack_utils, "is_internal_frame", return_value=True):
            frames = capture_stack_frames()
        self.assertEqual(frames, [])

    def test_handles_inspect_failure_gracefully(self):
        with mock.patch.object(_stack_utils.inspect, "currentframe", side_effect=RuntimeError("boom")):
            frames = capture_stack_frames()
        self.assertEqual(frames, [])

    def test_captured_frame_has_function_and_line(self):
        with mock.patch.object(_stack_utils, "is_internal_frame", return_value=False):
            frames = capture_stack_frames()
        top_frame = frames[0]
        self.assertTrue(top_frame.function)
        self.assertIsInstance(top_frame.line_number, int)
        self.assertGreater(top_frame.line_number, 0)


if __name__ == "__main__":
    unittest.main()
