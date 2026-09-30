# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for CaptureRateLimiter — fixed-window token bucket rate limiter."""

import unittest

from amazon.opentelemetry.distro.debugger._capture_rate_limiter import (
    DEFAULT_MAX_CAPTURES_PER_SECOND,
    CaptureRateLimiter,
)

ONE_SECOND_NS = 1_000_000_000


class TestCaptureRateLimiter(unittest.TestCase):
    """Tests mirroring the Java CaptureRateLimiterTest."""

    def test_default_rate_is_five_per_second(self):
        limiter = CaptureRateLimiter()
        self.assertEqual(limiter.max_captures_per_second, 5)
        self.assertEqual(DEFAULT_MAX_CAPTURES_PER_SECOND, 5)

    def test_custom_rate_is_respected(self):
        limiter = CaptureRateLimiter(10)
        self.assertEqual(limiter.max_captures_per_second, 10)

    def test_constructor_rejects_zero(self):
        with self.assertRaises(ValueError):
            CaptureRateLimiter(0)

    def test_constructor_rejects_negative(self):
        with self.assertRaises(ValueError):
            CaptureRateLimiter(-1)

    def test_allows_exactly_max_captures_in_one_window(self):
        limiter = CaptureRateLimiter(5)
        base_time = 1_000_000_000_000  # arbitrary start

        for i in range(5):
            self.assertTrue(limiter.try_acquire(base_time + i))

        # 6th in the same window should be rejected
        self.assertFalse(limiter.try_acquire(base_time + 5))
        self.assertFalse(limiter.try_acquire(base_time + 6))

    def test_resets_after_one_second_window(self):
        limiter = CaptureRateLimiter(3)
        base_time = 1_000_000_000_000

        # Use up all 3 permits in window 1
        self.assertTrue(limiter.try_acquire(base_time))
        self.assertTrue(limiter.try_acquire(base_time + 100))
        self.assertTrue(limiter.try_acquire(base_time + 200))
        self.assertFalse(limiter.try_acquire(base_time + 300))  # over limit

        # Advance past 1 second — new window
        new_window = base_time + ONE_SECOND_NS + 1
        self.assertTrue(limiter.try_acquire(new_window))
        self.assertTrue(limiter.try_acquire(new_window + 100))
        self.assertTrue(limiter.try_acquire(new_window + 200))
        self.assertFalse(limiter.try_acquire(new_window + 300))  # over limit again

    def test_single_capture_per_second(self):
        limiter = CaptureRateLimiter(1)
        base_time = 1_000_000_000_000

        self.assertTrue(limiter.try_acquire(base_time))
        self.assertFalse(limiter.try_acquire(base_time + 100))
        self.assertFalse(limiter.try_acquire(base_time + 1000))

        # Next second window
        self.assertTrue(limiter.try_acquire(base_time + ONE_SECOND_NS + 1))
        self.assertFalse(limiter.try_acquire(base_time + ONE_SECOND_NS + 2))

    def test_high_rate_allows_many(self):
        limiter = CaptureRateLimiter(1000)
        base_time = 1_000_000_000_000

        allowed = sum(1 for i in range(1500) if limiter.try_acquire(base_time + i))
        self.assertEqual(allowed, 1000)

    def test_current_count_tracks_captures(self):
        limiter = CaptureRateLimiter(10)
        base_time = 1_000_000_000_000

        self.assertEqual(limiter.current_count, 0)

        limiter.try_acquire(base_time)
        self.assertEqual(limiter.current_count, 1)

        limiter.try_acquire(base_time + 1)
        limiter.try_acquire(base_time + 2)
        self.assertEqual(limiter.current_count, 3)

    def test_multiple_window_rollovers(self):
        limiter = CaptureRateLimiter(2)
        base_time = 1_000_000_000_000

        # Window 1: allow 2
        self.assertTrue(limiter.try_acquire(base_time))
        self.assertTrue(limiter.try_acquire(base_time + 1))
        self.assertFalse(limiter.try_acquire(base_time + 2))

        # Window 2
        self.assertTrue(limiter.try_acquire(base_time + ONE_SECOND_NS + 1))
        self.assertTrue(limiter.try_acquire(base_time + ONE_SECOND_NS + 2))
        self.assertFalse(limiter.try_acquire(base_time + ONE_SECOND_NS + 3))

        # Window 3
        self.assertTrue(limiter.try_acquire(base_time + 2 * ONE_SECOND_NS + 1))
        self.assertTrue(limiter.try_acquire(base_time + 2 * ONE_SECOND_NS + 2))
        self.assertFalse(limiter.try_acquire(base_time + 2 * ONE_SECOND_NS + 3))

    def test_try_acquire_without_timestamp_uses_monotonic(self):
        """Verify that calling try_acquire() without args works (uses time.monotonic_ns)."""
        limiter = CaptureRateLimiter(5)
        # Should not raise and should return True for first call
        self.assertTrue(limiter.try_acquire())


if __name__ == "__main__":
    unittest.main()
