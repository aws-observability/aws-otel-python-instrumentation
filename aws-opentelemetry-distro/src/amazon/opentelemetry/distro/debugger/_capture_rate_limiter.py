# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Per-instrumentation rate limiter using a fixed-window token bucket algorithm.

Limits the number of snapshot captures per second for a single instrumentation
configuration (probe or breakpoint). This prevents high-throughput methods from
generating excessive capture overhead.

The algorithm divides time into 1-second windows. Each window allows up to
``max_captures_per_second`` captures. When the window rolls over, the counter resets.

Thread-safe using :class:`threading.Lock`. Designed to be called on the
application's hot path with minimal overhead.
"""

import threading
import time

DEFAULT_MAX_CAPTURES_PER_SECOND = 5

_ONE_SECOND_NS = 1_000_000_000


class CaptureRateLimiter:
    """Fixed-window token bucket rate limiter for snapshot captures."""

    def __init__(self, max_captures_per_second: int = DEFAULT_MAX_CAPTURES_PER_SECOND):
        if max_captures_per_second <= 0:
            raise ValueError(f"max_captures_per_second must be positive, got: {max_captures_per_second}")
        self._max_captures_per_second = max_captures_per_second
        self._window_start_ns: int = 0  # lazily set on first try_acquire
        self._capture_count: int = 0
        self._lock = threading.Lock()

    def try_acquire(self, now_ns: int = 0) -> bool:
        """Try to acquire a capture permit.

        Args:
            now_ns: Current monotonic time in nanoseconds. If 0, uses time.monotonic_ns().

        Returns:
            True if capture is allowed, False if rate limit exceeded.
        """
        if now_ns == 0:
            now_ns = time.monotonic_ns()

        with self._lock:
            # Lazy init: first call sets the window start
            if self._window_start_ns == 0:
                self._window_start_ns = now_ns
                self._capture_count = 1
                return True

            elapsed = now_ns - self._window_start_ns
            if elapsed >= _ONE_SECOND_NS:
                # Window expired — roll over
                self._window_start_ns = now_ns
                self._capture_count = 1
                return True

            if self._capture_count < self._max_captures_per_second:
                self._capture_count += 1
                return True

            return False

    @property
    def max_captures_per_second(self) -> int:
        return self._max_captures_per_second

    @property
    def current_count(self) -> int:
        return self._capture_count
