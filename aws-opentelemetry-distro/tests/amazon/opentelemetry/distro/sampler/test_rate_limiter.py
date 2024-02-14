# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import datetime
from unittest import TestCase

from mock_clock import MockClock

from amazon.opentelemetry.distro.sampler._rate_limiter import _RateLimiter


class TestRateLimiter(TestCase):
    def test_try_spend(self):
        time_now = datetime.datetime.fromtimestamp(1707551387.0)
        clock = MockClock(time_now)
        rate_limiter = _RateLimiter(1, 30, clock)

        spent = 0
        for _ in range(0, 100):
            if rate_limiter.try_spend(1, False):
                spent += 1
        self.assertEqual(spent, 0)

        spent = 0
        clock.add_time(0.5)
        for _ in range(0, 100):
            if rate_limiter.try_spend(1, False):
                spent += 1
        self.assertEqual(spent, 15)

        spent = 0
        clock.add_time(1)
        for _ in range(0, 100):
            if rate_limiter.try_spend(1, True):
                spent += 1
        self.assertEqual(spent, 1)

        spent = 0
        clock.add_time(1000)
        for _ in range(0, 100):
            if rate_limiter.try_spend(1, False):
                spent += 1
        self.assertEqual(spent, 30)
