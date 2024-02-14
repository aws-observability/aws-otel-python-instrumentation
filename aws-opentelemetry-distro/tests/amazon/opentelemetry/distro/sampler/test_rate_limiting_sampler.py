# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import datetime
from unittest import TestCase

from mock_clock import MockClock

from amazon.opentelemetry.distro.sampler._rate_limiting_sampler import _RateLimitingSampler
from opentelemetry.sdk.trace.sampling import Decision


class TestRateLimitingSampler(TestCase):
    def test_should_sample(self):
        time_now = datetime.datetime.fromtimestamp(1707551387.0)
        clock = MockClock(time_now)
        sampler = _RateLimitingSampler(30, clock)

        # Essentially the same tests as test_rate_limiter.py
        sampled = 0
        for _ in range(0, 100):
            if sampler.should_sample(None, 1234, "name").decision != Decision.DROP:
                sampled += 1
        self.assertEqual(sampled, 0)

        sampled = 0
        clock.add_time(0.5)
        for _ in range(0, 100):
            if sampler.should_sample(None, 1234, "name").decision != Decision.DROP:
                sampled += 1
        self.assertEqual(sampled, 15)

        sampler.borrowing = True
        sampled = 0
        clock.add_time(1)
        for _ in range(0, 100):
            if sampler.should_sample(None, 1234, "name").decision != Decision.DROP:
                sampled += 1
        self.assertEqual(sampled, 1)

        sampler.borrowing = False
        sampled = 0
        clock.add_time(1000)
        for _ in range(0, 100):
            if sampler.should_sample(None, 1234, "name").decision != Decision.DROP:
                sampled += 1
        self.assertEqual(sampled, 30)
