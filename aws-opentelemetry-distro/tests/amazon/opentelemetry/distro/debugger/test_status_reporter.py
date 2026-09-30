# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from amazon.opentelemetry.distro.debugger._status_reporter import StatusReporter


class TestStatusReporterTime(unittest.TestCase):
    """Tests for timestamp handling in status reporter."""

    def test_to_epoch_seconds_none_uses_time_time(self):
        with patch("amazon.opentelemetry.distro.debugger._status_reporter.time.time", return_value=1234.56):
            self.assertEqual(StatusReporter._to_epoch_seconds(None), 1234)

    def test_to_epoch_seconds_naive_assumes_utc(self):
        naive = datetime(2026, 2, 2, 15, 26, 48)
        expected = int(datetime(2026, 2, 2, 15, 26, 48, tzinfo=timezone.utc).timestamp())
        self.assertEqual(StatusReporter._to_epoch_seconds(naive), expected)

    def test_to_epoch_seconds_aware_respects_timezone(self):
        aware = datetime(2026, 2, 2, 7, 26, 48, tzinfo=timezone(timedelta(hours=-8)))
        expected = int(aware.timestamp())
        self.assertEqual(StatusReporter._to_epoch_seconds(aware), expected)
