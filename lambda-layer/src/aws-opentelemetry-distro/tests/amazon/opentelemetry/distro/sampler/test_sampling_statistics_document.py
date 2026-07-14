# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import datetime
from unittest import TestCase

from mock_clock import MockClock

from amazon.opentelemetry.distro.sampler._sampling_statistics_document import _SamplingStatisticsDocument


class TestSamplingStatisticsDocument(TestCase):
    def test_sampling_statistics_document_inputs(self):
        statistics = _SamplingStatisticsDocument("", "", "")
        self.assertEqual(statistics.ClientID, "")
        self.assertEqual(statistics.RuleName, "")
        self.assertEqual(statistics.BorrowCount, 0)
        self.assertEqual(statistics.SampleCount, 0)
        self.assertEqual(statistics.RequestCount, 0)
        self.assertEqual(statistics.TotalCount, 0)
        self.assertEqual(statistics.AnomalyCount, 0)
        self.assertEqual(statistics.SampledAnomalyCount, 0)

        statistics = _SamplingStatisticsDocument("client_id", "rule_name", "service_name", 1, 2, 3, 4, 5, 6)
        self.assertEqual(statistics.ClientID, "client_id")
        self.assertEqual(statistics.RuleName, "rule_name")
        self.assertEqual(statistics.RequestCount, 1)
        self.assertEqual(statistics.BorrowCount, 2)
        self.assertEqual(statistics.SampleCount, 3)
        self.assertEqual(statistics.TotalCount, 4)
        self.assertEqual(statistics.AnomalyCount, 5)
        self.assertEqual(statistics.SampledAnomalyCount, 6)

        clock = MockClock(datetime.datetime.fromtimestamp(1707551387.0))
        stats, boost_stats = statistics.snapshot(clock)
        self.assertEqual(stats.get("ClientID"), "client_id")
        self.assertEqual(stats.get("RuleName"), "rule_name")
        self.assertEqual(stats.get("Timestamp"), 1707551387.0)
        self.assertEqual(stats.get("RequestCount"), 1)
        self.assertEqual(stats.get("BorrowCount"), 2)
        self.assertEqual(stats.get("SampleCount"), 3)
        self.assertEqual(boost_stats.get("ClientID"), "client_id")
        self.assertEqual(boost_stats.get("RuleName"), "rule_name")
        self.assertEqual(boost_stats.get("ServiceName"), "service_name")
        self.assertEqual(boost_stats.get("Timestamp"), 1707551387.0)
        self.assertEqual(boost_stats.get("TotalCount"), 4)
        self.assertEqual(boost_stats.get("AnomalyCount"), 5)
        self.assertEqual(boost_stats.get("SampledAnomalyCount"), 6)
