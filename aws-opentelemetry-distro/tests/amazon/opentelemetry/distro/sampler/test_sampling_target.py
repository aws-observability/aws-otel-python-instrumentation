# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.sampler._sampling_target import _SamplingTarget, _SamplingTargetResponse


class TestSamplingTarget(TestCase):
    def test_sampling_target_with_none_inputs(self):
        target = _SamplingTarget()
        self.assertEqual(target.FixedRate, 0.0)
        self.assertEqual(target.RuleName, "")
        self.assertIsNone(target.Interval)
        self.assertIsNone(target.ReservoirQuota)
        self.assertIsNone(target.ReservoirQuotaTTL)

    def test_sampling_target_with_extra_inputs(self):
        inputs = {
            "FixedRate": 1.0,
            "RuleName": "cat",
            "Interval": 123,
            "ReservoirQuota": 456,
            "ReservoirQuotaTTL": 789,
            "ExtraField1": "cat",
            "ExtraField2": 123,
        }

        with patch("amazon.opentelemetry.distro.sampler._sampling_target._logger") as mock_logger:
            target = _SamplingTarget(**inputs)
            mock_logger.debug.assert_called_once_with(
                "Ignoring unknown fields in _SamplingTarget: %s", ["ExtraField1", "ExtraField2"]
            )

            self.assertEqual(target.FixedRate, 1.0)
            self.assertEqual(target.RuleName, "cat")
            self.assertEqual(target.Interval, 123)
            self.assertEqual(target.ReservoirQuota, 456)
            self.assertEqual(target.ReservoirQuotaTTL, 789)
            self.assertFalse(hasattr(target, "ExtraField2"))

    def test_sampling_target_response_with_none_inputs(self):
        target_response = _SamplingTargetResponse(None, None, None)
        self.assertEqual(target_response.LastRuleModification, 0.0)
        self.assertEqual(target_response.SamplingTargetDocuments, [])
        self.assertEqual(target_response.UnprocessedStatistics, [])

    def test_sampling_target_response_with_invalid_inputs(self):
        target_response = _SamplingTargetResponse(1.0, [{}], [{}])
        self.assertEqual(target_response.LastRuleModification, 1.0)
        self.assertEqual(len(target_response.SamplingTargetDocuments), 1)
        self.assertEqual(target_response.SamplingTargetDocuments[0].FixedRate, 0)
        self.assertEqual(target_response.SamplingTargetDocuments[0].Interval, None)
        self.assertEqual(target_response.SamplingTargetDocuments[0].ReservoirQuota, None)
        self.assertEqual(target_response.SamplingTargetDocuments[0].ReservoirQuotaTTL, None)
        self.assertEqual(target_response.SamplingTargetDocuments[0].RuleName, "")

        self.assertEqual(len(target_response.UnprocessedStatistics), 1)
        self.assertEqual(target_response.UnprocessedStatistics[0].ErrorCode, "")
        self.assertEqual(target_response.UnprocessedStatistics[0].Message, "")
        self.assertEqual(target_response.UnprocessedStatistics[0].RuleName, "")

        target_response = _SamplingTargetResponse(1.0, [{"foo": "bar"}], [{"dog": "cat"}])
        self.assertEqual(len(target_response.SamplingTargetDocuments), 1)
        self.assertEqual(len(target_response.UnprocessedStatistics), 1)

    def test_sampling_target_response_with_extra_inputs(self):
        inputs = {
            "LastRuleModification": 1.0,
            "SamplingTargetDocuments": [{}],
            "UnprocessedStatistics": [{}],
            "ExtraField1": "cat",
            "ExtraField2": 123,
        }

        # Does not throw an error and logs debug message about unknown fields
        with patch("amazon.opentelemetry.distro.sampler._sampling_target._logger") as mock_logger:
            target_response = _SamplingTargetResponse(**inputs)
            mock_logger.debug.assert_called_once_with(
                "Ignoring unknown fields in _SamplingTargetResponse: %s", ["ExtraField1", "ExtraField2"]
            )

            self.assertEqual(target_response.LastRuleModification, 1.0)
            self.assertEqual(len(target_response.SamplingTargetDocuments), 1)
            self.assertEqual(target_response.SamplingTargetDocuments[0].FixedRate, 0)
            self.assertEqual(target_response.SamplingTargetDocuments[0].Interval, None)
            self.assertEqual(target_response.SamplingTargetDocuments[0].ReservoirQuota, None)
            self.assertEqual(target_response.SamplingTargetDocuments[0].ReservoirQuotaTTL, None)
            self.assertEqual(target_response.SamplingTargetDocuments[0].RuleName, "")
