# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.sampler._aws_xray_adaptive_sampling_config import (
    _AnomalyCaptureLimit,
    _AnomalyConditions,
    _AWSXRayAdaptiveSamplingConfig,
    _UsageType,
)


class TestAWSXRayAdaptiveSamplingConfig(TestCase):
    def test_invalid_version(self):
        with self.assertRaises(ValueError) as context:
            _AWSXRayAdaptiveSamplingConfig(version="1.0")
        self.assertEqual(str(context.exception), "Invalid adaptive sampling configuration")

    def test_invalid_anomaly_conditions(self):
        with self.assertRaises(ValueError) as context:
            _AWSXRayAdaptiveSamplingConfig(version=1.0, anomaly_conditions="invalid")
        self.assertEqual(str(context.exception), "Invalid anomaly conditions configuration")

    def test_invalid_anomaly_capture_limit(self):
        with self.assertRaises(ValueError) as context:
            _AWSXRayAdaptiveSamplingConfig(version=1.0, anomaly_capture_limit="invalid")
        self.assertEqual(str(context.exception), "Invalid anomaly capture limit configuration")


class TestAnomalyConditions(TestCase):
    def test_invalid_error_code_regex(self):
        with self.assertRaises(ValueError) as context:
            _AnomalyConditions(error_code_regex=123)
        self.assertEqual(str(context.exception), "Invalid errorCodeRegex in anomaly condition")

    def test_invalid_operations(self):
        with self.assertRaises(ValueError) as context:
            _AnomalyConditions(operations="invalid")
        self.assertEqual(str(context.exception), "Invalid operations in anomaly condition")

    def test_invalid_high_latency_ms(self):
        with self.assertRaises(ValueError) as context:
            _AnomalyConditions(high_latency_ms="100")
        self.assertEqual(str(context.exception), "Invalid highLatencyMs in anomaly condition")

    def test_invalid_usage(self):
        with self.assertRaises(ValueError) as context:
            _AnomalyConditions(usage="both")
        self.assertEqual(str(context.exception), "Invalid usage in anomaly condition")


class TestAnomalyCaptureLimit(TestCase):
    def test_none_anomaly_traces_per_second(self):
        with self.assertRaises(ValueError) as context:
            _AnomalyCaptureLimit(anomaly_traces_per_second=None)
        self.assertEqual(str(context.exception), "Invalid anomalyTracesPerSecond in anomaly capture limit")

    def test_invalid_anomaly_traces_per_second(self):
        with self.assertRaises(ValueError) as context:
            _AnomalyCaptureLimit(anomaly_traces_per_second="10")
        self.assertEqual(str(context.exception), "Invalid anomalyTracesPerSecond in anomaly capture limit")


class TestUsageType(TestCase):
    def test_is_used_for_boost(self):
        self.assertTrue(_UsageType.is_used_for_boost(_UsageType.BOTH))
        self.assertTrue(_UsageType.is_used_for_boost(_UsageType.SAMPLING_BOOST))
        self.assertFalse(_UsageType.is_used_for_boost(_UsageType.ANOMALY_TRACE_CAPTURE))
        self.assertFalse(_UsageType.is_used_for_boost(_UsageType.NEITHER))

    def test_is_used_for_anomaly(self):
        self.assertTrue(_UsageType.is_used_for_anomaly_trace_capture(_UsageType.BOTH))
        self.assertTrue(_UsageType.is_used_for_anomaly_trace_capture(_UsageType.ANOMALY_TRACE_CAPTURE))
        self.assertFalse(_UsageType.is_used_for_anomaly_trace_capture(_UsageType.SAMPLING_BOOST))
        self.assertFalse(_UsageType.is_used_for_anomaly_trace_capture(_UsageType.NEITHER))
