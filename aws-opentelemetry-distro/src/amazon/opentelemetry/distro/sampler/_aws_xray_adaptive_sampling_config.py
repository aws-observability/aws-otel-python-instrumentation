# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from enum import Enum
from typing import List, Optional


class _UsageType(Enum):
    BOTH = "both"
    SAMPLING_BOOST = "sampling-boost"
    ANOMALY_TRACE_CAPTURE = "anomaly-trace-capture"
    NEITHER = "neither"

    @staticmethod
    def is_used_for_boost(usage: "_UsageType") -> bool:
        return usage in (_UsageType.BOTH, _UsageType.SAMPLING_BOOST)

    @staticmethod
    def is_used_for_anomaly_trace_capture(usage: "_UsageType") -> bool:
        return usage in (_UsageType.BOTH, _UsageType.ANOMALY_TRACE_CAPTURE)


class _AWSXRayAdaptiveSamplingConfig:
    def __init__(
        self,
        version: float,
        anomaly_conditions: Optional[List["_AnomalyConditions"]] = None,
        anomaly_capture_limit: Optional["_AnomalyCaptureLimit"] = None,
    ):
        if not isinstance(version, float):
            raise ValueError("Invalid adaptive sampling configuration")
        if anomaly_conditions is not None and not isinstance(anomaly_conditions, List):
            raise ValueError("Invalid anomaly conditions configuration")
        if anomaly_capture_limit is not None and not isinstance(anomaly_capture_limit, _AnomalyCaptureLimit):
            raise ValueError("Invalid anomaly capture limit configuration")

        self.version = version
        self.anomaly_conditions = anomaly_conditions
        self.anomaly_capture_limit = anomaly_capture_limit


class _AnomalyConditions:
    def __init__(
        self,
        error_code_regex: Optional[str] = None,
        operations: Optional[List[str]] = None,
        high_latency_ms: Optional[int] = None,
        usage: Optional[_UsageType] = None,
    ):
        if error_code_regex is not None and not isinstance(error_code_regex, str):
            raise ValueError("Invalid errorCodeRegex in anomaly condition")
        if operations is not None and not isinstance(operations, List):
            raise ValueError("Invalid operations in anomaly condition")
        if high_latency_ms is not None and not isinstance(high_latency_ms, int):
            raise ValueError("Invalid highLatencyMs in anomaly condition")
        if usage is not None and not isinstance(usage, _UsageType):
            raise ValueError("Invalid usage in anomaly condition")
        self.error_code_regex = error_code_regex
        self.operations = operations
        self.high_latency_ms = high_latency_ms
        self.usage = usage


class _AnomalyCaptureLimit:
    def __init__(self, anomaly_traces_per_second: int):
        if anomaly_traces_per_second is None or not isinstance(anomaly_traces_per_second, int):
            raise ValueError("Invalid anomalyTracesPerSecond in anomaly capture limit")
        self.anomaly_traces_per_second = anomaly_traces_per_second
