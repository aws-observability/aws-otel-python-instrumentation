# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amazon.opentelemetry.distro.sampler._clock import _Clock


# Disable snake_case naming style so this class can match the statistics document response from X-Ray
# pylint: disable=invalid-name
class _SamplingStatisticsDocument:
    def __init__(
        self,
        clientID: str,
        ruleName: str,
        serviceName: str,
        RequestCount: int = 0,
        BorrowCount: int = 0,
        SampleCount: int = 0,
        TotalCount: int = 0,
        AnomalyCount: int = 0,
        SampledAnomalyCount: int = 0,
    ):
        self.ClientID = clientID
        self.RuleName = ruleName
        self.ServiceName = serviceName
        self.Timestamp = None

        self.RequestCount = RequestCount
        self.BorrowCount = BorrowCount
        self.SampleCount = SampleCount

        self.TotalCount = TotalCount
        self.AnomalyCount = AnomalyCount
        self.SampledAnomalyCount = SampledAnomalyCount

    def snapshot(self, clock: _Clock) -> tuple[dict, dict]:
        return (
            {
                "ClientID": self.ClientID,
                "RuleName": self.RuleName,
                "Timestamp": clock.now().timestamp(),
                "RequestCount": self.RequestCount,
                "BorrowCount": self.BorrowCount,
                "SampleCount": self.SampleCount,
            },
            {
                "ClientID": self.ClientID,
                "RuleName": self.RuleName,
                "ServiceName": self.ServiceName,
                "Timestamp": clock.now().timestamp(),
                "TotalCount": self.TotalCount,
                "AnomalyCount": self.AnomalyCount,
                "SampledAnomalyCount": self.SampledAnomalyCount,
            },
        )
