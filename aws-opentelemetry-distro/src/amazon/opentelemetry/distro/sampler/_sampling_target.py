# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import getLogger
from typing import List, Optional

_logger = getLogger(__name__)


# Disable snake_case naming style so this class can match the sampling rules response from X-Ray
# pylint: disable=invalid-name
class _SamplingBoost:
    def __init__(self, BoostRate: float = None, BoostRateTTL: float = None, **kwargs):
        self.BoostRate = BoostRate if BoostRate is not None else 0.0
        self.BoostRateTTL = BoostRateTTL  # can be None

        # Log unknown fields for debugging/monitoring
        if kwargs:
            _logger.debug("Ignoring unknown fields in _SamplingBoost: %s", list(kwargs.keys()))


# Disable snake_case naming style so this class can match the sampling rules response from X-Ray
# pylint: disable=invalid-name
class _SamplingTarget:
    def __init__(
        self,
        FixedRate: float = None,
        Interval: int = None,
        ReservoirQuota: int = None,
        ReservoirQuotaTTL: float = None,
        SamplingBoost: Optional[_SamplingBoost] = None,
        RuleName: str = None,
        **kwargs,
    ):
        self.FixedRate = FixedRate if FixedRate is not None else 0.0
        self.Interval = Interval  # can be None
        self.ReservoirQuota = ReservoirQuota  # can be None
        self.ReservoirQuotaTTL = ReservoirQuotaTTL  # can be None
        self.SamplingBoost = _SamplingBoost(**SamplingBoost) if SamplingBoost else None
        self.RuleName = RuleName if RuleName is not None else ""

        # Log unknown fields for debugging/monitoring
        if kwargs:
            _logger.debug("Ignoring unknown fields in _SamplingTarget: %s", list(kwargs.keys()))


class _UnprocessedStatistics:
    def __init__(
        self,
        ErrorCode: str = None,
        Message: str = None,
        RuleName: str = None,
        **kwargs,
    ):
        self.ErrorCode = ErrorCode if ErrorCode is not None else ""
        self.Message = Message if ErrorCode is not None else ""
        self.RuleName = RuleName if ErrorCode is not None else ""

        # Log unknown fields for debugging/monitoring
        if kwargs:
            _logger.debug("Ignoring unknown fields in _UnprocessedStatistics: %s", list(kwargs.keys()))


class _SamplingTargetResponse:
    def __init__(
        self,
        LastRuleModification: float,
        SamplingTargetDocuments: List[dict] = None,
        UnprocessedStatistics: List[dict] = None,
        UnprocessedBoostStatistics: List[dict] = None,
        **kwargs,
    ):
        self.LastRuleModification: float = LastRuleModification if LastRuleModification is not None else 0.0

        self.SamplingTargetDocuments: List[_SamplingTarget] = []
        if SamplingTargetDocuments is not None:
            for document in SamplingTargetDocuments:
                try:
                    self.SamplingTargetDocuments.append(_SamplingTarget(**document))
                except TypeError as e:
                    _logger.debug("TypeError occurred: %s", e)

        self.UnprocessedStatistics: List[_UnprocessedStatistics] = []
        if UnprocessedStatistics is not None:
            for unprocessed in UnprocessedStatistics:
                try:
                    self.UnprocessedStatistics.append(_UnprocessedStatistics(**unprocessed))
                except TypeError as e:
                    _logger.debug("TypeError occurred: %s", e)

        self.UnprocessedBoostStatistics: List[_UnprocessedStatistics] = []
        if UnprocessedBoostStatistics is not None:
            for unprocessed in UnprocessedBoostStatistics:
                try:
                    self.UnprocessedBoostStatistics.append(_UnprocessedStatistics(**unprocessed))
                except TypeError as e:
                    _logger.debug("TypeError occurred: %s", e)

        # Log unknown fields for debugging/monitoring
        if kwargs:
            _logger.debug("Ignoring unknown fields in _SamplingTargetResponse: %s", list(kwargs.keys()))
