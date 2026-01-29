# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from logging import getLogger
from typing import Optional

_logger = getLogger(__name__)


# Disable snake_case naming style so this class can match the sampling rules response from X-Ray
# pylint: disable=invalid-name
class _SamplingRateBoost:
    def __init__(
        self,
        MaxRate: float = None,
        CooldownWindowMinutes: float = None,
        **kwargs,
    ):
        self.MaxRate = MaxRate if MaxRate is not None else 0.0
        self.CooldownWindowMinutes = CooldownWindowMinutes if CooldownWindowMinutes is not None else 1

        # Log unknown fields for debugging/monitoring
        if kwargs:
            _logger.debug("Ignoring unknown fields in _SamplingRateBoost: %s", list(kwargs.keys()))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _SamplingRateBoost):
            return False
        return self.MaxRate == other.MaxRate and self.CooldownWindowMinutes == other.CooldownWindowMinutes


# Disable snake_case naming style so this class can match the sampling rules response from X-Ray
# pylint: disable=invalid-name
class _SamplingRule:
    # pylint: disable=too-many-locals
    def __init__(
        self,
        Attributes: dict = None,
        FixedRate: float = None,
        HTTPMethod: str = None,
        Host: str = None,
        Priority: int = None,
        ReservoirSize: int = None,
        ResourceARN: str = None,
        RuleARN: str = None,
        RuleName: str = None,
        ServiceName: str = None,
        ServiceType: str = None,
        URLPath: str = None,
        Version: int = None,
        SamplingRateBoost: Optional[dict] = None,
        **kwargs,
    ):
        self.Attributes = Attributes if Attributes is not None else {}
        self.FixedRate = FixedRate if FixedRate is not None else 0.0
        self.HTTPMethod = HTTPMethod if HTTPMethod is not None else ""
        self.Host = Host if Host is not None else ""
        # Default to value with lower priority than default rule
        self.Priority = Priority if Priority is not None else 10001
        self.ReservoirSize = ReservoirSize if ReservoirSize is not None else 0
        self.ResourceARN = ResourceARN if ResourceARN is not None else ""
        self.RuleARN = RuleARN if RuleARN is not None else ""
        self.RuleName = RuleName if RuleName is not None else ""
        self.ServiceName = ServiceName if ServiceName is not None else ""
        self.ServiceType = ServiceType if ServiceType is not None else ""
        self.URLPath = URLPath if URLPath is not None else ""
        self.Version = Version if Version is not None else 0
        self.SamplingRateBoost: _SamplingRateBoost = (
            _SamplingRateBoost(**SamplingRateBoost) if SamplingRateBoost is not None else None
        )

        # Log unknown fields for debugging/monitoring
        if kwargs:
            _logger.debug("Ignoring unknown fields in _SamplingRule: %s", list(kwargs.keys()))

    def __lt__(self, other: "_SamplingRule") -> bool:
        if self.Priority == other.Priority:
            # String order priority example:
            # "A","Abc","a","ab","abc","abcdef"
            return self.RuleName < other.RuleName
        return self.Priority < other.Priority

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _SamplingRule):
            return False
        return (
            self.FixedRate == other.FixedRate
            and self.HTTPMethod == other.HTTPMethod
            and self.Host == other.Host
            and self.Priority == other.Priority
            and self.ReservoirSize == other.ReservoirSize
            and self.ResourceARN == other.ResourceARN
            and self.RuleARN == other.RuleARN
            and self.RuleName == other.RuleName
            and self.ServiceName == other.ServiceName
            and self.ServiceType == other.ServiceType
            and self.URLPath == other.URLPath
            and self.Version == other.Version
            and self.Attributes == other.Attributes
            and self.SamplingRateBoost == other.SamplingRateBoost
        )
