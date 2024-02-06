# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


# Disable snake_case naming style so this class can match the sampling rules response from X-Ray
# pylint: disable=invalid-name
class _SamplingRule:
    def __init__(
        self,
        Attributes: dict = None,
        FixedRate=None,
        HTTPMethod=None,
        Host=None,
        Priority=None,
        ReservoirSize=None,
        ResourceARN=None,
        RuleARN=None,
        RuleName=None,
        ServiceName=None,
        ServiceType=None,
        URLPath=None,
        Version=None,
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
