# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
class SamplingRule:
    def __init__(
        self, 
        Attributes={},
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
        Version=None
    ):
        self.Attributes=Attributes
        self.FixedRate=FixedRate
        self.HTTPMethod=HTTPMethod
        self.Host=Host
        self.Priority=Priority
        self.ReservoirSize=ReservoirSize
        self.ResourceARN=ResourceARN
        self.RuleARN=RuleARN
        self.RuleName=RuleName
        self.ServiceName=ServiceName
        self.ServiceType=ServiceType
        self.URLPath=URLPath
        self.Version=Version
