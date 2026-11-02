# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRateBoost, _SamplingRule


class TestSamplingRateBoost(TestCase):
    def test_sampling_rate_boost_with_extra_fields(self):
        input = {
            "MaxRate": 0.5,
            "CooldownWindowMinutes": 2,
            "ExtraField1": "cat",
            "ExtraField2": 123,
        }
        with patch("amazon.opentelemetry.distro.sampler._sampling_rule._logger") as mock_logger:
            _SamplingRateBoost(**input)
            mock_logger.debug.assert_called_once_with(
                "Ignoring unknown fields in _SamplingRateBoost: %s", ["ExtraField1", "ExtraField2"]
            )

    def test_sampling_rate_boost_equality(self):
        boost1 = _SamplingRateBoost(MaxRate=0.5, CooldownWindowMinutes=2)
        boost2 = _SamplingRateBoost(MaxRate=0.5, CooldownWindowMinutes=2)
        self.assertEqual(boost1, boost2)

        boost1 = _SamplingRateBoost(MaxRate=0.5, CooldownWindowMinutes=2)
        boost2 = _SamplingRateBoost(MaxRate=0.5, CooldownWindowMinutes=3)
        self.assertNotEqual(boost1, boost2)

        boost2 = {"other": "object"}
        self.assertNotEqual(boost1, boost2)


class TestSamplingRule(TestCase):
    def test_sampling_rule_ordering(self):
        rule1 = _SamplingRule(Priority=1, RuleName="abcdef", Version=1)
        rule2 = _SamplingRule(Priority=100, RuleName="A", Version=1)
        rule3 = _SamplingRule(Priority=100, RuleName="Abc", Version=1)
        rule4 = _SamplingRule(Priority=100, RuleName="ab", Version=1)
        rule5 = _SamplingRule(Priority=100, RuleName="abc", Version=1)
        rule6 = _SamplingRule(Priority=200, RuleName="abcdef", Version=1)

        self.assertTrue(rule1 < rule2 < rule3 < rule4 < rule5 < rule6)

    def test_sampling_rule_with_extra_fields(self):
        inputs = {
            "Attributes": {},
            "FixedRate": 0.1,
            "HTTPMethod": "GET",
            "Host": "localhost",
            "Priority": 20,
            "ReservoirSize": 1,
            "ResourceARN": "*",
            "RuleARN": "arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            "RuleName": "test",
            "ServiceName": "myServiceName",
            "ServiceType": "AWS::EKS::Container",
            "URLPath": "/helloworld",
            "Version": 1,
            "SamplingRateBoost": {"MaxRate": 0.5, "CooldownWindowMinutes": 2},
            "ExtraField1": "cat",
            "ExtraField2": 123,
        }

        # Does not throw an error and logs debug message about unknown fields
        with patch("amazon.opentelemetry.distro.sampler._sampling_rule._logger") as mock_logger:
            rule = _SamplingRule(**inputs)
            mock_logger.debug.assert_called_once_with(
                "Ignoring unknown fields in _SamplingRule: %s", ["ExtraField1", "ExtraField2"]
            )

            self.assertEqual(rule.FixedRate, 0.1)
            self.assertEqual(rule.RuleName, "test")
            self.assertEqual(rule.ServiceName, "myServiceName")

    def test_sampling_rule_equality(self):
        sampling_rule = _SamplingRule(
            Attributes={"abc": "123", "def": "4?6", "ghi": "*89"},
            FixedRate=0.11,
            HTTPMethod="GET",
            Host="localhost",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="myServiceName",
            ServiceType="AWS::EKS::Container",
            URLPath="/helloworld",
            Version=1,
        )

        sampling_rule_attr_unordered = _SamplingRule(
            Attributes={"ghi": "*89", "abc": "123", "def": "4?6"},
            FixedRate=0.11,
            HTTPMethod="GET",
            Host="localhost",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="myServiceName",
            ServiceType="AWS::EKS::Container",
            URLPath="/helloworld",
            Version=1,
        )

        self.assertTrue(sampling_rule == sampling_rule_attr_unordered)

        sampling_rule_updated = _SamplingRule(
            Attributes={"ghi": "*89", "abc": "123", "def": "4?6"},
            FixedRate=0.11,
            HTTPMethod="GET",
            Host="localhost",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="myServiceName",
            ServiceType="AWS::EKS::Container",
            URLPath="/helloworld_new",
            Version=1,
        )

        sampling_rule_updated_2 = _SamplingRule(
            Attributes={"abc": "128", "def": "4?6", "ghi": "*89"},
            FixedRate=0.11,
            HTTPMethod="GET",
            Host="localhost",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="myServiceName",
            ServiceType="AWS::EKS::Container",
            URLPath="/helloworld",
            Version=1,
        )

        self.assertFalse(sampling_rule == sampling_rule_updated)
        self.assertFalse(sampling_rule == sampling_rule_updated_2)

        sampling_rule_with_boost_1 = _SamplingRule(
            Attributes={},
            FixedRate=0,
            HTTPMethod="*",
            Host="*",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
            SamplingRateBoost={
                "MaxRate": 0.2,
                "CooldownWindowMinutes": 2,
            },
        )

        sampling_rule_with_boost_2 = _SamplingRule(
            Attributes={},
            FixedRate=0,
            HTTPMethod="*",
            Host="*",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
            SamplingRateBoost={
                "MaxRate": 0.1,
                "CooldownWindowMinutes": 1,
            },
        )

        self.assertFalse(sampling_rule_with_boost_1 == sampling_rule_with_boost_2)

        # Simple inequality check between sampling rule and object of another type
        self.assertNotEqual(sampling_rule, {"other": "object"})
