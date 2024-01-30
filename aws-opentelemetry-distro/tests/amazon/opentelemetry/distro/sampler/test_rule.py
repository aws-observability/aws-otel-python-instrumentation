# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
from unittest import TestCase

from amazon.opentelemetry.distro.sampler._rule import _Rule
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.util.types import Attributes

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(TEST_DIR, "data")


class TestRule(TestCase):
    def test_rule_attribute_matching_from_xray_response(self):
        default_rule = None
        with open(f"{DATA_DIR}/get-sampling-rules-response-sample-2.json", encoding="UTF-8") as file:
            sample_response = json.load(file)
            print(sample_response)
            all_rules = sample_response["SamplingRuleRecords"]
            default_rule = _SamplingRule(**all_rules[0]["SamplingRule"])
            file.close()

        res = Resource.create(
            attributes={
                ResourceAttributes.SERVICE_NAME: "test_service_name",
                ResourceAttributes.CLOUD_PLATFORM: "test_cloud_platform",
            }
        )
        attr: Attributes = {
            SpanAttributes.HTTP_TARGET: "target",
            SpanAttributes.HTTP_METHOD: "method",
            SpanAttributes.HTTP_URL: "url",
            SpanAttributes.HTTP_HOST: "host",
            "foo": "bar",
            "abc": "1234",
        }

        rule0 = _Rule(default_rule)
        self.assertTrue(rule0.matches(res, attr))

    def test_rule_matches_with_all_attributes(self):
        sampling_rule = _SamplingRule(
            Attributes={"abc": "123", "def": "4?6", "ghi": "*89"},
            FixedRate=0.11,
            HTTPMethod="GET",
            Host="localhost",
            Priority=20,
            ReservoirSize=1,
            # ResourceARN can only be "*"
            # See: https://docs.aws.amazon.com/xray/latest/devguide/xray-console-sampling.html#xray-console-sampling-options  # noqa: E501
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="myServiceName",
            ServiceType="AWS::EKS::Container",
            URLPath="/helloworld",
            Version=1,
        )

        attributes: Attributes = {
            "http.host": "localhost",
            SpanAttributes.HTTP_METHOD: "GET",
            "http.url": "http://127.0.0.1:5000/helloworld",
            "abc": "123",
            "def": "456",
            "ghi": "789",
        }

        resource_attr: Resource = {
            ResourceAttributes.SERVICE_NAME: "myServiceName",
            ResourceAttributes.CLOUD_PLATFORM: "aws_eks",
        }
        resource = Resource.create(attributes=resource_attr)

        rule = _Rule(sampling_rule)
        self.assertTrue(rule.matches(resource, attributes))

    def test_rule_wild_card_attributes_matches_span_attributes(self):
        sampling_rule = _SamplingRule(
            Attributes={
                "attr1": "*",
                "attr2": "*",
                "attr3": "HelloWorld",
                "attr4": "Hello*",
                "attr5": "*World",
                "attr6": "?ello*",
                "attr7": "Hell?W*d",
                "attr8": "*.World",
                "attr9": "*.World",
            },
            FixedRate=0.11,
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
        )

        attributes: Attributes = {
            "attr1": "",
            "attr2": "HelloWorld",
            "attr3": "HelloWorld",
            "attr4": "HelloWorld",
            "attr5": "HelloWorld",
            "attr6": "HelloWorld",
            "attr7": "HelloWorld",
            "attr8": "Hello.World",
            "attr9": "Bye.World",
        }

        rule = _Rule(sampling_rule)
        self.assertTrue(rule.matches(Resource.get_empty(), attributes))

    def test_rule_wild_card_attributes_matches_http_span_attributes(self):
        sampling_rule = _SamplingRule(
            Attributes={},
            FixedRate=0.11,
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
        )

        attributes: Attributes = {
            SpanAttributes.HTTP_HOST: "localhost",
            SpanAttributes.HTTP_METHOD: "GET",
            SpanAttributes.HTTP_URL: "http://127.0.0.1:5000/helloworld",
        }

        rule = _Rule(sampling_rule)
        self.assertTrue(rule.matches(Resource.get_empty(), attributes))

    def test_rule_wild_card_attributes_matches_with_empty_attributes(self):
        sampling_rule = _SamplingRule(
            Attributes={},
            FixedRate=0.11,
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
        )

        attributes: Attributes = {}
        resource_attr: Resource = {
            ResourceAttributes.SERVICE_NAME: "myServiceName",
            ResourceAttributes.CLOUD_PLATFORM: "aws_ec2",
        }
        resource = Resource.create(attributes=resource_attr)

        rule = _Rule(sampling_rule)
        self.assertTrue(rule.matches(resource, attributes))
        self.assertTrue(rule.matches(resource, None))
        self.assertTrue(rule.matches(Resource.get_empty(), attributes))
        self.assertTrue(rule.matches(Resource.get_empty(), None))
        self.assertTrue(rule.matches(None, attributes))
        self.assertTrue(rule.matches(None, None))

    def test_rule_does_not_match_without_http_target(self):
        sampling_rule = _SamplingRule(
            Attributes={},
            FixedRate=0.11,
            HTTPMethod="*",
            Host="*",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="*",
            ServiceType="*",
            URLPath="/helloworld",
            Version=1,
        )

        attributes: Attributes = {}
        resource_attr: Resource = {
            ResourceAttributes.SERVICE_NAME: "myServiceName",
            ResourceAttributes.CLOUD_PLATFORM: "aws_ec2",
        }
        resource = Resource.create(attributes=resource_attr)

        rule = _Rule(sampling_rule)
        self.assertFalse(rule.matches(resource, attributes))

    def test_rule_matches_with_http_target(self):
        sampling_rule = _SamplingRule(
            Attributes={},
            FixedRate=0.11,
            HTTPMethod="*",
            Host="*",
            Priority=20,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="*",
            ServiceType="*",
            URLPath="/hello*",
            Version=1,
        )

        attributes: Attributes = {SpanAttributes.HTTP_TARGET: "/helloworld"}
        resource_attr: Resource = {
            ResourceAttributes.SERVICE_NAME: "myServiceName",
            ResourceAttributes.CLOUD_PLATFORM: "aws_ec2",
        }
        resource = Resource.create(attributes=resource_attr)

        rule = _Rule(sampling_rule)
        self.assertTrue(rule.matches(resource, attributes))

    def test_rule_matches_with_span_attributes(self):
        sampling_rule = _SamplingRule(
            Attributes={"abc": "123", "def": "456", "ghi": "789"},
            FixedRate=0.11,
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
        )

        attributes: Attributes = {
            "http.host": "localhost",
            SpanAttributes.HTTP_METHOD: "GET",
            "http.url": "http://127.0.0.1:5000/helloworld",
            "abc": "123",
            "def": "456",
            "ghi": "789",
        }

        resource_attr: Resource = {
            ResourceAttributes.SERVICE_NAME: "myServiceName",
            ResourceAttributes.CLOUD_PLATFORM: "aws_eks",
        }
        resource = Resource.create(attributes=resource_attr)

        rule = _Rule(sampling_rule)
        self.assertTrue(rule.matches(resource, attributes))

    def test_rule_does_not_match_with_less_span_attributes(self):
        sampling_rule = _SamplingRule(
            Attributes={"abc": "123", "def": "456", "ghi": "789"},
            FixedRate=0.11,
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
        )

        attributes: Attributes = {
            "http.host": "localhost",
            SpanAttributes.HTTP_METHOD: "GET",
            "http.url": "http://127.0.0.1:5000/helloworld",
            "abc": "123",
        }

        resource_attr: Resource = {
            ResourceAttributes.SERVICE_NAME: "myServiceName",
            ResourceAttributes.CLOUD_PLATFORM: "aws_eks",
        }
        resource = Resource.create(attributes=resource_attr)

        rule = _Rule(sampling_rule)
        self.assertFalse(rule.matches(resource, attributes))
