# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import time
from logging import DEBUG
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import Decision

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(TEST_DIR, "data")


class TestAwsXRayRemoteSampler(TestCase):
    def test_create_remote_sampler_with_empty_resource(self):
        rs = AwsXRayRemoteSampler(resource=Resource.get_empty())
        self.assertIsNotNone(rs._rules_timer)
        self.assertEqual(rs._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__resource)
        self.assertTrue(len(rs._AwsXRayRemoteSampler__client_id), 24)

    def test_create_remote_sampler_with_populated_resource(self):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        self.assertIsNotNone(rs._rules_timer)
        self.assertEqual(rs._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__resource)
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["service.name"], "test-service-name")
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["cloud.platform"], "test-cloud-platform")

    def test_create_remote_sampler_with_all_fields_populated(self):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"}),
            endpoint="http://abc.com",
            polling_interval=120,
            log_level=DEBUG,
        )
        self.assertIsNotNone(rs._rules_timer)
        self.assertEqual(rs._AwsXRayRemoteSampler__polling_interval, 120)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__resource)
        self.assertEqual(
            rs._AwsXRayRemoteSampler__xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "http://abc.com/GetSamplingRules",
        )
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["service.name"], "test-service-name")
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["cloud.platform"], "test-cloud-platform")

    @staticmethod
    def mocked_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

        if kwargs["url"] == "http://127.0.0.1:2000/GetSamplingRules":
            with open(f"{DATA_DIR}/test-remote-sampler_sampling-rules-response-sample.json", encoding="UTF-8") as file:
                sample_response = json.load(file)
                file.close()
            return MockResponse(sample_response, 200)
        if kwargs["url"] == "http://127.0.0.1:2000/SamplingTargets":
            with open(
                f"{DATA_DIR}/test-remote-sampler_sampling-targets-response-sample.json", encoding="UTF-8"
            ) as file:
                sample_response = json.load(file)
                file.close()
            return MockResponse(sample_response, 200)
        return MockResponse(None, 404)

    @patch("requests.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", new=2)
    def test_update_sampling_rules_and_targets_with_pollers_and_should_sample(self, mock_post=None):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )

        time.sleep(1.0)
        self.assertEqual(
            rs._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "test"
        )
        self.assertEqual(rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.DROP)

        # wait 2 more seconds since targets polling was patched to 2 seconds (rather than 10s)
        time.sleep(2.0)
        self.assertNotEqual(rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.DROP)
        self.assertNotEqual(rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.DROP)
        self.assertNotEqual(rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.DROP)
