# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import threading
import time
from logging import DEBUG
from unittest import TestCase
from unittest.mock import patch

from mock_clock import MockClock

from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import Decision

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(TEST_DIR, "data")


def create_spans(sampled_array, thread_id, span_attributes, remote_sampler, number_of_spans):
    sampled = 0
    for _ in range(0, number_of_spans):
        if remote_sampler.should_sample(None, 0, "name", attributes=span_attributes).decision != Decision.DROP:
            sampled += 1
    sampled_array[thread_id] = sampled


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
        with open(f"{DATA_DIR}/test-remote-sampler_sampling-targets-response-sample.json", encoding="UTF-8") as file:
            sample_response = json.load(file)
            file.close()
        return MockResponse(sample_response, 200)
    return MockResponse(None, 404)


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

    @patch("requests.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", 2)
    def test_update_sampling_rules_and_targets_with_pollers_and_should_sample(self, mock_post=None):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        self.assertEqual(rs._AwsXRayRemoteSampler__target_polling_interval, 2)

        time.sleep(1.0)
        self.assertEqual(
            rs._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "test"
        )
        self.assertEqual(rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.DROP)

        # wait 2 more seconds since targets polling was patched to 2 seconds (rather than 10s)
        time.sleep(2.0)
        self.assertEqual(rs._AwsXRayRemoteSampler__target_polling_interval, 1000)
        self.assertEqual(
            rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.RECORD_AND_SAMPLE
        )
        self.assertEqual(
            rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.RECORD_AND_SAMPLE
        )
        self.assertEqual(
            rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.RECORD_AND_SAMPLE
        )

    @patch("requests.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", 3)
    def test_multithreading_with_large_reservoir_with_otel_sdk(self, mock_post=None):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        attributes = {"abc": "1234"}

        time.sleep(2.0)
        self.assertEqual(rs.should_sample(None, 0, "name", attributes=attributes).decision, Decision.DROP)

        # wait 3 more seconds since targets polling was patched to 2 seconds (rather than 10s)
        time.sleep(3.0)

        number_of_spans = 100
        thread_count = 1000
        sampled_array = []
        threads = []

        for idx in range(0, thread_count):
            sampled_array.append(0)
            threads.append(
                threading.Thread(
                    target=create_spans,
                    name="thread_" + str(idx),
                    daemon=True,
                    args=(sampled_array, idx, attributes, rs, number_of_spans),
                )
            )
            threads[idx].start()
        sum_sampled = 0

        for idx in range(0, thread_count):
            threads[idx].join()
            sum_sampled += sampled_array[idx]

        test_rule_applier = rs._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[0]
        self.assertEqual(
            test_rule_applier._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota,
            100000,
        )
        self.assertEqual(sum_sampled, 100000)

    # pylint: disable=no-member
    @patch("requests.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", 2)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler._Clock", MockClock)
    def test_multithreading_with_some_reservoir_with_otel_sdk(self, mock_post=None):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        attributes = {"abc": "non-matching attribute value, use default rule"}

        # Using normal clock, finishing all thread jobs will take more than a second,
        # which will eat up more than 1 second of reservoir. Using MockClock we can freeze time
        # and pretend all thread jobs start and end at the exact same time,
        # assume and test exactly 1 second of reservoir (100 quota) only
        mock_clock: MockClock = rs._clock

        time.sleep(1.0)
        mock_clock.add_time(1.0)
        self.assertEqual(mock_clock.now(), rs._clock.now())
        self.assertEqual(rs.should_sample(None, 0, "name", attributes=attributes).decision, Decision.RECORD_AND_SAMPLE)

        # wait 2 more seconds since targets polling was patched to 2 seconds (rather than 10s)
        time.sleep(2.0)
        mock_clock.add_time(2.0)
        self.assertEqual(mock_clock.now(), rs._clock.now())

        number_of_spans = 100
        thread_count = 1000
        sampled_array = []
        threads = []

        for idx in range(0, thread_count):
            sampled_array.append(0)
            threads.append(
                threading.Thread(
                    target=create_spans,
                    name="thread_" + str(idx),
                    daemon=True,
                    args=(sampled_array, idx, attributes, rs, number_of_spans),
                )
            )
            threads[idx].start()

        sum_sampled = 0
        for idx in range(0, thread_count):
            threads[idx].join()
            sum_sampled += sampled_array[idx]

        default_rule_applier = rs._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[1]
        self.assertEqual(
            default_rule_applier._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota,
            100,
        )
        self.assertEqual(sum_sampled, 100)
