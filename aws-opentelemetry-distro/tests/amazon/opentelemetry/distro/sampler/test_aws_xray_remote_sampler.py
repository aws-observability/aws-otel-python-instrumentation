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

from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler, _AwsXRayRemoteSampler
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Tracer, TracerProvider
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
    def setUp(self):
        self.rs = None

    def tearDown(self):
        # Clean up timers
        if self.rs is not None:
            self.rs._root._root._rules_timer.cancel()
            self.rs._root._root._targets_timer.cancel()

    def test_create_remote_sampler_with_empty_resource(self):
        self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty())
        self.assertIsNotNone(self.rs._root._root._rules_timer)
        self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__resource)
        self.assertTrue(len(self.rs._root._root._AwsXRayRemoteSampler__client_id), 24)

    def test_create_remote_sampler_with_populated_resource(self):
        self.rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        self.assertIsNotNone(self.rs._root._root._rules_timer)
        self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__resource)
        self.assertEqual(
            self.rs._root._root._AwsXRayRemoteSampler__resource.attributes["service.name"], "test-service-name"
        )
        self.assertEqual(
            self.rs._root._root._AwsXRayRemoteSampler__resource.attributes["cloud.platform"], "test-cloud-platform"
        )

    def test_create_remote_sampler_with_all_fields_populated(self):
        self.rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"}),
            endpoint="http://abc.com",
            polling_interval=120,
            log_level=DEBUG,
        )
        self.assertIsNotNone(self.rs._root._root._rules_timer)
        self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__polling_interval, 120)
        self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__resource)
        self.assertEqual(
            self.rs._root._root._AwsXRayRemoteSampler__xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "http://abc.com/GetSamplingRules",
        )
        self.assertEqual(
            self.rs._root._root._AwsXRayRemoteSampler__resource.attributes["service.name"], "test-service-name"
        )
        self.assertEqual(
            self.rs._root._root._AwsXRayRemoteSampler__resource.attributes["cloud.platform"], "test-cloud-platform"
        )

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", 2)
    def test_update_sampling_rules_and_targets_with_pollers_and_should_sample(self, mock_post=None):
        self.rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__target_polling_interval, 2)

        time.sleep(1.0)
        self.assertEqual(
            self.rs._root._root._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[0].sampling_rule.RuleName,
            "test",
        )
        self.assertEqual(self.rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision, Decision.DROP)

        # wait 2 more seconds since targets polling was patched to 2 seconds (rather than 10s)
        time.sleep(2.0)
        self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__target_polling_interval, 1000)
        self.assertEqual(
            self.rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision,
            Decision.RECORD_AND_SAMPLE,
        )
        self.assertEqual(
            self.rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision,
            Decision.RECORD_AND_SAMPLE,
        )
        self.assertEqual(
            self.rs.should_sample(None, 0, "name", attributes={"abc": "1234"}).decision,
            Decision.RECORD_AND_SAMPLE,
        )

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", 3)
    def test_multithreading_with_large_reservoir_with_otel_sdk(self, mock_post=None):
        self.rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        attributes = {"abc": "1234"}

        time.sleep(2.0)
        self.assertEqual(self.rs.should_sample(None, 0, "name", attributes=attributes).decision, Decision.DROP)

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
                    args=(sampled_array, idx, attributes, self.rs, number_of_spans),
                )
            )
            threads[idx].start()
        sum_sampled = 0

        for idx in range(0, thread_count):
            threads[idx].join()
            sum_sampled += sampled_array[idx]

        test_rule_applier = self.rs._root._root._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[0]
        self.assertEqual(
            test_rule_applier._SamplingRuleApplier__reservoir_sampler._RateLimitingSampler__reservoir._quota,
            100000,
        )
        self.assertEqual(sum_sampled, 100000)

    # pylint: disable=no-member
    @patch("requests.Session.post", side_effect=mocked_requests_get)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler.DEFAULT_TARGET_POLLING_INTERVAL_SECONDS", 2)
    @patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler._Clock", MockClock)
    def test_multithreading_with_some_reservoir_with_otel_sdk(self, mock_post=None):
        self.rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        attributes = {"abc": "non-matching attribute value, use default rule"}

        # Using normal clock, finishing all thread jobs will take more than a second,
        # which will eat up more than 1 second of reservoir. Using MockClock we can freeze time
        # and pretend all thread jobs start and end at the exact same time,
        # assume and test exactly 1 second of reservoir (100 quota) only
        mock_clock: MockClock = self.rs._root._root._clock

        time.sleep(1.0)
        mock_clock.add_time(1.0)
        self.assertEqual(mock_clock.now(), self.rs._root._root._clock.now())
        self.assertEqual(
            self.rs.should_sample(None, 0, "name", attributes=attributes).decision, Decision.RECORD_AND_SAMPLE
        )

        # wait 2 more seconds since targets polling was patched to 2 seconds (rather than 10s)
        time.sleep(2.0)
        mock_clock.add_time(2.0)
        self.assertEqual(mock_clock.now(), self.rs._root._root._clock.now())

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
                    args=(sampled_array, idx, attributes, self.rs, number_of_spans),
                )
            )
            threads[idx].start()

        sum_sampled = 0
        for idx in range(0, thread_count):
            threads[idx].join()
            sum_sampled += sampled_array[idx]

        default_rule_applier = self.rs._root._root._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[1]
        self.assertEqual(
            default_rule_applier._SamplingRuleApplier__reservoir_sampler._RateLimitingSampler__reservoir._quota,
            100,
        )
        self.assertEqual(sum_sampled, 100)

    def test_get_description(self) -> str:
        self.rs: AwsXRayRemoteSampler = AwsXRayRemoteSampler(resource=Resource.create({"service.name": "dummy_name"}))
        self.assertEqual(
            self.rs.get_description(),
            "AwsXRayRemoteSampler{root:ParentBased{root:_AwsXRayRemoteSampler{remote sampling with AWS X-Ray},remoteParentSampled:AlwaysOnSampler,remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,localParentNotSampled:AlwaysOffSampler}}",  # noqa: E501
        )

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    def test_parent_based_xray_sampler_updates_statistics_once_for_one_parent_span_with_two_children(
        self, mock_post=None
    ):
        self.rs: AwsXRayRemoteSampler = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "use-default-sample-all-rule"})
        )
        time.sleep(1.0)

        provider = TracerProvider(sampler=self.rs)
        tracer: Tracer = provider.get_tracer("test_tracer_1")

        # child1 and child2 are child spans of root parent0
        # For AwsXRayRemoteSampler (ParentBased), expect only parent0 to update statistics
        with tracer.start_as_current_span("parent0") as _:
            with tracer.start_as_current_span("child1") as _:
                pass
            with tracer.start_as_current_span("child2") as _:
                pass
        default_rule_applier = self.rs._root._root._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[1]
        self.assertEqual(
            default_rule_applier._SamplingRuleApplier__statistics.RequestCount,
            1,
        )
        self.assertEqual(
            default_rule_applier._SamplingRuleApplier__statistics.SampleCount,
            1,
        )

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    def test_non_parent_based_xray_sampler_updates_statistics_thrice_for_one_parent_span_with_two_children(
        self, mock_post=None
    ):
        non_parent_based_xray_sampler: _AwsXRayRemoteSampler = _AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "use-default-sample-all-rule"})
        )
        time.sleep(1.0)

        provider = TracerProvider(sampler=non_parent_based_xray_sampler)
        tracer: Tracer = provider.get_tracer("test_tracer_2")

        # child1 and child2 are child spans of root parent0
        # For _AwsXRayRemoteSampler (Non-ParentBased), expect all 3 spans to update statistics
        with tracer.start_as_current_span("parent0") as _:
            with tracer.start_as_current_span("child1") as _:
                pass
            with tracer.start_as_current_span("child2") as _:
                pass
        default_rule_applier = (
            non_parent_based_xray_sampler._AwsXRayRemoteSampler__rule_cache._RuleCache__rule_appliers[1]
        )
        self.assertEqual(
            default_rule_applier._SamplingRuleApplier__statistics.RequestCount,
            3,
        )
        self.assertEqual(
            default_rule_applier._SamplingRuleApplier__statistics.SampleCount,
            3,
        )

        non_parent_based_xray_sampler._rules_timer.cancel()
        non_parent_based_xray_sampler._targets_timer.cancel()

    def test_create_remote_sampler_with_none_resource(self):
        """Tests creating remote sampler with None resource"""
        with patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler._logger") as mock_logger:
            self.rs = AwsXRayRemoteSampler(resource=None)

            # Verify warning was logged for None resource
            mock_logger.warning.assert_called_once_with(
                "OTel Resource provided is `None`. Defaulting to empty resource"
            )

            # Verify empty resource was set
            self.assertIsNotNone(self.rs._root._root._AwsXRayRemoteSampler__resource)
            self.assertEqual(len(self.rs._root._root._AwsXRayRemoteSampler__resource.attributes), 0)

    def test_create_remote_sampler_with_small_polling_interval(self):
        """Tests creating remote sampler with polling interval < 10"""
        with patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler._logger") as mock_logger:
            self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty(), polling_interval=5)  # Less than 10

            # Verify info log was called for small polling interval
            mock_logger.info.assert_any_call("`polling_interval` is `None` or too small. Defaulting to %s", 300)

            # Verify default polling interval was set
            self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__polling_interval, 300)

    def test_create_remote_sampler_with_none_endpoint(self):
        """Tests creating remote sampler with None endpoint"""
        with patch("amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler._logger") as mock_logger:
            self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty(), endpoint=None)

            # Verify info log was called for None endpoint
            mock_logger.info.assert_any_call("`endpoint` is `None`. Defaulting to %s", "http://127.0.0.1:2000")

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    def test_should_sample_with_expired_rule_cache(self, mock_post=None):
        """Tests should_sample behavior when rule cache is expired"""
        self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty())

        # Mock rule cache to be expired
        with patch.object(
            self.rs._root._root._AwsXRayRemoteSampler__rule_cache, "expired", return_value=True
        ):  # pylint: disable=not-context-manager
            with patch(
                "amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler._logger"
            ) as mock_logger:  # pylint: disable=not-context-manager
                # Call should_sample when cache is expired
                result = self.rs._root._root.should_sample(None, 0, "test_span")  # pylint: disable=not-context-manager

                # Verify debug log was called
                mock_logger.debug.assert_called_once_with("Rule cache is expired so using fallback sampling strategy")

                # Verify fallback sampler was used (should return some result)
                self.assertIsNotNone(result)

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    def test_refresh_rules_when_targets_require_it(self, mock_post=None):
        """Tests that sampling rules are refreshed when targets polling indicates it"""
        self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty())

        # Mock the rule cache update_sampling_targets to return refresh_rules=True
        with patch.object(
            self.rs._root._root._AwsXRayRemoteSampler__rule_cache,
            "update_sampling_targets",
            return_value=(True, None),  # refresh_rules=True, min_polling_interval=None
        ):
            # Mock get_and_update_sampling_rules to track if it was called
            with patch.object(
                self.rs._root._root, "_AwsXRayRemoteSampler__get_and_update_sampling_rules"
            ) as mock_update_rules:
                # Call the method that should trigger rule refresh
                self.rs._root._root._AwsXRayRemoteSampler__get_and_update_sampling_targets()

                # Verify that rules were refreshed
                mock_update_rules.assert_called_once()

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    def test_update_target_polling_interval(self, mock_post=None):
        """Tests that target polling interval is updated when targets polling returns new interval"""
        self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty())

        # Mock the rule cache update_sampling_targets to return new polling interval
        new_interval = 500
        with patch.object(
            self.rs._root._root._AwsXRayRemoteSampler__rule_cache,
            "update_sampling_targets",
            return_value=(False, new_interval),  # refresh_rules=False, min_polling_interval=500
        ):
            # Store original interval
            original_interval = self.rs._root._root._AwsXRayRemoteSampler__target_polling_interval

            # Call the method that should update polling interval
            self.rs._root._root._AwsXRayRemoteSampler__get_and_update_sampling_targets()

            # Verify that polling interval was updated
            self.assertEqual(self.rs._root._root._AwsXRayRemoteSampler__target_polling_interval, new_interval)
            self.assertNotEqual(original_interval, new_interval)

    def test_generate_client_id_format(self):
        """Tests that client ID generation produces correctly formatted hex string"""
        self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty())
        client_id = self.rs._root._root._AwsXRayRemoteSampler__client_id

        # Verify client ID is 24 characters long
        self.assertEqual(len(client_id), 24)

        # Verify all characters are valid hex characters
        valid_hex_chars = set("0123456789abcdef")
        for char in client_id:
            self.assertIn(char, valid_hex_chars)

    def test_internal_sampler_get_description(self):
        """Tests get_description method of internal _AwsXRayRemoteSampler"""
        internal_sampler = _AwsXRayRemoteSampler(resource=Resource.get_empty())

        try:
            description = internal_sampler.get_description()
            self.assertEqual(description, "_AwsXRayRemoteSampler{remote sampling with AWS X-Ray}")
        finally:
            # Clean up timers
            internal_sampler._rules_timer.cancel()
            internal_sampler._targets_timer.cancel()

    @patch("requests.Session.post", side_effect=mocked_requests_get)
    def test_rule_and_target_pollers_start_correctly(self, mock_post=None):
        """Tests that both rule and target pollers are started and configured correctly"""
        self.rs = AwsXRayRemoteSampler(resource=Resource.get_empty())

        # Verify timers are created and started
        self.assertIsNotNone(self.rs._root._root._rules_timer)
        self.assertIsNotNone(self.rs._root._root._targets_timer)

        # Verify timers are daemon threads
        self.assertTrue(self.rs._root._root._rules_timer.daemon)
        self.assertTrue(self.rs._root._root._targets_timer.daemon)

        # Verify jitter values are within expected ranges
        rule_jitter = self.rs._root._root._AwsXRayRemoteSampler__rule_polling_jitter
        target_jitter = self.rs._root._root._AwsXRayRemoteSampler__target_polling_jitter

        self.assertGreaterEqual(rule_jitter, 0.0)
        self.assertLessEqual(rule_jitter, 5.0)
        self.assertGreaterEqual(target_jitter, 0.0)
        self.assertLessEqual(target_jitter, 0.1)
