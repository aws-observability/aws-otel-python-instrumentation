# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import datetime
from threading import Lock
from unittest import TestCase
from unittest.mock import MagicMock, patch

from mock_clock import MockClock

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_LOCAL_OPERATION,
    AWS_XRAY_ADAPTIVE_SAMPLING_CONFIGURED_ATTRIBUTE_KEY,
    AWS_XRAY_SAMPLING_RULE,
)
from amazon.opentelemetry.distro.sampler._aws_sampling_result import _AwsSamplingResult
from amazon.opentelemetry.distro.sampler._aws_xray_adaptive_sampling_config import (
    _AnomalyCaptureLimit,
    _AnomalyConditions,
    _AWSXRayAdaptiveSamplingConfig,
    _UsageType,
)
from amazon.opentelemetry.distro.sampler._clock import _Clock
from amazon.opentelemetry.distro.sampler._rule_cache import RULE_CACHE_TTL_SECONDS, _RuleCache
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from amazon.opentelemetry.distro.sampler._sampling_rule_applier import _SamplingRuleApplier
from amazon.opentelemetry.distro.sampler._sampling_statistics_document import _SamplingStatisticsDocument
from amazon.opentelemetry.distro.sampler._sampling_target import _SamplingTargetResponse
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.semconv._incubating.attributes.http_attributes import HTTP_STATUS_CODE
from opentelemetry.trace import TraceState

SERVICE_NAME = "my_service"
CLIENT_ID = "12345678901234567890abcd"


# pylint: disable=no-member
class TestRuleCache(TestCase):
    def test_cache_update_rules_and_sorts_rules(self):
        cache = _RuleCache(None, None, CLIENT_ID, _Clock(), Lock())
        self.assertTrue(len(cache._RuleCache__rule_appliers) == 0)

        rule1 = _SamplingRule(Priority=200, RuleName="only_one_rule", Version=1)
        rules = [rule1]
        cache.update_sampling_rules(rules)
        self.assertTrue(len(cache._RuleCache__rule_appliers) == 1)

        rule1 = _SamplingRule(Priority=200, RuleName="abcdef", Version=1)
        rule2 = _SamplingRule(Priority=100, RuleName="abc", Version=1)
        rule3 = _SamplingRule(Priority=100, RuleName="Abc", Version=1)
        rule4 = _SamplingRule(Priority=100, RuleName="ab", Version=1)
        rule5 = _SamplingRule(Priority=100, RuleName="A", Version=1)
        rule6 = _SamplingRule(Priority=1, RuleName="abcdef", Version=1)
        rules = [rule1, rule2, rule3, rule4, rule5, rule6]
        cache.update_sampling_rules(rules)

        self.assertTrue(len(cache._RuleCache__rule_appliers) == 6)
        self.assertEqual(cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "abcdef")
        self.assertEqual(cache._RuleCache__rule_appliers[1].sampling_rule.RuleName, "A")
        self.assertEqual(cache._RuleCache__rule_appliers[2].sampling_rule.RuleName, "Abc")
        self.assertEqual(cache._RuleCache__rule_appliers[3].sampling_rule.RuleName, "ab")
        self.assertEqual(cache._RuleCache__rule_appliers[4].sampling_rule.RuleName, "abc")
        self.assertEqual(cache._RuleCache__rule_appliers[5].sampling_rule.RuleName, "abcdef")

    def test_rule_cache_expiration_logic(self):
        dt = datetime
        cache = _RuleCache(None, Resource.get_empty(), CLIENT_ID, _Clock(), Lock())
        self.assertFalse(cache.expired())
        cache._last_modified = dt.datetime.now() - dt.timedelta(seconds=RULE_CACHE_TTL_SECONDS - 5)
        self.assertFalse(cache.expired())
        cache._last_modified = dt.datetime.now() - dt.timedelta(seconds=RULE_CACHE_TTL_SECONDS + 1)
        self.assertTrue(cache.expired())

    def test_update_cache_with_only_one_rule_changed(self):
        cache = _RuleCache(None, Resource.get_empty(), CLIENT_ID, _Clock(), Lock())
        rule1 = _SamplingRule(Priority=1, RuleName="abcdef", Version=1)
        rule2 = _SamplingRule(Priority=10, RuleName="ab", Version=1)
        rule3 = _SamplingRule(Priority=100, RuleName="Abc", Version=1)
        rules = [rule1, rule2, rule3]
        cache.update_sampling_rules(rules)

        cache_rules_copy = cache._RuleCache__rule_appliers

        new_rule3 = _SamplingRule(Priority=5, RuleName="Abc", Version=1)
        rules = [rule1, rule2, new_rule3]
        cache.update_sampling_rules(rules)

        self.assertTrue(len(cache._RuleCache__rule_appliers) == 3)
        self.assertEqual(cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "abcdef")
        self.assertEqual(cache._RuleCache__rule_appliers[1].sampling_rule.RuleName, "Abc")
        self.assertEqual(cache._RuleCache__rule_appliers[2].sampling_rule.RuleName, "ab")

        # Compare that only rule1 and rule2 objects have not changed due to new_rule3 even after sorting
        self.assertTrue(cache_rules_copy[0] is cache._RuleCache__rule_appliers[0])
        self.assertTrue(cache_rules_copy[1] is cache._RuleCache__rule_appliers[2])
        self.assertTrue(cache_rules_copy[2] is not cache._RuleCache__rule_appliers[1])

    def test_update_rules_removes_older_rule(self):
        cache = _RuleCache(None, None, CLIENT_ID, _Clock(), Lock())
        self.assertTrue(len(cache._RuleCache__rule_appliers) == 0)

        rule1 = _SamplingRule(Priority=200, RuleName="first_rule", Version=1)
        rules = [rule1]
        cache.update_sampling_rules(rules)
        self.assertTrue(len(cache._RuleCache__rule_appliers) == 1)
        self.assertEqual(cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "first_rule")

        rule1 = _SamplingRule(Priority=200, RuleName="second_rule", Version=1)
        rules = [rule1]
        cache.update_sampling_rules(rules)
        self.assertTrue(len(cache._RuleCache__rule_appliers) == 1)
        self.assertEqual(cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "second_rule")

    def test_update_sampling_targets(self):
        sampling_rule_1 = _SamplingRule(
            Attributes={},
            FixedRate=0.05,
            HTTPMethod="*",
            Host="*",
            Priority=10000,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/default",
            RuleName="default",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )

        sampling_rule_2 = _SamplingRule(
            Attributes={},
            FixedRate=0.20,
            HTTPMethod="*",
            Host="*",
            Priority=20,
            ReservoirSize=10,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )

        time_now = datetime.datetime.fromtimestamp(1707551387.0)
        mock_clock = MockClock(time_now)

        rule_cache = _RuleCache(Resource.get_empty(), None, "", mock_clock, Lock())
        rule_cache.update_sampling_rules([sampling_rule_1, sampling_rule_2])

        # quota should be 1 because of borrowing=true until targets are updated
        rule_applier_0 = rule_cache._RuleCache__rule_appliers[0]
        self.assertEqual(
            rule_applier_0._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota, 1
        )
        self.assertEqual(rule_applier_0._SamplingRuleApplier__fixed_rate_sampler._root._rate, sampling_rule_2.FixedRate)

        rule_applier_1 = rule_cache._RuleCache__rule_appliers[1]
        self.assertEqual(
            rule_applier_1._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota, 1
        )
        self.assertEqual(rule_applier_1._SamplingRuleApplier__fixed_rate_sampler._root._rate, sampling_rule_1.FixedRate)

        target_1 = {
            "FixedRate": 0.05,
            "Interval": 15,
            "ReservoirQuota": 1,
            "ReservoirQuotaTTL": mock_clock.now().timestamp() + 10,
            "RuleName": "default",
        }
        target_2 = {
            "FixedRate": 0.15,
            "Interval": 12,
            "ReservoirQuota": 5,
            "ReservoirQuotaTTL": mock_clock.now().timestamp() + 10,
            "RuleName": "test",
        }
        target_3 = {
            "FixedRate": 0.15,
            "Interval": 3,
            "ReservoirQuota": 5,
            "ReservoirQuotaTTL": mock_clock.now().timestamp() + 10,
            "RuleName": "associated rule does not exist",
        }
        target_response = _SamplingTargetResponse(mock_clock.now().timestamp() - 10, [target_1, target_2, target_3], [])
        refresh_rules, min_polling_interval = rule_cache.update_sampling_targets(target_response)
        self.assertFalse(refresh_rules)
        # target_3 Interval is ignored since it's not associated with a Rule Applier
        self.assertEqual(min_polling_interval, target_2["Interval"])

        # still only 2 rule appliers should exist if for some reason 3 targets are obtained
        self.assertEqual(len(rule_cache._RuleCache__rule_appliers), 2)

        # borrowing=false, use quota from targets
        rule_applier_0 = rule_cache._RuleCache__rule_appliers[0]
        self.assertEqual(
            rule_applier_0._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota,
            target_2["ReservoirQuota"],
        )
        self.assertEqual(rule_applier_0._SamplingRuleApplier__fixed_rate_sampler._root._rate, target_2["FixedRate"])

        rule_applier_1 = rule_cache._RuleCache__rule_appliers[1]
        self.assertEqual(
            rule_applier_1._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota,
            target_1["ReservoirQuota"],
        )
        self.assertEqual(rule_applier_1._SamplingRuleApplier__fixed_rate_sampler._root._rate, target_1["FixedRate"])

        # Test target response modified after Rule cache's last modified date
        target_response.LastRuleModification = mock_clock.now().timestamp() + 1
        refresh_rules, _ = rule_cache.update_sampling_targets(target_response)
        self.assertTrue(refresh_rules)

    def test_should_sample_without_rules(self):
        rule_cache = _RuleCache(Resource.get_empty(), ALWAYS_ON, "", _Clock(), Lock())

        with patch("amazon.opentelemetry.distro.sampler._rule_cache._logger") as mock_logger:
            self.assertTrue(rule_cache.should_sample(None, 0, "name").decision.is_sampled())
            mock_logger.debug.assert_called_once_with("No sampling rules were matched")

    def test_should_sample_with_adaptive_sampling_config(self):
        sampling_rule_1 = _SamplingRule(
            Attributes={},
            FixedRate=0.05,
            HTTPMethod="*",
            Host="*",
            Priority=10000,
            ReservoirSize=1,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/default",
            RuleName="default",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )
        sampling_rule_2 = _SamplingRule(
            Attributes={},
            FixedRate=0.20,
            HTTPMethod="*",
            Host="*",
            Priority=20,
            ReservoirSize=10,
            ResourceARN="*",
            RuleARN="arn:aws:xray:us-east-1:999999999999:sampling-rule/test",
            RuleName="test",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )

        mock_clock = MockClock()
        rule_cache = _RuleCache(Resource.get_empty(), None, "", mock_clock, Lock())
        rule_cache.update_sampling_rules([sampling_rule_1, sampling_rule_2])
        rule_cache.set_adaptive_sampling_config(
            _AWSXRayAdaptiveSamplingConfig(version=1.0, anomaly_capture_limit=_AnomalyCaptureLimit(2))
        )

        # Verify initial state with borrowing
        rule_applier_0 = rule_cache._RuleCache__rule_appliers[0]
        self.assertEqual(
            rule_applier_0._SamplingRuleApplier__reservoir_sampler._root._RateLimitingSampler__reservoir._quota, 1
        )

        # Validate sampling decisions
        result = rule_cache.should_sample(None, 0, "name")
        self.assertTrue(isinstance(result, _AwsSamplingResult))
        self.assertTrue(result.decision.is_sampled())
        self.assertEqual(
            result.trace_state.get(_AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY),
            _RuleCache._hash_rule_name("test"),
        )
        self.assertEqual(len(result.attributes), 2)
        self.assertEqual(result.attributes.get(AWS_XRAY_ADAPTIVE_SAMPLING_CONFIGURED_ATTRIBUTE_KEY), True)
        self.assertEqual(result.attributes.get(AWS_XRAY_SAMPLING_RULE), "test")

        # Should show 1 sample count
        stats, _ = rule_cache.get_all_statistics()
        self.assertEqual(stats[0]["RuleName"], "test")
        self.assertEqual(stats[0]["SampleCount"], 1)

        # Should reset after first call
        stats, _ = rule_cache.get_all_statistics()
        self.assertEqual(len(stats), 0)

    def test_no_adaptive_sampling_uses_no_space(self):
        sampling_rule = _SamplingRule(
            Attributes={"test": "cat-service"},
            FixedRate=1.0,
            HTTPMethod="*",
            Host="*",
            Priority=1,
            ReservoirSize=1,
            ResourceARN="*",
            RuleName="cat-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )

        mock_clock = MockClock()
        rule_cache = _RuleCache(Resource.get_empty(), None, CLIENT_ID, mock_clock, Lock())
        rule_cache.update_sampling_rules([sampling_rule])

        export_counter = 0
        readable_span_mock: ReadableSpan = MagicMock()

        def stubbed_consumer(span):
            nonlocal export_counter
            export_counter += 1

        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)

        self.assertEqual(len(rule_cache._trace_usage_cache), 0)
        self.assertEqual(export_counter, 0)

    def test_record_errors_with_error_code_regex(self):
        rule1 = _SamplingRule(
            Attributes={"test": "cat-service"},
            FixedRate=1.0,
            HTTPMethod="*",
            Host="*",
            Priority=1,
            ReservoirSize=1,
            ResourceARN="*",
            RuleName="cat-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )
        rule2 = _SamplingRule(
            Attributes={},
            FixedRate=0.0,
            HTTPMethod="*",
            Host="*",
            Priority=4,
            ReservoirSize=0,
            ResourceARN="*",
            RuleName="default-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
            SamplingRateBoost={"MaxRate": 1, "CooldownWindowMinutes": 5},
        )

        config = _AWSXRayAdaptiveSamplingConfig(
            version=1.0,
            anomaly_capture_limit=_AnomalyCaptureLimit(anomaly_traces_per_second=2),
            anomaly_conditions=[_AnomalyConditions(error_code_regex="^500$", usage=_UsageType.BOTH)],
        )

        mock_clock = MockClock()
        rule_cache = _RuleCache(Resource.get_empty(), None, CLIENT_ID, mock_clock, Lock())
        rule_cache.set_adaptive_sampling_config(config)
        rule_cache.update_sampling_rules([rule1, rule2])

        # Mock ReadableSpan with proper context, parent context, and attributes
        readable_span_mock: ReadableSpan = MagicMock()
        readable_span_mock.context.trace_state = TraceState()
        readable_span_mock.context.trace_flags.sampled = False
        readable_span_mock.context.is_remote = False
        readable_span_mock.parent.is_valid = False
        readable_span_mock.attributes = {HTTP_STATUS_CODE: 500}

        export_counter = 0

        def stubbed_consumer(x: ReadableSpan) -> None:
            nonlocal export_counter
            export_counter += 1

        # Ensure rate limiter is active
        mock_clock.add_time(seconds=1)
        # First 2 spans should be captured, third should be rate limited
        readable_span_mock.context.trace_id = 1
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 2
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 3
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        # Export trace 1 and all trace 2 spans as they are part of the same trace
        self.assertEqual(export_counter, 5)

        self.assertEqual(rule_cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "cat-rule")
        self.assertFalse(
            rule_cache._RuleCache__rule_appliers[0].matches(Resource.get_empty(), readable_span_mock.attributes)
        )
        self.assertEqual(rule_cache._RuleCache__rule_appliers[1].sampling_rule.RuleName, "default-rule")
        self.assertTrue(
            rule_cache._RuleCache__rule_appliers[1].matches(Resource.get_empty(), readable_span_mock.attributes)
        )

        _, boost_statistics = rule_cache.get_all_statistics()

        # Statistics only appear for rules that have been used
        self.assertEqual(len(boost_statistics), 1)
        self.assertEqual(boost_statistics[0]["RuleName"], "default-rule")
        self.assertEqual(boost_statistics[0]["TotalCount"], 3)
        self.assertEqual(boost_statistics[0]["AnomalyCount"], 3)
        self.assertEqual(boost_statistics[0]["SampledAnomalyCount"], 0)

        # Mock trace coming from upstream service where it was sampled by cat-rule
        readable_span_mock.context.trace_id = 4
        readable_span_mock.context.trace_flags.sampled = True
        readable_span_mock.context.trace_state = TraceState(
            [(_AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY, _RuleCache._hash_rule_name("cat-rule"))]
        )
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)

        # The cat-rule has no boost configured, so no statistics are generated
        _, boost_statistics = rule_cache.get_all_statistics()
        self.assertEqual(len(boost_statistics), 0)

        # Assert the trace ID cache is filled with data
        self.assertEqual(len(rule_cache._trace_usage_cache), 4)
        # Ensure the trace cache empties itself after > 600 seconds
        mock_clock.add_time(seconds=1000)
        self.assertEqual(len(rule_cache._trace_usage_cache), 0)

    def test_record_errors_with_high_latency(self):
        rule1 = _SamplingRule(
            Attributes={"test": "cat-service"},
            FixedRate=1.0,
            HTTPMethod="*",
            Host="*",
            Priority=1,
            ReservoirSize=1,
            ResourceARN="*",
            RuleName="cat-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )
        rule2 = _SamplingRule(
            Attributes={},
            FixedRate=0.0,
            HTTPMethod="*",
            Host="*",
            Priority=4,
            ReservoirSize=0,
            ResourceARN="*",
            RuleName="default-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
            SamplingRateBoost={"MaxRate": 1, "CooldownWindowMinutes": 5},
        )

        config = _AWSXRayAdaptiveSamplingConfig(
            version=1.0,
            anomaly_capture_limit=_AnomalyCaptureLimit(anomaly_traces_per_second=2),
            anomaly_conditions=[_AnomalyConditions(high_latency_ms=100, usage=_UsageType.BOTH)],
        )

        mock_clock = MockClock()
        rule_cache = _RuleCache(Resource.get_empty(), None, CLIENT_ID, mock_clock, Lock())
        rule_cache.set_adaptive_sampling_config(config)
        rule_cache.update_sampling_rules([rule1, rule2])

        # Mock ReadableSpan with proper context, parent context, and attributes
        readable_span_mock: ReadableSpan = MagicMock()
        readable_span_mock.context.trace_state = TraceState()
        readable_span_mock.context.trace_flags.sampled = False
        readable_span_mock.context.is_remote = False
        readable_span_mock.parent.is_valid = False
        readable_span_mock.attributes = {}
        readable_span_mock.start_time = 0
        readable_span_mock.end_time = 300_000_000

        export_counter = 0

        def stubbed_consumer(x: ReadableSpan) -> None:
            nonlocal export_counter
            export_counter += 1

        # Ensure rate limiter is active
        mock_clock.add_time(seconds=1)
        # First 2 spans should be captured, third should be rate limited
        readable_span_mock.context.trace_id = 1
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 2
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 3
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        # Export trace 1 and all trace 2 spans as they are part of the same trace
        self.assertEqual(export_counter, 5)

        self.assertEqual(rule_cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "cat-rule")
        self.assertFalse(
            rule_cache._RuleCache__rule_appliers[0].matches(Resource.get_empty(), readable_span_mock.attributes)
        )
        self.assertEqual(rule_cache._RuleCache__rule_appliers[1].sampling_rule.RuleName, "default-rule")
        self.assertTrue(
            rule_cache._RuleCache__rule_appliers[1].matches(Resource.get_empty(), readable_span_mock.attributes)
        )

        _, boost_statistics = rule_cache.get_all_statistics()

        # Statistics only appear for rules that have been used
        self.assertEqual(len(boost_statistics), 1)
        self.assertEqual(boost_statistics[0]["RuleName"], "default-rule")
        self.assertEqual(boost_statistics[0]["TotalCount"], 3)
        self.assertEqual(boost_statistics[0]["AnomalyCount"], 3)
        self.assertEqual(boost_statistics[0]["SampledAnomalyCount"], 0)

        # Mock trace coming from upstream service where it was sampled by cat-rule
        readable_span_mock.context.trace_id = 4
        readable_span_mock.context.trace_flags.sampled = True
        readable_span_mock.context.trace_state = TraceState(
            [(_AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY, _RuleCache._hash_rule_name("cat-rule"))]
        )
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)

        # The cat-rule has no boost configured, so no statistics are generated
        _, boost_statistics = rule_cache.get_all_statistics()
        self.assertEqual(len(boost_statistics), 0)

        # Assert the trace ID cache is filled with data
        self.assertEqual(len(rule_cache._trace_usage_cache), 4)
        # Ensure the trace cache empties itself after > 600 seconds
        mock_clock.add_time(seconds=1000)
        self.assertEqual(len(rule_cache._trace_usage_cache), 0)

    def test_record_errors_with_error_code_and_high_latency(self):
        rule1 = _SamplingRule(
            Attributes={"test": "cat-service"},
            FixedRate=1.0,
            HTTPMethod="*",
            Host="*",
            Priority=1,
            ReservoirSize=1,
            ResourceARN="*",
            RuleName="cat-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )
        rule2 = _SamplingRule(
            Attributes={},
            FixedRate=0.0,
            HTTPMethod="*",
            Host="*",
            Priority=4,
            ReservoirSize=0,
            ResourceARN="*",
            RuleName="default-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
            SamplingRateBoost={"MaxRate": 1, "CooldownWindowMinutes": 5},
        )

        config = _AWSXRayAdaptiveSamplingConfig(
            version=1.0,
            anomaly_capture_limit=_AnomalyCaptureLimit(anomaly_traces_per_second=2),
            anomaly_conditions=[
                _AnomalyConditions(error_code_regex="^500$", high_latency_ms=100, usage=_UsageType.BOTH)
            ],
        )

        mock_clock = MockClock()
        rule_cache = _RuleCache(Resource.get_empty(), None, CLIENT_ID, mock_clock, Lock())
        rule_cache.set_adaptive_sampling_config(config)
        rule_cache.update_sampling_rules([rule1, rule2])

        # Mock ReadableSpan with proper context, parent context, and attributes
        readable_span_mock: ReadableSpan = MagicMock()
        readable_span_mock.context.trace_state = TraceState()
        readable_span_mock.context.trace_flags.sampled = False
        readable_span_mock.context.is_remote = False
        readable_span_mock.parent.is_valid = False
        readable_span_mock.attributes = {HTTP_STATUS_CODE: 500}
        readable_span_mock.start_time = 0
        readable_span_mock.end_time = 300_000_000

        export_counter = 0

        def stubbed_consumer(x: ReadableSpan) -> None:
            nonlocal export_counter
            export_counter += 1

        # Ensure rate limiter is active
        mock_clock.add_time(seconds=1)
        # First 2 spans should be captured, third should be rate limited
        readable_span_mock.context.trace_id = 1
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 2
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 3
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        # Export trace 1 and all trace 2 spans as they are part of the same trace
        self.assertEqual(export_counter, 5)

        self.assertEqual(rule_cache._RuleCache__rule_appliers[0].sampling_rule.RuleName, "cat-rule")
        self.assertFalse(
            rule_cache._RuleCache__rule_appliers[0].matches(Resource.get_empty(), readable_span_mock.attributes)
        )
        self.assertEqual(rule_cache._RuleCache__rule_appliers[1].sampling_rule.RuleName, "default-rule")
        self.assertTrue(
            rule_cache._RuleCache__rule_appliers[1].matches(Resource.get_empty(), readable_span_mock.attributes)
        )

        _, boost_statistics = rule_cache.get_all_statistics()

        # Statistics only appear for rules that have been used
        self.assertEqual(len(boost_statistics), 1)
        self.assertEqual(boost_statistics[0]["RuleName"], "default-rule")
        self.assertEqual(boost_statistics[0]["TotalCount"], 3)
        self.assertEqual(boost_statistics[0]["AnomalyCount"], 3)
        self.assertEqual(boost_statistics[0]["SampledAnomalyCount"], 0)

        # Mock trace coming from upstream service where it was sampled by cat-rule
        readable_span_mock.context.trace_id = 4
        readable_span_mock.context.trace_flags.sampled = True
        readable_span_mock.context.trace_state = TraceState(
            [(_AwsSamplingResult.AWS_XRAY_SAMPLING_RULE_TRACE_STATE_KEY, _RuleCache._hash_rule_name("cat-rule"))]
        )
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)

        # The cat-rule has no boost configured, so no statistics are generated
        _, boost_statistics = rule_cache.get_all_statistics()
        self.assertEqual(len(boost_statistics), 0)

        # Assert the trace ID cache is filled with data
        self.assertEqual(len(rule_cache._trace_usage_cache), 4)
        # Ensure the trace cache empties itself after > 600 seconds
        mock_clock.add_time(seconds=1000)
        self.assertEqual(len(rule_cache._trace_usage_cache), 0)

    def test_record_errors_with_operations_filter(self):
        rule1 = _SamplingRule(
            Attributes={"test": "cat-service"},
            FixedRate=1.0,
            HTTPMethod="*",
            Host="*",
            Priority=1,
            ReservoirSize=1,
            ResourceARN="*",
            RuleName="cat-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
        )
        rule2 = _SamplingRule(
            Attributes={},
            FixedRate=0.0,
            HTTPMethod="*",
            Host="*",
            Priority=4,
            ReservoirSize=0,
            ResourceARN="*",
            RuleName="default-rule",
            ServiceName="*",
            ServiceType="*",
            URLPath="*",
            Version=1,
            SamplingRateBoost={"MaxRate": 1, "CooldownWindowMinutes": 5},
        )

        config = _AWSXRayAdaptiveSamplingConfig(
            version=1.0,
            anomaly_capture_limit=_AnomalyCaptureLimit(anomaly_traces_per_second=2),
            anomaly_conditions=[
                _AnomalyConditions(
                    error_code_regex="^500$",
                    operations=["GET /api1", "GET /api2"],
                    usage=_UsageType.ANOMALY_TRACE_CAPTURE,
                )
            ],
        )

        mock_clock = MockClock()
        rule_cache = _RuleCache(Resource.get_empty(), None, CLIENT_ID, mock_clock, Lock())
        rule_cache.set_adaptive_sampling_config(config)
        rule_cache.update_sampling_rules([rule1, rule2])

        readable_span_mock: ReadableSpan = MagicMock()
        readable_span_mock.context.trace_state = TraceState()
        readable_span_mock.context.trace_flags.sampled = False
        readable_span_mock.context.is_remote = False
        readable_span_mock.parent.is_valid = False

        export_counter = 0

        def stubbed_consumer(x: ReadableSpan) -> None:
            nonlocal export_counter
            export_counter += 1

        mock_clock.add_time(seconds=1)
        # Test with matching operation
        readable_span_mock.attributes = {HTTP_STATUS_CODE: 500, AWS_LOCAL_OPERATION: "GET /api1"}
        readable_span_mock.context.trace_id = 1
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        readable_span_mock.context.trace_id = 2
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        self.assertEqual(export_counter, 2)
        export_counter = 0

        # Test with non-matching operation
        readable_span_mock.attributes = {HTTP_STATUS_CODE: 500, AWS_LOCAL_OPERATION: "GET /non-matching"}
        readable_span_mock.context.trace_id = 3
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        self.assertEqual(export_counter, 0)

        # Test with previously matched trace but non-matching operation
        readable_span_mock.attributes = {HTTP_STATUS_CODE: 500, AWS_LOCAL_OPERATION: "GET /non-matching"}
        readable_span_mock.context.trace_id = 1
        rule_cache.adapt_sampling(readable_span_mock, stubbed_consumer)
        self.assertEqual(export_counter, 1)

    def test_set_adaptive_sampling_config(self):
        with patch("amazon.opentelemetry.distro.sampler._rule_cache._logger") as mock_logger:
            mock_clock = MockClock()
            rule_cache = _RuleCache(Resource.get_empty(), None, CLIENT_ID, mock_clock, Lock())

            config = _AWSXRayAdaptiveSamplingConfig(version=1.0)

            rule_cache.set_adaptive_sampling_config(config)
            mock_logger.warning.assert_not_called()

            rule_cache.set_adaptive_sampling_config(config)
            mock_logger.warning.assert_called_once_with("Programming bug - Adaptive sampling config is already set")

    def test_get_all_statistics(self):
        time_now = datetime.datetime.fromtimestamp(1707551387.0)
        mock_clock = MockClock(time_now)
        rule_applier_1 = _SamplingRuleApplier(_SamplingRule(RuleName="test"), SERVICE_NAME, CLIENT_ID, mock_clock)
        rule_applier_2 = _SamplingRuleApplier(_SamplingRule(RuleName="default"), SERVICE_NAME, CLIENT_ID, mock_clock)

        rule_applier_1._SamplingRuleApplier__statistics = _SamplingStatisticsDocument(
            CLIENT_ID, "test", SERVICE_NAME, 4, 2, 2
        )
        rule_applier_2._SamplingRuleApplier__statistics = _SamplingStatisticsDocument(
            CLIENT_ID, "default", SERVICE_NAME, 5, 5, 5
        )

        rule_cache = _RuleCache(Resource.get_empty(), None, "", mock_clock, Lock())
        rule_cache._RuleCache__rule_appliers = [rule_applier_1, rule_applier_2]

        mock_clock.add_time(10)
        statistics, _ = rule_cache.get_all_statistics()

        self.assertEqual(
            statistics,
            [
                {
                    "ClientID": CLIENT_ID,
                    "RuleName": "test",
                    "Timestamp": mock_clock.now().timestamp(),
                    "RequestCount": 4,
                    "BorrowCount": 2,
                    "SampleCount": 2,
                },
                {
                    "ClientID": CLIENT_ID,
                    "RuleName": "default",
                    "Timestamp": mock_clock.now().timestamp(),
                    "RequestCount": 5,
                    "BorrowCount": 5,
                    "SampleCount": 5,
                },
            ],
        )
