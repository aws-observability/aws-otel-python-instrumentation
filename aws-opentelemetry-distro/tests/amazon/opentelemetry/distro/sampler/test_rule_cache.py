# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import datetime
from threading import Lock
from unittest import TestCase

from amazon.opentelemetry.distro.sampler._rule_cache import CACHE_TTL_SECONDS, _RuleCache
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from opentelemetry.sdk.resources import Resource


class TestRuleCache(TestCase):
    def test_cache_update_rules_and_sorts_rules(self):
        cache = _RuleCache(None, None, datetime, Lock())
        self.assertTrue(len(cache.rules) == 0)

        rule1 = _SamplingRule(Priority=200, RuleName="only_one_rule", Version=1)
        rules = [rule1]
        cache.update_sampling_rules(rules)
        self.assertTrue(len(cache.rules) == 1)

        rule1 = _SamplingRule(Priority=200, RuleName="abcdef", Version=1)
        rule2 = _SamplingRule(Priority=100, RuleName="abc", Version=1)
        rule3 = _SamplingRule(Priority=100, RuleName="Abc", Version=1)
        rule4 = _SamplingRule(Priority=100, RuleName="ab", Version=1)
        rule5 = _SamplingRule(Priority=100, RuleName="A", Version=1)
        rule6 = _SamplingRule(Priority=1, RuleName="abcdef", Version=1)
        rules = [rule1, rule2, rule3, rule4, rule5, rule6]
        cache.update_sampling_rules(rules)

        self.assertTrue(len(cache.rules) == 6)
        self.assertEqual(cache.rules[0].sampling_rule.RuleName, "abcdef")
        self.assertEqual(cache.rules[1].sampling_rule.RuleName, "A")
        self.assertEqual(cache.rules[2].sampling_rule.RuleName, "Abc")
        self.assertEqual(cache.rules[3].sampling_rule.RuleName, "ab")
        self.assertEqual(cache.rules[4].sampling_rule.RuleName, "abc")
        self.assertEqual(cache.rules[5].sampling_rule.RuleName, "abcdef")

    def test_rule_cache_expiration_logic(self):
        dt = datetime
        cache = _RuleCache(None, Resource.get_empty(), dt, Lock())
        self.assertFalse(cache.expired())
        cache._last_modified = dt.datetime.now() - dt.timedelta(seconds=CACHE_TTL_SECONDS - 5)
        self.assertFalse(cache.expired())
        cache._last_modified = dt.datetime.now() - dt.timedelta(seconds=CACHE_TTL_SECONDS + 1)
        self.assertTrue(cache.expired())

    def test_update_cache_with_only_one_rule_changed(self):
        dt = datetime
        cache = _RuleCache(None, Resource.get_empty(), dt, Lock())
        rule1 = _SamplingRule(Priority=1, RuleName="abcdef", Version=1)
        rule2 = _SamplingRule(Priority=10, RuleName="ab", Version=1)
        rule3 = _SamplingRule(Priority=100, RuleName="Abc", Version=1)
        rules = [rule1, rule2, rule3]
        cache.update_sampling_rules(rules)

        cache_rules_copy = cache.rules

        new_rule3 = _SamplingRule(Priority=5, RuleName="Abc", Version=1)
        rules = [rule1, rule2, new_rule3]
        cache.update_sampling_rules(rules)

        self.assertTrue(len(cache.rules) == 3)
        self.assertEqual(cache.rules[0].sampling_rule.RuleName, "abcdef")
        self.assertEqual(cache.rules[1].sampling_rule.RuleName, "Abc")
        self.assertEqual(cache.rules[2].sampling_rule.RuleName, "ab")

        # Compare that only rule1 and rule2 objects have not changed due to new_rule3 even after sorting
        self.assertTrue(cache_rules_copy[0] is cache.rules[0])
        self.assertTrue(cache_rules_copy[1] is cache.rules[2])
        self.assertTrue(cache_rules_copy[2] is not cache.rules[1])

    def test_update_rules_removes_older_rule(self):
        cache = _RuleCache(None, None, datetime, Lock())
        self.assertTrue(len(cache.rules) == 0)

        rule1 = _SamplingRule(Priority=200, RuleName="first_rule", Version=1)
        rules = [rule1]
        cache.update_sampling_rules(rules)
        self.assertTrue(len(cache.rules) == 1)
        self.assertEqual(cache.rules[0].sampling_rule.RuleName, "first_rule")

        rule1 = _SamplingRule(Priority=200, RuleName="second_rule", Version=1)
        rules = [rule1]
        cache.update_sampling_rules(rules)
        self.assertTrue(len(cache.rules) == 1)
        self.assertEqual(cache.rules[0].sampling_rule.RuleName, "second_rule")
