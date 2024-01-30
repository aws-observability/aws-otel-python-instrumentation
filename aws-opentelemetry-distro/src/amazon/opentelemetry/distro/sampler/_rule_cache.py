# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import copy
import datetime
from logging import getLogger
from threading import Lock
from typing import Optional, Sequence

from amazon.opentelemetry.distro.sampler._fallback_sampler import _FallbackSampler
from amazon.opentelemetry.distro.sampler._rule import _Rule
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

CACHE_TTL_SECONDS = 3600


class _RuleCache:
    rules: [_Rule] = []

    def __init__(self, resource: Resource, fallback_sampler: _FallbackSampler, date_time: datetime, lock: Lock):
        self.__cache_lock = lock
        self.__resource = resource
        self._fallback_sampler = fallback_sampler
        self._date_time = date_time
        self._last_modified = self._date_time.datetime.now()

    def should_sample(
        self,
        parent_context: Optional[Context],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence[Link] = None,
        trace_state: TraceState = None,
    ) -> SamplingResult:
        for rule in self.rules:
            if rule.matches(self.__resource, attributes):
                return rule.should_sample(
                    parent_context,
                    trace_id,
                    name,
                    kind=kind,
                    attributes=attributes,
                    links=links,
                    trace_state=trace_state,
                )

        return self._fallback_sampler.should_sample(
            parent_context, trace_id, name, kind=kind, attributes=attributes, links=links, trace_state=trace_state
        )

    def update_sampling_rules(self, new_sampling_rules: [_SamplingRule]) -> None:
        new_sampling_rules.sort()
        temp_rules = []
        for sampling_rule in new_sampling_rules:
            if sampling_rule.RuleName == "":
                _logger.info("sampling rule without rule name is not supported")
                continue
            if sampling_rule.Version != 1:
                _logger.info("sampling rule without Version 1 is not supported: RuleName: %s", sampling_rule.RuleName)
                continue
            temp_rules.append(_Rule(copy.deepcopy(sampling_rule)))

        self.__cache_lock.acquire()

        # map list of rules by each rule's sampling_rule name
        rule_map = {rule.sampling_rule.RuleName: rule for rule in self.rules}

        # If a sampling rule has not changed, keep its respective rule in the cache.
        for index, new_rule in enumerate(temp_rules):
            rule_name_to_check = new_rule.sampling_rule.RuleName
            if rule_name_to_check in rule_map:
                previous_rule = rule_map[rule_name_to_check]
                if new_rule.sampling_rule == previous_rule.sampling_rule:
                    temp_rules[index] = previous_rule
        self.rules = temp_rules
        self._last_modified = datetime.datetime.now()

        self.__cache_lock.release()

    def expired(self) -> bool:
        self.__cache_lock.acquire()
        try:
            return datetime.datetime.now() > self._last_modified + datetime.timedelta(seconds=CACHE_TTL_SECONDS)
        finally:
            self.__cache_lock.release()
