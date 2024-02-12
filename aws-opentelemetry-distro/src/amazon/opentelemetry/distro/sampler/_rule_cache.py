# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import datetime
from logging import getLogger
from threading import Lock
from typing import Optional, Sequence

from amazon.opentelemetry.distro.sampler._fallback_sampler import _FallbackSampler
from amazon.opentelemetry.distro.sampler._sampling_rule import _SamplingRule
from amazon.opentelemetry.distro.sampler._sampling_rule_applier import _SamplingRuleApplier
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

CACHE_TTL_SECONDS = 3600


class _RuleCache:
    def __init__(self, resource: Resource, fallback_sampler: _FallbackSampler, date_time: datetime, lock: Lock):
        self.__rule_appliers: [_SamplingRuleApplier] = []
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
        for rule_applier in self.__rule_appliers:
            if rule_applier.matches(self.__resource, attributes):
                return rule_applier.should_sample(
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
        temp_rule_appliers = []
        for sampling_rule in new_sampling_rules:
            if sampling_rule.RuleName == "":
                _logger.debug("sampling rule without rule name is not supported")
                continue
            if sampling_rule.Version != 1:
                _logger.debug("sampling rule without Version 1 is not supported: RuleName: %s", sampling_rule.RuleName)
                continue
            temp_rule_appliers.append(_SamplingRuleApplier(sampling_rule))

        self.__cache_lock.acquire()

        # map list of rule appliers by each applier's sampling_rule name
        rule_applier_map = {rule.sampling_rule.RuleName: rule for rule in self.__rule_appliers}

        # If a sampling rule has not changed, keep its respective applier in the cache.
        for index, new_applier in enumerate(temp_rule_appliers):
            rule_name_to_check = new_applier.sampling_rule.RuleName
            if rule_name_to_check in rule_applier_map:
                old_applier = rule_applier_map[rule_name_to_check]
                if new_applier.sampling_rule == old_applier.sampling_rule:
                    temp_rule_appliers[index] = old_applier
        self.__rule_appliers = temp_rule_appliers
        self._last_modified = datetime.datetime.now()

        self.__cache_lock.release()

    def expired(self) -> bool:
        self.__cache_lock.acquire()
        try:
            return datetime.datetime.now() > self._last_modified + datetime.timedelta(seconds=CACHE_TTL_SECONDS)
        finally:
            self.__cache_lock.release()
