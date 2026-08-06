# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Sequence

from typing_extensions import override

from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class AlwaysRecordSampler(Sampler):
    _root_sampler: Sampler

    def __init__(self, root_sampler: Sampler):
        if not root_sampler:
            raise ValueError("root_sampler must not be None")
        self._root_sampler = root_sampler
        self.enabled = True

    @override
    def should_sample(
        self,
        parent_context: Optional["Context"],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence["Link"] = None,
        trace_state: "TraceState" = None,
    ) -> SamplingResult:
        result: SamplingResult = self._root_sampler.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )
        if self.enabled and result.decision is Decision.DROP:
            merged = {**(attributes or {}), **(result.attributes or {})}
            result = SamplingResult(Decision.RECORD_ONLY, merged, result.trace_state)
        return result

    @override
    def get_description(self) -> str:
        return "AlwaysRecordSampler{" + self._root_sampler.get_description() + "}"
