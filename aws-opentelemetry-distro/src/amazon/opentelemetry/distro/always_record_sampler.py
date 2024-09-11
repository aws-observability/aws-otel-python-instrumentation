# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import Logger, getLogger
from typing import Optional, Sequence

from typing_extensions import override

from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

_logger: Logger = getLogger(__name__)

_OTEL_SAMPLER_ENTRY_POINT_GROUP: str = "opentelemetry_traces_sampler"


class AlwaysRecordSampler(Sampler):
    """
    This sampler will return the sampling result of the provided `_root_sampler`, unless the
    sampling result contains the sampling decision `Decision.DROP`, in which case, a
    new sampling result will be returned that is functionally equivalent to the original, except that
    it contains the sampling decision `SamplingDecision.RECORD_ONLY`. This ensures that all
    spans are recorded, with no change to sampling.

    The intended use case of this sampler is to provide a means of sending all spans to a
    processor without having an impact on the sampling rate. This may be desirable if a user wishes
    to count or otherwise measure all spans produced in a service, without incurring the cost of 100%
    sampling.
    """

    _root_sampler: Sampler

    def __init__(self, root_sampler: Sampler):
        if not root_sampler:
            raise ValueError("root_sampler must not be None")
        self._root_sampler = root_sampler

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
        if result.decision is Decision.DROP:
            result = _wrap_result_with_record_only_result(result, attributes)
        return result

    @override
    def get_description(self) -> str:
        return "AlwaysRecordSampler{" + self._root_sampler.get_description() + "}"


def _wrap_result_with_record_only_result(result: SamplingResult, attributes: Attributes) -> SamplingResult:
    return SamplingResult(
        Decision.RECORD_ONLY,
        attributes,
        result.trace_state,
    )
