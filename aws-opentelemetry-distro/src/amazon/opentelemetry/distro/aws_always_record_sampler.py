# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from logging import getLogger
from typing import Callable, Optional, Sequence

from importlib_metadata import entry_points

from opentelemetry.context import Context
from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG
from opentelemetry.sdk.trace import sampling
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

logger = getLogger(__name__)

OTEL_SAMPLER_ENTRY_POINT_GROUP = "opentelemetry_traces_sampler"


class AwsAlwaysRecordSampler(Sampler):
    """Sampler that always returns the same decision."""

    _sampler: Sampler

    def __init__(self):
        self._sampler = self._get_sampler()

    def should_sample(
        self,
        parent_context: Optional["Context"],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence["Link"] = None,
        trace_state: "TraceState" = None,
    ) -> "SamplingResult":
        result: SamplingResult = self._sampler.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )
        if result.decision is Decision.DROP:
            result = self._wrap_result_with_record_only_result(result)
        return result

    def get_description(self) -> str:
        return "AwsAlwaysRecordSampler{" + self._sampler.get_description() + "}"

    def _get_sampler(self) -> Optional[Sampler]:
        sampler_name: str = os.environ.get(OTEL_TRACES_SAMPLER, None)
        if not sampler_name:
            return sampling._get_from_env_or_default()
        try:
            sampler_implementation: Callable[[str], Sampler] = self._import_sampler_implementation(sampler_name)
            if sampler_name in ("traceidratio", "parentbased_traceidratio"):
                try:
                    rate: float = float(os.getenv(OTEL_TRACES_SAMPLER_ARG))
                except (ValueError, TypeError):
                    logger.warning("Could not convert TRACES_SAMPLER_ARG to float. Using default value 1.0.")
                    rate = 1.0
                arg: float = rate
            else:
                arg: str = os.getenv(OTEL_TRACES_SAMPLER_ARG)
            sampler: Sampler = sampler_implementation(arg)
            return sampler
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Using default sampler. Failed to initialize sampler, %s: %s",
                sampler_name,
                exc,
            )
            return sampling._get_from_env_or_default()

    # pylint: disable=no-self-use
    def _import_sampler_implementation(self, sampler_name: str) -> Callable[[str], Sampler]:
        try:
            sampler_implementation: Callable[[str], Sampler] = next(
                iter(entry_points(group=OTEL_SAMPLER_ENTRY_POINT_GROUP, name=sampler_name))
            ).load()
        except KeyError:
            raise RuntimeError(f"Requested entry point '{OTEL_SAMPLER_ENTRY_POINT_GROUP}' not found")
        except StopIteration:
            raise RuntimeError(
                f"Requested sampler '{sampler_name}' not found in " f"entry point '{OTEL_SAMPLER_ENTRY_POINT_GROUP}'"
            )
        return sampler_implementation

    # pylint: disable=no-self-use
    def _wrap_result_with_record_only_result(self, result: SamplingResult) -> SamplingResult:
        return SamplingResult(
            Decision.RECORD_ONLY,
            result.attributes,
            result.trace_state,
        )
