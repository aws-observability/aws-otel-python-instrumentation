# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Collection

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.instrumentation.crewai._event_handler import (
    OpenTelemetryEventHandler,
    _EventBusEmitWrapper,
)
from amazon.opentelemetry.distro.instrumentation.crewai._wrappers import _KickoffWrapper
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor


class CrewAIInstrumentor(BaseInstrumentor):
    """
    OpenTelemetry instrumentor for CrewAI.

    Instrumentation currently follows OpenTelemetry semantic conventions v1.39 for gen_ai attributes.
    See: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
    Note: Semantic conventions may change in future versions.

    Errors: failed crew, agent, LLM and tool spans set error.type to the class name of the exception that caused
    the failure (for example TimeoutError or RateLimitError), or _OTHER when it is unknown. The error message is
    recorded only as the span status description.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._handler: OpenTelemetryEventHandler | None = None

    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("crewai >= 1.10.0, < 2",)

    def _instrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)
        self._handler = OpenTelemetryEventHandler(tracer)

        try_wrap("crewai.events", "crewai_event_bus.emit", _EventBusEmitWrapper(self._handler))
        # Crew.kickoff_async is not wrapped: it runs the wrapped Crew.kickoff in a worker thread, which keeps running
        # after the awaiting task is cancelled and ends its spans normally. Agent.akickoff awaits Agent.kickoff_async.
        kickoff_wrapper = _KickoffWrapper(self._handler)
        try_wrap("crewai", "Crew.kickoff", kickoff_wrapper)
        try_wrap("crewai", "Crew.akickoff", kickoff_wrapper)
        try_wrap("crewai", "Crew.replay", kickoff_wrapper)
        try_wrap("crewai", "Agent.kickoff", kickoff_wrapper)
        try_wrap("crewai", "Agent.kickoff_async", kickoff_wrapper)

    def _uninstrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        from crewai.events import crewai_event_bus  # pylint: disable=import-outside-toplevel

        try_unwrap(crewai_event_bus, "emit")
        try_unwrap("crewai.Crew", "kickoff")
        try_unwrap("crewai.Crew", "akickoff")
        try_unwrap("crewai.Crew", "replay")
        try_unwrap("crewai.Agent", "kickoff")
        try_unwrap("crewai.Agent", "kickoff_async")
