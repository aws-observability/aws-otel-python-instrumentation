# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Collection

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.instrumentation.crewai._event_handler import (
    OpenTelemetryEventHandler,
    _EventBusEmitWrapper,
    _LLMToolCallCompletedEventPatch,
)
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor


class CrewAIInstrumentor(BaseInstrumentor):
    """
    OpenTelemetry instrumentor for CrewAI.

    Instrumentation currently follows OpenTelemetry semantic conventions v1.39 for gen_ai attributes.
    See: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
    Note: Semantic conventions may change in future versions.
    """

    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("crewai >= 1.9.0",)

    def _instrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)
        llm_patch = _LLMToolCallCompletedEventPatch()
        handler = OpenTelemetryEventHandler(tracer)

        try_wrap("crewai.llm", "LLM._handle_non_streaming_response", llm_patch)
        try_wrap("crewai.llm", "LLM._ahandle_non_streaming_response", llm_patch)
        try_wrap("crewai.events", "crewai_event_bus.emit", _EventBusEmitWrapper(handler))

    def _uninstrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        from crewai.events import crewai_event_bus  # pylint: disable=import-outside-toplevel
        from crewai.llm import LLM  # pylint: disable=import-outside-toplevel

        try_unwrap(crewai_event_bus, "emit")
        try_unwrap(LLM, "_handle_non_streaming_response")
        try_unwrap(LLM, "_ahandle_non_streaming_response")
