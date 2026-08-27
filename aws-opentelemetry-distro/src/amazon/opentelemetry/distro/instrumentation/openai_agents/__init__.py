# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Collection

from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore


class OpenAIAgentsInstrumentor(BaseInstrumentor):  # type: ignore
    """Instrument OpenAI Agents SDK tracing callbacks with OpenTelemetry spans."""

    _processor = None
    _previous_processors = None

    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("openai-agents >= 0.3.3",)

    def _instrument(self, **kwargs: Any) -> None:
        if self._processor is not None:
            return

        from agents.tracing import (  # pylint: disable=import-outside-toplevel
            add_trace_processor,
            get_trace_provider,
            set_trace_processors,
        )

        from ._processor import _OpenAIAgentsTracingProcessor  # pylint: disable=import-outside-toplevel

        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)
        self._processor = _OpenAIAgentsTracingProcessor(tracer)

        if kwargs.get("disable_openai_trace_export"):
            trace_provider = get_trace_provider()
            multi_processor = getattr(trace_provider, "_multi_processor", None)
            self._previous_processors = tuple(getattr(multi_processor, "_processors", ()))
            set_trace_processors([self._processor])
        else:
            add_trace_processor(self._processor)

    def _uninstrument(self, **kwargs: Any) -> None:
        if self._processor is None:
            return

        from agents.tracing import get_trace_provider, set_trace_processors  # pylint: disable=import-outside-toplevel

        processor = self._processor
        try:
            if self._previous_processors is not None:
                set_trace_processors(list(self._previous_processors))
            else:
                trace_provider = get_trace_provider()
                multi_processor = getattr(trace_provider, "_multi_processor", None)
                current_processors = getattr(multi_processor, "_processors", ())
                set_trace_processors([item for item in current_processors if item is not processor])
            processor.shutdown()
        finally:
            self._processor = None
            self._previous_processors = None
