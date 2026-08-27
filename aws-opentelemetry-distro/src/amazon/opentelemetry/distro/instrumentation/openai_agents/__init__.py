# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Collection

from typing_extensions import override

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore

_RESPONSES_MODULE = "openai.resources.responses.responses"
_COMPLETIONS_MODULE = "openai.resources.chat.completions.completions"


class OpenAIAgentsInstrumentor(BaseInstrumentor):  # type: ignore
    """Instrument OpenAI Agents SDK tracing callbacks with OpenTelemetry spans."""

    _processor = None
    _previous_processors = None

    @override
    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("openai-agents >= 0.3.3",)

    @override
    def _instrument(self, **kwargs: Any) -> None:
        if self._processor is not None:
            return

        from agents.tracing import (  # pylint: disable=import-outside-toplevel
            add_trace_processor,
            get_trace_provider,
            set_trace_processors,
        )

        from ._processor import OpenTelemetryTracingProcessor  # pylint: disable=import-outside-toplevel
        from ._request_capture import record_request  # pylint: disable=import-outside-toplevel

        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)
        self._processor = OpenTelemetryTracingProcessor(tracer, getattr(tracer_provider, "force_flush", None))

        try_wrap(_RESPONSES_MODULE, "Responses.create", record_request)
        try_wrap(_RESPONSES_MODULE, "AsyncResponses.create", record_request)
        try_wrap(_COMPLETIONS_MODULE, "Completions.create", record_request)
        try_wrap(_COMPLETIONS_MODULE, "AsyncCompletions.create", record_request)

        if kwargs.get("disable_openai_trace_export"):
            trace_provider = get_trace_provider()
            multi_processor = getattr(trace_provider, "_multi_processor", None)
            self._previous_processors = tuple(getattr(multi_processor, "_processors", ()))
            set_trace_processors([self._processor])
        else:
            add_trace_processor(self._processor)

    @override
    def _uninstrument(self, **kwargs: Any) -> None:
        if self._processor is None:
            return

        from agents.tracing import get_trace_provider, set_trace_processors  # pylint: disable=import-outside-toplevel

        from ._request_capture import reset_request_params  # pylint: disable=import-outside-toplevel

        try_unwrap(f"{_RESPONSES_MODULE}.Responses", "create")
        try_unwrap(f"{_RESPONSES_MODULE}.AsyncResponses", "create")
        try_unwrap(f"{_COMPLETIONS_MODULE}.Completions", "create")
        try_unwrap(f"{_COMPLETIONS_MODULE}.AsyncCompletions", "create")
        reset_request_params()

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
