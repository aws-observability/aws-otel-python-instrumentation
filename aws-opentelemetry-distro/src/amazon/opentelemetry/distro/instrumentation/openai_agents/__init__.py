# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Collection

from typing_extensions import override

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.utils import suppress_http_instrumentation


class OpenAIAgentsInstrumentor(BaseInstrumentor):  # type: ignore
    """Instrument OpenAI Agents SDK tracing callbacks with OpenTelemetry spans."""

    _capture = None
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

        from ._gen_ai_context_capture import GenAIContextCapture  # pylint: disable=import-outside-toplevel
        from ._processor import OpenTelemetryTracingProcessor  # pylint: disable=import-outside-toplevel

        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)
        self._processor = OpenTelemetryTracingProcessor(tracer)
        self._capture = GenAIContextCapture(self._processor.get_otel_span)

        try_wrap(
            "agents.extensions.models.litellm_model",
            "model_config_for_trace",
            self._capture.record_litellm_model_config,
        )
        try_wrap("openai.resources.responses.responses", "Responses.create", self._capture.record_openai_request)
        try_wrap(
            "openai.resources.responses.responses",
            "AsyncResponses.create",
            self._capture.record_openai_request,
        )
        try_wrap(
            "openai.resources.chat.completions.completions",
            "Completions.create",
            self._capture.record_openai_request,
        )
        try_wrap(
            "openai.resources.chat.completions.completions",
            "AsyncCompletions.create",
            self._capture.record_openai_request,
        )
        try_wrap("litellm", "completion", self._capture.record_litellm_completion)
        try_wrap("litellm", "acompletion", self._capture.record_litellm_acompletion)
        try_wrap("agents.items", "ItemHelpers.tool_call_output_item", self._capture.record_tool_call)
        try_wrap("agents.tool_context", "ToolContext.from_agent_context", self._capture.record_tool_call)
        # disables http spans created from spans sent OpenAI's tracing backend
        try_wrap("agents.tracing.processors", "BackendSpanExporter.export", _suppress_http_instrumentation)

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

        try_unwrap("agents.extensions.models.litellm_model", "model_config_for_trace")
        try_unwrap("openai.resources.responses.responses.Responses", "create")
        try_unwrap("openai.resources.responses.responses.AsyncResponses", "create")
        try_unwrap("openai.resources.chat.completions.completions.Completions", "create")
        try_unwrap("openai.resources.chat.completions.completions.AsyncCompletions", "create")
        try_unwrap("litellm", "completion")
        try_unwrap("litellm", "acompletion")
        try_unwrap("agents.items.ItemHelpers", "tool_call_output_item")
        try_unwrap("agents.tool_context.ToolContext", "from_agent_context")
        try_unwrap("agents.tracing.processors.BackendSpanExporter", "export")
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
            self._capture = None
            self._processor = None
            self._previous_processors = None


def _suppress_http_instrumentation(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
    with suppress_http_instrumentation():
        return wrapped(*args, **kwargs)
