# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import inspect
import logging
from time import time_ns
from typing import Any, AsyncGenerator, Dict, Optional

from llama_index.core.agent.workflow import AgentWorkflow, BaseWorkflowAgent
from llama_index.core.agent.workflow.workflow_events import AgentSetup
from llama_index.core.base.base_query_engine import BaseQueryEngine
from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM
from llama_index.core.instrumentation.event_handlers import BaseEventHandler
from llama_index.core.instrumentation.events import BaseEvent
from llama_index.core.instrumentation.span_handlers import BaseSpanHandler
from llama_index.core.multi_modal_llms import MultiModalLLM
from llama_index.core.response_synthesizers.base import BaseSynthesizer
from llama_index.core.tools import BaseTool
from llama_index.core.tools.types import ToolOutput
from pydantic import PrivateAttr

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import skip_instrumentation_if_suppressed
from opentelemetry import context as context_api
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.trace import SpanKind, Tracer, set_span_in_context

from ._span import (  # noqa: F401  # pylint: disable=unused-import
    GEN_AI_TOOL_CALL_RESULT,
    ExceptionEvent,
    _detect_llm_provider,
    _PassthroughSpan,
    _Span,
)

logger = logging.getLogger(__name__)

# Instance types that produce semantically meaningful spans.
# Only these types get real OTel spans; all others get passthrough spans.
_INSTRUMENTED_TYPES = (
    BaseLLM,
    MultiModalLLM,
    BaseEmbedding,
    BaseTool,
    BaseWorkflowAgent,
    AgentWorkflow,
    BaseQueryEngine,
    BaseRetriever,
    BaseSynthesizer,
)

# Methods on instrumented types that produce user-facing spans.
# Any method NOT in this set becomes a passthrough span (fail-safe:
# new/unknown methods are invisible rather than creating ghost spans).
_ALLOWED_METHODS = frozenset(
    (
        # QueryEngine
        "query",
        "aquery",
        # Retriever
        "retrieve",
        "aretrieve",
        # Synthesizer
        "synthesize",
        "asynthesize",
        # LLM (complete/stream_complete are passthrough because
        # Bedrock converts them to chat internally, and the chat
        # span carries the semantic operation name from events)
        "chat",
        "achat",
        "stream_chat",
        "astream_chat",
        # Embedding
        "get_query_embedding",
        "aget_query_embedding",
        "get_text_embedding",
        "aget_text_embedding",
        "get_text_embedding_batch",
        "aget_text_embedding_batch",
        # Tool
        "call",
        "acall",
        # Agent
        "run",
    )
)


class _SpanHandler(BaseSpanHandler[_Span], extra="allow"):
    _otel_tracer: Tracer = PrivateAttr()
    _separate_trace_from_runtime_context: bool = PrivateAttr()

    def __init__(
        self,
        tracer: Tracer,
        separate_trace_from_runtime_context: bool = False,
    ) -> None:
        """Initialize the span handler.

        Args:
            tracer (trace_api.Tracer): The OpenTelemetry tracer for creating spans.
            separate_trace_from_runtime_context (bool): When True, always start a new trace for each
                span without a parent, isolating it from any existing trace in the runtime context.
        """
        super().__init__()
        self._otel_tracer = tracer
        self._separate_trace_from_runtime_context = separate_trace_from_runtime_context

    @staticmethod
    def _get_agent_setup(bound_args: inspect.BoundArguments) -> Optional[AgentSetup]:
        ev = bound_args.arguments.get("ev")
        return ev if isinstance(ev, AgentSetup) else None

    @staticmethod
    def _should_suppress(instance: Any, id_: str, bound_args: inspect.BoundArguments) -> bool:
        """Check if this span should be a passthrough (no OTel span created).

        A span is suppressed (passthrough) when:
        - The instance is not one of _INSTRUMENTED_TYPES, OR
        - The method is internal plumbing not in _ALLOWED_METHODS.
        """
        if _SpanHandler._get_agent_setup(bound_args) is not None:
            return False
        if not isinstance(instance, _INSTRUMENTED_TYPES):
            return True
        method = id_.partition("-")[0].rpartition(".")[-1]
        return method not in _ALLOWED_METHODS

    @skip_instrumentation_if_suppressed
    def new_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Optional[_Span]:
        with self.lock:
            parent = self.open_spans.get(parent_span_id) if parent_span_id else None

        if self._should_suppress(instance, id_, bound_args):
            return _PassthroughSpan(
                parent=parent,
                id_=id_,
                parent_id=parent_span_id,
            )

        otel_span = self._otel_tracer.start_span(
            name="llama_index.operation",  # generic operation name, updated in span.end()
            start_time=time_ns(),
            attributes={},
            kind=SpanKind.INTERNAL,
            context=(
                parent.context
                if parent
                else (context_api.Context() if self._separate_trace_from_runtime_context else None)
            ),
        )

        token = context_api.attach(set_span_in_context(otel_span, parent.context if parent else None))

        span = _Span(
            otel_span=otel_span,
            parent=parent,
            context_token=token,
            id_=id_,
            parent_id=parent_span_id,
        )

        agent_setup = self._get_agent_setup(bound_args)
        if agent_setup is not None:
            span[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.INVOKE_AGENT.value
            agent_name = getattr(agent_setup, "current_agent_name", None)
            if agent_name:
                span[GEN_AI_AGENT_NAME] = agent_name
                span._span_name = f"run_agent_step {agent_name}"
        else:
            span.process_instance(instance)
            span.process_input(instance, bound_args)

        return span

    @skip_instrumentation_if_suppressed
    def prepare_to_exit_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        result: Optional[Any] = None,
        **kwargs: Any,
    ) -> Any:
        with self.lock:
            span = self.open_spans.get(id_)
        if not span:
            logger.debug("Open span is missing for id_=%s", id_)
            return None
        if span.is_passthrough:
            return span

        if isinstance(result, AsyncGenerator):
            span.set_deferred()
            return None

        if isinstance(result, ToolOutput):
            span._attributes[GEN_AI_TOOL_CALL_RESULT] = result.content
        span.end()
        return span

    @skip_instrumentation_if_suppressed
    def prepare_to_drop_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        err: Optional[BaseException] = None,
        **kwargs: Any,
    ) -> Any:
        with self.lock:
            span = self.open_spans.get(id_)
        if not span:
            logger.debug("Open span is missing for id_=%s", id_)
            return None
        if span.is_passthrough:
            return span
        span.end(err)
        return span


class EventHandler(BaseEventHandler, extra="allow"):
    _span_handler: _SpanHandler = PrivateAttr()

    def __init__(self, span_handler: _SpanHandler) -> None:
        super().__init__()
        self._span_handler = span_handler

    @skip_instrumentation_if_suppressed
    def handle(self, event: BaseEvent, **kwargs: Any) -> Any:
        if not event.span_id:
            return event
        span = self._span_handler.open_spans.get(event.span_id)
        if span is None:
            logger.debug("Open span is missing for span_id=%s, event_id=%s", event.span_id, event.id_)
        else:
            try:
                span.process_event(event)
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception("Error processing event of type %s", event.__class__.__qualname__)
        return event
