# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import inspect
import logging
from time import time_ns
from typing import Any, Dict, Optional

try:
    from llama_index.core.agent import BaseAgent  # type: ignore[attr-defined]
except ImportError:
    from llama_index.core.agent.workflow import BaseWorkflowAgent as BaseAgent  # type: ignore[attr-defined]
try:
    from llama_index.core.agent.workflow import AgentWorkflow  # type: ignore[attr-defined]
except ImportError:
    AgentWorkflow = None  # type: ignore[assignment,misc]
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
    GEN_AI_SYSTEM_INSTRUCTIONS,
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

_INSTRUMENTED_TYPES = tuple(
    filter(
        None,
        (
            BaseLLM,
            MultiModalLLM,
            BaseEmbedding,
            BaseTool,
            BaseAgent,
            AgentWorkflow,
            BaseQueryEngine,
            BaseRetriever,
            BaseSynthesizer,
        ),
    )
)

_ALLOWED_METHODS = frozenset(
    (
        "query",
        "aquery",
        "retrieve",
        "aretrieve",
        "synthesize",
        "asynthesize",
        "chat",
        "achat",
        "stream_chat",
        "astream_chat",
        "get_query_embedding",
        "aget_query_embedding",
        "get_text_embedding",
        "aget_text_embedding",
        "get_text_embedding_batch",
        "aget_text_embedding_batch",
        "call",
        "acall",
        "run",
    )
)

_AGENT_STEP_METHOD = "run_agent_step"


def _is_agent_step(id_: str) -> bool:
    return _AGENT_STEP_METHOD in id_.partition("-")[0]


def _is_workflow_run(id_: str) -> bool:
    return id_.partition("-")[0] == "AgentWorkflow.run"


def _get_agent_name(bound_args: inspect.BoundArguments) -> Optional[str]:
    ev = bound_args.arguments.get("ev")
    return getattr(ev, "current_agent_name", None) if ev else None


def _get_system_prompt(bound_args: inspect.BoundArguments) -> Optional[str]:
    ev = bound_args.arguments.get("ev")
    if ev is None:
        return None
    messages = getattr(ev, "input", None)
    if not messages:
        return None
    first = messages[0]
    role = getattr(first, "role", None)
    if role and str(role.value) == "system":
        blocks = getattr(first, "blocks", None)
        if blocks:
            return getattr(blocks[0], "text", None)
    return None


class _SpanHandler(BaseSpanHandler[_Span], extra="allow"):
    _otel_tracer: Tracer = PrivateAttr()
    _separate_trace_from_runtime_context: bool = PrivateAttr()
    _workflow_agent_span: Optional[_Span] = PrivateAttr(default=None)

    def __init__(self, tracer: Tracer, separate_trace_from_runtime_context: bool = False) -> None:
        super().__init__()
        self._otel_tracer = tracer
        self._separate_trace_from_runtime_context = separate_trace_from_runtime_context
        self._workflow_agent_span = None

    @staticmethod
    def _should_suppress(instance: Any, id_: str) -> bool:
        if _is_agent_step(id_):
            return False
        if not isinstance(instance, _INSTRUMENTED_TYPES):
            return True
        method = id_.partition("-")[0].rpartition(".")[-1]
        return method not in _ALLOWED_METHODS

    def _start_span(self, id_: str, parent: Optional[_Span], parent_span_id: Optional[str]) -> _Span:
        otel_span = self._otel_tracer.start_span(
            name="llama_index.operation",
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
        return _Span(otel_span=otel_span, parent=parent, context_token=token, id_=id_, parent_id=parent_span_id)

    def _close_workflow_agent_span(self, err: Optional[BaseException] = None) -> None:
        if self._workflow_agent_span:
            self._workflow_agent_span.end(err)
            self._workflow_agent_span = None

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

        if _is_agent_step(id_):
            agent_name = _get_agent_name(bound_args)

            if self._workflow_agent_span:
                current = self._workflow_agent_span._attributes.get(GEN_AI_AGENT_NAME)
                if current == agent_name:
                    return _PassthroughSpan(parent=self._workflow_agent_span, id_=id_, parent_id=parent_span_id)
                self._close_workflow_agent_span()

            span = self._start_span(id_, parent, parent_span_id)
            span[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.INVOKE_AGENT.value
            if agent_name:
                span[GEN_AI_AGENT_NAME] = agent_name
            system_prompt = _get_system_prompt(bound_args)
            if system_prompt:
                span[GEN_AI_SYSTEM_INSTRUCTIONS] = system_prompt
            self._workflow_agent_span = span
            return span

        if self._should_suppress(instance, id_):
            return _PassthroughSpan(parent=parent, id_=id_, parent_id=parent_span_id)

        span = self._start_span(id_, parent, parent_span_id)
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
            logger.warning("Open span is missing for id_=%s", id_)
            return None
        if span.is_passthrough:
            return span
        if span is self._workflow_agent_span:
            return span
        if _is_workflow_run(id_):
            self._close_workflow_agent_span()
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
            logger.warning("Open span is missing for id_=%s", id_)
            return None
        if span.is_passthrough:
            return span
        if span is self._workflow_agent_span:
            self._close_workflow_agent_span(err)
            return span
        if _is_workflow_run(id_):
            self._close_workflow_agent_span(err)
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
            logger.warning("Open span is missing for span_id=%s, event_id=%s", event.span_id, event.id_)
        else:
            try:
                span.process_event(event)
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception("Error processing event of type %s", event.__class__.__qualname__)
        return event
