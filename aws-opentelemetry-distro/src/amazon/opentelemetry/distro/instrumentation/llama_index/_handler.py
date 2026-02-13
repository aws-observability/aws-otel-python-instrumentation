import asyncio
import inspect
import json
import logging
import weakref
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from functools import singledispatchmethod
from importlib.metadata import version
from queue import SimpleQueue
from threading import RLock, Thread
from time import sleep, time, time_ns
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    DefaultDict,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_NAME,
    GEN_AI_EMBEDDINGS_DIMENSION_COUNT,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
)
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_TYPE
from opentelemetry import context as context_api
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer, set_span_in_context
from opentelemetry.util.types import AttributeValue
from pydantic import PrivateAttr

try:
    from llama_index.core.agent import BaseAgent, BaseAgentWorker  # type: ignore[attr-defined]
except ImportError:
    # Fallback for newer versions where BaseAgent/BaseAgentWorker don't exist
    from llama_index.core.agent.workflow import BaseWorkflowAgent as BaseAgent  # type: ignore[attr-defined]
    BaseAgentWorker = None  # type: ignore[assignment,misc]
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM
from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    CompletionResponse,
)
from llama_index.core.instrumentation.event_handlers import BaseEventHandler
from llama_index.core.instrumentation.events import BaseEvent
from llama_index.core.instrumentation.events.agent import (
    AgentChatWithStepEndEvent,
    AgentChatWithStepStartEvent,
    AgentRunStepEndEvent,
    AgentRunStepStartEvent,
    AgentToolCallEvent,
)
from llama_index.core.instrumentation.events.chat_engine import (
    StreamChatDeltaReceivedEvent,
    StreamChatEndEvent,
    StreamChatErrorEvent,
    StreamChatStartEvent,
)
from llama_index.core.instrumentation.events.embedding import (
    EmbeddingEndEvent,
    EmbeddingStartEvent,
)
from llama_index.core.instrumentation.events.llm import (
    LLMChatEndEvent,
    LLMChatInProgressEvent,
    LLMChatStartEvent,
    LLMCompletionEndEvent,
    LLMCompletionInProgressEvent,
    LLMCompletionStartEvent,
    LLMPredictEndEvent,
    LLMPredictStartEvent,
    LLMStructuredPredictEndEvent,
    LLMStructuredPredictStartEvent,
)
from llama_index.core.instrumentation.events.query import QueryEndEvent, QueryStartEvent
from llama_index.core.instrumentation.events.rerank import (
    ReRankEndEvent,
    ReRankStartEvent,
)
from llama_index.core.instrumentation.events.retrieval import (
    RetrievalEndEvent,
    RetrievalStartEvent,
)
from llama_index.core.instrumentation.events.span import SpanDropEvent  # type: ignore[attr-defined]
from llama_index.core.instrumentation.events.synthesis import (
    GetResponseEndEvent,
    GetResponseStartEvent,
    SynthesizeEndEvent,
    SynthesizeStartEvent,
)
from llama_index.core.instrumentation.span import BaseSpan
from llama_index.core.instrumentation.span_handlers import BaseSpanHandler
from llama_index.core.multi_modal_llms import MultiModalLLM
from llama_index.core.tools import BaseTool
from llama_index.core.tools.types import ToolOutput
from llama_index.core.workflow.errors import WorkflowDone  # type: ignore[attr-defined]
from llama_index.core.workflow.handler import WorkflowHandler  # type: ignore[attr-defined]

from llama_index.core.tools import FunctionTool  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

LLAMA_INDEX_VERSION = tuple(map(int, version("llama-index-core").split(".")[:3]))

# OpenTelemetry well-known operation names for gen_ai.operation.name
_OPERATION_CHAT = "chat"
_OPERATION_TEXT_COMPLETION = "text_completion"
_OPERATION_EMBEDDINGS = "embeddings"
_OPERATION_INVOKE_AGENT = "invoke_agent"
_OPERATION_EXECUTE_TOOL = "execute_tool"

# Custom operation names for LlamaIndex-specific operations
_OPERATION_RERANK = "rerank"
_OPERATION_RETRIEVE = "retrieve"
_OPERATION_SYNTHESIZE = "synthesize"
_OPERATION_QUERY = "query"

# Default value for gen_ai.provider.name, a required attribute per OpenTelemetry
# semantic conventions.
# "llama_index" is not a standard provider name in semconv v1.39, but serves as a fallback when the
# underlying LLM provider cannot be determined.
_PROVIDER_LLAMA_INDEX = "llama_index"

STREAMING_FINISHED_EVENTS = (
    LLMChatEndEvent,
    LLMCompletionEndEvent,
    StreamChatEndEvent,
)
STREAMING_IN_PROGRESS_EVENTS = (
    LLMChatInProgressEvent,
    LLMCompletionInProgressEvent,
    StreamChatDeltaReceivedEvent,
)

if LLAMA_INDEX_VERSION < (0, 10, 44):

    class ExceptionEvent:  # Dummy substitute
        exception: BaseException

elif not TYPE_CHECKING:
    from llama_index.core.instrumentation.events.exception import ExceptionEvent


def _detect_llm_provider(instance: Any) -> Optional[str]:
    """
    Detect LLM provider using lazy imports to avoid import errors when
    optional LLM provider packages are not installed.

    Args:
        instance: The LLM instance to check

    Returns:
        Provider string if detected, None otherwise
    """
    # Try specific provider imports with lazy loading
    try:
        from llama_index.llms.openai import OpenAI as LlamaIndexOpenAI

        if isinstance(instance, LlamaIndexOpenAI):
            return "openai"
    except ImportError:
        pass

    try:
        from llama_index.llms.anthropic import Anthropic as LlamaIndexAnthropic

        if isinstance(instance, LlamaIndexAnthropic):
            return "anthropic"
    except ImportError:
        pass

    try:
        from llama_index.llms.azure_openai import AzureOpenAI as LlamaIndexAzureOpenAI

        if isinstance(instance, LlamaIndexAzureOpenAI):
            return "azure.ai.openai"
    except ImportError:
        pass

    try:
        from llama_index.llms.vertex import Vertex as LlamaIndexVertex

        if isinstance(instance, LlamaIndexVertex):
            return "gcp.vertex_ai"
    except ImportError:
        pass

    # Fallback: check class name if imports fail
    class_name = instance.__class__.__name__.lower()
    if "openai" in class_name:
        if "azure" in class_name:
            return "azure.ai.openai"
        return "openai"
    elif "anthropic" in class_name:
        return "anthropic"
    elif "vertex" in class_name:
        return "gcp.vertex_ai"
    elif "gemini" in class_name:
        return "gcp.gemini"

    return None


class _StreamingStatus(Enum):
    FINISHED = auto()
    IN_PROGRESS = auto()


class _Span(BaseSpan):
    _otel_span: Span = PrivateAttr()
    _attributes: Dict[str, AttributeValue] = PrivateAttr()
    _active: bool = PrivateAttr()
    _parent: Optional["_Span"] = PrivateAttr()
    _first_token_timestamp: Optional[int] = PrivateAttr()
    _context_token: Optional[object] = PrivateAttr()

    _end_time: Optional[int] = PrivateAttr()
    _waiting_for_streaming: bool = PrivateAttr()
    _last_updated_at: float = PrivateAttr()

    def __init__(
        self,
        otel_span: Span,
        parent: Optional["_Span"] = None,
        context_token: Optional[object] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._otel_span = otel_span
        self._active = otel_span.is_recording()
        self._parent = parent
        self._first_token_timestamp = None
        self._attributes = {}
        self._end_time = None
        self._waiting_for_streaming = False
        self._last_updated_at = time()
        self._list_attr_len: DefaultDict[str, int] = defaultdict(int)
        self._context_token = context_token

    def __setitem__(self, key: str, value: AttributeValue) -> None:
        self._attributes[key] = value

    def record_exception(self, exception: BaseException) -> None:
        self._otel_span.record_exception(exception)

    def _get_span_name(self) -> str:
        operation_name = self._attributes.get(GEN_AI_OPERATION_NAME)
        
        # generic fallback if no operation name
        if not operation_name:
            return "llama_index.operation"
        
        if operation_name == _OPERATION_INVOKE_AGENT:
            if agent_name := self._attributes.get(GEN_AI_AGENT_NAME):
                return f"{operation_name} {agent_name}"
        elif operation_name == _OPERATION_EXECUTE_TOOL:
            if tool_name := self._attributes.get(GEN_AI_TOOL_NAME):
                return f"{operation_name} {tool_name}"
        elif model := self._attributes.get(GEN_AI_REQUEST_MODEL):
            return f"{operation_name} {model}"
        
        return operation_name

    def end(self, exception: Optional[BaseException] = None) -> None:
        if not self._active:
            return
        self._active = False
        
        if self._context_token is not None:
            try:
                context_api.detach(self._context_token)
            except Exception:
                pass
            finally:
                self._context_token = None
        
        if exception is None:
            status = Status(status_code=StatusCode.OK)
        else:
            self._otel_span.record_exception(exception)
            self._attributes[ERROR_TYPE] = type(exception).__name__
            # Follow the format in OTEL SDK for description, see:
            # https://github.com/open-telemetry/opentelemetry-python/blob/2b9dcfc5d853d1c10176937a6bcaade54cda1a31/opentelemetry-api/src/opentelemetry/trace/__init__.py#L588  # noqa E501
            description = f"{type(exception).__name__}: {exception}"
            status = Status(status_code=StatusCode.ERROR, description=description)
        
        if GEN_AI_PROVIDER_NAME not in self._attributes:
            self._attributes[GEN_AI_PROVIDER_NAME] = _PROVIDER_LLAMA_INDEX
        
        self._otel_span.update_name(self._get_span_name())
        
        self._otel_span.set_status(status=status)
        self._otel_span.set_attributes(self._attributes)
        self._otel_span.end(end_time=self._end_time)

    @property
    def waiting_for_streaming(self) -> bool:
        return self._active and self._waiting_for_streaming

    @property
    def active(self) -> bool:
        return self._active

    @property
    def context(self) -> context_api.Context:
        return set_span_in_context(self._otel_span)

    def process_input(self, instance: Any, bound_args: inspect.BoundArguments) -> None:
        from llama_index.core.llms.function_calling import FunctionCallingLLM

        if isinstance(instance, FunctionCallingLLM) and isinstance(
            (tools := bound_args.kwargs.get("tools")), Iterable
        ):
            tools_list = list(tools)
            if tools_list:
                # Convert FunctionTool objects to OpenAI tool format
                tool_defs = []
                for tool in tools_list:
                    try:
                        # Try to get the OpenAI tool format from metadata
                        if hasattr(tool, 'metadata') and hasattr(tool.metadata, 'to_openai_tool'):
                            tool_defs.append(tool.metadata.to_openai_tool())
                        else:
                            tool_defs.append(str(tool))
                    except Exception:
                        tool_defs.append(str(tool))
                self[GEN_AI_TOOL_DEFINITIONS] = json.dumps(tool_defs, default=str, ensure_ascii=False)

        # Capture tool call arguments for FunctionTool invocations
        if isinstance(instance, (BaseTool, FunctionTool)):
            kwargs = bound_args.kwargs
            if kwargs:
                self[GEN_AI_TOOL_CALL_ARGUMENTS] = json.dumps(kwargs, default=str, ensure_ascii=False)

    @singledispatchmethod
    def process_instance(self, instance: Any) -> None: ...

    @process_instance.register(BaseLLM)
    @process_instance.register(MultiModalLLM)
    def _(self, instance: Union[BaseLLM, MultiModalLLM]) -> None:
        if metadata := instance.metadata:
            self[GEN_AI_REQUEST_MODEL] = metadata.model_name

        # Add LLM provider detection
        if provider := _detect_llm_provider(instance):
            self[GEN_AI_PROVIDER_NAME] = provider
        
        # Capture temperature if available
        if hasattr(instance, 'temperature') and instance.temperature is not None:
            self[GEN_AI_REQUEST_TEMPERATURE] = instance.temperature
        
        # Capture max_tokens if available
        if hasattr(instance, 'max_tokens') and instance.max_tokens is not None:
            self[GEN_AI_REQUEST_MAX_TOKENS] = instance.max_tokens

    @process_instance.register
    def _(self, instance: BaseEmbedding) -> None:
        if name := instance.model_name:
            self[GEN_AI_REQUEST_MODEL] = name

    @process_instance.register
    def _(self, instance: BaseTool) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_EXECUTE_TOOL
        metadata = instance.metadata
        self[GEN_AI_TOOL_DESCRIPTION] = metadata.description
        try:
            self[GEN_AI_TOOL_NAME] = metadata.get_name()
        except BaseException:
            pass

    def process_event(self, event: BaseEvent) -> None:
        self._process_event(event)
        if not self.waiting_for_streaming:
            return
        if isinstance(event, STREAMING_FINISHED_EVENTS):
            self.end()
            self.notify_parent(_StreamingStatus.FINISHED)
        elif isinstance(event, STREAMING_IN_PROGRESS_EVENTS):
            if self._first_token_timestamp is None:
                timestamp = time_ns()
                self._otel_span.add_event("First Token Stream Event", timestamp=timestamp)
                self._first_token_timestamp = timestamp
            self._last_updated_at = time()
            self.notify_parent(_StreamingStatus.IN_PROGRESS)
        elif isinstance(event, ExceptionEvent):
            self.end(event.exception)
            self.notify_parent(_StreamingStatus.FINISHED)

    def notify_parent(self, status: _StreamingStatus) -> None:
        if not (parent := self._parent) or not parent.waiting_for_streaming:
            return
        if status is _StreamingStatus.IN_PROGRESS:
            parent._last_updated_at = time()
        else:
            parent.end()
        parent.notify_parent(status)

    @singledispatchmethod
    def _process_event(self, event: BaseEvent) -> None:
        logger.warning(f"Unhandled event of type {event.__class__.__qualname__}")

    @_process_event.register
    def _(self, event: ExceptionEvent) -> None: ...

    @_process_event.register
    def _(self, event: AgentChatWithStepStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_INVOKE_AGENT

    @_process_event.register
    def _(self, event: AgentChatWithStepEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_INVOKE_AGENT

    @_process_event.register
    def _(self, event: AgentRunStepStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_INVOKE_AGENT

    @_process_event.register
    def _(self, event: AgentRunStepEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_INVOKE_AGENT
        # FIXME: not sure what to do here with interim outputs since
        # there is no corresponding semantic convention.
        ...

    @_process_event.register
    def _(self, event: AgentToolCallEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_EXECUTE_TOOL
        tool = event.tool
        if name := tool.name:
            self[GEN_AI_TOOL_NAME] = name
        self[GEN_AI_TOOL_DESCRIPTION] = tool.description

    @_process_event.register
    def _(self, event: EmbeddingStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_EMBEDDINGS

    @_process_event.register
    def _(self, event: EmbeddingEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_EMBEDDINGS
        # Capture embedding dimension count if available
        if event.embeddings and len(event.embeddings) > 0:
            first_embedding = event.embeddings[0]
            if hasattr(first_embedding, '__len__'):
                self[GEN_AI_EMBEDDINGS_DIMENSION_COUNT] = len(first_embedding)

    @_process_event.register
    def _(self, event: StreamChatStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_CHAT

    @_process_event.register
    def _(self, event: StreamChatDeltaReceivedEvent) -> None: ...

    @_process_event.register
    def _(self, event: StreamChatErrorEvent) -> None:
        self.record_exception(event.exception)

    @_process_event.register
    def _(self, event: StreamChatEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_CHAT

    @_process_event.register
    def _(self, event: LLMPredictStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_TEXT_COMPLETION

    @_process_event.register
    def _(self, event: LLMPredictEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_TEXT_COMPLETION

    @_process_event.register
    def _(self, event: LLMStructuredPredictStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_TEXT_COMPLETION

    @_process_event.register
    def _(self, event: LLMStructuredPredictEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_TEXT_COMPLETION

    @_process_event.register
    def _(self, event: LLMCompletionStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_TEXT_COMPLETION

    @_process_event.register
    def _(self, event: LLMCompletionInProgressEvent) -> None: ...

    @_process_event.register
    def _(self, event: LLMCompletionEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_TEXT_COMPLETION
        self._extract_token_counts(event.response)

    @_process_event.register
    def _(self, event: LLMChatStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_CHAT
        self._process_messages(
            GEN_AI_INPUT_MESSAGES,
            *event.messages,
        )

    @_process_event.register
    def _(self, event: LLMChatInProgressEvent) -> None: ...

    @_process_event.register
    def _(self, event: LLMChatEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_CHAT
        if (response := event.response) is None:
            return
        self._extract_token_counts(response)
        self._process_messages(
            GEN_AI_OUTPUT_MESSAGES,
            response.message,
        )

    @_process_event.register
    def _(self, event: QueryStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_QUERY

    @_process_event.register
    def _(self, event: QueryEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_QUERY

    @_process_event.register
    def _(self, event: ReRankStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_RERANK
        self[GEN_AI_REQUEST_MODEL] = event.model_name

    @_process_event.register
    def _(self, event: ReRankEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_RERANK

    @_process_event.register
    def _(self, event: RetrievalStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_RETRIEVE

    @_process_event.register
    def _(self, event: RetrievalEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_RETRIEVE

    @_process_event.register
    def _(self, event: SpanDropEvent) -> None:
        # Not needed because `prepare_to_drop_span()` provides the same information.
        ...

    @_process_event.register
    def _(self, event: SynthesizeStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_SYNTHESIZE

    @_process_event.register
    def _(self, event: SynthesizeEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_SYNTHESIZE

    @_process_event.register
    def _(self, event: GetResponseStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_SYNTHESIZE

    @_process_event.register
    def _(self, event: GetResponseEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_SYNTHESIZE

    def _extract_token_counts(self, response: Union[ChatResponse, CompletionResponse]) -> None:
        if raw := getattr(response, "raw", None):
            usage = raw.get("usage") if isinstance(raw, Mapping) else getattr(raw, "usage", None)
            if usage:
                for k, v in _get_token_counts(usage):
                    self[k] = v
            if (
                (model_extra := getattr(raw, "model_extra", None))
                and hasattr(model_extra, "get")
                and (x_groq := model_extra.get("x_groq"))
                and hasattr(x_groq, "get")
                and (usage := x_groq.get("usage"))
            ):
                for k, v in _get_token_counts(usage):
                    self[k] = v

            # Check for VertexAI usage_metadata
            # VertexAI stores usage_metadata inside _raw_response
            if isinstance(raw, Mapping) and (raw_response := raw.get("_raw_response")):
                usage_metadata = (
                    raw_response.get("usage_metadata")
                    if isinstance(raw_response, Mapping)
                    else getattr(raw_response, "usage_metadata", None)
                )
            else:
                usage_metadata = getattr(raw, "usage_metadata", None)
            if usage_metadata:
                for k, v in _get_token_counts(usage_metadata):
                    self[k] = v
        # Look for token counts in additional_kwargs of the completion payload
        # This is needed for non-OpenAI models
        if additional_kwargs := getattr(response, "additional_kwargs", None):
            for k, v in _get_token_counts(additional_kwargs):
                self[k] = v

    def _process_messages(
        self,
        prefix: str,
        *messages: ChatMessage,
    ) -> None:
        if messages:
            self[prefix] = json.dumps(list(messages), default=str, ensure_ascii=False)


    @process_instance.register(FunctionTool)
    def _(self, instance: FunctionTool) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_EXECUTE_TOOL
        metadata = instance.metadata
        self[GEN_AI_TOOL_DESCRIPTION] = metadata.description
        try:
            self[GEN_AI_TOOL_NAME] = metadata.get_name()
        except BaseException:
            pass

    @process_instance.register(BaseAgent)
    def _(self, instance: BaseAgent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_INVOKE_AGENT
        if hasattr(instance, 'name') and instance.name:
            self[GEN_AI_AGENT_NAME] = instance.name
        if hasattr(instance, 'description') and instance.description:
            self[GEN_AI_AGENT_DESCRIPTION] = instance.description
        if hasattr(instance, 'system_prompt') and instance.system_prompt:
            self[GEN_AI_SYSTEM_INSTRUCTIONS] = instance.system_prompt

END_OF_QUEUE = None


@dataclass
class _QueueItem:
    last_touched_at: float
    span: _Span


class _ExportQueue:
    """
    Container for spans that have ended but are waiting for streaming events. The
    list is periodically swept to evict items that are no longer active or have not
    been updated for over 60 seconds.
    """

    def __init__(self) -> None:
        self.lock: RLock = RLock()
        self.spans: Dict[str, _Span] = {}
        self.queue: "SimpleQueue[Optional[_QueueItem]]" = SimpleQueue()
        weakref.finalize(self, self.queue.put, END_OF_QUEUE)
        Thread(target=self._sweep, args=(self.queue,), daemon=True).start()

    def put(self, span: _Span) -> None:
        with self.lock:
            self.spans[span.id_] = span
        self.queue.put(_QueueItem(time(), span))

    def find(self, id_: str) -> Optional[_Span]:
        with self.lock:
            return self.spans.get(id_)

    def _del(self, item: _QueueItem) -> None:
        with self.lock:
            del self.spans[item.span.id_]

    def _sweep(self, q: "SimpleQueue[Optional[_QueueItem]]") -> None:
        while True:
            t = time()
            while not q.empty():
                if (item := q.get()) is END_OF_QUEUE:
                    return
                if t == item.last_touched_at:
                    # we have gone through the whole list
                    q.put(item)
                    break
                span = item.span
                if not span.active:
                    self._del(item)
                    continue
                if t - span._last_updated_at > 60:
                    span.end()
                    self._del(item)
                    continue
                item.last_touched_at = t
                q.put(item)
            sleep(0.1)


class _SpanHandler(BaseSpanHandler[_Span], extra="allow"):
    _otel_tracer: Tracer = PrivateAttr()
    _separate_trace_from_runtime_context: bool = PrivateAttr()
    _export_queue: _ExportQueue = PrivateAttr()

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
        self._export_queue = _ExportQueue()

    def new_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Optional[_Span]:
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return None
        
        with self.lock:
            parent = self.open_spans.get(parent_span_id) if parent_span_id else None

        if instance is None:
            return None

        span_method = id_.partition("-")[0]

        if type(instance).__name__ in ("TokenTextSplitter", "DefaultRefineProgram", "SentenceSplitter", "CompactAndRefine"):
            return None

        # Suppress internal workflow coordination steps that add noise without semantic value
        _SUPPRESSED_METHODS = {
            "parse_agent_output",
            "aggregate_tool_results",
            "setup_agent",
            "init_run",
            "run_agent_step",
            "call_tool",
            "_prepare_chat_with_tools",
            "_get_text_embedding",
            "_query",
            "_retrieve",
            "_get_query_embedding",
            "predict_and_call",
            "__call__",
        }
        method_suffix = span_method.rpartition(".")[-1]
        if method_suffix in _SUPPRESSED_METHODS:
            return None
        
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
        span.process_instance(instance)
        span.process_input(instance, bound_args)

        return span

    def prepare_to_exit_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        result: Optional[Any] = None,
        **kwargs: Any,
    ) -> Any:
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return None
        with self.lock:
            span = self.open_spans.get(id_)
        if span:
            if isinstance(instance, (BaseLLM, MultiModalLLM)) and (
                isinstance(result, Generator)
                and result.gi_frame is not None
                or isinstance(result, AsyncGenerator)
                and result.ag_frame is not None
            ):
                span._waiting_for_streaming = True
                self._export_queue.put(span)
                return span
            # For WorkflowHandler results (e.g. FunctionAgent.run), attach a
            # done callback to the handler's result task so the span closes
            # when the workflow actually completes, not when run() returns.
            if isinstance(result, WorkflowHandler):
                def _on_workflow_done(task: "asyncio.Task[Any]", s: _Span = span) -> None:
                    exc = None
                    try:
                        exc = task.exception()
                    except (asyncio.CancelledError, Exception):
                        pass
                    s.end(exception=exc)
                result._result_task.add_done_callback(_on_workflow_done)
                return span
            if isinstance(result, ToolOutput):
                span._attributes[GEN_AI_TOOL_CALL_RESULT] = result.content
            span.end()
        else:
            logger.warning(f"Open span is missing for {id_=}")
        return span

    def prepare_to_drop_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        err: Optional[BaseException] = None,
        **kwargs: Any,
    ) -> Any:
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return None
        with self.lock:
            span = self.open_spans.get(id_)
        if span:
            if err and isinstance(err, WorkflowDone):
                span.end()
                return span
            span.end(err)
        else:
            logger.warning(f"Open span is missing for {id_=}")
        return span


class EventHandler(BaseEventHandler, extra="allow"):
    _span_handler: _SpanHandler = PrivateAttr()

    def __init__(self, span_handler: _SpanHandler) -> None:
        super().__init__()
        self._span_handler = span_handler

    def handle(self, event: BaseEvent, **kwargs: Any) -> Any:
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return None
        if not event.span_id:
            return event
        span = self._span_handler.open_spans.get(event.span_id)
        if span is None:
            span = self._span_handler._export_queue.find(event.span_id)
        if span is None:
            logger.warning(f"Open span is missing for {event.span_id=}, {event.id_=}")
        else:
            try:
                span.process_event(event)
            except Exception:
                logger.exception(f"Error processing event of type {event.__class__.__qualname__}")
                pass
        return event


def _get_token_counts(usage: Union[object, Mapping[str, Any]]) -> Iterator[Tuple[str, Any]]:
    if isinstance(usage, Mapping):
        return _get_token_counts_from_mapping(usage)
    if isinstance(usage, object):
        return _get_token_counts_from_object(usage)


def _get_token_counts_from_object(usage: object) -> Iterator[Tuple[str, Any]]:
    def get_value(obj: object, key: str) -> Any:
        return getattr(obj, key, None)

    yield from _get_token_counts_impl(usage, get_value)


def _get_token_counts_from_mapping(
    usage_mapping: Mapping[str, Any],
) -> Iterator[Tuple[str, Any]]:
    def get_value(obj: Mapping[str, Any], key: str) -> Any:
        return obj.get(key)

    yield from _get_token_counts_impl(usage_mapping, get_value)


def _get_token_counts_impl(
    usage: Union[object, Mapping[str, Any]], get_value: Callable[[Any, str], Any]
) -> Iterator[Tuple[str, Any]]:
    # OpenAI
    if (prompt_tokens := get_value(usage, "prompt_tokens")) is not None:
        try:
            yield GEN_AI_USAGE_INPUT_TOKENS, int(prompt_tokens)
        except BaseException:
            pass
    if (completion_tokens := get_value(usage, "completion_tokens")) is not None:
        try:
            yield GEN_AI_USAGE_OUTPUT_TOKENS, int(completion_tokens)
        except BaseException:
            pass

    # Anthropic
    if (output_tokens := get_value(usage, "output_tokens")) is not None:
        try:
            yield GEN_AI_USAGE_OUTPUT_TOKENS, int(output_tokens)
        except BaseException:
            pass
    if (input_tokens := get_value(usage, "input_tokens")) is not None:
        try:
            yield GEN_AI_USAGE_INPUT_TOKENS, int(input_tokens)
        except BaseException:
            pass

    # VertexAI
    if (prompt_token_count := get_value(usage, "prompt_token_count")) is not None:
        try:
            yield GEN_AI_USAGE_INPUT_TOKENS, int(prompt_token_count)
        except BaseException:
            pass
    if (candidates_token_count := get_value(usage, "candidates_token_count")) is not None:
        try:
            yield GEN_AI_USAGE_OUTPUT_TOKENS, int(candidates_token_count)
        except BaseException:
            pass


