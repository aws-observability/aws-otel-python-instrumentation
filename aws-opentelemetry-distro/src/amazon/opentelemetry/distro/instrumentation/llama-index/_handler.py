import copy
import dataclasses
import inspect
import json
import logging
import weakref
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from functools import singledispatch, singledispatchmethod
from importlib.metadata import version
from io import IOBase
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
    SupportsFloat,
    Tuple,
    TypeVar,
    Union,
)

from amazon.opentelemetry.distro.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_EMBEDDINGS_DIMENSION_COUNT,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
)
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS
)
from opentelemetry import context as context_api
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.trace import Span, Status, StatusCode, Tracer, set_span_in_context
from opentelemetry.util.types import AttributeValue
from pydantic import BaseModel as PydanticBaseModel
from pydantic import PrivateAttr
from pydantic.v1.json import pydantic_encoder
from typing_extensions import assert_never

from llama_index.core import QueryBundle

# Conditionally import agent base classes (they may not exist in all versions)
try:
    from llama_index.core.agent import BaseAgent, BaseAgentWorker  # type: ignore[attr-defined]
except ImportError:
    # Fallback for older versions
    try:
        from llama_index.core.base.agent.types import (  # type: ignore[import-not-found]
            BaseAgent,
            BaseAgentWorker,
        )
    except ImportError:
        BaseAgent = None  # type: ignore[misc,assignment]
        BaseAgentWorker = None  # type: ignore[misc,assignment]
try:
    from llama_index.core.base.llms.types import ToolCallBlock  # type: ignore
except ImportError:
    ToolCallBlock = None  # type: ignore
from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM
from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    CompletionResponse,
    ContentBlock,
    ImageBlock,
    TextBlock,
)
from llama_index.core.base.response.schema import (
    RESPONSE_TYPE,
    AsyncStreamingResponse,
    PydanticResponse,
    Response,
    StreamingResponse,
)
from llama_index.core.bridge.pydantic import BaseModel
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
from llama_index.core.schema import BaseNode, NodeWithScore, QueryType
from llama_index.core.tools import BaseTool
from llama_index.core.types import RESPONSE_TEXT_TYPE
from llama_index.core.workflow.errors import WorkflowDone  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

LLAMA_INDEX_VERSION = tuple(map(int, version("llama-index-core").split(".")[:3]))

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
    _span_kind: Optional[str] = PrivateAttr()
    _parent: Optional["_Span"] = PrivateAttr()
    _first_token_timestamp: Optional[int] = PrivateAttr()

    _end_time: Optional[int] = PrivateAttr()
    _last_updated_at: float = PrivateAttr()

    def __init__(
        self,
        otel_span: Span,
        span_kind: Optional[str] = None,
        parent: Optional["_Span"] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._otel_span = otel_span
        self._active = otel_span.is_recording()
        self._span_kind = span_kind
        self._parent = parent
        self._first_token_timestamp = None
        self._attributes = {}
        self._end_time = None
        self._last_updated_at = time()
        self._list_attr_len: DefaultDict[str, int] = defaultdict(int)

    def __setitem__(self, key: str, value: AttributeValue) -> None:
        self._attributes[key] = value

    def record_exception(self, exception: BaseException) -> None:
        self._otel_span.record_exception(exception)

    def end(self, exception: Optional[BaseException] = None) -> None:
        if not self._active:
            return
        self._active = False
        if exception is None:
            status = Status(status_code=StatusCode.OK)
        else:
            self._otel_span.record_exception(exception)
            # Follow the format in OTEL SDK for description, see:
            # https://github.com/open-telemetry/opentelemetry-python/blob/2b9dcfc5d853d1c10176937a6bcaade54cda1a31/opentelemetry-api/src/opentelemetry/trace/__init__.py#L588  # noqa E501
            description = f"{type(exception).__name__}: {exception}"
            status = Status(status_code=StatusCode.ERROR, description=description)
        # self[OPENINFERENCE_SPAN_KIND] = self._span_kind or CHAIN
        self._otel_span.set_status(status=status)
        self._otel_span.set_attributes(self._attributes)
        self._otel_span.end(end_time=self._end_time)

    @property
    def waiting_for_streaming(self) -> bool:
        return self._active and bool(self._end_time)

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
            # Convert tools to a list and set as gen_ai.tool.definitions
            # OTel expects an array of tool definitions
            tools_list = list(tools)
            if tools_list:
                self[GEN_AI_TOOL_DEFINITIONS] = json.dumps(tools_list, default=str, ensure_ascii=False)

    @singledispatchmethod
    def process_instance(self, instance: Any) -> None: ...

    @process_instance.register(BaseLLM)
    @process_instance.register(MultiModalLLM)
    def _(self, instance: Union[BaseLLM, MultiModalLLM]) -> None:
        if metadata := instance.metadata:
            self[GEN_AI_REQUEST_MODEL] = metadata.model_name
            # self[LLM_INVOCATION_PARAMETERS] = metadata.json(exclude_unset=True)

        # Add LLM provider detection
        if provider := _detect_llm_provider(instance):
            self[GEN_AI_PROVIDER_NAME] = provider

    @process_instance.register
    def _(self, instance: BaseEmbedding) -> None:
        if name := instance.model_name:
            self[GEN_AI_REQUEST_MODEL] = name

    @process_instance.register
    def _(self, instance: BaseTool) -> None:
        metadata = instance.metadata
        self[GEN_AI_TOOL_DESCRIPTION] = metadata.description
        try:
            self[GEN_AI_TOOL_NAME] = metadata.get_name()
        except BaseException:
            pass
        # Note: Tool schema (metadata.fn_schema_str) is not captured here.
        # gen_ai.tool.definitions is meant for an array of all available tools at the
        # agent/model level, not a single tool's schema. Tool name and description are
        # sufficient for individual tool spans. Actual call arguments are captured in
        # AgentToolCallEvent via gen_ai.tool.call.arguments.

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
        if not self._span_kind:
            self._span_kind = AGENT

    @_process_event.register
    def _(self, event: AgentChatWithStepEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: AgentRunStepStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = AGENT

    @_process_event.register
    def _(self, event: AgentRunStepEndEvent) -> None:
        # FIXME: not sure what to do here with interim outputs since
        # there is no corresponding semantic convention.
        ...

    @_process_event.register
    def _(self, event: AgentToolCallEvent) -> None:
        tool = event.tool
        if name := tool.name:
            self[GEN_AI_TOOL_NAME] = name
        self[GEN_AI_TOOL_DESCRIPTION] = tool.description
        # Note: tool.get_parameters_dict() returns the tool's parameter schema (definition),
        # not the actual arguments passed during this call. OTel's gen_ai.tool.call.arguments
        # is for actual call arguments, and gen_ai.tool.definitions is for an array of tool
        # definitions at the agent level. Neither is appropriate for a single tool's schema
        # in a tool call event, so we omit it.
        # Omitted: tool parameter schema from get_parameters_dict()

    @_process_event.register
    def _(self, event: EmbeddingStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = EMBEDDING

    @_process_event.register
    def _(self, event: EmbeddingEndEvent) -> None:
        # Note: OpenTelemetry gen_ai semantic conventions (as of v1.39.0) do not include
        # attributes for embedding vectors or input texts. These can be very large and would
        # bloat spans significantly. OTel focuses on metadata like dimension count.
        # Omitted OpenInference attributes: embedding.embeddings[].text, embedding.embeddings[].vector
        
        # Capture embedding dimension count if available
        if event.embeddings and len(event.embeddings) > 0:
            first_embedding = event.embeddings[0]
            if hasattr(first_embedding, '__len__'):
                self[GEN_AI_EMBEDDINGS_DIMENSION_COUNT] = len(first_embedding)

    @_process_event.register
    def _(self, event: StreamChatStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = LLM

    @_process_event.register
    def _(self, event: StreamChatDeltaReceivedEvent) -> None: ...

    @_process_event.register
    def _(self, event: StreamChatErrorEvent) -> None:
        self.record_exception(event.exception)

    @_process_event.register
    def _(self, event: StreamChatEndEvent) -> None: ...

    @_process_event.register
    def _(self, event: LLMPredictStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = LLM
        # Note: LLMPredictStartEvent uses templates, but OTel doesn't have
        # standard attributes for prompt templates. The actual LLM input
        # will be captured via gen_ai.input.messages in chat events.

    @_process_event.register
    def _(self, event: LLMPredictEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: LLMStructuredPredictStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = LLM

    @_process_event.register
    def _(self, event: LLMStructuredPredictEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: LLMCompletionStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = LLM
        # Note: OpenTelemetry gen_ai semantic conventions (as of v1.39.0) do not include
        # attributes for completion-style prompts. The deprecated gen_ai.prompt attribute
        # was removed in favor of gen_ai.input.messages for chat-style interactions.
        # Completion API prompts are not captured to avoid span bloat.
        # Omitted OpenInference attribute: llm.prompts

    @_process_event.register
    def _(self, event: LLMCompletionInProgressEvent) -> None: ...

    @_process_event.register
    def _(self, event: LLMCompletionEndEvent) -> None:
        self._extract_token_counts(event.response)

    @_process_event.register
    def _(self, event: LLMChatStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = LLM
        self._process_messages(
            GEN_AI_INPUT_MESSAGES,
            *event.messages,
        )

    @_process_event.register
    def _(self, event: LLMChatInProgressEvent) -> None: ...

    @_process_event.register
    def _(self, event: LLMChatEndEvent) -> None:
        if (response := event.response) is None:
            return
        self._extract_token_counts(response)
        self._process_messages(
            GEN_AI_OUTPUT_MESSAGES,
            response.message,
        )

    @_process_event.register
    def _(self, event: QueryStartEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: QueryEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: ReRankStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = RERANKER
        # Map reranker model name to standard gen_ai attribute
        self[GEN_AI_REQUEST_MODEL] = event.model_name
        # Note: OpenTelemetry gen_ai semantic conventions (as of v1.39.0) do not include
        # attributes for reranking operations. These are RAG-specific concepts not yet
        # standardized in OTel semconv.
        # Omitted OpenInference attributes: reranker.query, reranker.top_k,
        # reranker.input_documents, reranker.output_documents

    @_process_event.register
    def _(self, event: ReRankEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: RetrievalStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = RETRIEVER
        # Note: OpenTelemetry gen_ai semantic conventions (as of v1.39.0) do not include
        # attributes for retrieval operations or document results. These are RAG-specific
        # concepts not yet standardized in OTel semconv.
        # Omitted OpenInference attributes: retrieval.documents (document content, IDs, scores, metadata)

    @_process_event.register
    def _(self, event: RetrievalEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: SpanDropEvent) -> None:
        # Not needed because `prepare_to_drop_span()` provides the same information.
        ...

    @_process_event.register
    def _(self, event: SynthesizeStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = CHAIN

    @_process_event.register
    def _(self, event: SynthesizeEndEvent) -> None:
        pass

    @_process_event.register
    def _(self, event: GetResponseStartEvent) -> None:
        if not self._span_kind:
            self._span_kind = CHAIN

    @_process_event.register
    def _(self, event: GetResponseEndEvent) -> None:
        pass

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
        otel_span = self._otel_tracer.start_span(
            name=id_.partition("-")[0],
            start_time=time_ns(),
            attributes={},
            context=(
                parent.context
                if parent
                else (context_api.Context() if self._separate_trace_from_runtime_context else None)
            ),
        )
        span = _Span(
            otel_span=otel_span,
            span_kind=_init_span_kind(instance),
            parent=parent,
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
                span._end_time = time_ns()
                self._export_queue.put(span)
                return span
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


def _get_tool_call(tool_call: object) -> Iterator[Tuple[str, Any]]:
    if isinstance(tool_call, dict):
        if tool_call_id := tool_call.get("id"):
            yield GEN_AI_TOOL_CALL_ID, tool_call_id
        if name := tool_call.get("name"):
            yield GEN_AI_TOOL_NAME, name
        if function := tool_call.get("function"):
            yield GEN_AI_TOOL_NAME, function.get("name")
            if isinstance(function.get("arguments"), str):
                yield GEN_AI_TOOL_CALL_ARGUMENTS, function.get("arguments")
            else:
                yield GEN_AI_TOOL_CALL_ARGUMENTS, json.dumps(function.get("arguments"), default=str, ensure_ascii=False)
        if arguments := tool_call.get("input"):
            if isinstance(arguments, str):
                yield GEN_AI_TOOL_CALL_ARGUMENTS, arguments
            elif isinstance(arguments, dict):
                yield GEN_AI_TOOL_CALL_ARGUMENTS, json.dumps(arguments, default=str, ensure_ascii=False)
    elif ToolCallBlock is not None and isinstance(tool_call, ToolCallBlock):
        if tool_call_id := getattr(tool_call, "tool_call_id", None):
            yield GEN_AI_TOOL_CALL_ID, tool_call_id
        if name := getattr(tool_call, "tool_name", None):
            yield GEN_AI_TOOL_NAME, name
        if isinstance(getattr(tool_call, "tool_kwargs", None), str):
            yield GEN_AI_TOOL_CALL_ARGUMENTS, getattr(tool_call, "tool_kwargs", None)
        else:
            yield (
                GEN_AI_TOOL_CALL_ARGUMENTS,
                json.dumps(getattr(tool_call, "tool_kwargs", None), default=str, ensure_ascii=False),
            )
    elif function := getattr(tool_call, "function", None):
        if tool_call_id := getattr(tool_call, "id", None):
            yield GEN_AI_TOOL_CALL_ID, tool_call_id
        if name := getattr(function, "name", None):
            yield GEN_AI_TOOL_NAME, name
        if arguments := getattr(function, "arguments", None):
            if isinstance(arguments, str):
                yield GEN_AI_TOOL_CALL_ARGUMENTS, arguments
            else:
                yield GEN_AI_TOOL_CALL_ARGUMENTS, json.dumps(arguments, default=str, ensure_ascii=False)


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
    # Note: OpenTelemetry gen_ai semantic conventions (as of v1.39.0) only support
    # input and output token counts. Total tokens and detailed token breakdowns
    # (cached tokens, audio tokens, reasoning tokens) are not part of the standard.
    # Omitted OpenInference attributes: llm.token_count.total,
    # llm.token_count.prompt.details.cache_read, llm.token_count.prompt.details.cache_write,
    # llm.token_count.prompt.details.audio, llm.token_count.completion.details.reasoning,
    # llm.token_count.completion.details.audio
    
    # OpenAI format
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

    # Anthropic format
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

    # VertexAI format
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


@singledispatch
def _init_span_kind(_: Any) -> Optional[str]:
    return None


# Only register agent handlers if the classes exist
if BaseAgent is not None:

    @_init_span_kind.register
    def _agent_span_kind(_: BaseAgent) -> str:  # type: ignore[misc]
        return AGENT


if BaseAgentWorker is not None:

    @_init_span_kind.register
    def _agent_worker_span_kind(_: BaseAgentWorker) -> str:  # type: ignore[misc]
        return AGENT


@_init_span_kind.register
def _(_: BaseLLM) -> str:
    return LLM


@_init_span_kind.register
def _(_: BaseRetriever) -> str:
    return RETRIEVER


@_init_span_kind.register
def _(_: BaseEmbedding) -> str:
    return EMBEDDING


@_init_span_kind.register
def _(_: BaseTool) -> str:
    return TOOL


class _Encoder(json.JSONEncoder):
    def __init__(self, **kwargs: Any) -> None:
        kwargs.pop("default", None)
        super().__init__(**kwargs)

    def default(self, obj: Any) -> Any:
        return _encoder(obj)


def _encoder(obj: Any) -> Any:
    if repr_str := _show_repr_str(obj):
        return repr_str
    if isinstance(obj, QueryBundle):
        d = obj.to_dict()
        if obj.embedding:
            d["embedding"] = f"<{len(obj.embedding)}-dimensional vector>"
        return d
    if dataclasses.is_dataclass(obj):
        return _asdict(obj)
    try:
        return pydantic_encoder(obj)
    except BaseException:
        return repr(obj)


def _show_repr_str(obj: Any) -> Optional[str]:
    if isinstance(obj, (Generator, AsyncGenerator)):
        return f"<{obj.__class__.__qualname__} object>"
    if callable(obj):
        try:
            return f"<{obj.__qualname__}{str(inspect.signature(obj))}>"
        except BaseException:
            return f"<{obj.__class__.__qualname__} object>"
    if isinstance(obj, BaseNode):
        return f"<{obj.__class__.__qualname__}(id_={obj.id_})>"
    if isinstance(obj, NodeWithScore):
        return (
            f"<{obj.__class__.__qualname__}(node={obj.node.__class__.__qualname__}"
            f"(id_={obj.node.id_}), score={obj.score})>"
        )
    return None


def _asdict(obj: Any) -> Any:
    """
    This is a copy of Python's `_asdict_inner` function (linked below) but modified primarily to
    not throw exceptions for objects that cannot be deep-copied, e.g. Generators.
    https://github.com/python/cpython/blob/b134f47574c36e842253266ecf0d144fb6f3b546/Lib/dataclasses.py#L1332
    """  # noqa: E501
    if dataclasses.is_dataclass(obj):
        result = []
        for f in dataclasses.fields(obj):
            value = _asdict(getattr(obj, f.name))
            result.append((f.name, value))
        return dict(result)
    elif isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return type(obj)(*[_asdict(v) for v in obj])
    elif isinstance(obj, (list, tuple)):
        return type(obj)(_asdict(v) for v in obj)
    elif isinstance(obj, dict):
        return type(obj)((_asdict(k), _asdict(v)) for k, v in obj.items())
    else:
        if repr_str := _show_repr_str(obj):
            return repr_str
        try:
            return copy.deepcopy(obj)
        except BaseException:
            return repr(obj)


def _ensure_result_model_is_serializable(result: BaseModel) -> None:
    """
    Some LlamaIndex result types have a `raw` attribute containing the original
    result object, e.g., from the OpenAI Python SDK. OpenAI's Pydantic models
    are configured to defer instantiating model serializers, which can cause
    serialization of the LlamaIndex result object to fail. This method forces
    the OpenAI model to instantiate its serializer to avoid this issue.

    For reference, see:
    - https://github.com/Arize-ai/phoenix/issues/4423
    - https://github.com/openai/openai-python/issues/1306
    - https://github.com/pydantic/pydantic/issues/7713
    """
    if isinstance(raw := getattr(result, "raw", None), PydanticBaseModel):
        raw.model_rebuild()


T = TypeVar("T", bound=type)


def is_iterable_of(lst: Iterable[object], tp: T) -> bool:
    return isinstance(lst, Iterable) and all(isinstance(x, tp) for x in lst)


def is_base64_url(url: str) -> bool:
    return url.startswith("data:image/") and "base64" in url


AGENT = OpenInferenceSpanKindValues.AGENT.value
CHAIN = OpenInferenceSpanKindValues.CHAIN.value
EMBEDDING = OpenInferenceSpanKindValues.EMBEDDING.value
LLM = OpenInferenceSpanKindValues.LLM.value
RERANKER = OpenInferenceSpanKindValues.RERANKER.value
RETRIEVER = OpenInferenceSpanKindValues.RETRIEVER.value
TOOL = OpenInferenceSpanKindValues.TOOL.value