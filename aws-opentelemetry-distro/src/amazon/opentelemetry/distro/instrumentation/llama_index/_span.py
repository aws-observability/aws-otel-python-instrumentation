# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import inspect
import logging
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Tuple, Union

from llama_index.core.agent.workflow import AgentWorkflow, BaseWorkflowAgent
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.base.llms.base import BaseLLM
from llama_index.core.base.llms.types import ChatMessage, ChatResponse, CompletionResponse
from llama_index.core.instrumentation.events import BaseEvent
from llama_index.core.instrumentation.events.chat_engine import (
    StreamChatDeltaReceivedEvent,
    StreamChatEndEvent,
    StreamChatErrorEvent,
)
from llama_index.core.instrumentation.events.embedding import EmbeddingEndEvent, EmbeddingStartEvent
from llama_index.core.instrumentation.events.llm import (
    LLMChatEndEvent,
    LLMChatInProgressEvent,
    LLMChatStartEvent,
    LLMCompletionEndEvent,
    LLMCompletionInProgressEvent,
    LLMPredictStartEvent,
)
from llama_index.core.instrumentation.events.query import QueryStartEvent
from llama_index.core.instrumentation.events.rerank import ReRankStartEvent
from llama_index.core.instrumentation.events.retrieval import RetrievalStartEvent
from llama_index.core.instrumentation.events.synthesis import GetResponseStartEvent, SynthesizeStartEvent
from llama_index.core.instrumentation.span import BaseSpan
from llama_index.core.multi_modal_llms import MultiModalLLM
from llama_index.core.tools import FunctionTool  # type: ignore[attr-defined]
from llama_index.core.tools import BaseTool
from pydantic import PrivateAttr

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    GEN_AI_WORKFLOW_NAME,
    OPERATION_INVOKE_WORKFLOW,
    serialize_to_json_string,
    try_detach,
)
from opentelemetry import context as context_api
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_TYPE

# pylint: disable=unused-import
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (  # noqa: F401
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
    GenAiOperationNameValues,
)
from opentelemetry.trace import Span, Status, StatusCode, set_span_in_context
from opentelemetry.util.types import AttributeValue

logger = logging.getLogger(__name__)

# Custom operation names for LlamaIndex-specific operations (not in OTel semconv)
_OPERATION_RERANK = "rerank"
_OPERATION_RETRIEVE = "retrieve"
_OPERATION_SYNTHESIZE = "synthesize"
_OPERATION_QUERY = "query"


if not TYPE_CHECKING:
    from llama_index.core.instrumentation.events.exception import ExceptionEvent


# Provider detection: (module_path, class_name, provider_string)
# Order matters — azure must come before openai since AzureOpenAI is a subclass of OpenAI.
_PROVIDER_IMPORTS = (
    ("llama_index.llms.azure_openai", "AzureOpenAI", "azure.ai.openai"),
    ("llama_index.llms.openai", "OpenAI", "openai"),
    ("llama_index.llms.anthropic", "Anthropic", "anthropic"),
    ("llama_index.llms.vertex", "Vertex", "gcp.vertex_ai"),
    ("llama_index.llms.bedrock_converse", "BedrockConverse", "aws.bedrock"),
)

# Fallback: match class name keywords when provider packages are not installed.
# Order matters — "azure" must come before "openai" since AzureOpenAI contains both.
_PROVIDER_CLASS_NAME_KEYWORDS = (
    (("azure", "openai"), "azure.ai.openai"),
    (("openai",), "openai"),
    (("anthropic",), "anthropic"),
    (("vertex",), "gcp.vertex_ai"),
    (("gemini",), "gcp.gemini"),
    (("bedrock",), "aws.bedrock"),
)


def _detect_llm_provider(instance: Any) -> Optional[str]:
    """Detect LLM provider from instance type or class name.

    Tries isinstance checks via lazy imports first, then falls back to
    matching keywords in the class name.
    """
    for module_path, cls_name, provider in _PROVIDER_IMPORTS:
        try:
            import importlib  # pylint: disable=import-outside-toplevel

            module = importlib.import_module(module_path)
            if isinstance(instance, getattr(module, cls_name)):
                return provider
        except (ImportError, AttributeError):
            pass

    class_name = instance.__class__.__name__.lower()
    for keywords, provider in _PROVIDER_CLASS_NAME_KEYWORDS:
        if all(kw in class_name for kw in keywords):
            return provider

    return None


def _format_messages(messages: Iterable[ChatMessage]) -> tuple:
    """Convert LlamaIndex ChatMessages to OTel GenAI semconv format.

    Returns (system_instructions, conversation) where:
    - system_instructions: list of {"type": "text", "content": "..."} for system messages
    - conversation: list of {"role": "...", "parts": [{"type": "text", "content": "..."}]}

    See:
    - https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-input-messages.json
    - https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-output-messages.json
    - https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-system-instructions.json
    """
    role_map = {"system": "system", "user": "user", "assistant": "assistant", "tool": "tool"}
    system_instructions = []
    conversation = []
    for msg in messages:
        raw_role = getattr(msg, "role", "user")
        # LlamaIndex uses MessageRole enum; extract the value string
        role = role_map.get(str(raw_role).rsplit(".", 1)[-1].lower(), str(raw_role))
        content = str(getattr(msg, "content", ""))
        if role == "system":
            system_instructions.append({"type": "text", "content": content})
        else:
            conversation.append({"role": role, "parts": [{"type": "text", "content": content}]})
    return system_instructions, conversation


class _Span(BaseSpan):
    _otel_span: Span = PrivateAttr()
    _attributes: Dict[str, AttributeValue] = PrivateAttr()
    _parent: Optional["_Span"] = PrivateAttr()
    _context_token: Optional[object] = PrivateAttr()

    def __init__(
        self,
        otel_span: Span,
        parent: Optional["_Span"] = None,
        context_token: Optional[object] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._otel_span = otel_span
        self._parent = parent
        self._attributes = {}
        self._context_token = context_token

    @property
    def is_passthrough(self) -> bool:
        return False

    def __setitem__(self, key: str, value: AttributeValue) -> None:
        self._attributes[key] = value

    def record_exception(self, exception: BaseException) -> None:
        self._otel_span.record_exception(exception)

    def _get_span_name(self) -> str:
        operation_name = self._attributes.get(GEN_AI_OPERATION_NAME)

        # generic fallback if no operation name
        if not operation_name:
            return "llama_index.operation"

        if operation_name == GenAiOperationNameValues.INVOKE_AGENT.value:
            if agent_name := self._attributes.get(GEN_AI_AGENT_NAME):
                return f"{operation_name} {agent_name}"
        elif operation_name == OPERATION_INVOKE_WORKFLOW:
            if workflow_name := self._attributes.get(GEN_AI_WORKFLOW_NAME):
                return f"{operation_name} {workflow_name}"
        elif operation_name == GenAiOperationNameValues.EXECUTE_TOOL.value:
            if tool_name := self._attributes.get(GEN_AI_TOOL_NAME):
                return f"{operation_name} {tool_name}"
        elif model := self._attributes.get(GEN_AI_REQUEST_MODEL):
            return f"{operation_name} {model}"

        return operation_name

    def end(self, exception: Optional[BaseException] = None) -> None:
        if not self._otel_span.is_recording():
            return

        if self._context_token is not None:
            try_detach(self._context_token)  # type: ignore[arg-type]
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

        self._otel_span.update_name(self._get_span_name())

        self._otel_span.set_status(status=status)
        self._otel_span.set_attributes(self._attributes)
        self._otel_span.end()

    @property
    def active(self) -> bool:
        return self._otel_span.is_recording()

    @property
    def context(self) -> context_api.Context:
        """OTel context with this span as the active span.

        Used by child spans to establish parent-child relationships:
            otel_span = tracer.start_span(name, context=parent.context)

        _PassthroughSpan overrides this to delegate to its parent's context,
        so child spans transparently skip over suppressed spans in the
        OTel trace tree. The recursion terminates when it reaches a real
        _Span (returns its own context) or a parentless passthrough
        (returns the ambient context).
        """
        return set_span_in_context(self._otel_span)

    def process_input(self, instance: Any, bound_args: inspect.BoundArguments) -> None:
        from llama_index.core.llms.function_calling import FunctionCallingLLM  # pylint: disable=import-outside-toplevel

        if isinstance(instance, FunctionCallingLLM) and isinstance((tools := bound_args.kwargs.get("tools")), Iterable):
            tools_list = list(tools)
            if tools_list:
                # Convert FunctionTool objects to OpenAI tool format
                tool_defs = []
                for tool in tools_list:
                    try:
                        # Try to get the OpenAI tool format from metadata
                        if hasattr(tool, "metadata") and hasattr(tool.metadata, "to_openai_tool"):
                            tool_defs.append(tool.metadata.to_openai_tool())
                        else:
                            tool_defs.append(str(tool))
                    except Exception:  # pylint: disable=broad-exception-caught
                        tool_defs.append(str(tool))
                self[GEN_AI_TOOL_DEFINITIONS] = serialize_to_json_string(tool_defs)

        # Capture tool call arguments for FunctionTool invocations
        if isinstance(instance, (BaseTool, FunctionTool)):
            kwargs = bound_args.kwargs
            if kwargs:
                self[GEN_AI_TOOL_CALL_ARGUMENTS] = serialize_to_json_string(kwargs)

    @singledispatchmethod
    def process_instance(self, instance: Any) -> None: ...  # noqa: E704  # pylint: disable=no-self-use

    @process_instance.register(BaseLLM)
    @process_instance.register(MultiModalLLM)
    def _(self, instance: Union[BaseLLM, MultiModalLLM]) -> None:
        if metadata := instance.metadata:
            self[GEN_AI_REQUEST_MODEL] = metadata.model_name

        # Add LLM provider detection
        if provider := _detect_llm_provider(instance):
            self[GEN_AI_PROVIDER_NAME] = provider

        # Capture temperature if available
        if hasattr(instance, "temperature") and instance.temperature is not None:
            self[GEN_AI_REQUEST_TEMPERATURE] = instance.temperature

        # Capture max_tokens if available
        if hasattr(instance, "max_tokens") and instance.max_tokens is not None:
            self[GEN_AI_REQUEST_MAX_TOKENS] = instance.max_tokens

    @process_instance.register
    def _(self, instance: BaseEmbedding) -> None:
        if name := instance.model_name:
            self[GEN_AI_REQUEST_MODEL] = name

    @process_instance.register
    def _(self, instance: BaseTool) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.EXECUTE_TOOL.value
        metadata = instance.metadata
        self[GEN_AI_TOOL_DESCRIPTION] = metadata.description
        try:
            self[GEN_AI_TOOL_NAME] = metadata.get_name()
        except BaseException:
            pass

    def process_event(self, event: BaseEvent) -> None:
        self._process_event(event)

    @singledispatchmethod
    def _process_event(self, event: BaseEvent) -> None: ...  # noqa: E704  # pylint: disable=no-self-use

    @_process_event.register
    def _(self, event: ExceptionEvent) -> None: ...  # noqa: E704  # pylint: disable=no-self-use

    @_process_event.register
    def _(self, event: EmbeddingStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.EMBEDDINGS.value

    @_process_event.register
    def _(self, event: EmbeddingEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.EMBEDDINGS.value
        if event.embeddings and len(event.embeddings) > 0:
            first_embedding = event.embeddings[0]
            if hasattr(first_embedding, "__len__"):
                self[GEN_AI_EMBEDDINGS_DIMENSION_COUNT] = len(first_embedding)

    @_process_event.register
    def _(self, event: StreamChatDeltaReceivedEvent) -> None: ...  # noqa: E704  # pylint: disable=no-self-use

    @_process_event.register
    def _(self, event: StreamChatErrorEvent) -> None:
        self.record_exception(event.exception)

    @_process_event.register
    def _(self, event: StreamChatEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.CHAT.value

    @_process_event.register
    def _(self, event: LLMPredictStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.TEXT_COMPLETION.value

    @_process_event.register
    def _(self, event: LLMCompletionInProgressEvent) -> None: ...  # noqa: E704  # pylint: disable=no-self-use

    @_process_event.register
    def _(self, event: LLMCompletionEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.TEXT_COMPLETION.value
        self._extract_token_counts(event.response)

    @_process_event.register
    def _(self, event: LLMChatStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.CHAT.value
        system_instructions, conversation = _format_messages(event.messages)
        if system_instructions:
            self[GEN_AI_SYSTEM_INSTRUCTIONS] = serialize_to_json_string(system_instructions)
        if conversation:
            self[GEN_AI_INPUT_MESSAGES] = serialize_to_json_string(conversation)

    @_process_event.register
    def _(self, event: LLMChatInProgressEvent) -> None: ...  # noqa: E704  # pylint: disable=no-self-use

    @_process_event.register
    def _(self, event: LLMChatEndEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.CHAT.value
        if (response := event.response) is None:
            return
        self._extract_token_counts(response)
        _, output = _format_messages([response.message])
        if output:
            # Output messages require finish_reason per OTel semconv
            for msg in output:
                msg.setdefault("finish_reason", "stop")
            self[GEN_AI_OUTPUT_MESSAGES] = serialize_to_json_string(output)

    @_process_event.register
    def _(self, event: ReRankStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_RERANK
        self[GEN_AI_REQUEST_MODEL] = event.model_name

    @_process_event.register
    def _(self, event: QueryStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_QUERY

    @_process_event.register
    def _(self, event: RetrievalStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_RETRIEVE

    @_process_event.register
    def _(self, event: SynthesizeStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_SYNTHESIZE

    @_process_event.register
    def _(self, event: GetResponseStartEvent) -> None:
        self[GEN_AI_OPERATION_NAME] = _OPERATION_SYNTHESIZE

    def _extract_token_counts(self, response: Union[ChatResponse, CompletionResponse]) -> None:
        if raw := getattr(response, "raw", None):
            usage = raw.get("usage") if isinstance(raw, Mapping) else getattr(raw, "usage", None)
            if usage:
                for attr_key, attr_val in _get_token_counts(usage):
                    self[attr_key] = attr_val
            if (
                (model_extra := getattr(raw, "model_extra", None))
                and hasattr(model_extra, "get")
                and (x_groq := model_extra.get("x_groq"))
                and hasattr(x_groq, "get")
                and (usage := x_groq.get("usage"))
            ):
                for attr_key, attr_val in _get_token_counts(usage):
                    self[attr_key] = attr_val

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
                for attr_key, attr_val in _get_token_counts(usage_metadata):
                    self[attr_key] = attr_val
        # Look for token counts in additional_kwargs of the completion payload
        # This is needed for non-OpenAI models
        if additional_kwargs := getattr(response, "additional_kwargs", None):
            for attr_key, attr_val in _get_token_counts(additional_kwargs):
                self[attr_key] = attr_val

    @process_instance.register(FunctionTool)
    def _(self, instance: FunctionTool) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.EXECUTE_TOOL.value
        metadata = instance.metadata
        self[GEN_AI_TOOL_DESCRIPTION] = metadata.description
        try:
            self[GEN_AI_TOOL_NAME] = metadata.get_name()
        except BaseException:
            pass

    @process_instance.register(BaseWorkflowAgent)
    def _(self, instance: BaseWorkflowAgent) -> None:
        self[GEN_AI_OPERATION_NAME] = GenAiOperationNameValues.INVOKE_AGENT.value
        if hasattr(instance, "name") and instance.name:
            self[GEN_AI_AGENT_NAME] = instance.name
        if hasattr(instance, "description") and instance.description:
            self[GEN_AI_AGENT_DESCRIPTION] = instance.description
        if hasattr(instance, "system_prompt") and instance.system_prompt:
            self[GEN_AI_SYSTEM_INSTRUCTIONS] = instance.system_prompt

    @process_instance.register(AgentWorkflow)
    def _(self, instance: AgentWorkflow) -> None:
        self[GEN_AI_OPERATION_NAME] = OPERATION_INVOKE_WORKFLOW
        workflow_name = getattr(instance, "workflow_name", None)
        if workflow_name:
            self[GEN_AI_WORKFLOW_NAME] = workflow_name


class _PassthroughSpan(_Span):
    """A span that maintains the parent chain but produces no OTel span.

    Suppressed methods (e.g. _query, _retrieve) get a passthrough span so that:
    - Child spans can find their grandparent's OTel context via parent.context
    - The notify_parent chain remains intact for streaming propagation
    - No spurious "Open span is missing" warnings are logged on exit
    """

    def __init__(self, parent: Optional[_Span] = None, **kwargs: Any) -> None:
        from opentelemetry.trace import INVALID_SPAN  # pylint: disable=import-outside-toplevel

        super().__init__(otel_span=INVALID_SPAN, parent=parent, **kwargs)

    @property
    def is_passthrough(self) -> bool:
        return True

    @property
    def context(self) -> context_api.Context:
        # Delegate to parent's context so child spans skip over this
        # invisible span. Recurses up through any chain of passthrough
        # spans until it hits a real _Span (which returns its own OTel
        # context) or runs out of parents (falls back to ambient context).
        if self._parent:
            return self._parent.context
        return context_api.get_current()

    def end(self, exception: Optional[BaseException] = None) -> None:
        pass

    def __setitem__(self, key: str, value: AttributeValue) -> None:
        pass

    def record_exception(self, exception: BaseException) -> None:
        pass

    def process_input(self, instance: Any, bound_args: inspect.BoundArguments) -> None:
        pass

    def process_instance(self, instance: Any) -> None:
        pass

    def process_event(self, event: BaseEvent) -> None:
        pass


def _get_token_counts(usage: Union[object, Mapping[str, Any]]) -> Iterator[Tuple[str, Any]]:
    if isinstance(usage, Mapping):
        return _get_token_counts_from_mapping(usage)
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
