# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# flake8: noqa: E402
# pylint: disable=wrong-import-position
import asyncio
import importlib
import importlib.util
import inspect
import json
import sys
import unittest
from unittest.mock import Mock

from conftest import validate_otel_genai_schema

if sys.version_info < (3, 10):
    raise unittest.SkipTest("llama-index requires Python >= 3.10")

from llama_index.core.agent.workflow import AgentWorkflow, FunctionAgent
from llama_index.core.base.llms.types import ChatMessage, ChatResponse, CompletionResponse, LLMMetadata, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM
from llama_index.core.llms.llm import ToolSelection
from llama_index.core.tools import FunctionTool
from llama_index.core.tools.types import ToolOutput

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    GEN_AI_WORKFLOW_NAME,
    OPERATION_INVOKE_WORKFLOW,
)
from amazon.opentelemetry.distro.instrumentation.llama_index import LlamaIndexInstrumentor
from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_TYPE
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


def _has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


class TestLlamaIndexInstrumentor(unittest.TestCase):

    def setUp(self):
        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.instrumentor = LlamaIndexInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        self._Span = handler_module._Span
        self._SpanHandler = handler_module._SpanHandler
        self._EventHandler = handler_module.EventHandler
        self.tracer = trace.get_tracer(__name__, tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        self.span_exporter.clear()

    def test_llm_chat_start_event(self):
        from llama_index.core.instrumentation.events.llm import LLMChatStartEvent

        messages = [
            ChatMessage(role=MessageRole.USER, content="Hello"),
            ChatMessage(role=MessageRole.ASSISTANT, content="Hi"),
        ]
        event = LLMChatStartEvent(messages=messages, additional_kwargs={}, model_dict={})
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "chat")
        self.assertIn(GEN_AI_INPUT_MESSAGES, span._attributes)
        input_messages = json.loads(span._attributes[GEN_AI_INPUT_MESSAGES])
        self.assertEqual(len(input_messages), 2)
        otel_span.end()

    def test_llm_chat_end_event(self):
        from llama_index.core.instrumentation.events.llm import LLMChatEndEvent

        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="Response"),
            raw={"usage": {"prompt_tokens": 10, "completion_tokens": 8}},
        )
        event = LLMChatEndEvent(messages=[], response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "chat")
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 8)
        self.assertIn(GEN_AI_OUTPUT_MESSAGES, span._attributes)
        otel_span.end()

    def test_llm_completion_end_event(self):
        from llama_index.core.instrumentation.events.llm import LLMCompletionEndEvent

        response = CompletionResponse(
            text="Response text", raw={"usage": {"prompt_tokens": 15, "completion_tokens": 10}}
        )
        event = LLMCompletionEndEvent(prompt="Test", response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "text_completion")
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 15)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 10)
        otel_span.end()

    def test_llm_predict_start_event(self):
        from llama_index.core.instrumentation.events.llm import LLMPredictStartEvent
        from llama_index.core.prompts import PromptTemplate

        template = PromptTemplate("Test {var}")
        event = LLMPredictStartEvent(template=template, template_args={"var": "value"})
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "text_completion")
        otel_span.end()

    def test_stream_chat_end_event(self):
        from llama_index.core.instrumentation.events.chat_engine import StreamChatEndEvent

        response = ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="Response"))
        event = StreamChatEndEvent(messages=[], response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "chat")
        otel_span.end()

    def test_stream_chat_error_event(self):
        from llama_index.core.instrumentation.events.chat_engine import StreamChatErrorEvent

        exception = RuntimeError("Stream error")
        event = StreamChatErrorEvent(exception=exception)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        otel_span.end()

    def test_embedding_start_event(self):
        from llama_index.core.instrumentation.events.embedding import EmbeddingStartEvent

        event = EmbeddingStartEvent(model_dict={})
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "embeddings")
        otel_span.end()

    def test_embedding_end_event(self):
        from llama_index.core.instrumentation.events.embedding import EmbeddingEndEvent

        embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
        event = EmbeddingEndEvent(chunks=["text1", "text2"], embeddings=embeddings)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "embeddings")
        self.assertEqual(span._attributes[GEN_AI_EMBEDDINGS_DIMENSION_COUNT], 3)
        otel_span.end()

    def test_query_start_event(self):
        from llama_index.core import QueryBundle
        from llama_index.core.instrumentation.events.query import QueryStartEvent

        query = QueryBundle(query_str="Test query")
        event = QueryStartEvent(query=query)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "query")
        otel_span.end()

    def test_retrieval_start_event(self):
        from llama_index.core import QueryBundle
        from llama_index.core.instrumentation.events.retrieval import RetrievalStartEvent

        query = QueryBundle(query_str="Test query")
        event = RetrievalStartEvent(str_or_query_bundle=query)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "retrieve")
        otel_span.end()

    def test_rerank_start_event(self):
        from llama_index.core.instrumentation.events.rerank import ReRankStartEvent

        event = ReRankStartEvent(model_name="cohere-rerank-v3", query="Test", nodes=[], top_n=5)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "rerank")
        self.assertEqual(span._attributes[GEN_AI_REQUEST_MODEL], "cohere-rerank-v3")
        otel_span.end()

    def test_synthesize_start_event(self):
        from llama_index.core import QueryBundle
        from llama_index.core.instrumentation.events.synthesis import SynthesizeStartEvent

        query = QueryBundle(query_str="Test query")
        event = SynthesizeStartEvent(query=query)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "synthesize")
        otel_span.end()

    def test_get_response_start_event(self):
        from llama_index.core.instrumentation.events.synthesis import GetResponseStartEvent

        event = GetResponseStartEvent(query_str="Test query", text_chunks=[])
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "synthesize")
        otel_span.end()

    def test_llm_chat_in_progress_event(self):
        from llama_index.core.instrumentation.events.llm import LLMChatInProgressEvent

        response = ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="partial"))
        event = LLMChatInProgressEvent(response=response, messages=[])
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        otel_span.end()

    def test_llm_completion_in_progress_event(self):
        from llama_index.core.instrumentation.events.llm import LLMCompletionInProgressEvent

        response = CompletionResponse(text="partial")
        event = LLMCompletionInProgressEvent(response=response, prompt="test")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        otel_span.end()

    def test_stream_chat_delta_received_event(self):
        from llama_index.core.instrumentation.events.chat_engine import StreamChatDeltaReceivedEvent

        event = StreamChatDeltaReceivedEvent(delta="chunk")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        otel_span.end()

    def test_process_instance_openai_llm(self):
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-3.5-turbo", api_key="fake-key")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(llm)
        self.assertEqual(span._attributes[GEN_AI_REQUEST_MODEL], "gpt-3.5-turbo")
        self.assertEqual(span._attributes[GEN_AI_PROVIDER_NAME], "openai")
        otel_span.end()

    def test_process_instance_llm_with_temperature_and_max_tokens(self):
        """Test that temperature and max_tokens are captured from LLM instances."""
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake-key", temperature=0.7, max_tokens=100)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(llm)
        self.assertEqual(span._attributes[GEN_AI_REQUEST_TEMPERATURE], 0.7)
        self.assertEqual(span._attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
        otel_span.end()

    def test_process_instance_embedding(self):
        from llama_index.core.base.embeddings.base import BaseEmbedding

        embedding_model = Mock(spec=BaseEmbedding)
        embedding_model.model_name = "text-embedding-ada-002"
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(embedding_model)
        self.assertEqual(span._attributes[GEN_AI_REQUEST_MODEL], "text-embedding-ada-002")
        otel_span.end()

    def test_process_instance_function_tool(self):
        """Test FunctionTool registration sets execute_tool operation."""

        def calc(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        tool = FunctionTool.from_defaults(fn=calc)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(tool)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "execute_tool")
        self.assertEqual(span._attributes[GEN_AI_TOOL_NAME], "calc")
        self.assertIn("Add two numbers", span._attributes[GEN_AI_TOOL_DESCRIPTION])
        otel_span.end()

    def test_process_instance_base_agent(self):
        """Test BaseAgent registration sets invoke_agent operation."""
        from llama_index.core.agent.workflow import FunctionAgent
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-3.5-turbo", api_key="fake-key")
        agent = FunctionAgent(
            tools=[],
            llm=llm,
            name="TestAgent",
            description="A test agent",
            system_prompt="You are helpful.",
        )
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(agent)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "invoke_agent")
        self.assertEqual(span._attributes[GEN_AI_AGENT_NAME], "TestAgent")
        self.assertEqual(span._attributes[GEN_AI_AGENT_DESCRIPTION], "A test agent")
        self.assertEqual(span._attributes[GEN_AI_SYSTEM_INSTRUCTIONS], "You are helpful.")
        otel_span.end()

    def test_process_instance_base_agent_no_name(self):
        """Test BaseAgent without a name uses class name."""
        from llama_index.core.agent.workflow import FunctionAgent
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-3.5-turbo", api_key="fake-key")
        agent = FunctionAgent(tools=[], llm=llm)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(agent)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "invoke_agent")
        otel_span.end()

    def test_process_instance_default_handler_none(self):
        """Test default process_instance handler with None does nothing."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(None)
        self.assertNotIn(GEN_AI_OPERATION_NAME, span._attributes)
        otel_span.end()

    def test_process_instance_default_handler_unknown_type(self):
        """Test default process_instance handler with unknown type does nothing."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance("some_string")
        self.assertNotIn(GEN_AI_OPERATION_NAME, span._attributes)
        otel_span.end()

    def test_process_input_with_tools(self):
        from llama_index.llms.openai import OpenAI

        def get_weather(location: str) -> str:
            return f"Weather in {location}"

        weather_tool = FunctionTool.from_defaults(fn=get_weather)
        llm = OpenAI(model="gpt-3.5-turbo", api_key="fake-key")
        bound_args = Mock()
        bound_args.kwargs = {"tools": [weather_tool]}
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_input(llm, bound_args)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, span._attributes)
        tool_defs = json.loads(span._attributes[GEN_AI_TOOL_DEFINITIONS])
        self.assertIsInstance(tool_defs, list)
        self.assertEqual(len(tool_defs), 1)
        self.assertEqual(tool_defs[0]["function"]["name"], "get_weather")
        otel_span.end()

    def test_process_input_tool_call_arguments(self):
        """Test that tool call arguments are captured for BaseTool/FunctionTool."""

        def my_tool(x: int) -> int:
            """A tool."""
            return x

        tool = FunctionTool.from_defaults(fn=my_tool)
        bound_args = Mock()
        bound_args.kwargs = {"x": 42}
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_input(tool, bound_args)
        self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, span._attributes)
        args = json.loads(span._attributes[GEN_AI_TOOL_CALL_ARGUMENTS])
        self.assertEqual(args["x"], 42)
        otel_span.end()

    def test_span_get_span_name_no_operation(self):
        """Test span name fallback when no operation name is set."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        self.assertEqual(span._get_span_name(), "llama_index.operation")
        otel_span.end()

    def test_span_get_span_name_agent(self):
        """Test span name for invoke_agent with agent name."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._attributes[GEN_AI_OPERATION_NAME] = "invoke_agent"
        span._attributes[GEN_AI_AGENT_NAME] = "MyAgent"
        self.assertEqual(span._get_span_name(), "invoke_agent MyAgent")
        otel_span.end()

    def test_span_get_span_name_tool(self):
        """Test span name for execute_tool with tool name."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._attributes[GEN_AI_OPERATION_NAME] = "execute_tool"
        span._attributes[GEN_AI_TOOL_NAME] = "calculator"
        self.assertEqual(span._get_span_name(), "execute_tool calculator")
        otel_span.end()

    def test_span_get_span_name_chat_with_model(self):
        """Test span name for chat with model name."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._attributes[GEN_AI_OPERATION_NAME] = "chat"
        span._attributes[GEN_AI_REQUEST_MODEL] = "gpt-4"
        self.assertEqual(span._get_span_name(), "chat gpt-4")
        otel_span.end()

    def test_span_get_span_name_operation_only(self):
        """Test span name when operation name is set but no model/agent/tool."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._attributes[GEN_AI_OPERATION_NAME] = "embeddings"
        self.assertEqual(span._get_span_name(), "embeddings")
        otel_span.end()

    def test_span_record_exception(self):
        """Test that record_exception delegates to the otel span."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        exc = ValueError("test error")
        span.record_exception(exc)
        otel_span.end()

    def test_span_end_with_context_token(self):
        """Test that end() detaches the context token."""
        otel_span = self.tracer.start_span("test")
        token = context_api.attach(context_api.Context())
        span = self._Span(otel_span=otel_span, context_token=token)
        self.assertIsNotNone(span._context_token)
        span.end()
        self.assertIsNone(span._context_token)
        self.assertFalse(span.active)

    def test_span_end_idempotent(self):
        """Test that calling end() twice is safe."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.end()
        span.end()  # should not raise
        self.assertFalse(span.active)

    def test_span_properties(self):
        """Test active and context properties."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        self.assertTrue(span.active)
        ctx = span.context
        self.assertIsNotNone(ctx)
        span.end()
        self.assertFalse(span.active)

    def test_error_handling(self):
        exception = ValueError("API error")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.end(exception=exception)
        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        error_span = spans[0]
        self.assertIn(ERROR_TYPE, error_span.attributes)
        self.assertEqual(error_span.attributes[ERROR_TYPE], "ValueError")

    def test_token_count_extraction_openai_format(self):
        from llama_index.core.instrumentation.events.llm import LLMCompletionEndEvent

        response = CompletionResponse(text="Test response")
        response.raw = {"usage": {"prompt_tokens": 10, "completion_tokens": 20}}
        event = LLMCompletionEndEvent(prompt="Test", response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)
        otel_span.end()

    def test_token_count_extraction_anthropic_format(self):
        from llama_index.core.instrumentation.events.llm import LLMCompletionEndEvent

        response = CompletionResponse(text="Test response")
        response.raw = {"usage": {"input_tokens": 15, "output_tokens": 25}}
        event = LLMCompletionEndEvent(prompt="Test", response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 15)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 25)
        otel_span.end()

    def test_token_count_extraction_vertex_format(self):
        from llama_index.core.instrumentation.events.llm import LLMChatEndEvent

        mock_raw = Mock()
        mock_raw.usage = None
        mock_raw.usage_metadata = Mock()
        mock_raw.usage_metadata.prompt_token_count = 30
        mock_raw.usage_metadata.candidates_token_count = 20
        response = ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"), raw=mock_raw)
        event = LLMChatEndEvent(messages=[], response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 30)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)
        otel_span.end()

    def test_token_count_extraction_vertex_raw_response_mapping(self):
        """Test VertexAI token extraction via _raw_response in a Mapping raw."""
        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"),
            raw={
                "_raw_response": {
                    "usage_metadata": {
                        "prompt_token_count": 40,
                        "candidates_token_count": 30,
                    }
                }
            },
        )
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._extract_token_counts(response)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 40)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 30)
        otel_span.end()

    def test_token_count_extraction_vertex_raw_response_object(self):
        """Test VertexAI token extraction via _raw_response as an object attribute."""
        raw_response = Mock()
        raw_response.usage_metadata = Mock()
        raw_response.usage_metadata.prompt_token_count = 50
        raw_response.usage_metadata.candidates_token_count = 35
        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"), raw={"_raw_response": raw_response}
        )
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._extract_token_counts(response)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 50)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 35)
        otel_span.end()

    def test_token_count_extraction_groq_format(self):
        """Test Groq x_groq usage extraction."""
        mock_raw = Mock()
        mock_raw.usage = None
        mock_raw.usage_metadata = None
        mock_raw.model_extra = {"x_groq": {"usage": {"prompt_tokens": 12, "completion_tokens": 8}}}
        response = ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"), raw=mock_raw)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._extract_token_counts(response)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 12)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 8)
        otel_span.end()

    def test_token_count_extraction_additional_kwargs(self):
        """Test token extraction from additional_kwargs."""
        response = CompletionResponse(text="Test")
        response.raw = None
        response.additional_kwargs = {"prompt_tokens": 5, "completion_tokens": 3}
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._extract_token_counts(response)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 5)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 3)
        otel_span.end()

    @unittest.skipUnless(_has_module("llama_index.llms.openai"), "llama-index-llms-openai not installed")
    def test_detect_provider_openai(self):
        """Test OpenAI provider detection via isinstance."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        self.assertEqual(handler._detect_llm_provider(llm), "openai")

    @unittest.skipUnless(_has_module("llama_index.llms.anthropic"), "llama-index-llms-anthropic not installed")
    def test_detect_provider_anthropic(self):
        """Test Anthropic provider detection via isinstance."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        from llama_index.llms.anthropic import Anthropic

        llm = Anthropic(model="claude-3-haiku-20240307", api_key="fake")
        self.assertEqual(handler._detect_llm_provider(llm), "anthropic")

    @unittest.skipUnless(_has_module("llama_index.llms.azure_openai"), "llama-index-llms-azure-openai not installed")
    def test_detect_provider_azure_openai(self):
        """Test Azure OpenAI provider detection via isinstance."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        from llama_index.llms.azure_openai import AzureOpenAI

        llm = AzureOpenAI(
            model="gpt-4",
            engine="gpt-4",
            api_key="fake",
            azure_endpoint="https://fake.openai.azure.com",
            api_version="2024-02-01",
        )
        self.assertEqual(handler._detect_llm_provider(llm), "azure.ai.openai")

    @unittest.skipUnless(_has_module("llama_index.llms.vertex"), "llama-index-llms-vertex not installed")
    def test_detect_provider_vertex(self):
        """Test Vertex provider detection via isinstance."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        from llama_index.llms.vertex import Vertex

        llm = Vertex.__new__(Vertex)  # Skip __init__ to avoid credential setup
        self.assertEqual(handler._detect_llm_provider(llm), "gcp.vertex_ai")

    @unittest.skipUnless(
        _has_module("llama_index.llms.bedrock_converse"), "llama-index-llms-bedrock-converse not installed"
    )
    def test_detect_provider_bedrock(self):
        """Test Bedrock provider detection via isinstance."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        from llama_index.llms.bedrock_converse import BedrockConverse

        llm = BedrockConverse(model="anthropic.claude-3-haiku-20240307-v1:0", region_name="us-east-1")
        self.assertEqual(handler._detect_llm_provider(llm), "aws.bedrock")

    def test_detect_provider_class_name_fallback_anthropic(self):
        """Test class-name fallback for Anthropic."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("AnthropicLLM", (), {})
        self.assertEqual(handler._detect_llm_provider(mock_llm), "anthropic")

    def test_detect_provider_class_name_fallback_azure(self):
        """Test class-name fallback for Azure OpenAI."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("AzureOpenAI", (), {})
        self.assertEqual(handler._detect_llm_provider(mock_llm), "azure.ai.openai")

    def test_detect_provider_class_name_fallback_vertex(self):
        """Test class-name fallback for Vertex."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("VertexLLM", (), {})
        self.assertEqual(handler._detect_llm_provider(mock_llm), "gcp.vertex_ai")

    def test_detect_provider_class_name_fallback_gemini(self):
        """Test class-name fallback for Gemini."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("GeminiModel", (), {})
        self.assertEqual(handler._detect_llm_provider(mock_llm), "gcp.gemini")

    def test_detect_provider_class_name_fallback_openai(self):
        """Test class-name fallback for OpenAI (non-Azure)."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("CustomOpenAIWrapper", (), {})
        self.assertEqual(handler._detect_llm_provider(mock_llm), "openai")

    def test_detect_provider_class_name_fallback_bedrock(self):
        """Test class-name fallback for Bedrock."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("BedrockCustom", (), {})
        self.assertEqual(handler._detect_llm_provider(mock_llm), "aws.bedrock")

    def test_detect_provider_unknown(self):
        """Test that unknown providers return None."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        mock_llm = Mock()
        mock_llm.__class__ = type("CustomLLM", (), {})
        self.assertIsNone(handler._detect_llm_provider(mock_llm))

    def _make_span_handler(self):
        tracer = trace.get_tracer("test", tracer_provider=self.tracer_provider)
        return self._SpanHandler(tracer=tracer)

    def _make_bound_args(self, **kwargs):
        bound = Mock(spec=inspect.BoundArguments)
        bound.kwargs = kwargs
        return bound

    def test_new_span_returns_none_when_suppressed(self):
        """Test that new_span returns None when instrumentation is suppressed."""
        handler = self._make_span_handler()
        token = context_api.attach(context_api.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            result = handler.new_span(id_="test-1", bound_args=self._make_bound_args(), instance=Mock())
            self.assertIsNone(result)
        finally:
            context_api.detach(token)

    def test_new_span_returns_passthrough_for_none_instance(self):
        """Test that new_span returns a passthrough span when instance is None."""
        handler = self._make_span_handler()
        result = handler.new_span(id_="test-1", bound_args=self._make_bound_args(), instance=None)
        self.assertIsNotNone(result)
        self.assertTrue(result.is_passthrough)

    def test_new_span_suppresses_utility_classes(self):
        """Test that utility classes like TokenTextSplitter are suppressed."""
        handler = self._make_span_handler()
        for cls_name in ("TokenTextSplitter", "DefaultRefineProgram", "SentenceSplitter", "CompactAndRefine"):
            cls = type(cls_name, (), {})
            instance = cls()
            result = handler.new_span(
                id_=f"{cls_name}.do_thing-1", bound_args=self._make_bound_args(), instance=instance
            )
            self.assertIsNotNone(result, f"{cls_name} should return passthrough span")
            self.assertTrue(result.is_passthrough, f"{cls_name} should be a passthrough span")

    def test_new_span_suppresses_internal_methods(self):
        """Test that internal workflow methods are suppressed."""
        handler = self._make_span_handler()
        suppressed_methods = [
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
        ]
        for method in suppressed_methods:
            instance = Mock()
            instance.__class__ = type("SomeClass", (), {})
            result = handler.new_span(
                id_=f"SomeClass.{method}-1",
                bound_args=self._make_bound_args(),
                instance=instance,
            )
            self.assertIsNotNone(result, f"Method {method} should return passthrough span")
            self.assertTrue(result.is_passthrough, f"Method {method} should be a passthrough span")

    def test_passthrough_span_context_delegates_to_parent(self):
        """Test that passthrough span's context property returns parent's context."""
        span_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._span")
        parent_otel = self.tracer.start_span("parent")
        parent_span = self._Span(otel_span=parent_otel, id_="parent-1")
        child_span = span_module._PassthroughSpan(parent=parent_span, id_="child-1")
        self.assertTrue(child_span.is_passthrough)
        self.assertEqual(child_span.context, parent_span.context)

    def test_passthrough_span_end_is_noop(self):
        """Test that passthrough span's end() does nothing."""
        span_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._span")
        span = span_module._PassthroughSpan(id_="test-1")
        self.assertTrue(span.is_passthrough)
        span.end()
        span.end(exception=RuntimeError("test"))

    def test_passthrough_span_setitem_is_noop(self):
        """Test that passthrough span ignores attribute writes."""
        span_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._span")
        span = span_module._PassthroughSpan(id_="test-1")
        self.assertTrue(span.is_passthrough)
        span["key"] = "value"
        span.record_exception(RuntimeError("test"))

    def test_passthrough_span_process_methods_are_noop(self):
        """Test that passthrough span's process methods do nothing."""
        span_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._span")
        span = span_module._PassthroughSpan(id_="test-1")
        self.assertTrue(span.is_passthrough)
        span.process_input(Mock(), self._make_bound_args())
        span.process_instance(Mock())
        span.process_event(Mock())

    def test_passthrough_span_prepare_to_exit_returns_span(self):
        """Test that prepare_to_exit_span handles passthrough spans without warnings."""
        handler = self._make_span_handler()
        instance = Mock()
        instance.__class__ = type("CompactAndRefine", (), {})
        span = handler.new_span(
            id_="CompactAndRefine.get_response-1", bound_args=self._make_bound_args(), instance=instance
        )
        handler.open_spans["CompactAndRefine.get_response-1"] = span
        result = handler.prepare_to_exit_span(
            id_="CompactAndRefine.get_response-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIs(result, span)

    def test_passthrough_span_prepare_to_drop_returns_span(self):
        """Test that prepare_to_drop_span handles passthrough spans without warnings."""
        handler = self._make_span_handler()
        instance = Mock()
        instance.__class__ = type("SentenceSplitter", (), {})
        span = handler.new_span(id_="SentenceSplitter.split-1", bound_args=self._make_bound_args(), instance=instance)
        handler.open_spans["SentenceSplitter.split-1"] = span
        result = handler.prepare_to_drop_span(
            id_="SentenceSplitter.split-1",
            bound_args=self._make_bound_args(),
            instance=instance,
            err=RuntimeError("test"),
        )
        self.assertIs(result, span)

    def test_passthrough_span_context_without_parent(self):
        """Test that passthrough span without parent returns current context."""
        span_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._span")
        span = span_module._PassthroughSpan(id_="orphan-1")
        self.assertTrue(span.is_passthrough)
        # Should return current context without error
        ctx = span.context
        self.assertIsNotNone(ctx)

    def test_prepare_to_exit_span_missing_span_logs_warning(self):
        """Test that prepare_to_exit_span logs warning for missing span id."""
        handler = self._make_span_handler()
        result = handler.prepare_to_exit_span(id_="nonexistent-1", bound_args=self._make_bound_args(), instance=Mock())
        self.assertIsNone(result)

    def test_prepare_to_drop_span_missing_span_logs_warning(self):
        """Test that prepare_to_drop_span logs warning for missing span id."""
        handler = self._make_span_handler()
        result = handler.prepare_to_drop_span(
            id_="nonexistent-1", bound_args=self._make_bound_args(), instance=Mock(), err=RuntimeError("test")
        )
        self.assertIsNone(result)

    def test_span_name_fallback_without_operation_name(self):
        """Test that _get_span_name returns fallback when no operation name is set."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span, id_="test-1")
        self.assertEqual(span._get_span_name(), "llama_index.operation")

    def test_new_span_creates_span_for_valid_instance(self):
        """Test that new_span creates a span for a valid instance."""
        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        bound_args = self._make_bound_args()
        span = handler.new_span(id_="OpenAI.chat-1", bound_args=bound_args, instance=llm)
        self.assertIsNotNone(span)
        self.assertTrue(span.active)
        self.assertIsNotNone(span._context_token)
        span.end()

    def test_new_span_with_parent(self):
        """Test that new_span correctly links to a parent span."""
        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        bound_args = self._make_bound_args()
        parent_span = handler.new_span(id_="parent-1", bound_args=bound_args, instance=llm)
        self.assertIsNotNone(parent_span)
        handler.open_spans["parent-1"] = parent_span
        child_instance = Mock()
        child_instance.__class__ = type("ChildClass", (), {})
        child_span = handler.new_span(
            id_="ChildClass.do_thing-2",
            bound_args=bound_args,
            instance=child_instance,
            parent_span_id="parent-1",
        )
        self.assertIsNotNone(child_span)
        self.assertEqual(child_span._parent, parent_span)
        child_span.end()
        parent_span.end()

    def test_new_span_separate_trace_from_runtime_context(self):
        """Test that separate_trace_from_runtime_context creates isolated traces."""
        tracer = trace.get_tracer("test", tracer_provider=self.tracer_provider)
        handler = self._SpanHandler(tracer=tracer, separate_trace_from_runtime_context=True)
        instance = Mock()
        instance.__class__ = type("SomeClass", (), {})
        span = handler.new_span(id_="SomeClass.method-1", bound_args=self._make_bound_args(), instance=instance)
        self.assertIsNotNone(span)
        span.end()

    def test_prepare_to_exit_span_ends_span(self):
        """Test that prepare_to_exit_span ends the span."""
        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        span = handler.new_span(id_="OpenAI.chat-1", bound_args=self._make_bound_args(), instance=llm)
        self.assertIsNotNone(span)
        handler.open_spans["OpenAI.chat-1"] = span
        result = handler.prepare_to_exit_span(
            id_="OpenAI.chat-1", bound_args=self._make_bound_args(), instance=llm, result="done"
        )
        self.assertIsNotNone(result)
        self.assertFalse(span.active)

    def test_prepare_to_exit_span_returns_none_when_suppressed(self):
        """Test that prepare_to_exit_span returns None when suppressed."""
        handler = self._make_span_handler()
        token = context_api.attach(context_api.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            result = handler.prepare_to_exit_span(id_="test-1", bound_args=self._make_bound_args())
            self.assertIsNone(result)
        finally:
            context_api.detach(token)

    def test_prepare_to_exit_span_missing_span(self):
        """Test that prepare_to_exit_span handles missing spans gracefully."""
        handler = self._make_span_handler()
        result = handler.prepare_to_exit_span(id_="nonexistent-1", bound_args=self._make_bound_args())
        self.assertIsNone(result)

    def test_prepare_to_exit_span_with_tool_output(self):
        """Test that ToolOutput result content is captured."""
        handler = self._make_span_handler()

        def my_tool(x: int) -> int:
            """A tool."""
            return x

        tool = FunctionTool.from_defaults(fn=my_tool)
        span = handler.new_span(id_="FunctionTool.call-1", bound_args=self._make_bound_args(), instance=tool)
        self.assertIsNotNone(span)
        handler.open_spans["FunctionTool.call-1"] = span
        tool_output = ToolOutput(content="42", tool_name="my_tool", raw_input={}, raw_output="42")
        handler.prepare_to_exit_span(
            id_="FunctionTool.call-1", bound_args=self._make_bound_args(), instance=tool, result=tool_output
        )
        self.assertEqual(span._attributes.get(GEN_AI_TOOL_CALL_RESULT), "42")

    def test_prepare_to_drop_span_returns_none_when_suppressed(self):
        """Test that prepare_to_drop_span returns None when suppressed."""
        handler = self._make_span_handler()
        token = context_api.attach(context_api.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            result = handler.prepare_to_drop_span(id_="test-1", bound_args=self._make_bound_args())
            self.assertIsNone(result)
        finally:
            context_api.detach(token)

    def test_prepare_to_drop_span_with_error(self):
        """Test that prepare_to_drop_span ends span with error."""
        handler = self._make_span_handler()
        instance = Mock()
        instance.__class__ = type("SomeClass", (), {})
        span = handler.new_span(id_="SomeClass.method-1", bound_args=self._make_bound_args(), instance=instance)
        self.assertIsNotNone(span)
        handler.open_spans["SomeClass.method-1"] = span
        err = RuntimeError("something failed")
        handler.prepare_to_drop_span(
            id_="SomeClass.method-1", bound_args=self._make_bound_args(), instance=instance, err=err
        )
        self.assertFalse(span.active)

    def test_prepare_to_drop_span_missing_span(self):
        """Test that prepare_to_drop_span handles missing spans gracefully."""
        handler = self._make_span_handler()
        result = handler.prepare_to_drop_span(id_="nonexistent-1", bound_args=self._make_bound_args())
        self.assertIsNone(result)

    def test_event_handler_returns_none_when_suppressed(self):
        """Test that EventHandler returns None when instrumentation is suppressed."""
        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)
        token = context_api.attach(context_api.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            event = Mock(spec=["span_id", "id_"])
            event.span_id = "test-1"
            result = event_handler.handle(event)
            self.assertIsNone(result)
        finally:
            context_api.detach(token)

    def test_event_handler_returns_event_when_no_span_id(self):
        """Test that EventHandler returns event when span_id is empty."""
        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)
        event = Mock()
        event.span_id = None
        result = event_handler.handle(event)
        self.assertEqual(result, event)

    def test_event_handler_dispatches_to_span(self):
        """Test that EventHandler dispatches events to the correct span."""
        from llama_index.core.instrumentation.events.embedding import EmbeddingStartEvent

        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)

        from llama_index.core.base.embeddings.base import BaseEmbedding

        instance = Mock(spec=BaseEmbedding)
        instance.model_name = "test-model"
        span = span_handler.new_span(
            id_="BaseEmbedding.get_query_embedding-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIsNotNone(span)
        self.assertFalse(span.is_passthrough)
        span_handler.open_spans["BaseEmbedding.get_query_embedding-1"] = span

        event = EmbeddingStartEvent(model_dict={})
        event.span_id = "BaseEmbedding.get_query_embedding-1"
        result = event_handler.handle(event)
        self.assertEqual(result, event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "embeddings")
        span.end()

    def test_event_handler_missing_span_logs_warning(self):
        """Test that EventHandler logs warning for missing span."""
        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)
        event = Mock()
        event.span_id = "nonexistent-1"
        event.id_ = "event-1"
        result = event_handler.handle(event)
        self.assertEqual(result, event)

    def test_instrumentation_lifecycle(self):
        self.assertTrue(self.instrumentor.is_instrumented_by_opentelemetry)
        self.instrumentor.uninstrument()
        self.assertFalse(self.instrumentor.is_instrumented_by_opentelemetry)
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.assertTrue(self.instrumentor.is_instrumented_by_opentelemetry)

    def test_uninstrument_removes_handlers(self):
        """Test that uninstrument removes span and event handlers from dispatcher."""
        from llama_index.core.instrumentation import get_dispatcher

        dispatcher = get_dispatcher()
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")

        # Verify handlers are present
        span_handler_types = [type(h) for h in dispatcher.span_handlers]
        self.assertIn(handler_module._SpanHandler, span_handler_types)

        self.instrumentor.uninstrument()

        span_handler_types = [type(h) for h in dispatcher.span_handlers]
        self.assertNotIn(handler_module._SpanHandler, span_handler_types)

    def test_instrument_idempotent(self):
        """Test that calling _instrument twice doesn't add duplicate handlers."""
        from llama_index.core.instrumentation import get_dispatcher

        dispatcher = get_dispatcher()
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")

        count_before = sum(1 for h in dispatcher.span_handlers if isinstance(h, handler_module._SpanHandler))
        # Call _instrument directly to bypass BaseInstrumentor's guard
        self.instrumentor._instrument(tracer_provider=self.tracer_provider)
        count_after = sum(1 for h in dispatcher.span_handlers if isinstance(h, handler_module._SpanHandler))
        self.assertEqual(count_before, count_after)

    def test_uninstrument_cleans_up(self):
        """Test that _uninstrument removes handlers and sets event_handler to None."""
        self.instrumentor.uninstrument()
        self.assertIsNone(self.instrumentor._event_handler)
        # Re-instrument for tearDown
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def test_uninstrument_noop_when_already_uninstrumented(self):
        """Test that _uninstrument is safe to call when already uninstrumented."""
        self.instrumentor.uninstrument()
        # Calling again should not raise
        self.instrumentor._uninstrument()
        # Re-instrument for tearDown
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def test_span_get_span_name_agent_no_name(self):
        """Test span name for invoke_agent without agent name falls back to operation."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._attributes[GEN_AI_OPERATION_NAME] = "invoke_agent"
        self.assertEqual(span._get_span_name(), "invoke_agent")
        otel_span.end()

    def test_span_get_span_name_tool_no_name(self):
        """Test span name for execute_tool without tool name falls back to operation."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._attributes[GEN_AI_OPERATION_NAME] = "execute_tool"
        self.assertEqual(span._get_span_name(), "execute_tool")
        otel_span.end()

    def test_event_handler_exception_in_process_event(self):
        """Test that EventHandler catches exceptions from process_event."""
        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)

        from llama_index.core.base.embeddings.base import BaseEmbedding

        instance = Mock(spec=BaseEmbedding)
        instance.model_name = "test-model"
        span = span_handler.new_span(
            id_="BaseEmbedding.get_query_embedding-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIsNotNone(span)
        self.assertFalse(span.is_passthrough)
        span_handler.open_spans["BaseEmbedding.get_query_embedding-1"] = span

        # Create an event that will cause process_event to raise via a bad attribute access
        event = Mock()
        event.span_id = "BaseEmbedding.get_query_embedding-1"
        event.id_ = "event-1"
        # Monkey-patch the span's process_event at the object level using object.__setattr__
        original = span.process_event
        object.__setattr__(span, "process_event", Mock(side_effect=RuntimeError("boom")))
        result = event_handler.handle(event)
        self.assertEqual(result, event)
        object.__setattr__(span, "process_event", original)
        span.end()

    def test_process_instance_default_handler_does_nothing_for_non_agent(self):
        """Test that default process_instance does nothing for non-agent, non-None types."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        # Use a real instance of a custom class (not a Mock)
        SomeClass = type("SomeClass", (), {})
        span.process_instance(SomeClass())
        self.assertNotIn(GEN_AI_OPERATION_NAME, span._attributes)
        otel_span.end()

    def test_process_instance_custom_base_tool(self):
        """Test BaseTool handler with a custom BaseTool subclass (not FunctionTool)."""
        from llama_index.core.tools import BaseTool
        from llama_index.core.tools.types import ToolMetadata

        class CustomTool(BaseTool):
            @property
            def metadata(self):
                return ToolMetadata(name="custom_tool", description="A custom tool")

            def __call__(self, *args, **kwargs):
                return "result"

        tool = CustomTool()
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(tool)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "execute_tool")
        self.assertEqual(span._attributes[GEN_AI_TOOL_NAME], "custom_tool")
        self.assertEqual(span._attributes[GEN_AI_TOOL_DESCRIPTION], "A custom tool")
        otel_span.end()

    def test_unhandled_event_type(self):
        """Test that unhandled event types trigger a warning log."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        # Create a custom event type that isn't registered
        from llama_index.core.instrumentation.events import BaseEvent

        class CustomEvent(BaseEvent):
            pass

        event = CustomEvent()
        # Should not raise, just log a warning
        span._process_event(event)
        otel_span.end()

    def test_process_input_tool_without_to_openai_tool(self):
        """Test process_input falls back to str() when tool has no to_openai_tool."""
        from llama_index.llms.openai import OpenAI

        tool = Mock()
        tool.metadata = Mock()
        del tool.metadata.to_openai_tool  # Ensure to_openai_tool doesn't exist
        bound_args = Mock()
        bound_args.kwargs = {"tools": [tool]}
        llm = OpenAI(model="gpt-4", api_key="fake")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_input(llm, bound_args)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, span._attributes)
        otel_span.end()

    def test_context_token_detach_exception_handled(self):
        """Test that context token detach exceptions are silently caught."""
        otel_span = self.tracer.start_span("test")
        # Use an invalid token that will cause detach to fail
        span = self._Span(otel_span=otel_span, context_token="invalid_token")
        # Should not raise
        span.end()
        self.assertIsNone(span._context_token)
        self.assertFalse(span.active)

    def test_function_tool_get_name_exception(self):
        """Test FunctionTool handler when get_name() raises."""
        tool = Mock(spec=FunctionTool)
        metadata = Mock()
        metadata.description = "A tool"
        metadata.get_name = Mock(side_effect=RuntimeError("no name"))
        tool.metadata = metadata
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        # Call the FunctionTool-specific handler directly
        importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        # Use the registered handler for FunctionTool
        span.process_instance(tool)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "execute_tool")
        self.assertNotIn(GEN_AI_TOOL_NAME, span._attributes)
        otel_span.end()

    def test_process_instance_base_tool_get_name_exception(self):
        """Test BaseTool handler when get_name() raises."""
        from llama_index.core.tools import BaseTool
        from llama_index.core.tools.types import ToolMetadata

        class BrokenTool(BaseTool):
            @property
            def metadata(self):
                m = ToolMetadata(name="broken", description="A broken tool")
                # Override get_name to raise
                m.get_name = Mock(side_effect=RuntimeError("no name"))
                return m

            def __call__(self, *args, **kwargs):
                return "result"

        tool = BrokenTool()
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(tool)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "execute_tool")
        self.assertEqual(span._attributes[GEN_AI_TOOL_DESCRIPTION], "A broken tool")
        self.assertNotIn(GEN_AI_TOOL_NAME, span._attributes)
        otel_span.end()

    def test_process_input_tool_exception_fallback(self):
        """Test process_input falls back to str() when tool.metadata.to_openai_tool raises."""
        from llama_index.llms.openai import OpenAI

        tool = Mock()
        tool.metadata = Mock()
        tool.metadata.to_openai_tool = Mock(side_effect=RuntimeError("fail"))
        bound_args = Mock()
        bound_args.kwargs = {"tools": [tool]}
        llm = OpenAI(model="gpt-4", api_key="fake")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_input(llm, bound_args)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, span._attributes)
        otel_span.end()

    # ---- OTel GenAI Schema Validation Tests ----

    def test_input_messages_conform_to_otel_schema(self):
        """Validate gen_ai.input.messages against OTel GenAI JSON Schema."""
        from llama_index.core.instrumentation.events.llm import LLMChatStartEvent

        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        span = handler.new_span(id_="OpenAI.chat-1", bound_args=self._make_bound_args(), instance=llm)
        self.assertFalse(span.is_passthrough)

        event = LLMChatStartEvent(
            messages=[
                ChatMessage(role="user", content="What is the weather?"),
            ],
            additional_kwargs={},
            model_dict={},
        )
        span.process_event(event)

        raw = span._attributes.get(GEN_AI_INPUT_MESSAGES)
        self.assertIsNotNone(raw)
        messages = json.loads(raw)
        validate_otel_genai_schema(messages, "gen-ai-input-messages")
        self.assertEqual(messages[0]["role"], "user")
        self.assertEqual(messages[0]["parts"][0]["type"], "text")
        self.assertIn("weather", messages[0]["parts"][0]["content"])
        span.end()

    def test_output_messages_conform_to_otel_schema(self):
        """Validate gen_ai.output.messages against OTel GenAI JSON Schema."""
        from llama_index.core.instrumentation.events.llm import LLMChatEndEvent

        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        span = handler.new_span(id_="OpenAI.chat-1", bound_args=self._make_bound_args(), instance=llm)

        response = Mock(spec=ChatResponse)
        response.message = ChatMessage(role="assistant", content="It is sunny today.")
        response.raw = None
        response.additional_kwargs = {}
        event = LLMChatEndEvent(response=response, messages=[], model_dict={})
        span.process_event(event)

        raw = span._attributes.get(GEN_AI_OUTPUT_MESSAGES)
        self.assertIsNotNone(raw)
        output = json.loads(raw)
        validate_otel_genai_schema(output, "gen-ai-output-messages")
        self.assertEqual(output[0]["role"], "assistant")
        self.assertEqual(output[0]["parts"][0]["type"], "text")
        self.assertIn("sunny", output[0]["parts"][0]["content"])
        span.end()

    def test_system_instructions_conform_to_otel_schema(self):
        """Validate gen_ai.system_instructions against OTel GenAI JSON Schema."""
        from llama_index.core.instrumentation.events.llm import LLMChatStartEvent

        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(model="gpt-4", api_key="fake")
        span = handler.new_span(id_="OpenAI.chat-1", bound_args=self._make_bound_args(), instance=llm)

        event = LLMChatStartEvent(
            messages=[
                ChatMessage(role="system", content="You are a helpful assistant."),
                ChatMessage(role="user", content="Hello"),
            ],
            additional_kwargs={},
            model_dict={},
        )
        span.process_event(event)

        # System instructions should be separated
        raw_instructions = span._attributes.get(GEN_AI_SYSTEM_INSTRUCTIONS)
        self.assertIsNotNone(raw_instructions)
        instructions = json.loads(raw_instructions)
        validate_otel_genai_schema(instructions, "gen-ai-system-instructions")
        self.assertEqual(instructions[0]["type"], "text")
        self.assertIn("helpful assistant", instructions[0]["content"])

        # Input messages should only contain non-system messages
        raw_input = span._attributes.get(GEN_AI_INPUT_MESSAGES)
        self.assertIsNotNone(raw_input)
        input_msgs = json.loads(raw_input)
        validate_otel_genai_schema(input_msgs, "gen-ai-input-messages")
        self.assertEqual(len(input_msgs), 1)
        self.assertEqual(input_msgs[0]["role"], "user")
        span.end()


    def test_agent_workflow(self):
        class _AsyncChatStream:
            def __init__(self, resp):
                self._resp = resp
                self._done = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._done:
                    raise StopAsyncIteration
                self._done = True
                return self._resp

        class _FakeLLM(FunctionCallingLLM):
            model_config = {"arbitrary_types_allowed": True}
            model: str = "fake-model"
            _call_count: int = 0

            @property
            def metadata(self):
                return LLMMetadata(model_name="fake-model", is_function_calling_model=True)

            def chat(self, messages, **kwargs):
                return ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="Done"))

            def complete(self, prompt, **kwargs):
                return CompletionResponse(text="Done")

            def stream_chat(self, messages, **kwargs):
                yield self.chat(messages)

            def stream_complete(self, prompt, **kwargs):
                yield self.complete(prompt)

            async def achat(self, messages, **kwargs):
                return self.chat(messages)

            async def acomplete(self, prompt, **kwargs):
                return self.complete(prompt)

            async def astream_chat(self, messages, **kwargs):
                return _AsyncChatStream(self.chat(messages))

            async def astream_complete(self, prompt, **kwargs):
                return self.complete(prompt)

            def _prepare_chat_with_tools(self, tools, **kwargs):
                return {"messages": [], "tools": tools}

            async def astream_chat_with_tools(self, tools, **kwargs):
                self._call_count += 1
                if self._call_count == 1 and tools:
                    t = tools[0]
                    msg = ChatMessage(role=MessageRole.ASSISTANT, content="")
                    msg.additional_kwargs = {
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "function": {
                                    "name": t.metadata.name,
                                    "arguments": '{"name": "World"}',
                                },
                            }
                        ]
                    }
                    return _AsyncChatStream(ChatResponse(message=msg))
                return _AsyncChatStream(
                    ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="Greeting complete!"))
                )

            def get_tool_calls_from_response(self, response, **kwargs):
                calls = response.message.additional_kwargs.get("tool_calls", [])
                return [
                    ToolSelection(
                        tool_id=c["id"],
                        tool_name=c["function"]["name"],
                        tool_kwargs=json.loads(c["function"]["arguments"]),
                    )
                    for c in calls
                ]

        def greet(name: str) -> str:
            """Greet someone by name"""
            return f"Hello {name}!"

        tool = FunctionTool.from_defaults(fn=greet, name="greet", description="Greet someone")
        llm = _FakeLLM()
        agent = FunctionAgent(name="Greeter", tools=[tool], llm=llm, system_prompt="You greet people.")
        wf = AgentWorkflow(agents=[agent], workflow_name="greeting_workflow")

        async def _run():
            await wf.run(user_msg="Greet World")

        asyncio.run(_run())
        spans = self.span_exporter.get_finished_spans()

        with self.subTest("produces invoke_workflow span"):
            wf_spans = [s for s in spans if s.attributes.get(GEN_AI_OPERATION_NAME) == OPERATION_INVOKE_WORKFLOW]
            self.assertEqual(len(wf_spans), 1)
            self.assertEqual(wf_spans[0].name, "invoke_workflow greeting_workflow")
            self.assertEqual(wf_spans[0].attributes[GEN_AI_WORKFLOW_NAME], "greeting_workflow")

        with self.subTest("produces single invoke_agent span"):
            agent_spans = [s for s in spans if s.attributes.get(GEN_AI_OPERATION_NAME) == "invoke_agent"]
            self.assertEqual(len(agent_spans), 1)
            self.assertEqual(agent_spans[0].name, "invoke_agent Greeter")
            self.assertEqual(agent_spans[0].attributes[GEN_AI_AGENT_NAME], "Greeter")

        with self.subTest("produces execute_tool span"):
            tool_spans = [s for s in spans if s.attributes.get(GEN_AI_OPERATION_NAME) == "execute_tool"]
            self.assertEqual(len(tool_spans), 1)
            self.assertIn("greet", tool_spans[0].name)

        with self.subTest("invoke_agent is child of invoke_workflow"):
            wf_span = [s for s in spans if s.attributes.get(GEN_AI_OPERATION_NAME) == OPERATION_INVOKE_WORKFLOW][0]
            agent_span = [s for s in spans if s.attributes.get(GEN_AI_OPERATION_NAME) == "invoke_agent"][0]
            self.assertEqual(agent_span.parent.span_id, wf_span.context.span_id)


if __name__ == "__main__":
    unittest.main()
