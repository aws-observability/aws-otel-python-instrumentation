# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import importlib
import importlib.util
import inspect
import json
import unittest
from unittest.mock import Mock, MagicMock, patch, PropertyMock


def _has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry import context as context_api, trace
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
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

from amazon.opentelemetry.distro.instrumentation.llama_index import LlamaIndexInstrumentor

from llama_index.core.base.llms.types import ChatMessage, ChatResponse, CompletionResponse, MessageRole
from llama_index.core.tools import BaseTool, FunctionTool
from llama_index.core.tools.types import ToolOutput


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
        self._ExportQueue = handler_module._ExportQueue
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
            raw={"usage": {"prompt_tokens": 10, "completion_tokens": 8}}
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
            text="Response text",
            raw={"usage": {"prompt_tokens": 15, "completion_tokens": 10}}
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
        from llama_index.core.instrumentation.events.query import QueryStartEvent
        from llama_index.core import QueryBundle
        query = QueryBundle(query_str="Test query")
        event = QueryStartEvent(query=query)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "query")
        otel_span.end()

    def test_retrieval_start_event(self):
        from llama_index.core.instrumentation.events.retrieval import RetrievalStartEvent
        from llama_index.core import QueryBundle
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
        from llama_index.core.instrumentation.events.synthesis import SynthesizeStartEvent
        from llama_index.core import QueryBundle
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
        self.assertFalse(span._active)

    def test_span_end_idempotent(self):
        """Test that calling end() twice is safe."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.end()
        span.end()  # should not raise
        self.assertFalse(span._active)

    def test_span_properties(self):
        """Test active, waiting_for_streaming, and context properties."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        self.assertTrue(span.active)
        self.assertFalse(span.waiting_for_streaming)
        ctx = span.context
        self.assertIsNotNone(ctx)
        span._waiting_for_streaming = True
        self.assertTrue(span.waiting_for_streaming)
        span.end()
        self.assertFalse(span.active)
        self.assertFalse(span.waiting_for_streaming)

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
        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"),
            raw=mock_raw
        )
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
            }
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
            message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"),
            raw={"_raw_response": raw_response}
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
        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="Test"),
            raw=mock_raw
        )
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

    @unittest.skipUnless(_has_module("llama_index.llms.bedrock_converse"), "llama-index-llms-bedrock-converse not installed")
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

    def test_new_span_returns_none_for_none_instance(self):
        """Test that new_span returns None when instance is None."""
        handler = self._make_span_handler()
        result = handler.new_span(id_="test-1", bound_args=self._make_bound_args(), instance=None)
        self.assertIsNone(result)

    def test_new_span_suppresses_utility_classes(self):
        """Test that utility classes like TokenTextSplitter are suppressed."""
        handler = self._make_span_handler()
        for cls_name in ("TokenTextSplitter", "DefaultRefineProgram", "SentenceSplitter", "CompactAndRefine"):
            cls = type(cls_name, (), {})
            instance = cls()
            result = handler.new_span(id_=f"{cls_name}.do_thing-1", bound_args=self._make_bound_args(), instance=instance)
            self.assertIsNone(result, f"{cls_name} should be suppressed")

    def test_new_span_suppresses_internal_methods(self):
        """Test that internal workflow methods are suppressed."""
        handler = self._make_span_handler()
        suppressed_methods = [
            "parse_agent_output", "aggregate_tool_results", "setup_agent",
            "init_run", "run_agent_step", "call_tool", "_prepare_chat_with_tools",
            "_get_text_embedding", "_query", "_retrieve", "_get_query_embedding",
            "predict_and_call", "__call__",
        ]
        for method in suppressed_methods:
            instance = Mock()
            instance.__class__ = type("SomeClass", (), {})
            result = handler.new_span(
                id_=f"SomeClass.{method}-1",
                bound_args=self._make_bound_args(),
                instance=instance,
            )
            self.assertIsNone(result, f"Method {method} should be suppressed")

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

    def test_prepare_to_exit_span_streaming_llm(self):
        """Test that streaming LLM results set waiting_for_streaming."""
        handler = self._make_span_handler()
        from llama_index.llms.openai import OpenAI
        llm = OpenAI(model="gpt-4", api_key="fake")
        span = handler.new_span(id_="OpenAI.stream_chat-1", bound_args=self._make_bound_args(), instance=llm)
        self.assertIsNotNone(span)
        handler.open_spans["OpenAI.stream_chat-1"] = span

        # Create a real generator to pass isinstance checks
        def gen():
            yield "chunk"
        g = gen()
        result = handler.prepare_to_exit_span(
            id_="OpenAI.stream_chat-1", bound_args=self._make_bound_args(), instance=llm, result=g
        )
        self.assertIsNotNone(result)
        self.assertTrue(span._waiting_for_streaming)
        span.end()

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

    def test_prepare_to_drop_span_with_workflow_done(self):
        """Test that WorkflowDone ends span without error."""
        from llama_index.core.workflow.errors import WorkflowDone
        handler = self._make_span_handler()
        instance = Mock()
        instance.__class__ = type("SomeClass", (), {})
        span = handler.new_span(id_="SomeClass.method-1", bound_args=self._make_bound_args(), instance=instance)
        self.assertIsNotNone(span)
        handler.open_spans["SomeClass.method-1"] = span
        handler.prepare_to_drop_span(
            id_="SomeClass.method-1", bound_args=self._make_bound_args(), instance=instance, err=WorkflowDone()
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
            id_="BaseEmbedding.embed-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIsNotNone(span)
        span_handler.open_spans["BaseEmbedding.embed-1"] = span

        event = EmbeddingStartEvent(model_dict={})
        event.span_id = "BaseEmbedding.embed-1"
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

    def test_event_handler_finds_span_in_export_queue(self):
        """Test that EventHandler checks export queue for streaming spans."""
        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)

        from llama_index.core.base.embeddings.base import BaseEmbedding
        instance = Mock(spec=BaseEmbedding)
        instance.model_name = "test-model"
        span = span_handler.new_span(
            id_="BaseEmbedding.embed-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIsNotNone(span)
        # Put in export queue (not open_spans) to simulate streaming
        span_handler._export_queue.put(span)

        from llama_index.core.instrumentation.events.embedding import EmbeddingStartEvent
        event = EmbeddingStartEvent(model_dict={})
        event.span_id = "BaseEmbedding.embed-1"
        result = event_handler.handle(event)
        self.assertEqual(result, event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "embeddings")
        span.end()

    def test_process_event_streaming_finished(self):
        """Test that streaming finished events end the span."""
        from llama_index.core.instrumentation.events.llm import LLMChatEndEvent
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._waiting_for_streaming = True
        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="done"),
            raw={"usage": {"prompt_tokens": 1, "completion_tokens": 1}}
        )
        event = LLMChatEndEvent(messages=[], response=response)
        span.process_event(event)
        self.assertFalse(span.active)

    def test_process_event_streaming_in_progress(self):
        """Test that streaming in-progress events update timestamps."""
        from llama_index.core.instrumentation.events.llm import LLMChatInProgressEvent
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._waiting_for_streaming = True
        response = ChatResponse(message=ChatMessage(role=MessageRole.ASSISTANT, content="partial"))
        event = LLMChatInProgressEvent(response=response, messages=[])
        span.process_event(event)
        self.assertTrue(span.active)
        self.assertIsNotNone(span._first_token_timestamp)
        otel_span.end()

    def test_process_event_not_streaming_does_not_end(self):
        """Test that non-streaming spans don't end on finished events."""
        from llama_index.core.instrumentation.events.llm import LLMChatEndEvent
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="done"),
            raw={"usage": {"prompt_tokens": 1, "completion_tokens": 1}}
        )
        event = LLMChatEndEvent(messages=[], response=response)
        span.process_event(event)
        self.assertTrue(span.active)
        otel_span.end()

    def test_notify_parent_streaming(self):
        """Test that notify_parent propagates streaming status to parent."""
        parent_otel = self.tracer.start_span("parent")
        parent_span = self._Span(otel_span=parent_otel)
        parent_span._waiting_for_streaming = True

        child_otel = self.tracer.start_span("child")
        child_span = self._Span(otel_span=child_otel, parent=parent_span)
        child_span._waiting_for_streaming = True

        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        child_span.notify_parent(handler._StreamingStatus.IN_PROGRESS)
        self.assertTrue(parent_span.active)

        child_span.notify_parent(handler._StreamingStatus.FINISHED)
        self.assertFalse(parent_span.active)

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

    def test_get_current_span_returns_none_when_no_active_span(self):
        """Test get_current_span returns None when no span is active."""
        from amazon.opentelemetry.distro.instrumentation.llama_index import get_current_span
        result = get_current_span()
        self.assertIsNone(result)

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

    def test_get_current_span_with_uninstrumented(self):
        """Test get_current_span returns None when instrumentor has no span_handler."""
        from amazon.opentelemetry.distro.instrumentation.llama_index import get_current_span
        self.instrumentor.uninstrument()
        result = get_current_span()
        self.assertIsNone(result)
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

    def test_process_event_streaming_exception(self):
        """Test that ExceptionEvent during streaming ends the span with error."""
        handler = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._waiting_for_streaming = True
        exc = RuntimeError("stream failed")
        event = handler.ExceptionEvent(exception=exc)
        span.process_event(event)
        self.assertFalse(span.active)

    def test_event_handler_exception_in_process_event(self):
        """Test that EventHandler catches exceptions from process_event."""
        span_handler = self._make_span_handler()
        event_handler = self._EventHandler(span_handler=span_handler)

        from llama_index.core.base.embeddings.base import BaseEmbedding
        instance = Mock(spec=BaseEmbedding)
        instance.model_name = "test-model"
        span = span_handler.new_span(
            id_="BaseEmbedding.embed-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIsNotNone(span)
        span_handler.open_spans["BaseEmbedding.embed-1"] = span

        # Create an event that will cause process_event to raise via a bad attribute access
        event = Mock()
        event.span_id = "BaseEmbedding.embed-1"
        event.id_ = "event-1"
        # Monkey-patch the span's process_event at the object level using object.__setattr__
        original = span.process_event
        object.__setattr__(span, 'process_event', Mock(side_effect=RuntimeError("boom")))
        result = event_handler.handle(event)
        self.assertEqual(result, event)
        object.__setattr__(span, 'process_event', original)
        span.end()

    def test_prepare_to_exit_span_workflow_handler(self):
        """Test that WorkflowHandler results attach a done callback."""
        handler = self._make_span_handler()
        from llama_index.core.agent.workflow import FunctionAgent
        from llama_index.llms.openai import OpenAI
        llm = OpenAI(model="gpt-4", api_key="fake")
        agent = FunctionAgent(tools=[], llm=llm, name="TestAgent")
        span = handler.new_span(id_="FunctionAgent.run-1", bound_args=self._make_bound_args(), instance=agent)
        self.assertIsNotNone(span)
        handler.open_spans["FunctionAgent.run-1"] = span

        # Mock a WorkflowHandler with a _result_task
        from llama_index.core.workflow.handler import WorkflowHandler
        mock_handler = Mock(spec=WorkflowHandler)
        mock_task = Mock()
        mock_handler._result_task = mock_task

        result = handler.prepare_to_exit_span(
            id_="FunctionAgent.run-1", bound_args=self._make_bound_args(), instance=agent, result=mock_handler
        )
        self.assertIsNotNone(result)
        # The span should still be active (waiting for callback)
        self.assertTrue(span.active)
        # Verify add_done_callback was called
        mock_task.add_done_callback.assert_called_once()

        # Now simulate the callback being invoked (covers lines 826-831)
        callback = mock_task.add_done_callback.call_args[0][0]
        # Simulate a completed task with no exception
        mock_completed_task = Mock()
        mock_completed_task.exception.return_value = None
        callback(mock_completed_task)
        self.assertFalse(span.active)

    def test_prepare_to_exit_span_workflow_handler_with_exception(self):
        """Test WorkflowHandler callback with task exception."""
        handler = self._make_span_handler()
        from llama_index.core.agent.workflow import FunctionAgent
        from llama_index.llms.openai import OpenAI
        llm = OpenAI(model="gpt-4", api_key="fake")
        agent = FunctionAgent(tools=[], llm=llm, name="TestAgent2")
        span = handler.new_span(id_="FunctionAgent.run-2", bound_args=self._make_bound_args(), instance=agent)
        self.assertIsNotNone(span)
        handler.open_spans["FunctionAgent.run-2"] = span

        from llama_index.core.workflow.handler import WorkflowHandler
        mock_handler = Mock(spec=WorkflowHandler)
        mock_task = Mock()
        mock_handler._result_task = mock_task

        handler.prepare_to_exit_span(
            id_="FunctionAgent.run-2", bound_args=self._make_bound_args(), instance=agent, result=mock_handler
        )
        callback = mock_task.add_done_callback.call_args[0][0]
        # Simulate a task that raised an exception
        mock_failed_task = Mock()
        mock_failed_task.exception.return_value = RuntimeError("workflow failed")
        callback(mock_failed_task)
        self.assertFalse(span.active)

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

    def test_get_current_span_full_path(self):
        """Test get_current_span returns the otel span when a span is active."""
        from amazon.opentelemetry.distro.instrumentation.llama_index import get_current_span
        from llama_index.core.instrumentation.span import active_span_id
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")

        # Create a span via the span handler
        span_handler = self.instrumentor._span_handler
        from llama_index.core.base.embeddings.base import BaseEmbedding
        instance = Mock(spec=BaseEmbedding)
        instance.model_name = "test"
        span = span_handler.new_span(
            id_="test-span-1", bound_args=self._make_bound_args(), instance=instance
        )
        self.assertIsNotNone(span)
        span_handler.open_spans["test-span-1"] = span

        # Set the active span id
        token = active_span_id.set("test-span-1")
        try:
            result = get_current_span()
            self.assertIsNotNone(result)
            self.assertEqual(result, span._otel_span)
        finally:
            active_span_id.reset(token)
            span.end()

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
        self.assertFalse(span._active)

    def test_get_current_span_no_matching_open_span(self):
        """Test get_current_span returns None when span_id doesn't match any open span."""
        from amazon.opentelemetry.distro.instrumentation.llama_index import get_current_span
        from llama_index.core.instrumentation.span import active_span_id
        token = active_span_id.set("nonexistent-span-id")
        try:
            result = get_current_span()
            self.assertIsNone(result)
        finally:
            active_span_id.reset(token)

    def test_process_messages_empty(self):
        """Test _process_messages with no messages does nothing."""
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_messages("gen_ai.input.messages")
        self.assertNotIn("gen_ai.input.messages", span._attributes)
        otel_span.end()

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
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama_index._handler")
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


if __name__ == "__main__":
    unittest.main()
