# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import importlib
import json
import unittest
from unittest.mock import Mock

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry import trace
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
)

llama_index_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama-index")
LlamaIndexInstrumentor = llama_index_module.LlamaIndexInstrumentor

from amazon.opentelemetry.distro.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_EMBEDDINGS_DIMENSION_COUNT,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_TOOL_DEFINITIONS,
)
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_TYPE

from llama_index.core.base.llms.types import ChatMessage, ChatResponse, CompletionResponse, MessageRole
from llama_index.core.tools import FunctionTool


class TestLlamaIndexInstrumentor(unittest.TestCase):

    def setUp(self):
        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.instrumentor = LlamaIndexInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama-index._handler")
        self._Span = handler_module._Span
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

    def test_llm_completion_start_event(self):
        from llama_index.core.instrumentation.events.llm import LLMCompletionStartEvent
        event = LLMCompletionStartEvent(prompt="Test prompt", additional_kwargs={}, model_dict={})
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "text_completion")
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

    def test_llm_predict_end_event(self):
        from llama_index.core.instrumentation.events.llm import LLMPredictEndEvent
        event = LLMPredictEndEvent(template="Template", template_args={}, output="Output")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "text_completion")
        otel_span.end()

    def test_llm_structured_predict_start_event(self):
        from llama_index.core.instrumentation.events.llm import LLMStructuredPredictStartEvent
        from llama_index.core.prompts import PromptTemplate
        from pydantic import BaseModel
        class OutputModel(BaseModel):
            field: str
        template = PromptTemplate("Test {var}")
        event = LLMStructuredPredictStartEvent(template=template, template_args={"var": "value"}, output_cls=OutputModel)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "text_completion")
        otel_span.end()

    def test_llm_structured_predict_end_event(self):
        from llama_index.core.instrumentation.events.llm import LLMStructuredPredictEndEvent
        event = LLMStructuredPredictEndEvent(template="Template", template_args={}, output="Output")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "text_completion")
        otel_span.end()

    def test_stream_chat_start_event(self):
        from llama_index.core.instrumentation.events.chat_engine import StreamChatStartEvent
        messages = [ChatMessage(role=MessageRole.USER, content="Hello")]
        event = StreamChatStartEvent(messages=messages, additional_kwargs={}, model_dict={})
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "chat")
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

    def test_query_end_event(self):
        from llama_index.core.instrumentation.events.query import QueryEndEvent
        from llama_index.core import QueryBundle
        from llama_index.core.base.response.schema import Response
        query = QueryBundle(query_str="Test query")
        response = Response(response="Test response")
        event = QueryEndEvent(query=query, response=response)
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

    def test_retrieval_end_event(self):
        from llama_index.core.instrumentation.events.retrieval import RetrievalEndEvent
        from llama_index.core import QueryBundle
        query = QueryBundle(query_str="Test query")
        event = RetrievalEndEvent(str_or_query_bundle=query, nodes=[])
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

    def test_rerank_end_event(self):
        from llama_index.core.instrumentation.events.rerank import ReRankEndEvent
        event = ReRankEndEvent(nodes=[], model_name="cohere-rerank-v3", query="Test", top_n=5)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "rerank")
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

    def test_synthesize_end_event(self):
        from llama_index.core.instrumentation.events.synthesis import SynthesizeEndEvent
        from llama_index.core import QueryBundle
        from llama_index.core.base.response.schema import Response
        query = QueryBundle(query_str="Test query")
        response = Response(response="Test response")
        event = SynthesizeEndEvent(query=query, response=response)
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

    def test_get_response_end_event(self):
        from llama_index.core.instrumentation.events.synthesis import GetResponseEndEvent
        event = GetResponseEndEvent(query_str="Test query", response="Response")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "synthesize")
        otel_span.end()

    def test_agent_chat_with_step_start_event(self):
        from llama_index.core.instrumentation.events.agent import AgentChatWithStepStartEvent
        event = AgentChatWithStepStartEvent(user_msg="Hello")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "invoke_agent")
        otel_span.end()

    def test_agent_chat_with_step_end_event(self):
        from llama_index.core.instrumentation.events.agent import AgentChatWithStepEndEvent
        from llama_index.core.chat_engine.types import AgentChatResponse
        response = AgentChatResponse(response="Test response", sources=[], source_nodes=[])
        event = AgentChatWithStepEndEvent(user_msg="Hello", response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "invoke_agent")
        otel_span.end()

    def test_agent_run_step_start_event(self):
        from llama_index.core.instrumentation.events.agent import AgentRunStepStartEvent
        step = Mock()
        step.task_id = "task123"
        step.step_id = "step1"
        event = AgentRunStepStartEvent(task_id="task123", step=step, input="test input")
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "invoke_agent")
        otel_span.end()

    def test_agent_run_step_end_event(self):
        from llama_index.core.instrumentation.events.agent import AgentRunStepEndEvent
        step_output = Mock()
        step_output.output = "test output"
        step_output.is_last = True
        event = AgentRunStepEndEvent(task_id="task123", step_output=step_output)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "invoke_agent")
        otel_span.end()

    def test_agent_tool_call_event(self):
        from llama_index.core.instrumentation.events.agent import AgentToolCallEvent
        from llama_index.core.tools.types import ToolMetadata
        tool_metadata = ToolMetadata(name="test_tool", description="Test tool description")
        event = AgentToolCallEvent(arguments="{}", tool=tool_metadata)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_OPERATION_NAME], "execute_tool")
        self.assertEqual(span._attributes[GEN_AI_TOOL_NAME], "test_tool")
        self.assertEqual(span._attributes[GEN_AI_TOOL_DESCRIPTION], "Test tool description")
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

    def test_process_instance_embedding(self):
        from llama_index.core.base.embeddings.base import BaseEmbedding
        embedding_model = Mock(spec=BaseEmbedding)
        embedding_model.model_name = "text-embedding-ada-002"
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span.process_instance(embedding_model)
        self.assertEqual(span._attributes[GEN_AI_REQUEST_MODEL], "text-embedding-ada-002")
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
        from llama_index.core.base.llms.types import CompletionResponse
        response = CompletionResponse(text="Test response")
        response.raw = {
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20
            }
        }
        event = LLMCompletionEndEvent(prompt="Test", response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)
        otel_span.end()

    def test_token_count_extraction_anthropic_format(self):
        from llama_index.core.instrumentation.events.llm import LLMCompletionEndEvent
        from llama_index.core.base.llms.types import CompletionResponse
        response = CompletionResponse(text="Test response")
        response.raw = {
            "usage": {
                "input_tokens": 15,
                "output_tokens": 25
            }
        }
        event = LLMCompletionEndEvent(prompt="Test", response=response)
        otel_span = self.tracer.start_span("test")
        span = self._Span(otel_span=otel_span)
        span._process_event(event)
        self.assertEqual(span._attributes[GEN_AI_USAGE_INPUT_TOKENS], 15)
        self.assertEqual(span._attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 25)
        otel_span.end()

    def test_provider_fallback_constant(self):
        handler_module = importlib.import_module("amazon.opentelemetry.distro.instrumentation.llama-index._handler")
        _PROVIDER_LLAMA_INDEX = getattr(handler_module, "_PROVIDER_LLAMA_INDEX")
        self.assertEqual(_PROVIDER_LLAMA_INDEX, "llama_index")

    def test_instrumentation_lifecycle(self):
        self.assertTrue(self.instrumentor.is_instrumented_by_opentelemetry)
        self.instrumentor.uninstrument()
        self.assertFalse(self.instrumentor.is_instrumented_by_opentelemetry)
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.assertTrue(self.instrumentor.is_instrumented_by_opentelemetry)


if __name__ == "__main__":
    unittest.main()
