# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
from unittest import TestCase

from langchain.agents import create_agent
from langchain_core.language_models.fake import FakeListLLM
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain_core.tools import StructuredTool, tool

try:
    from langchain.agents import AgentType, initialize_agent

    HAS_LEGACY_LANGCHAIN = True
except ImportError:
    try:
        from langchain_classic.agents import AgentType, initialize_agent

        HAS_LEGACY_LANGCHAIN = True
    except ImportError:
        HAS_LEGACY_LANGCHAIN = False

from amazon.opentelemetry.distro.instrumentation.langchain import LangChainInstrumentor
from opentelemetry import context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
)
from opentelemetry.trace.status import StatusCode


class FakeChatModel(GenericFakeChatModel):
    model_id: str = "test-model-id"
    temperature: float = 0.7
    top_p: float = 0.9
    max_tokens: int = 100

    @classmethod
    def is_lc_serializable(cls) -> bool:
        return True

    @classmethod
    def get_lc_namespace(cls):
        return ["langchain", "chat_models", "fake"]

    def bind_tools(self, tools, **kwargs):
        return self

    def with_structured_output(self, schema, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        return ChatResult(
            generations=[ChatGeneration(message=AIMessage(content="Done."))],
            llm_output={
                "model_name": "test-model",
                "id": "test-response-id",
                "token_usage": {"prompt_tokens": 10, "completion_tokens": 20},
            },
        )


# https://pypi.org/project/langchain/
@unittest.skipIf(sys.version_info < (3, 10) or sys.version_info >= (4, 0), "langchain requires >=3.10, <4.0")
class TestLangChainInstrumentor(TestCase):
    def setUp(self):
        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.instrumentor = LangChainInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        self.span_exporter.clear()

    def test_suppressed_instrumentation_generates_no_spans(self):
        token = context.attach(context.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            FakeChatModel(messages=iter([AIMessage(content="Done.")])).invoke("test")
            FakeListLLM(responses=["hello"]).invoke("test")
            StructuredTool.from_function(func=lambda: "ok", name="t", description="d").invoke({})
            (RunnableLambda(lambda x: x) | FakeChatModel(messages=iter([AIMessage(content="Done.")]))).invoke("test")
        finally:
            context.detach(token)

        self.assertEqual(len(self.span_exporter.get_finished_spans()), 0)

    def test_chat_model_invoke_creates_span(self):
        llm = FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
        llm.invoke("Say hello")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertIn("chat", span.name)
        self.assertEqual(span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.CHAT.value)

    def test_llm_response_attributes(self):
        llm = FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
        llm.invoke("Say hello")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_MODEL], "test-model")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_ID], "test-response-id")
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)

    def test_llm_request_params(self):
        llm = FakeChatModel(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], "test-model-id")
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.7)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TOP_P], 0.9)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)

    def test_create_agent_creates_invoke_agent_span(self):
        @tool
        def get_weather(city: str) -> str:
            """Get weather for a city."""
            return f"Weather in {city}: sunny"

        llm = FakeChatModel(messages=iter([AIMessage(content="Done.")]))
        tools = [get_weather]
        agent = create_agent(llm, tools, name="TestAgent")

        agent.invoke({"messages": [("human", "What's the weather in Paris?")]})

        spans = self.span_exporter.get_finished_spans()
        agent_spans = [s for s in spans if "invoke_agent" in s.name]
        self.assertGreater(len(agent_spans), 0)
        agent_span = agent_spans[0]
        self.assertEqual(agent_span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.INVOKE_AGENT.value)
        self.assertEqual(agent_span.attributes[GEN_AI_AGENT_NAME], "TestAgent")

    def test_tool_execution_creates_span(self):
        def add_numbers(a: int, b: int) -> int:
            return a + b

        add_tool = StructuredTool.from_function(func=add_numbers, name="add_numbers", description="Add two numbers")
        result = add_tool.invoke({"a": 1, "b": 2})

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertIn("execute_tool", span.name)
        self.assertEqual(span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.EXECUTE_TOOL.value)
        self.assertEqual(span.attributes[GEN_AI_TOOL_NAME], "add_numbers")
        self.assertEqual(span.attributes[GEN_AI_TOOL_DESCRIPTION], "Add two numbers")
        self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, span.attributes)
        self.assertEqual(span.attributes[GEN_AI_TOOL_CALL_RESULT], str(result))

    def test_internal_chains_suppressed(self):
        chain_factories = [
            ("RunnableLambda", lambda llm: RunnableLambda(lambda x: x) | llm),
            ("RunnablePassthrough", lambda llm: RunnablePassthrough() | llm),
            ("RunnableSequence", lambda llm: RunnableLambda(lambda x: x) | RunnableLambda(lambda x: x) | llm),
            ("PromptTemplate", lambda llm: PromptTemplate.from_template("{input}") | llm),
            ("ChatPromptTemplate", lambda llm: ChatPromptTemplate.from_template("{input}") | llm),
            ("StrOutputParser", lambda llm: llm | StrOutputParser()),
        ]
        for name, factory in chain_factories:
            with self.subTest(chain_type=name):
                self.span_exporter.clear()
                llm = FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
                chain = factory(llm)
                chain.invoke({"input": "hello"} if "Prompt" in name else "hello")

                spans = self.span_exporter.get_finished_spans()
                self.assertGreater(len(spans), 0, f"Expected at least one span for {name}")
                suppressed = [s for s in spans if "Runnable" in s.name or "Parser" in s.name or "Prompt" in s.name]
                self.assertEqual(len(suppressed), 0, f"Internal spans should be suppressed for {name}")

    def test_langgraph_internal_nodes_suppressed(self):
        @tool
        def dummy_tool() -> str:
            """Dummy tool."""
            return "done"

        llm = FakeChatModel(messages=iter([AIMessage(content="Done.")]))
        agent = create_agent(llm, [dummy_tool], name="TestAgent")
        agent.invoke({"messages": [("human", "test")]})

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)
        invoke_agent_spans = [s for s in spans if "invoke_agent" in s.name]
        should_skip_internal_spans = [
            s for s in spans if "langgraph" in s.name.lower() and "agent" not in s.name.lower()
        ]

        self.assertGreater(len(invoke_agent_spans), 0)
        self.assertEqual(len(should_skip_internal_spans), 0)

    def test_uninstrument_removes_handler(self):
        self.instrumentor.uninstrument()

        llm = FakeChatModel(messages=iter([AIMessage(content="test")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

    def test_llm_error_sets_error_status(self):
        class FailingChatModel(FakeChatModel):
            def _generate(self, messages, stop=None, run_manager=None, **kwargs):
                raise ValueError("Test error")

        llm = FailingChatModel(messages=iter([]))
        with self.assertRaises(ValueError):
            llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertIn("chat", span.name)
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_tool_error_sets_error_status(self):
        def failing_tool() -> str:
            raise RuntimeError("Tool failed")

        fail_tool = StructuredTool.from_function(func=failing_tool, name="fail", description="Fails")
        with self.assertRaises(RuntimeError):
            fail_tool.invoke({})

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertIn("execute_tool", span.name)
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_chain_error_does_not_crash_instrumentation(self):
        llm = FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
        chain = RunnableLambda(lambda x: x) | llm | RunnableLambda(lambda x: 1 / 0)
        with self.assertRaises(ZeroDivisionError):
            chain.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)
        chat_spans = [s for s in spans if "chat" in s.name]
        self.assertEqual(len(chat_spans), 1)

    def test_text_completion_llm_creates_span(self):
        llm = FakeListLLM(responses=["hello"])
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertIn("text_completion", span.name)
        self.assertEqual(span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.TEXT_COMPLETION.value)

    def test_provider_extracted_from_model_id_prefix(self):
        class ModelWithProvider(FakeChatModel):
            def _get_invocation_params(self, stop=None, **kwargs):
                return {"model_id": "anthropic/claude-3"}

        llm = ModelWithProvider(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].attributes.get(GEN_AI_PROVIDER_NAME), "anthropic")

    @unittest.skipUnless(HAS_LEGACY_LANGCHAIN, "langchain_classic not available")
    def test_legacy_agent_executor_triggers_agent_callbacks(self):
        responses = iter(
            [
                AIMessage(content="I need to search for information.\nAction: search\nAction Input: test query"),
                AIMessage(content="Final Answer: The answer is 42"),
            ]
        )

        class ReActChatModel(FakeChatModel):
            def _generate(self, messages, stop=None, run_manager=None, **kwargs):
                return ChatResult(generations=[ChatGeneration(message=next(responses))])

        @tool
        def search(query: str) -> str:
            """Search for information."""
            return f"Result for: {query}"

        llm = ReActChatModel(messages=iter([]))
        agent = initialize_agent([search], llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=False)
        try:
            agent.run("test query")
        except Exception:
            pass

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)

    @unittest.skipUnless(HAS_LEGACY_LANGCHAIN, "langchain_classic not available")
    def test_legacy_agent_error_triggers_callback(self):
        class FailingChatModel(FakeChatModel):
            def _generate(self, messages, stop=None, run_manager=None, **kwargs):
                raise ValueError("LLM failed")

        @tool
        def search(query: str) -> str:
            """Search."""
            return "result"

        llm = FailingChatModel(messages=iter([]))
        agent = initialize_agent([search], llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=False)
        try:
            agent.run("test")
        except Exception:
            pass

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)
        error_spans = [s for s in spans if s.status.status_code == StatusCode.ERROR]
        self.assertGreater(len(error_spans), 0)

    def test_provider_from_type_prefix(self):
        class OpenAIStyleModel(FakeChatModel):
            def _get_invocation_params(self, stop=None, **kwargs):
                return {"_type": "openai-chat"}

        llm = OpenAIStyleModel(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].attributes.get(GEN_AI_PROVIDER_NAME), "openai")

    def test_provider_from_serialized_id(self):
        class OpenAIChatModel(FakeChatModel):
            @classmethod
            def get_lc_namespace(cls):
                return ["langchain", "chat_models", "openai"]

        llm = OpenAIChatModel(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].attributes.get(GEN_AI_PROVIDER_NAME), "openai")

    def test_model_id_from_invocation_params(self):
        class ModelWithInvocationParams(FakeChatModel):
            @classmethod
            def is_lc_serializable(cls) -> bool:
                return False

            def _get_invocation_params(self, stop=None, **kwargs):
                return {"model_id": "custom-model-from-params"}

        llm = ModelWithInvocationParams(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertIn("custom-model-from-params", spans[0].name)


if __name__ == "__main__":
    unittest.main()
