# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import sys
import unittest
from unittest import TestCase
from unittest.mock import patch

from conftest import validate_otel_genai_schema

if sys.version_info < (3, 10):
    raise unittest.SkipTest("langchain requires >=3.10")

try:
    from langchain.agents import create_agent
except ImportError:
    create_agent = None

try:
    from langchain.agents import AgentType, initialize_agent
except ImportError:
    try:
        from langchain_classic.agents import AgentType, initialize_agent
    except ImportError:
        initialize_agent = None
from langchain_core.language_models.fake import FakeListLLM
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain_core.tools import StructuredTool, tool

from amazon.opentelemetry.distro.instrumentation.langchain import LangChainInstrumentor
from opentelemetry import context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_FREQUENCY_PENALTY,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_PRESENCE_PENALTY,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_K,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
    GenAiProviderNameValues,
)
from opentelemetry.trace.status import StatusCode


# https://pypi.org/project/langchain/
class TestLangChainInstrumentor(TestCase):

    class FakeChatModel(GenericFakeChatModel):
        model_id: str = "test-model-id"
        temperature: float = 0.7
        top_p: float = 0.9
        top_k: int = 40
        max_tokens: int = 100
        frequency_penalty: float = 0.5
        presence_penalty: float = 0.3

        @property
        def _default_params(self) -> dict:
            return {
                "model_id": self.model_id,
                "temperature": self.temperature,
                "top_p": self.top_p,
                "top_k": self.top_k,
                "max_tokens": self.max_tokens,
                "frequency_penalty": self.frequency_penalty,
                "presence_penalty": self.presence_penalty,
            }

        @classmethod
        def is_lc_serializable(cls) -> bool:
            return True

        @classmethod
        def get_lc_namespace(cls):
            return ["langchain", "chat_models", "openai"]

        def bind_tools(self, tools, **kwargs):
            return self

        def with_structured_output(self, schema, **kwargs):
            return self

        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            return ChatResult(
                generations=[
                    ChatGeneration(
                        message=AIMessage(content="Final Answer: Done."),
                        generation_info={"finish_reason": "stop"},
                    )
                ],
                llm_output={
                    "model_name": "test-model",
                    "id": "test-response-id",
                    "token_usage": {"prompt_tokens": 10, "completion_tokens": 20},
                },
            )

    def setUp(self):
        try:
            from langchain.agents import AgentType, initialize_agent

            self.AgentType = AgentType
            self.initialize_agent = initialize_agent
            self.HAS_LEGACY_LANGCHAIN = True
        except ImportError:
            try:
                from langchain_classic.agents import AgentType, initialize_agent

                self.AgentType = AgentType
                self.initialize_agent = initialize_agent
                self.HAS_LEGACY_LANGCHAIN = True
            except ImportError:
                self.HAS_LEGACY_LANGCHAIN = False

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
            self.FakeChatModel(messages=iter([AIMessage(content="Done.")])).invoke("test")
            FakeListLLM(responses=["hello"]).invoke("test")
            StructuredTool.from_function(func=lambda: "ok", name="t", description="d").invoke({})
            (RunnableLambda(lambda x: x) | self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))).invoke(
                "test"
            )
        finally:
            context.detach(token)

        self.assertEqual(len(self.span_exporter.get_finished_spans()), 0)

    def test_chat_model_span_has_all_attributes(self):
        llm = self.FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
        llm.invoke("Say hello")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]

        self.assertIn("chat", span.name)
        self.assertEqual(span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.CHAT.value)
        self.assertEqual(span.attributes[GEN_AI_PROVIDER_NAME], "openai")

        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], "test-model-id")
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.7)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TOP_P], 0.9)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TOP_K], 40)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_FREQUENCY_PENALTY], 0.5)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_PRESENCE_PENALTY], 0.3)

        self.assertIsNotNone(span.attributes.get(GEN_AI_INPUT_MESSAGES))
        input_messages = json.loads(span.attributes[GEN_AI_INPUT_MESSAGES])
        self.assertIsInstance(input_messages, list)
        self.assertGreater(len(input_messages), 0)
        self.assertIn("role", input_messages[0])
        self.assertIn("parts", input_messages[0])

        self.assertIsNotNone(span.attributes.get(GEN_AI_OUTPUT_MESSAGES))
        output_messages = json.loads(span.attributes[GEN_AI_OUTPUT_MESSAGES])
        self.assertIsInstance(output_messages, list)
        self.assertGreater(len(output_messages), 0)
        self.assertEqual(output_messages[0]["role"], "assistant")
        self.assertIn("parts", output_messages[0])

        self.assertEqual(span.attributes[GEN_AI_RESPONSE_MODEL], "test-model")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_ID], "test-response-id")
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)
        self.assertEqual(span.attributes.get(GEN_AI_RESPONSE_FINISH_REASONS), ("stop",))

    def test_create_agent_creates_invoke_agent_span(self):
        @tool
        def get_weather(query: str) -> str:
            """Get weather for a city."""
            return f"Weather in {query}: sunny"

        llm = self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))
        tools = [get_weather]

        if create_agent:
            agent = create_agent(llm, tools, name="TestAgent")
            agent.invoke({"messages": [("human", "What's the weather in Paris?")]})
        elif initialize_agent:
            agent = initialize_agent(tools, llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION)
            agent.invoke({"input": "What's the weather in Paris?"})
        else:
            self.skipTest("no agent API available")

        spans = self.span_exporter.get_finished_spans()
        agent_spans = [s for s in spans if "invoke_agent" in s.name]
        self.assertGreater(len(agent_spans), 0)
        agent_span = agent_spans[0]
        self.assertEqual(agent_span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.INVOKE_AGENT.value)

    def test_tool_span_has_all_attributes(self):
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
        self.assertEqual(span.attributes[GEN_AI_TOOL_TYPE], "function")
        self.assertIsNotNone(span.attributes.get(GEN_AI_TOOL_CALL_ARGUMENTS))
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
                llm = self.FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
                chain = factory(llm)
                chain.invoke({"input": "hello"} if "Prompt" in name else "hello")

                spans = self.span_exporter.get_finished_spans()
                self.assertGreater(len(spans), 0, f"Expected at least one span for {name}")
                suppressed = [s for s in spans if "Runnable" in s.name or "Parser" in s.name or "Prompt" in s.name]
                self.assertEqual(len(suppressed), 0, f"Internal spans should be suppressed for {name}")

    def test_langgraph_internal_nodes_suppressed(self):
        @tool
        def dummy_tool(query: str) -> str:
            """Dummy tool."""
            return "done"

        llm = self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))

        if create_agent:
            agent = create_agent(llm, [dummy_tool], name="TestAgent")
            agent.invoke({"messages": [("human", "test")]})
        elif initialize_agent:
            agent = initialize_agent([dummy_tool], llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION)
            agent.invoke({"input": "test"})
        else:
            self.skipTest("no agent API available")

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)
        invoke_agent_spans = [s for s in spans if "invoke_agent" in s.name]
        self.assertGreater(len(invoke_agent_spans), 0)

        if create_agent:
            should_skip_internal_spans = [
                s for s in spans if "langgraph" in s.name.lower() and "agent" not in s.name.lower()
            ]
            self.assertEqual(len(should_skip_internal_spans), 0)

    def test_uninstrument_removes_handler(self):
        self.instrumentor.uninstrument()

        llm = self.FakeChatModel(messages=iter([AIMessage(content="test")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

    def test_llm_error_sets_error_status(self):
        FakeChatModel = self.FakeChatModel

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
        llm = self.FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
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

    def test_llm_attributes_langchain_models(self):
        from langchain_anthropic import ChatAnthropic
        from langchain_aws import ChatBedrock, ChatBedrockConverse
        from langchain_cohere import ChatCohere
        from langchain_deepseek import ChatDeepSeek
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_groq import ChatGroq
        from langchain_mistralai import ChatMistralAI
        from langchain_openai import AzureChatOpenAI, ChatOpenAI
        from langchain_xai import ChatXAI

        fake_result = ChatResult(
            generations=[ChatGeneration(message=AIMessage(content="ok"), generation_info={"finish_reason": "stop"})],
            llm_output={
                "model_name": "test",
                "token_usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

        cases = [
            (ChatBedrock, {"model_id": "test", "region_name": "us-east-1"}, GenAiProviderNameValues.AWS_BEDROCK.value),
            (
                ChatBedrockConverse,
                {"model": "test", "region_name": "us-east-1"},
                GenAiProviderNameValues.AWS_BEDROCK.value,
            ),
            (ChatOpenAI, {"api_key": "fake"}, GenAiProviderNameValues.OPENAI.value),
            (
                AzureChatOpenAI,
                {"api_key": "fake", "azure_endpoint": "https://fake.openai.azure.com", "api_version": "2024-01-01"},
                GenAiProviderNameValues.AZURE_AI_OPENAI.value,
            ),
            (
                ChatAnthropic,
                {"anthropic_api_key": "fake", "model_name": "claude-3"},
                GenAiProviderNameValues.ANTHROPIC.value,
            ),
            (
                ChatGoogleGenerativeAI,
                {"google_api_key": "fake", "model": "gemini-pro"},
                GenAiProviderNameValues.GCP_GEN_AI.value,
            ),
            (ChatMistralAI, {"api_key": "fake", "model": "test"}, GenAiProviderNameValues.MISTRAL_AI.value),
            (ChatGroq, {"api_key": "fake", "model": "test"}, GenAiProviderNameValues.GROQ.value),
            (ChatCohere, {"cohere_api_key": "fake", "model": "test"}, GenAiProviderNameValues.COHERE.value),
            (ChatDeepSeek, {"api_key": "fake", "model": "test"}, GenAiProviderNameValues.DEEPSEEK.value),
            (ChatXAI, {"api_key": "fake", "model": "test"}, GenAiProviderNameValues.X_AI.value),
        ]

        for model_cls, init_kwargs, expected_provider in cases:
            with self.subTest(model=model_cls.__name__):
                self.span_exporter.clear()

                llm = model_cls(**init_kwargs)
                with patch.object(type(llm), "_generate", return_value=fake_result):
                    llm.invoke("test")

                spans = self.span_exporter.get_finished_spans()
                chat_spans = [s for s in spans if "chat" in s.name or "text_completion" in s.name]
                self.assertGreaterEqual(len(chat_spans), 1, f"No chat span for {model_cls.__name__}")
                attrs = chat_spans[0].attributes
                self.assertEqual(attrs.get(GEN_AI_PROVIDER_NAME), expected_provider)
                self.assertEqual(attrs.get(GEN_AI_OPERATION_NAME), GenAiOperationNameValues.CHAT.value)
                self.assertIsNotNone(attrs.get(GEN_AI_INPUT_MESSAGES))
                self.assertIsNotNone(attrs.get(GEN_AI_OUTPUT_MESSAGES))
                validate_otel_genai_schema(json.loads(attrs[GEN_AI_INPUT_MESSAGES]), "gen-ai-input-messages")
                validate_otel_genai_schema(json.loads(attrs[GEN_AI_OUTPUT_MESSAGES]), "gen-ai-output-messages")
                self.assertEqual(attrs.get(GEN_AI_RESPONSE_FINISH_REASONS), ("stop",))

    def test_provider_extracted_from_serialized_id(self):
        FakeChatModel = self.FakeChatModel

        llm = FakeChatModel(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].attributes.get(GEN_AI_PROVIDER_NAME), "openai")

    def test_legacy_agent_executor_triggers_agent_callbacks(self):
        if not self.HAS_LEGACY_LANGCHAIN:
            self.skipTest("langchain_classic not available")

        FakeChatModel = self.FakeChatModel

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
        agent = self.initialize_agent([search], llm, agent=self.AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=False)
        try:
            agent.run("test query")
        except Exception:
            pass

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)

    def test_legacy_agent_error_triggers_callback(self):
        if not self.HAS_LEGACY_LANGCHAIN:
            self.skipTest("langchain_classic not available")

        FakeChatModel = self.FakeChatModel

        class FailingChatModel(FakeChatModel):
            def _generate(self, messages, stop=None, run_manager=None, **kwargs):
                raise ValueError("LLM failed")

        @tool
        def search(query: str) -> str:
            """Search."""
            return "result"

        llm = FailingChatModel(messages=iter([]))
        agent = self.initialize_agent([search], llm, agent=self.AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=False)
        try:
            agent.run("test")
        except Exception:
            pass

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), 0)
        error_spans = [s for s in spans if s.status.status_code == StatusCode.ERROR]
        self.assertGreater(len(error_spans), 0)

    def test_provider_from_type_prefix(self):
        FakeChatModel = self.FakeChatModel

        class OpenAIStyleModel(FakeChatModel):
            def _get_invocation_params(self, stop=None, **kwargs):
                return {"_type": "openai-chat"}

        llm = OpenAIStyleModel(messages=iter([AIMessage(content="Done.")]))
        llm.invoke("test")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].attributes.get(GEN_AI_PROVIDER_NAME), "openai")

    def test_provider_from_serialized_id(self):
        FakeChatModel = self.FakeChatModel

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
        FakeChatModel = self.FakeChatModel

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

    def test_chat_model_propagates_to_parent_agent(self):
        @tool
        def dummy_tool(query: str) -> str:
            """Dummy tool."""
            return "done"

        llm = self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))

        if create_agent:
            agent = create_agent(llm, [dummy_tool], name="TestAgent")
            agent.invoke({"messages": [("human", "test")]})
        elif initialize_agent:
            agent = initialize_agent([dummy_tool], llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION)
            agent.invoke({"input": "test"})
        else:
            self.skipTest("no agent API available")

        spans = self.span_exporter.get_finished_spans()
        agent_span = next((s for s in spans if "invoke_agent" in s.name), None)
        self.assertIsNotNone(agent_span)
        self.assertEqual(agent_span.attributes[GEN_AI_REQUEST_MODEL], "test-model-id")
        self.assertEqual(agent_span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.7)
        self.assertEqual(agent_span.attributes[GEN_AI_PROVIDER_NAME], "openai")

    def test_text_completion_propagates_to_parent_agent(self):
        if not self.HAS_LEGACY_LANGCHAIN:
            self.skipTest("langchain_classic not available")

        @tool
        def search(query: str) -> str:
            """Search."""
            return "result"

        llm = FakeListLLM(responses=["Final Answer: done"])
        agent = self.initialize_agent([search], llm, agent=self.AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=False)
        try:
            agent.run("test")
        except Exception:
            pass

        spans = self.span_exporter.get_finished_spans()
        agent_span = next((s for s in spans if "invoke_agent" in s.name), None)
        self.assertIsNotNone(agent_span)
        self.assertIsNotNone(agent_span.attributes.get(GEN_AI_REQUEST_MODEL))

    def test_create_agent_detects_agent_with_and_without_name(self):
        if create_agent:
            cases = [
                (None, "LangGraph"),
                ("CustomAgentName", "CustomAgentName"),
            ]
            for agent_name, expected_name in cases:
                with self.subTest(agent_name=agent_name):
                    self.span_exporter.clear()

                    @tool
                    def dummy_tool(query: str) -> str:
                        """Dummy tool."""
                        return "done"

                    llm = self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))
                    agent = (
                        create_agent(llm, [dummy_tool], name=agent_name)
                        if agent_name
                        else create_agent(llm, [dummy_tool])
                    )
                    agent.invoke({"messages": [("human", "test")]})

                    spans = self.span_exporter.get_finished_spans()
                    agent_spans = [s for s in spans if "invoke_agent" in s.name]
                    self.assertGreater(len(agent_spans), 0)
                    self.assertEqual(
                        agent_spans[0].attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.INVOKE_AGENT.value
                    )
                    self.assertEqual(agent_spans[0].attributes[GEN_AI_AGENT_NAME], expected_name)
        elif initialize_agent:

            @tool
            def dummy_tool(query: str) -> str:
                """Dummy tool."""
                return "done"

            llm = self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))
            agent = initialize_agent([dummy_tool], llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION)
            agent.invoke({"input": "test"})

            spans = self.span_exporter.get_finished_spans()
            agent_spans = [s for s in spans if "invoke_agent" in s.name]
            self.assertGreater(len(agent_spans), 0)
            self.assertEqual(
                agent_spans[0].attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.INVOKE_AGENT.value
            )

    def test_chat_model_records_input_and_output_messages(self):
        llm = self.FakeChatModel(messages=iter([AIMessage(content="Hello!")]))
        llm.invoke("Say hello")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]

        self.assertIsNotNone(span.attributes.get(GEN_AI_INPUT_MESSAGES))
        messages = json.loads(span.attributes[GEN_AI_INPUT_MESSAGES])
        validate_otel_genai_schema(messages, "gen-ai-input-messages")
        self.assertEqual(messages[0]["role"], "user")
        self.assertEqual(messages[0]["parts"][0]["type"], "text")

        self.assertIsNotNone(span.attributes.get(GEN_AI_OUTPUT_MESSAGES))
        output = json.loads(span.attributes[GEN_AI_OUTPUT_MESSAGES])
        validate_otel_genai_schema(output, "gen-ai-output-messages")
        self.assertEqual(output[0]["role"], "assistant")
        self.assertEqual(output[0]["parts"][0]["type"], "text")
        self.assertIn("Final Answer: Done.", output[0]["parts"][0]["content"])

    def test_system_instructions_schema_validation(self):
        llm = self.FakeChatModel(messages=iter([AIMessage(content="Hi!")]))
        llm.invoke([SystemMessage(content="You are a helpful assistant."), HumanMessage(content="Hello")])

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]

        self.assertIsNotNone(span.attributes.get(GEN_AI_SYSTEM_INSTRUCTIONS))
        instructions = json.loads(span.attributes[GEN_AI_SYSTEM_INSTRUCTIONS])
        validate_otel_genai_schema(instructions, "gen-ai-system-instructions")
        self.assertEqual(instructions[0]["type"], "text")
        self.assertIn("helpful assistant", instructions[0]["content"])

    def test_text_completion_records_input_messages_and_output(self):
        llm = FakeListLLM(responses=["hello"])
        llm.invoke("test prompt")

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]

        self.assertIsNotNone(span.attributes.get(GEN_AI_INPUT_MESSAGES))
        input_messages = json.loads(span.attributes[GEN_AI_INPUT_MESSAGES])
        self.assertIsInstance(input_messages, list)
        self.assertEqual(input_messages[0]["role"], "user")
        self.assertIn("test prompt", input_messages[0]["parts"][0]["content"])

        self.assertIsNotNone(span.attributes.get(GEN_AI_OUTPUT_MESSAGES))
        output = json.loads(span.attributes[GEN_AI_OUTPUT_MESSAGES])
        self.assertIsInstance(output, list)
        self.assertGreater(len(output), 0)
        self.assertEqual(output[0]["role"], "assistant")
        self.assertEqual(output[0]["parts"][0]["content"], "hello")

    def test_langgraph_attributes_on_agent_spans(self):
        @tool
        def dummy_tool(query: str) -> str:
            """Dummy tool."""
            return "done"

        llm = self.FakeChatModel(messages=iter([AIMessage(content="Done.")]))

        if create_agent:
            agent = create_agent(llm, [dummy_tool], name="TestAgent")
            agent.invoke({"messages": [("human", "test")]})
        elif initialize_agent:
            agent = initialize_agent([dummy_tool], llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION)
            agent.invoke({"input": "test"})
        else:
            self.skipTest("no agent API available")

        spans = self.span_exporter.get_finished_spans()
        agent_span = next((s for s in spans if "invoke_agent" in s.name), None)
        self.assertIsNotNone(agent_span)
        chat_spans = [s for s in spans if "chat" in s.name]
        self.assertGreater(len(chat_spans), 0)


if __name__ == "__main__":
    unittest.main()
