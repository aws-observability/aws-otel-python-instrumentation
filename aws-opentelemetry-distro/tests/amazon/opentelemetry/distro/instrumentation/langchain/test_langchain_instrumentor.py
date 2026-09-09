# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import sys
import unittest
from unittest import TestCase
from unittest.mock import patch

from conftest import call_mock_llm, validate_otel_genai_schema

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
from amazon.opentelemetry.distro.instrumentation.langchain.wrapper import PregelWrapper
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
    GEN_AI_REQUEST_CHOICE_COUNT,
    GEN_AI_REQUEST_FREQUENCY_PENALTY,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_PRESENCE_PENALTY,
    GEN_AI_REQUEST_SEED,
    GEN_AI_REQUEST_STOP_SEQUENCES,
    GEN_AI_REQUEST_STREAM,
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
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
    GEN_AI_WORKFLOW_NAME,
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
        stop: tuple[str, ...] = ("STOP",)

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
                "stop": self.stop,
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
        self.assertEqual(span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("STOP",))

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

        input_messages = json.loads(agent_span.attributes[GEN_AI_INPUT_MESSAGES])
        validate_otel_genai_schema(input_messages, "gen-ai-input-messages")
        self.assertTrue(
            any(
                part.get("type") == "text" and "What's the weather in Paris?" in part.get("content", "")
                for message in input_messages
                if message.get("role") == "user"
                for part in message.get("parts", [])
            )
        )
        output_messages = json.loads(agent_span.attributes[GEN_AI_OUTPUT_MESSAGES])
        validate_otel_genai_schema(output_messages, "gen-ai-output-messages")
        self.assertTrue(any(message.get("role") == "assistant" for message in output_messages))

    def test_raw_stategraph_stream_creates_invoke_agent_span(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="RawStateGraph")

        list(graph.stream({"messages": [HumanMessage(content="Hello")]}))

        spans = self.span_exporter.get_finished_spans()
        agent_spans = [span for span in spans if span.name == "invoke_agent RawStateGraph"]
        self.assertEqual(len(agent_spans), 1)
        self.assertEqual(
            agent_spans[0].attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.INVOKE_AGENT.value,
        )
        self.assertEqual(agent_spans[0].attributes[GEN_AI_AGENT_NAME], "RawStateGraph")
        self.assertIn(GEN_AI_INPUT_MESSAGES, agent_spans[0].attributes)
        self.assertIn(GEN_AI_OUTPUT_MESSAGES, agent_spans[0].attributes)
        self.assertIsNone(PregelWrapper.get_active_agent_name())

    def test_raw_stategraph_astream_creates_invoke_agent_span(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="AsyncRawStateGraph")

        async def consume_graph():
            return [chunk async for chunk in graph.astream({"messages": [HumanMessage(content="Hello")]})]

        asyncio.run(consume_graph())

        spans = self.span_exporter.get_finished_spans()
        agent_spans = [span for span in spans if span.name == "invoke_agent AsyncRawStateGraph"]
        self.assertEqual(len(agent_spans), 1)
        self.assertEqual(
            agent_spans[0].attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.INVOKE_AGENT.value,
        )
        self.assertEqual(agent_spans[0].attributes[GEN_AI_AGENT_NAME], "AsyncRawStateGraph")
        self.assertIn(GEN_AI_INPUT_MESSAGES, agent_spans[0].attributes)
        self.assertIn(GEN_AI_OUTPUT_MESSAGES, agent_spans[0].attributes)
        self.assertIsNone(PregelWrapper.get_active_agent_name())

    def test_raw_stategraph_invoke_uses_stream_fallback(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="InvokedRawStateGraph")

        result = graph.invoke({"messages": [HumanMessage(content="Hello")]})

        self.assertEqual(result["messages"][-1].content, "Done.")
        agent_spans = [
            span for span in self.span_exporter.get_finished_spans() if span.name == "invoke_agent InvokedRawStateGraph"
        ]
        self.assertEqual(len(agent_spans), 1)
        self.assertIsNone(PregelWrapper.get_active_agent_name())

    def test_raw_stategraph_ainvoke_uses_astream_fallback(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="AsyncInvokedRawStateGraph")

        result = asyncio.run(graph.ainvoke({"messages": [HumanMessage(content="Hello")]}))

        self.assertEqual(result["messages"][-1].content, "Done.")
        agent_spans = [
            span
            for span in self.span_exporter.get_finished_spans()
            if span.name == "invoke_agent AsyncInvokedRawStateGraph"
        ]
        self.assertEqual(len(agent_spans), 1)
        self.assertIsNone(PregelWrapper.get_active_agent_name())

    def test_raw_stategraph_stream_close_preserves_application_behavior(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        executed_nodes = []

        def first_node(_):
            self.assertEqual(PregelWrapper.get_active_agent_name(), "ClosableRawStateGraph")
            executed_nodes.append("first")
            return {"messages": [AIMessage(content="First node complete.")]}

        def second_node(_):
            executed_nodes.append("second")
            return {"messages": [AIMessage(content="Second node complete.")]}

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("first", first_node)
        graph_builder.add_node("second", second_node)
        graph_builder.add_edge(START, "first")
        graph_builder.add_edge("first", "second")
        graph_builder.add_edge("second", END)
        graph = graph_builder.compile(name="ClosableRawStateGraph")

        stream = graph.stream(
            {"messages": [HumanMessage(content="Hello")]},
            stream_mode="updates",
        )
        first_update = next(stream)
        self.assertEqual(first_update["first"]["messages"][0].content, "First node complete.")
        self.assertIsNone(PregelWrapper.get_active_agent_name())

        stream.close()

        self.assertEqual(executed_nodes, ["first"])
        self.assertIsNone(PregelWrapper.get_active_agent_name())
        agent_spans = [
            span
            for span in self.span_exporter.get_finished_spans()
            if span.name == "invoke_agent ClosableRawStateGraph"
        ]
        self.assertEqual(len(agent_spans), 1)

    def test_raw_stategraph_astream_aclose_preserves_application_behavior(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        executed_nodes = []

        async def first_node(_):
            self.assertEqual(PregelWrapper.get_active_agent_name(), "ClosableAsyncRawStateGraph")
            executed_nodes.append("first")
            return {"messages": [AIMessage(content="First async node complete.")]}

        async def second_node(_):
            executed_nodes.append("second")
            return {"messages": [AIMessage(content="Second async node complete.")]}

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("first", first_node)
        graph_builder.add_node("second", second_node)
        graph_builder.add_edge(START, "first")
        graph_builder.add_edge("first", "second")
        graph_builder.add_edge("second", END)
        graph = graph_builder.compile(name="ClosableAsyncRawStateGraph")

        async def consume_first_update():
            stream = graph.astream(
                {"messages": [HumanMessage(content="Hello")]},
                stream_mode="updates",
            )
            first_update = await anext(stream)
            self.assertEqual(first_update["first"]["messages"][0].content, "First async node complete.")
            self.assertIsNone(PregelWrapper.get_active_agent_name())
            await stream.aclose()

        asyncio.run(consume_first_update())

        self.assertEqual(executed_nodes, ["first"])
        self.assertIsNone(PregelWrapper.get_active_agent_name())
        agent_spans = [
            span
            for span in self.span_exporter.get_finished_spans()
            if span.name == "invoke_agent ClosableAsyncRawStateGraph"
        ]
        self.assertEqual(len(agent_spans), 1)

    def test_raw_stategraph_stream_propagates_application_error(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        expected_error = RuntimeError("sync graph failed")

        def failing_node(_):
            self.assertEqual(PregelWrapper.get_active_agent_name(), "FailingRawStateGraph")
            raise expected_error

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("fail", failing_node)
        graph_builder.add_edge(START, "fail")
        graph_builder.add_edge("fail", END)
        graph = graph_builder.compile(name="FailingRawStateGraph")

        with self.assertRaises(RuntimeError) as raised:
            list(graph.stream({"messages": [HumanMessage(content="Hello")]}))

        self.assertIs(raised.exception, expected_error)
        self.assertIsNone(PregelWrapper.get_active_agent_name())
        agent_span = next(
            span for span in self.span_exporter.get_finished_spans() if span.name == "invoke_agent FailingRawStateGraph"
        )
        self.assertEqual(agent_span.status.status_code, StatusCode.ERROR)

    def test_raw_stategraph_astream_propagates_application_error(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        expected_error = RuntimeError("async graph failed")

        async def failing_node(_):
            self.assertEqual(PregelWrapper.get_active_agent_name(), "FailingAsyncRawStateGraph")
            raise expected_error

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("fail", failing_node)
        graph_builder.add_edge(START, "fail")
        graph_builder.add_edge("fail", END)
        graph = graph_builder.compile(name="FailingAsyncRawStateGraph")

        async def consume_graph():
            return [chunk async for chunk in graph.astream({"messages": [HumanMessage(content="Hello")]})]

        with self.assertRaises(RuntimeError) as raised:
            asyncio.run(consume_graph())

        self.assertIs(raised.exception, expected_error)
        self.assertIsNone(PregelWrapper.get_active_agent_name())
        agent_span = next(
            span
            for span in self.span_exporter.get_finished_spans()
            if span.name == "invoke_agent FailingAsyncRawStateGraph"
        )
        self.assertEqual(agent_span.status.status_code, StatusCode.ERROR)

    def test_explicit_agent_marker_takes_precedence_over_pregel_fallback(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="RawStateGraph").with_config(
            {
                "metadata": {
                    "otel_agent_span": True,
                    "agent_name": "ExplicitStateGraphAgent",
                    "agent_type": "stategraph",
                }
            }
        )

        graph.invoke({"messages": [HumanMessage(content="Hello")]})

        spans = self.span_exporter.get_finished_spans()
        agent_spans = [span for span in spans if span.name == "invoke_agent ExplicitStateGraphAgent"]
        self.assertEqual(len(agent_spans), 1)
        self.assertEqual(agent_spans[0].attributes[GEN_AI_AGENT_NAME], "ExplicitStateGraphAgent")
        self.assertEqual(
            agent_spans[0].attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.INVOKE_AGENT.value,
        )

    def test_otel_agent_span_false_does_not_disable_pregel_fallback(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="AutomaticStateGraphAgent").with_config(
            {"metadata": {"otel_agent_span": False}}
        )

        graph.invoke({"messages": [HumanMessage(content="Hello")]})

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual(
            len([span for span in spans if span.name == "invoke_agent AutomaticStateGraphAgent"]),
            1,
        )

    def test_explicit_workflow_marker_overrides_pregel_fallback(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="ExplicitStateGraphWorkflow").with_config(
            {"metadata": {"otel_workflow_span": True}}
        )

        graph.invoke({"messages": [HumanMessage(content="Hello")]})

        spans = self.span_exporter.get_finished_spans()
        workflow_spans = [span for span in spans if span.name == "invoke_workflow ExplicitStateGraphWorkflow"]
        self.assertEqual(len(workflow_spans), 1)
        self.assertEqual(
            workflow_spans[0].attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.INVOKE_WORKFLOW.value,
        )
        self.assertEqual(workflow_spans[0].attributes[GEN_AI_WORKFLOW_NAME], "ExplicitStateGraphWorkflow")
        self.assertIn(GEN_AI_INPUT_MESSAGES, workflow_spans[0].attributes)
        self.assertIn(GEN_AI_OUTPUT_MESSAGES, workflow_spans[0].attributes)
        self.assertFalse(any(span.name == "invoke_agent ExplicitStateGraphWorkflow" for span in spans))

    def test_explicit_workflow_marker_overrides_incidental_agent_metadata(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Done.")]})
        graph_builder.add_edge(START, "respond")
        graph_builder.add_edge("respond", END)
        graph = graph_builder.compile(name="WorkflowWithAgentMetadata").with_config(
            {
                "metadata": {
                    "otel_workflow_span": True,
                    "agent_name": "UpstreamAgentName",
                }
            }
        )

        graph.invoke({"messages": [HumanMessage(content="Hello")]})

        spans = self.span_exporter.get_finished_spans()
        workflow_span = next(span for span in spans if span.name == "invoke_workflow WorkflowWithAgentMetadata")
        self.assertEqual(
            workflow_span.attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.INVOKE_WORKFLOW.value,
        )
        self.assertNotIn(GEN_AI_AGENT_NAME, workflow_span.attributes)
        self.assertFalse(any(span.name.startswith("invoke_agent") for span in spans))

    def test_skipped_chains_parent_model_span_to_nearest_ancestor(self):
        try:
            from langchain_aws import ChatBedrockConverse
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph or langchain-aws is not available")

        def invoke_graph(client):
            llm = ChatBedrockConverse(model="anthropic.claude-fable-5", client=client)
            nested_runnables = (
                RunnableLambda(lambda state: state["messages"])
                | RunnableLambda(lambda messages: messages)
                | llm
                | RunnableLambda(lambda message: {"messages": [message]})
            )

            graph_builder = StateGraph(MessagesState)
            graph_builder.add_node("nested_runnables", nested_runnables)
            graph_builder.add_edge(START, "nested_runnables")
            graph_builder.add_edge("nested_runnables", END)
            graph = graph_builder.compile(name="SkippedChainParentingAgent")

            graph.invoke({"messages": [HumanMessage(content="Hello")]})

        call_mock_llm("bedrock", invoke_llm_callback=invoke_graph)

        spans = self.span_exporter.get_finished_spans()
        agent_span = next(span for span in spans if span.name == "invoke_agent SkippedChainParentingAgent")
        chat_span = next(
            span for span in spans if span.attributes.get(GEN_AI_OPERATION_NAME) == GenAiOperationNameValues.CHAT.value
        )
        self.assertEqual(chat_span.context.trace_id, agent_span.context.trace_id)
        self.assertEqual(chat_span.parent.span_id, agent_span.context.span_id)
        self.assertLess(agent_span.start_time, chat_span.start_time)
        self.assertGreater(agent_span.end_time, chat_span.end_time)
        self.assertFalse(any("Runnable" in span.name for span in spans))

    def test_stategraph_with_create_agents_and_nested_stategraph_preserves_agent_hierarchy(self):
        try:
            from langchain_aws import ChatBedrockConverse
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph or langchain-aws is not available")
        if not create_agent:
            self.skipTest("langchain create_agent is not available")

        @tool
        def search_knowledge_base(query: str) -> str:
            """Search the internal knowledge base."""
            return f"Found research for: {query}"

        @tool
        def check_draft(draft: str) -> str:
            """Check a draft for correctness."""
            return f"Draft approved: {draft}"

        def text_response(text: str) -> dict:
            return {
                "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
                "metrics": {"latencyMs": 1},
            }

        def tool_response(tool_use_id: str, tool_name: str, tool_input: dict) -> dict:
            return {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "toolUse": {
                                    "toolUseId": tool_use_id,
                                    "name": tool_name,
                                    "input": tool_input,
                                }
                            }
                        ],
                    }
                },
                "stopReason": "tool_use",
                "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
                "metrics": {"latencyMs": 1},
            }

        def invoke_graph(client):
            llm = ChatBedrockConverse(model="anthropic.claude-fable-5", client=client)
            research_agent = create_agent(llm, tools=[search_knowledge_base], name="ResearchCreateAgent")
            writer_agent = create_agent(llm, tools=[], name="WriterCreateAgent")
            review_agent = create_agent(llm, tools=[check_draft], name="ReviewCreateAgent")

            synthesis_builder = StateGraph(MessagesState)
            synthesis_builder.add_node(
                "synthesize",
                lambda _: {"messages": [AIMessage(content="StateGraph synthesis complete.")]},
            )
            synthesis_builder.add_edge(START, "synthesize")
            synthesis_builder.add_edge("synthesize", END)
            nested_synthesis_graph = synthesis_builder.compile(name="NestedSynthesisStateGraph")

            supervisor_builder = StateGraph(MessagesState)
            supervisor_builder.add_node(
                "prepare",
                RunnableLambda(lambda _: {"messages": [HumanMessage(content="Prepared for the nested agent.")]}),
            )
            supervisor_builder.add_node("research_agent", research_agent)
            supervisor_builder.add_node("nested_synthesis_graph", nested_synthesis_graph)
            supervisor_builder.add_node("writer_agent", writer_agent)
            supervisor_builder.add_node("review_agent", review_agent)
            supervisor_builder.add_edge(START, "prepare")
            supervisor_builder.add_edge("prepare", "research_agent")
            supervisor_builder.add_edge("research_agent", "nested_synthesis_graph")
            supervisor_builder.add_edge("nested_synthesis_graph", "writer_agent")
            supervisor_builder.add_edge("writer_agent", "review_agent")
            supervisor_builder.add_edge("review_agent", END)
            supervisor = supervisor_builder.compile(name="MixedStateGraphSupervisor")

            supervisor.invoke({"messages": [HumanMessage(content="Start the mixed graph.")]})

        call_mock_llm(
            "bedrock",
            invoke_llm_callback=invoke_graph,
            responses=[
                tool_response("research-tool-use", "search_knowledge_base", {"query": "agent tracing"}),
                text_response("Research complete."),
                text_response("Draft complete."),
                tool_response("review-tool-use", "check_draft", {"draft": "Draft complete."}),
                text_response("Review complete."),
            ],
        )

        spans = self.span_exporter.get_finished_spans()
        supervisor_span = next(span for span in spans if span.name == "invoke_agent MixedStateGraphSupervisor")
        create_agent_spans = [
            next(span for span in spans if span.name == f"invoke_agent {agent_name}")
            for agent_name in ("ResearchCreateAgent", "WriterCreateAgent", "ReviewCreateAgent")
        ]
        synthesis_graph_span = next(span for span in spans if span.name == "invoke_agent NestedSynthesisStateGraph")
        chat_spans = [
            span for span in spans if span.attributes.get(GEN_AI_OPERATION_NAME) == GenAiOperationNameValues.CHAT.value
        ]
        tool_spans = [
            span
            for span in spans
            if span.attributes.get(GEN_AI_OPERATION_NAME) == GenAiOperationNameValues.EXECUTE_TOOL.value
        ]
        model_step_spans = [span for span in spans if span.name == "chain model"]
        tool_step_spans = [span for span in spans if span.name == "chain tools"]

        for create_agent_span in create_agent_spans:
            self.assertEqual(create_agent_span.parent.span_id, supervisor_span.context.span_id)
        self.assertEqual(synthesis_graph_span.parent.span_id, supervisor_span.context.span_id)
        self.assertCountEqual(
            [
                (span.attributes["langgraph.node"], span.attributes["langgraph.step"])
                for span in (*create_agent_spans, synthesis_graph_span)
            ],
            [
                ("research_agent", 2),
                ("writer_agent", 4),
                ("review_agent", 5),
                ("nested_synthesis_graph", 3),
            ],
        )
        self.assertEqual(len(chat_spans), 5)
        self.assertEqual(len(model_step_spans), 5)
        self.assertEqual(len(tool_step_spans), 2)
        self.assertCountEqual(
            [(span.attributes["langgraph.node"], span.attributes["langgraph.step"]) for span in model_step_spans],
            [("model", 1), ("model", 3), ("model", 1), ("model", 1), ("model", 3)],
        )
        self.assertCountEqual(
            [(span.attributes["langgraph.node"], span.attributes["langgraph.step"]) for span in tool_step_spans],
            [("tools", 2), ("tools", 2)],
        )
        model_step_parent_span_ids = [span.parent.span_id for span in model_step_spans]
        self.assertEqual(model_step_parent_span_ids.count(create_agent_spans[0].context.span_id), 2)
        self.assertEqual(model_step_parent_span_ids.count(create_agent_spans[1].context.span_id), 1)
        self.assertEqual(model_step_parent_span_ids.count(create_agent_spans[2].context.span_id), 2)
        self.assertEqual(
            {span.parent.span_id for span in chat_spans},
            {span.context.span_id for span in model_step_spans},
        )
        self.assertEqual(
            {span.attributes[GEN_AI_TOOL_NAME] for span in tool_spans},
            {"search_knowledge_base", "check_draft"},
        )
        self.assertEqual(
            {span.parent.span_id for span in tool_step_spans},
            {create_agent_spans[0].context.span_id, create_agent_spans[2].context.span_id},
        )
        self.assertEqual(
            {span.parent.span_id for span in tool_spans},
            {span.context.span_id for span in tool_step_spans},
        )
        self.assertEqual(
            {
                span.context.trace_id
                for span in (
                    supervisor_span,
                    synthesis_graph_span,
                    *create_agent_spans,
                    *model_step_spans,
                    *tool_step_spans,
                    *chat_spans,
                    *tool_spans,
                )
            },
            {supervisor_span.context.trace_id},
        )
        self.assertFalse(any("Runnable" in span.name for span in spans))

    def test_nested_raw_stategraph_uses_pregel_fallback_under_explicit_agent(self):
        try:
            from langgraph.graph import END, START, MessagesState, StateGraph
        except ImportError:
            self.skipTest("langgraph is not available")

        child_builder = StateGraph(MessagesState)
        child_builder.add_node("respond", lambda _: {"messages": [AIMessage(content="Child done.")]})
        child_builder.add_edge(START, "respond")
        child_builder.add_edge("respond", END)
        child = child_builder.compile(name="RawChildStateGraph")

        parent_builder = StateGraph(MessagesState)
        parent_builder.add_node("child", child)
        parent_builder.add_edge(START, "child")
        parent_builder.add_edge("child", END)
        parent = parent_builder.compile(name="ExplicitParentStateGraph").with_config(
            {
                "metadata": {
                    "otel_agent_span": True,
                    "agent_name": "ExplicitParentStateGraph",
                    "agent_type": "stategraph",
                }
            }
        )

        parent.invoke({"messages": [HumanMessage(content="Hello")]})

        spans = self.span_exporter.get_finished_spans()
        agent_spans = [span for span in spans if span.name.startswith("invoke_agent")]
        agent_span_names = [span.name for span in agent_spans]
        self.assertEqual(agent_span_names.count("invoke_agent ExplicitParentStateGraph"), 1)
        self.assertEqual(agent_span_names.count("invoke_agent RawChildStateGraph"), 1)
        parent_span = next(span for span in agent_spans if span.name == "invoke_agent ExplicitParentStateGraph")
        child_span = next(span for span in agent_spans if span.name == "invoke_agent RawChildStateGraph")
        self.assertEqual(child_span.context.trace_id, parent_span.context.trace_id)
        self.assertEqual(child_span.parent.span_id, parent_span.context.span_id)

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
        self.assertEqual(span.attributes[GEN_AI_TOOL_CALL_RESULT], result)

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
        messages = [
            SystemMessage(content="You are a helpful assistant."),
            HumanMessage(content="Hello"),
        ]

        cases = [
            (
                ChatBedrock,
                {
                    "model_id": "anthropic.claude-v2",
                    "region_name": "us-east-1",
                    "temperature": 0.7,
                    "max_tokens": 100,
                    "stop": ["STOP"],
                    "streaming": False,
                    "beta_use_converse_api": True,
                    "model_kwargs": {
                        "top_p": 0.9,
                        "additional_model_request_fields": {"top_k": 40},
                    },
                },
                GenAiProviderNameValues.AWS_BEDROCK.value,
            ),
            (
                ChatBedrockConverse,
                {"model": "test", "region_name": "us-east-1"},
                GenAiProviderNameValues.AWS_BEDROCK.value,
            ),
            # LangChain routes gpt-5.6-sol to Responses by default after:
            # https://github.com/langchain-ai/langchain/pull/40133
            # Keep separate cases because some request attributes are Chat Completions-only.
            (ChatOpenAI, {"use_responses_api": False}, GenAiProviderNameValues.OPENAI.value),
            (ChatOpenAI, {"use_responses_api": True}, GenAiProviderNameValues.OPENAI.value),
            (
                AzureChatOpenAI,
                {
                    "api_key": "fake",
                    "azure_endpoint": "https://fake.openai.azure.com",
                    "api_version": "2024-01-01",
                    "model": "gpt-4o",
                    "temperature": 0.0,
                    "top_p": 0.9,
                    "max_completion_tokens": 100,
                    "frequency_penalty": 0.0,
                    "presence_penalty": 0.0,
                    "seed": 0,
                    "streaming": False,
                    "n": 2,
                    "stop": ["STOP"],
                },
                GenAiProviderNameValues.AZURE_AI_OPENAI.value,
            ),
            (
                ChatAnthropic,
                {"anthropic_api_key": "fake", "model_name": "claude-3"},
                GenAiProviderNameValues.ANTHROPIC.value,
            ),
            (
                ChatGoogleGenerativeAI,
                {
                    "google_api_key": "fake",
                    "model": "gemini-pro",
                    "temperature": 0.0,
                    "top_p": 0.9,
                    "top_k": 40,
                    "max_tokens": 100,
                    "n": 2,
                    "stop": ["STOP"],
                    "streaming": False,
                },
                GenAiProviderNameValues.GCP_GEN_AI.value,
            ),
            (
                ChatMistralAI,
                {
                    "api_key": "fake",
                    "model": "mistral-large-latest",
                    "temperature": 0.0,
                    "top_p": 0.9,
                    "max_tokens": 100,
                    "random_seed": 0,
                    "streaming": True,
                },
                GenAiProviderNameValues.MISTRAL_AI.value,
            ),
            (
                ChatGroq,
                {
                    "api_key": "fake",
                    "model": "llama-3.3-70b-versatile",
                    "temperature": 0.5,
                    "max_tokens": 100,
                    "streaming": True,
                    "n": 1,
                    "stop": ["STOP"],
                    "model_kwargs": {
                        "seed": 0,
                        "top_p": 0.0,
                        "frequency_penalty": 0.0,
                        "presence_penalty": 0.0,
                    },
                },
                GenAiProviderNameValues.GROQ.value,
            ),
            (
                ChatCohere,
                {"cohere_api_key": "fake", "model": "command-a-03-2025", "temperature": 0.0},
                GenAiProviderNameValues.COHERE.value,
            ),
            (
                ChatDeepSeek,
                {
                    "api_key": "fake",
                    "model": "deepseek-chat",
                    "temperature": 0.0,
                    "top_p": 0.9,
                    "max_tokens": 100,
                    "frequency_penalty": 0.0,
                    "presence_penalty": 0.0,
                    "seed": 0,
                    "streaming": False,
                    "n": 2,
                    "stop": ["STOP"],
                },
                GenAiProviderNameValues.DEEPSEEK.value,
            ),
            (
                ChatXAI,
                {
                    "api_key": "fake",
                    "model": "grok-4",
                    "temperature": 0.0,
                    "top_p": 0.9,
                    "max_tokens": 100,
                    "frequency_penalty": 0.0,
                    "presence_penalty": 0.0,
                    "seed": 0,
                    "streaming": False,
                    "n": 2,
                    "stop": ["STOP"],
                },
                GenAiProviderNameValues.X_AI.value,
            ),
        ]

        for model_cls, init_kwargs, expected_provider in cases:
            subtest_kwargs = {"model": model_cls.__name__}
            if model_cls is ChatOpenAI:
                subtest_kwargs["api"] = "responses" if init_kwargs["use_responses_api"] else "chat_completions"
            with self.subTest(**subtest_kwargs):
                self.span_exporter.clear()
                expected_request_attributes = {}

                if model_cls is ChatOpenAI:
                    use_responses_api = init_kwargs["use_responses_api"]

                    def invoke_openai(client, use_responses_api=use_responses_api):
                        chat_completions_kwargs = (
                            {
                                "frequency_penalty": 0.5,
                                "presence_penalty": 0.3,
                                "extra_body": {"top_k": 40, "seed": 42},
                                "n": 2,
                                "stop": "STOP",
                            }
                            if not use_responses_api
                            else {}
                        )
                        ChatOpenAI(
                            model="gpt-5.6-sol",
                            api_key="fake-key",
                            client=client.chat.completions,
                            root_client=client,
                            temperature=1.0,
                            top_p=0.9,
                            max_completion_tokens=100,
                            streaming=False,
                            use_responses_api=use_responses_api,
                            **chat_completions_kwargs,
                        ).invoke(messages)

                    call_mock_llm("openai", invoke_llm_callback=invoke_openai)
                    expected_request_attributes = {
                        GEN_AI_REQUEST_MODEL: "gpt-5.6-sol",
                        GEN_AI_REQUEST_TEMPERATURE: 1.0,
                        GEN_AI_REQUEST_TOP_P: 0.9,
                        GEN_AI_REQUEST_MAX_TOKENS: 100,
                        GEN_AI_REQUEST_STREAM: False,
                    }
                    if not use_responses_api:
                        expected_request_attributes.update(
                            {
                                GEN_AI_REQUEST_TOP_K: 40,
                                GEN_AI_REQUEST_FREQUENCY_PENALTY: 0.5,
                                GEN_AI_REQUEST_PRESENCE_PENALTY: 0.3,
                                GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                                GEN_AI_REQUEST_SEED: 42,
                                GEN_AI_REQUEST_CHOICE_COUNT: 2,
                            }
                        )
                elif model_cls is ChatAnthropic:

                    def invoke_anthropic(client):
                        llm = ChatAnthropic(
                            model="claude-opus-5",
                            api_key="fake-key",
                            max_tokens=100,
                            temperature=1.0,
                            top_p=0.9,
                            top_k=40,
                            stop=["STOP"],
                        )
                        # LangChain has no public seam for replacing this provider client with the mock transport.
                        llm._client = client
                        llm.invoke(messages)

                    call_mock_llm("anthropic", invoke_llm_callback=invoke_anthropic)
                    expected_request_attributes = {
                        GEN_AI_REQUEST_MODEL: "claude-opus-5",
                        GEN_AI_REQUEST_TEMPERATURE: 1.0,
                        GEN_AI_REQUEST_TOP_P: 0.9,
                        GEN_AI_REQUEST_TOP_K: 40,
                        GEN_AI_REQUEST_MAX_TOKENS: 100,
                        GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                        GEN_AI_REQUEST_STREAM: False,
                    }
                elif model_cls is ChatBedrockConverse:

                    def invoke_bedrock(client):
                        ChatBedrockConverse(
                            model="anthropic.claude-fable-5",
                            client=client,
                            temperature=0.7,
                            top_p=0.9,
                            max_tokens=100,
                            stop=["STOP"],
                            additional_model_request_fields={"top_k": 40},
                        ).invoke(messages)

                    call_mock_llm("bedrock", invoke_llm_callback=invoke_bedrock)
                    expected_request_attributes = {
                        GEN_AI_REQUEST_MODEL: "anthropic.claude-fable-5",
                        GEN_AI_REQUEST_TEMPERATURE: 0.7,
                        GEN_AI_REQUEST_TOP_P: 0.9,
                        GEN_AI_REQUEST_TOP_K: 40,
                        GEN_AI_REQUEST_MAX_TOKENS: 100,
                        GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                    }
                else:
                    llm = model_cls(**init_kwargs)
                    with (
                        patch.object(type(llm), "_generate", return_value=fake_result),
                        patch.object(type(llm), "_should_stream", return_value=False),
                    ):
                        if model_cls is ChatMistralAI:
                            llm.invoke("test", stop=["STOP"])
                        elif model_cls is ChatCohere:
                            llm.invoke(
                                "test",
                                stop=["STOP"],
                                max_tokens=100,
                                p=0.0,
                                k=0,
                                seed=0,
                                num_generations=2,
                            )
                        else:
                            llm.invoke("test")
                    if model_cls is ChatMistralAI:
                        expected_request_attributes = {
                            GEN_AI_REQUEST_MODEL: "mistral-large-latest",
                            GEN_AI_REQUEST_TEMPERATURE: 0.0,
                            GEN_AI_REQUEST_TOP_P: 0.9,
                            GEN_AI_REQUEST_MAX_TOKENS: 100,
                            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                            GEN_AI_REQUEST_SEED: 0,
                            GEN_AI_REQUEST_STREAM: True,
                        }
                    elif model_cls is ChatBedrock:
                        expected_request_attributes = {
                            GEN_AI_REQUEST_MODEL: "anthropic.claude-v2",
                            GEN_AI_REQUEST_TEMPERATURE: 0.7,
                            GEN_AI_REQUEST_TOP_P: 0.9,
                            GEN_AI_REQUEST_TOP_K: 40,
                            GEN_AI_REQUEST_MAX_TOKENS: 100,
                            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                            GEN_AI_REQUEST_STREAM: False,
                        }
                    elif model_cls is ChatGoogleGenerativeAI:
                        expected_request_attributes = {
                            GEN_AI_REQUEST_MODEL: llm.model,
                            GEN_AI_REQUEST_TEMPERATURE: 0.0,
                            GEN_AI_REQUEST_TOP_P: 0.9,
                            GEN_AI_REQUEST_TOP_K: 40,
                            GEN_AI_REQUEST_MAX_TOKENS: 100,
                            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                            GEN_AI_REQUEST_CHOICE_COUNT: 2,
                            GEN_AI_REQUEST_STREAM: False,
                        }
                    elif model_cls is ChatGroq:
                        expected_request_attributes = {
                            GEN_AI_REQUEST_MODEL: "llama-3.3-70b-versatile",
                            GEN_AI_REQUEST_TEMPERATURE: 0.5,
                            GEN_AI_REQUEST_TOP_P: 0.0,
                            GEN_AI_REQUEST_MAX_TOKENS: 100,
                            GEN_AI_REQUEST_FREQUENCY_PENALTY: 0.0,
                            GEN_AI_REQUEST_PRESENCE_PENALTY: 0.0,
                            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                            GEN_AI_REQUEST_SEED: 0,
                            GEN_AI_REQUEST_CHOICE_COUNT: 1,
                            GEN_AI_REQUEST_STREAM: True,
                        }
                    elif model_cls is ChatCohere:
                        expected_request_attributes = {
                            GEN_AI_REQUEST_MODEL: "command-a-03-2025",
                            GEN_AI_REQUEST_TEMPERATURE: 0.0,
                            GEN_AI_REQUEST_TOP_P: 0.0,
                            GEN_AI_REQUEST_TOP_K: 0,
                            GEN_AI_REQUEST_MAX_TOKENS: 100,
                            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                            GEN_AI_REQUEST_SEED: 0,
                            GEN_AI_REQUEST_CHOICE_COUNT: 2,
                        }
                    elif model_cls in (AzureChatOpenAI, ChatDeepSeek, ChatXAI):
                        expected_request_attributes = {
                            GEN_AI_REQUEST_MODEL: init_kwargs["model"],
                            GEN_AI_REQUEST_TEMPERATURE: 0.0,
                            GEN_AI_REQUEST_TOP_P: 0.9,
                            GEN_AI_REQUEST_MAX_TOKENS: 100,
                            GEN_AI_REQUEST_FREQUENCY_PENALTY: 0.0,
                            GEN_AI_REQUEST_PRESENCE_PENALTY: 0.0,
                            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
                            GEN_AI_REQUEST_SEED: 0,
                            GEN_AI_REQUEST_CHOICE_COUNT: 2,
                            GEN_AI_REQUEST_STREAM: False,
                        }

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
                for attribute, value in expected_request_attributes.items():
                    self.assertEqual(attrs.get(attribute), value)

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

    def test_multimodal_input_and_output_messages(self):
        from langchain_anthropic import ChatAnthropic
        from langchain_aws import ChatBedrock, ChatBedrockConverse
        from langchain_cohere import ChatCohere
        from langchain_deepseek import ChatDeepSeek
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_groq import ChatGroq
        from langchain_mistralai import ChatMistralAI
        from langchain_openai import AzureChatOpenAI, ChatOpenAI
        from langchain_xai import ChatXAI

        models = [
            (ChatBedrock, {"model_id": "test", "region_name": "us-east-1"}),
            (ChatBedrockConverse, {"model": "test", "region_name": "us-east-1"}),
            (ChatOpenAI, {"api_key": "fake"}),
            (
                AzureChatOpenAI,
                {"api_key": "fake", "azure_endpoint": "https://fake.openai.azure.com", "api_version": "2024-01-01"},
            ),
            (ChatAnthropic, {"anthropic_api_key": "fake", "model_name": "claude-3"}),
            (ChatGoogleGenerativeAI, {"google_api_key": "fake", "model": "gemini-pro"}),
            (ChatMistralAI, {"api_key": "fake", "model": "test"}),
            (ChatGroq, {"api_key": "fake", "model": "test"}),
            (ChatCohere, {"cohere_api_key": "fake", "model": "test"}),
            (ChatDeepSeek, {"api_key": "fake", "model": "test"}),
            (ChatXAI, {"api_key": "fake", "model": "test"}),
        ]

        input_content = [
            {"type": "text", "text": "describe"},
            {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
        ]
        expected_input_parts = [
            {"type": "text", "content": "describe"},
            {"type": "uri", "modality": "image", "uri": "https://example.com/cat.png"},
            {"type": "blob", "modality": "image", "mime_type": "image/png", "content": "AAAA"},
        ]

        output_content = [
            {"type": "text", "text": "The answer is blue."},
            {"type": "thinking", "thinking": "Let me reason about this."},
            {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
        ]
        expected_output_parts = [
            {"type": "text", "content": "The answer is blue."},
            {"type": "reasoning", "content": "Let me reason about this."},
            {"type": "uri", "modality": "image", "uri": "https://example.com/cat.png"},
            {"type": "blob", "modality": "image", "mime_type": "image/png", "content": "AAAA"},
        ]

        fake_result = ChatResult(
            generations=[
                ChatGeneration(message=AIMessage(content=output_content), generation_info={"finish_reason": "stop"})
            ],
            llm_output={"model_name": "test", "token_usage": {"prompt_tokens": 1, "completion_tokens": 1}},
        )

        for model_cls, init_kwargs in models:
            with self.subTest(model=model_cls.__name__):
                self.span_exporter.clear()
                llm = model_cls(**init_kwargs)
                with patch.object(type(llm), "_generate", return_value=fake_result):
                    llm.invoke([HumanMessage(content=input_content)])

                chat_spans = [
                    s
                    for s in self.span_exporter.get_finished_spans()
                    if "chat" in s.name or "text_completion" in s.name
                ]
                self.assertGreaterEqual(len(chat_spans), 1, f"No chat span for {model_cls.__name__}")
                attrs = chat_spans[0].attributes

                input_messages = json.loads(attrs[GEN_AI_INPUT_MESSAGES])
                validate_otel_genai_schema(input_messages, "gen-ai-input-messages")
                self.assertEqual(input_messages[0]["parts"], expected_input_parts)

                output_messages = json.loads(attrs[GEN_AI_OUTPUT_MESSAGES])
                validate_otel_genai_schema(output_messages, "gen-ai-output-messages")
                self.assertEqual(output_messages[0]["parts"], expected_output_parts)

                for parts in (input_messages[0]["parts"], output_messages[0]["parts"]):
                    for part in parts:
                        value = part.get("content", "")
                        if isinstance(value, str):
                            self.assertFalse(value.lstrip().startswith("[{") and "'type'" in value)

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

    def test_token_usage_provider_key_variants(self):
        from langchain_aws import ChatBedrock, ChatBedrockConverse
        from langchain_openai import ChatOpenAI

        def result_with_llm_output(usage, usage_metadata=None):
            return ChatResult(
                generations=[
                    ChatGeneration(
                        message=AIMessage(content="Hello!", usage_metadata=usage_metadata),
                        generation_info={"finish_reason": "stop"},
                    )
                ],
                llm_output={"model_name": "test", "token_usage": usage},
            )

        def result_with_usage_metadata(usage_metadata):
            return ChatResult(
                generations=[
                    ChatGeneration(
                        message=AIMessage(content="Hello!", usage_metadata=usage_metadata),
                        generation_info={"finish_reason": "stop"},
                    )
                ],
                llm_output=None,
            )

        # (model class, init kwargs, fake result matching that provider's real usage shape, expected in/out)
        cases = [
            (
                ChatOpenAI,
                {"api_key": "fake"},
                result_with_llm_output(
                    {"prompt_tokens": 11, "completion_tokens": 22},
                    {
                        "input_tokens": 11,
                        "output_tokens": 22,
                        "total_tokens": 33,
                        "input_token_details": {"cache_read": 3},
                        "output_token_details": {"reasoning": 4},
                    },
                ),
                11,
                22,
                {
                    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS: 3,
                    GEN_AI_USAGE_REASONING_OUTPUT_TOKENS: 4,
                },
            ),
            (
                ChatBedrock,
                {"model_id": "test", "region_name": "us-east-1"},
                result_with_llm_output(
                    {"prompt_tokens": 33, "completion_tokens": 44},
                    {
                        "input_tokens": 33,
                        "output_tokens": 44,
                        "total_tokens": 77,
                        "input_token_details": {"cache_read": 5, "cache_creation": 6},
                    },
                ),
                33,
                44,
                {
                    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS: 5,
                    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS: 6,
                },
            ),
            (
                ChatBedrockConverse,
                {"model": "test", "region_name": "us-east-1"},
                result_with_usage_metadata(
                    {
                        "input_tokens": 55,
                        "output_tokens": 66,
                        "total_tokens": 121,
                        "input_token_details": {"cache_read": 7, "cache_creation": 8},
                    }
                ),
                55,
                66,
                {
                    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS: 7,
                    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS: 8,
                },
            ),
        ]

        try:
            from langchain_ibm import ChatWatsonx

            cases.append(
                (
                    ChatWatsonx,
                    {
                        "model_id": "test",
                        "url": "https://us-south.ml.cloud.ibm.com",
                        "apikey": "fake",
                        "project_id": "p",
                    },
                    result_with_llm_output({"input_token_count": 77, "generated_token_count": 88}),
                    77,
                    88,
                    {},
                )
            )
        except ImportError:
            pass

        for model_cls, init_kwargs, fake_result, expected_input, expected_output, expected_details in cases:
            with self.subTest(model=model_cls.__name__):
                self.span_exporter.clear()
                llm = model_cls(**init_kwargs)
                with patch.object(type(llm), "_generate", return_value=fake_result):
                    llm.invoke("Say hello")

                spans = self.span_exporter.get_finished_spans()
                chat_spans = [s for s in spans if "chat" in s.name]
                self.assertGreaterEqual(len(chat_spans), 1, f"No chat span for {model_cls.__name__}")
                attrs = chat_spans[0].attributes
                self.assertEqual(attrs[GEN_AI_USAGE_INPUT_TOKENS], expected_input)
                self.assertEqual(attrs[GEN_AI_USAGE_OUTPUT_TOKENS], expected_output)
                self.assertEqual(
                    attrs.get(GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS),
                    expected_details.get(GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS),
                )
                self.assertEqual(
                    attrs.get(GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS),
                    expected_details.get(GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS),
                )
                self.assertEqual(
                    attrs.get(GEN_AI_USAGE_REASONING_OUTPUT_TOKENS),
                    expected_details.get(GEN_AI_USAGE_REASONING_OUTPUT_TOKENS),
                )


if __name__ == "__main__":
    unittest.main()
