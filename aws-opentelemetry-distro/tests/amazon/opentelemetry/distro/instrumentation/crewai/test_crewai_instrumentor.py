# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import os
import sys
import unittest
from typing import Any, Dict, Optional, Sequence
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.instrumentation.crewai import CrewAIInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_MESSAGE
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
)

_OTEL_SCHEMA_BASE = "https://opentelemetry.io/docs/specs/semconv/gen-ai"
_SCHEMA_CACHE: dict = {}


def _validate_otel_schema(data: list, schema_name: str) -> None:
    import urllib.request  # pylint: disable=import-outside-toplevel

    import jsonschema  # pylint: disable=import-outside-toplevel

    if schema_name not in _SCHEMA_CACHE:
        url = f"{_OTEL_SCHEMA_BASE}/{schema_name}.json"
        with urllib.request.urlopen(url) as resp:
            _SCHEMA_CACHE[schema_name] = json.loads(resp.read())
    jsonschema.validate(data, _SCHEMA_CACHE[schema_name])


# https://pypi.org/project/crewai/
@unittest.skipIf(sys.version_info < (3, 10) or sys.version_info >= (3, 14), "crewai requires >=3.10, <3.14")
class TestCrewAIInstrumentor(TestCase):
    def setUp(self):
        # pylint: disable=import-outside-toplevel
        from crewai import LLM, Agent, Crew, Task
        from crewai.tools import tool

        self.LLM = LLM
        self.Agent = Agent
        self.Crew = Crew
        self.Task = Task
        self.tool = tool

        self._env_backup = {}
        self._set_env("CREWAI_DISABLE_TELEMETRY", "true")
        self._set_env("OPENAI_API_KEY", "fake-key")
        self._set_env("ANTHROPIC_API_KEY", "fake-key")
        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.instrumentor = CrewAIInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        self.span_exporter.clear()
        self._restore_env()

    def _set_env(self, key: str, value: str):
        self._env_backup[key] = os.environ.get(key)
        os.environ[key] = value

    def _restore_env(self):
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_bedrock_crew_kickoff(self):
        self._run_crew_kickoff_test(
            "bedrock/anthropic.claude-3-haiku-20240307-v1:0",
            "aws.bedrock",
            "anthropic.claude-3-haiku-20240307-v1:0",
        )

    def test_openai_crew_kickoff(self):
        self._run_crew_kickoff_test("openai/gpt-4", "openai", "gpt-4")

    def test_anthropic_crew_kickoff(self):
        self._run_crew_kickoff_test("anthropic/claude-3-sonnet-20240229", "anthropic", "claude-3-sonnet-20240229")

    def test_crew_kickoff_error_handling(self):
        mock_llm = MagicMock()
        mock_llm.provider = "openai"
        mock_llm.model = "gpt-4"
        mock_llm.temperature = 0.7
        mock_llm.max_tokens = 1024
        mock_llm.call.side_effect = RuntimeError("LLM call failed")

        with patch.object(self.LLM, "__new__", return_value=mock_llm):
            crew = self._create_test_crew("openai/gpt-4")
            with self.assertRaises(RuntimeError):
                crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        error_span = next((s for s in spans if s.status.status_code.name == "ERROR"), None)
        self.assertIsNotNone(error_span, "Expected at least one span with ERROR status")
        self.assertIn("LLM call failed", error_span.attributes.get(ERROR_MESSAGE, ""))

    def test_text_based_tool_calling(self):
        mock_llm = MagicMock()
        mock_llm.provider = "openai"
        mock_llm.model = "gpt-4"
        mock_llm.temperature = 0.7
        mock_llm.max_tokens = 1024
        mock_llm.supports_function_calling.return_value = False
        mock_llm.supports_stop_words.return_value = True
        mock_llm.call.side_effect = [
            'Thought: I should greet the user.\nAction: get_greeting\nAction Input: {"name": "World"}',
            "Thought: I now know the final answer\nFinal Answer: Hello! Welcome!",
        ]

        with patch.object(self.LLM, "__new__", return_value=mock_llm):
            crew = self._create_test_crew("openai/gpt-4")
            crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        tool_span = next((s for s in spans if s.name == "execute_tool get_greeting"), None)
        self.assertIsNotNone(tool_span)
        self.assertIsNotNone(tool_span.attributes)
        self._assert_span_attributes(
            spans,
            "execute_tool get_greeting",
            {
                GEN_AI_OPERATION_NAME: "execute_tool",
                GEN_AI_TOOL_NAME: "get_greeting",
                GEN_AI_TOOL_TYPE: "function",
                GEN_AI_PROVIDER_NAME: "openai",
                GEN_AI_REQUEST_MODEL: "gpt-4",
            },
        )
        self.assertIn(GEN_AI_TOOL_DESCRIPTION, tool_span.attributes)
        self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, tool_span.attributes)
        self.assertIn(GEN_AI_TOOL_CALL_RESULT, tool_span.attributes)

    def test_single_agent_no_tools(self):
        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(role="Simple", goal="Say hi", backstory="Simple agent.", llm=llm, tools=[])
        task = self.Task(description="Say hi.", expected_output="A greeting.", agent=agent)
        crew = self.Crew(name="SimpleCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", return_value=self._mock_response("Final Answer: Hello!")):
            crew.kickoff()

        crew_span = self._find_span("crew_kickoff SimpleCrew")
        agent_span = self._find_span("invoke_agent Simple")
        chat_span = self._find_span("chat gpt-4")
        self.assertIsNotNone(crew_span)
        self.assertIsNotNone(agent_span)
        self.assertIsNotNone(chat_span)
        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(chat_span, agent_span)
        self._assert_spans_all_ended()

    def test_single_agent_multiple_tool_calls(self):
        @self.tool
        def tool_a(value: str) -> str:
            """Tool A."""
            return f"A: {value}"

        @self.tool
        def tool_b(value: str) -> str:
            """Tool B."""
            return f"B: {value}"

        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(role="Multi", goal="Use tools", backstory="Agent.", llm=llm, tools=[tool_a, tool_b])
        task = self.Task(description="Use tools.", expected_output="Results.", agent=agent)
        crew = self.Crew(name="MultiToolCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", side_effect=[
            self._mock_response(tool_calls=[self._mock_tool_call("c1", "tool_a", '{"value": "1"}')]),
            self._mock_response(tool_calls=[self._mock_tool_call("c2", "tool_b", '{"value": "2"}')]),
            self._mock_response("Final Answer: Done!"),
        ]):
            crew.kickoff()

        self.assertIsNotNone(self._find_span("execute_tool tool_a"))
        self.assertIsNotNone(self._find_span("execute_tool tool_b"))
        agent_span = self._find_span("invoke_agent Multi")
        self._assert_span_parent(self._find_span("execute_tool tool_a"), agent_span)
        self._assert_span_parent(self._find_span("execute_tool tool_b"), agent_span)
        self._assert_spans_all_ended()

    def test_multiple_agents_sequential_tasks(self):
        @self.tool
        def t1(v: str) -> str:
            """T1."""
            return f"t1:{v}"

        @self.tool
        def t2(v: str) -> str:
            """T2."""
            return f"t2:{v}"

        llm1 = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm1.supports_function_calling = lambda: True
        llm2 = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm2.supports_function_calling = lambda: True

        a1 = self.Agent(role="A1", goal="Task1", backstory="First.", llm=llm1, tools=[t1])
        a2 = self.Agent(role="A2", goal="Task2", backstory="Second.", llm=llm2, tools=[t2])
        task1 = self.Task(description="Do task1.", expected_output="R1.", agent=a1)
        task2 = self.Task(description="Do task2.", expected_output="R2.", agent=a2)
        crew = self.Crew(name="MultiAgent", agents=[a1, a2], tasks=[task1, task2])

        with patch("litellm.completion", side_effect=[
            self._mock_response(tool_calls=[self._mock_tool_call("c1", "t1", '{"v":"x"}')]),
            self._mock_response("Final Answer: R1"),
            self._mock_response(tool_calls=[self._mock_tool_call("c2", "t2", '{"v":"y"}')]),
            self._mock_response("Final Answer: R2"),
        ]):
            crew.kickoff()

        crew_span = self._find_span("crew_kickoff MultiAgent")
        a1_span = self._find_span("invoke_agent A1")
        a2_span = self._find_span("invoke_agent A2")
        self.assertIsNotNone(a1_span)
        self.assertIsNotNone(a2_span)
        self._assert_span_parent(a1_span, crew_span)
        self._assert_span_parent(a2_span, crew_span)
        self.assertIsNotNone(self._find_span("execute_tool t1"))
        self.assertIsNotNone(self._find_span("execute_tool t2"))
        self._assert_spans_all_ended()

    def test_multiple_agents_shared_llm(self):
        shared_llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        shared_llm.supports_function_calling = lambda: True
        a1 = self.Agent(role="Shared1", goal="G1", backstory="S1.", llm=shared_llm, tools=[])
        a2 = self.Agent(role="Shared2", goal="G2", backstory="S2.", llm=shared_llm, tools=[])
        task1 = self.Task(description="T1.", expected_output="R1.", agent=a1)
        task2 = self.Task(description="T2.", expected_output="R2.", agent=a2)
        crew = self.Crew(name="SharedLLM", agents=[a1, a2], tasks=[task1, task2])

        with patch("litellm.completion", side_effect=[
            self._mock_response("Final Answer: R1"),
            self._mock_response("Final Answer: R2"),
        ]):
            crew.kickoff()

        self.assertIsNotNone(self._find_span("invoke_agent Shared1"))
        self.assertIsNotNone(self._find_span("invoke_agent Shared2"))
        chat_spans = [s for s in self.span_exporter.get_finished_spans() if "chat" in s.name]
        self.assertGreaterEqual(len(chat_spans), 2)
        self._assert_spans_all_ended()

    def test_llm_call_failure(self):
        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(role="Failing", goal="Fail", backstory="Will fail.", llm=llm, tools=[])
        task = self.Task(description="Fail.", expected_output="N/A.", agent=agent)
        crew = self.Crew(name="FailCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", side_effect=RuntimeError("Service unavailable")):
            with self.assertRaises(RuntimeError):
                crew.kickoff()

        error_spans = [s for s in self.span_exporter.get_finished_spans() if s.status.status_code.name == "ERROR"]
        self.assertGreater(len(error_spans), 0)
        self.assertIn(ERROR_MESSAGE, error_spans[0].attributes)
        self._assert_spans_all_ended()

    def test_tool_execution_error(self):
        @self.tool
        def bad_tool(value: str) -> str:
            """A tool that fails."""
            raise ValueError(f"Tool failed: {value}")

        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(role="ToolErr", goal="Handle errors", backstory="Agent.", llm=llm, tools=[bad_tool])
        task = self.Task(description="Use bad tool.", expected_output="Result.", agent=agent)
        crew = self.Crew(name="ToolErrCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", side_effect=[
            self._mock_response(tool_calls=[self._mock_tool_call("c1", "bad_tool", '{"value": "x"}')]),
            self._mock_response("Final Answer: Handled error."),
        ]):
            crew.kickoff()

        self.assertIsNotNone(self._find_span("crew_kickoff ToolErrCrew"))
        self.assertIsNotNone(self._find_span("invoke_agent ToolErr"))
        self._assert_spans_all_ended()

    def test_large_message_payloads(self):
        @self.tool
        def big_tool(size: int) -> str:
            """Returns large data."""
            return "X" * size

        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        large_args = json.dumps({"size": 10000, "extra": "Y" * 5000})
        agent = self.Agent(role="BigData", goal="Handle big data", backstory="Agent.", llm=llm, tools=[big_tool])
        task = self.Task(description="Big data.", expected_output="Result.", agent=agent)
        crew = self.Crew(name="BigCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", side_effect=[
            self._mock_response(tool_calls=[self._mock_tool_call("c1", "big_tool", large_args)]),
            self._mock_response("Final Answer: " + "Z" * 5000),
        ]):
            crew.kickoff()

        tool_span = self._find_span("execute_tool big_tool")
        self.assertIsNotNone(tool_span)
        parsed_args = json.loads(tool_span.attributes[GEN_AI_TOOL_CALL_ARGUMENTS])
        self.assertEqual(parsed_args["size"], 10000)
        self._assert_spans_all_ended()

    def test_multiple_sequential_crew_kickoffs(self):
        @self.tool
        def seq_tool(v: str) -> str:
            """Sequential tool."""
            return f"seq:{v}"

        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(role="Seq", goal="Run twice", backstory="Agent.", llm=llm, tools=[seq_tool])
        task = self.Task(description="Sequential.", expected_output="Result.", agent=agent)
        crew = self.Crew(name="SeqCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", side_effect=[
            self._mock_response(tool_calls=[self._mock_tool_call("c1", "seq_tool", '{"v":"1"}')]),
            self._mock_response("Final Answer: Run 1"),
            self._mock_response(tool_calls=[self._mock_tool_call("c2", "seq_tool", '{"v":"2"}')]),
            self._mock_response("Final Answer: Run 2"),
        ]):
            crew.kickoff()
            first_count = len(self.span_exporter.get_finished_spans())
            crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), first_count)
        crew_spans = [s for s in spans if "crew_kickoff SeqCrew" in s.name]
        self.assertEqual(len(crew_spans), 2)
        tool_spans = [s for s in spans if "execute_tool seq_tool" in s.name]
        self.assertEqual(len(tool_spans), 2)
        self._assert_spans_all_ended()

    def test_async_crew_kickoff(self):
        @self.tool
        def async_tool(name: str) -> str:
            """Async tool."""
            return f"Hello, {name}!"

        llm = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(role="AsyncAgent", goal="Greet", backstory="Async.", llm=llm, tools=[async_tool])
        task = self.Task(description="Greet.", expected_output="Greeting.", agent=agent)
        crew = self.Crew(name="AsyncCrew", agents=[agent], tasks=[task])

        responses = iter([
            self._mock_response(tool_calls=[self._mock_tool_call("c1", "async_tool", '{"name":"World"}')]),
            self._mock_response("Final Answer: Hi!"),
        ])

        async def mock_acompletion(*args, **kwargs):
            return next(responses)

        async def run():
            with patch("litellm.acompletion", side_effect=mock_acompletion):
                return await crew.akickoff()

        asyncio.run(run())

        crew_span = self._find_span("crew_kickoff AsyncCrew")
        agent_span = self._find_span("invoke_agent AsyncAgent")
        tool_span = self._find_span("execute_tool async_tool")
        self.assertIsNotNone(crew_span)
        self.assertIsNotNone(agent_span)
        self.assertIsNotNone(tool_span)
        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(tool_span, agent_span)
        self._assert_spans_all_ended()

    def test_async_multiple_agents(self):
        llm1 = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm1.supports_function_calling = lambda: True
        llm2 = self.LLM(model="openai/gpt-4", is_litellm=True)
        llm2.supports_function_calling = lambda: True
        a1 = self.Agent(role="AsyncA1", goal="G1", backstory="A1.", llm=llm1, tools=[])
        a2 = self.Agent(role="AsyncA2", goal="G2", backstory="A2.", llm=llm2, tools=[])
        t1 = self.Task(description="T1.", expected_output="R1.", agent=a1)
        t2 = self.Task(description="T2.", expected_output="R2.", agent=a2)
        crew = self.Crew(name="AsyncMulti", agents=[a1, a2], tasks=[t1, t2])

        responses = iter([
            self._mock_response("Final Answer: R1"),
            self._mock_response("Final Answer: R2"),
        ])

        async def mock_acompletion(*args, **kwargs):
            return next(responses)

        async def run():
            with patch("litellm.acompletion", side_effect=mock_acompletion):
                return await crew.akickoff()

        asyncio.run(run())

        crew_span = self._find_span("crew_kickoff AsyncMulti")
        a1_span = self._find_span("invoke_agent AsyncA1")
        a2_span = self._find_span("invoke_agent AsyncA2")
        self.assertIsNotNone(a1_span)
        self.assertIsNotNone(a2_span)
        self._assert_span_parent(a1_span, crew_span)
        self._assert_span_parent(a2_span, crew_span)
        self._assert_spans_all_ended()

    def _run_crew_kickoff_test(self, model: str, provider: str, model_id: str):
        test_tracer = self.tracer_provider.get_tracer("test")
        tc = self._mock_tool_call()

        @self.tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            with test_tracer.start_as_current_span("custom_downstream_span"):
                return f"Hello, {name}!"

        llm = self.LLM(model=model, is_litellm=True, temperature=0.7, max_tokens=1024)
        llm.supports_function_calling = lambda: True
        agent = self.Agent(
            role="Greeter", goal="Greet the user", backstory="You are a friendly greeter.",
            llm=llm, tools=[get_greeting], verbose=True,
        )
        task = self.Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=agent)
        crew = self.Crew(name="GreetingCrew", agents=[agent], tasks=[task], verbose=True)

        with patch("litellm.completion", side_effect=[
            self._mock_response(content="", tool_calls=[tc]),
            self._mock_response(content="Thought: I now know the final answer\nFinal Answer: Hello! Welcome!"),
        ]):
            crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        crew_span = next((s for s in spans if s.name == "crew_kickoff GreetingCrew"), None)
        agent_span = next((s for s in spans if s.name == "invoke_agent Greeter"), None)
        tool_span = next((s for s in spans if s.name == "execute_tool get_greeting"), None)

        self._assert_span_attributes(spans, "crew_kickoff GreetingCrew", {
            GEN_AI_OPERATION_NAME: "invoke_agent",
            GEN_AI_AGENT_NAME: "GreetingCrew",
            GEN_AI_AGENT_ID: str(crew.id),
        })
        self.assertNotIn(GEN_AI_PROVIDER_NAME, crew_span.attributes)
        self.assertNotIn(GEN_AI_REQUEST_MODEL, crew_span.attributes)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, crew_span.attributes)
        self.assertIn("get_greeting", crew_span.attributes[GEN_AI_TOOL_DEFINITIONS])

        self._assert_span_attributes(spans, "invoke_agent Greeter", {
            GEN_AI_OPERATION_NAME: "invoke_agent",
            GEN_AI_AGENT_NAME: "Greeter",
            GEN_AI_PROVIDER_NAME: provider,
            GEN_AI_REQUEST_MODEL: model_id,
            GEN_AI_AGENT_ID: str(crew.agents[0].id),
            GEN_AI_AGENT_DESCRIPTION: "Greet the user",
            GEN_AI_REQUEST_TEMPERATURE: 0.7,
            GEN_AI_REQUEST_MAX_TOKENS: 1024,
            GEN_AI_SYSTEM_INSTRUCTIONS: "You are a friendly greeter.",
        })

        self._assert_span_attributes(spans, "execute_tool get_greeting", {
            GEN_AI_OPERATION_NAME: "execute_tool",
            GEN_AI_TOOL_NAME: "get_greeting",
            GEN_AI_TOOL_TYPE: "function",
        })
        self.assertIsNotNone(tool_span)
        self.assertIsNotNone(tool_span.attributes)
        self.assertIn("get_greeting", tool_span.attributes[GEN_AI_TOOL_DESCRIPTION])
        self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, tool_span.attributes)
        self.assertEqual('"Hello, World!"', tool_span.attributes[GEN_AI_TOOL_CALL_RESULT])

        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(tool_span, agent_span)

        custom_span = next((s for s in spans if s.name == "custom_downstream_span"), None)
        self.assertIsNotNone(custom_span, "custom_downstream_span not found")
        self._assert_span_parent(custom_span, tool_span)

        chat_span = next(
            (s for s in spans if s.name == f"chat {model_id}" and s.attributes.get(GEN_AI_OUTPUT_MESSAGES)), None,
        )
        self.assertIsNotNone(chat_span, f"chat {model_id} span with output not found")
        input_messages = json.loads(chat_span.attributes[GEN_AI_INPUT_MESSAGES])
        _validate_otel_schema(input_messages, "gen-ai-input-messages")
        self.assertTrue(any(m["role"] == "user" for m in input_messages))
        system_instructions = json.loads(chat_span.attributes[GEN_AI_SYSTEM_INSTRUCTIONS])
        _validate_otel_schema(system_instructions, "gen-ai-system-instructions")
        self.assertTrue(any("friendly greeter" in i.get("content", "") for i in system_instructions))
        output_messages = json.loads(chat_span.attributes[GEN_AI_OUTPUT_MESSAGES])
        _validate_otel_schema(output_messages, "gen-ai-output-messages")
        self.assertEqual(chat_span.attributes.get(GEN_AI_RESPONSE_MODEL), model_id)

    def _create_test_crew(self, model: str):
        test_tracer = self.tracer_provider.get_tracer("test")

        @self.tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            with test_tracer.start_as_current_span("custom_downstream_span"):
                return f"Hello, {name}!"

        llm = self.LLM(model=model, temperature=0.7)
        agent = self.Agent(
            role="Greeter", goal="Greet the user", backstory="You are a friendly greeter.",
            llm=llm, tools=[get_greeting], verbose=True,
        )
        task = self.Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=agent)
        return self.Crew(name="GreetingCrew", agents=[agent], tasks=[task], verbose=True)

    def _assert_span_attributes(
        self,
        spans: Sequence[ReadableSpan],
        expected_name: str,
        expected_attrs: Dict[str, Any],
    ) -> None:
        span: ReadableSpan | None = next((s for s in spans if s.name == expected_name), None)
        self.assertIsNotNone(span, f"Span '{expected_name}' not found")
        self.assertIsNotNone(span.attributes)  # type: ignore[union-attr]

        for key, value in expected_attrs.items():
            self.assertIn(key, span.attributes, f"Attribute '{key}' missing from span '{expected_name}'")
            self.assertEqual(span.attributes.get(key), value)  # type: ignore[union-attr]


    def _assert_span_parent(self, child: ReadableSpan, parent: ReadableSpan):
        self.assertIsNotNone(child.parent)
        self.assertEqual(
            format(child.parent.span_id, "016x"),
            format(parent.context.span_id, "016x"),
        )

    def _assert_spans_all_ended(self):
        for span in self.span_exporter.get_finished_spans():
            self.assertIsNotNone(span.end_time, f"Span {span.name} was not ended")

    def _find_span(self, name_contains: str) -> Optional[ReadableSpan]:
        return next((s for s in self.span_exporter.get_finished_spans() if name_contains in s.name), None)

    @staticmethod
    def _mock_response(content: str = "", tool_calls: Optional[list] = None):
        r = MagicMock()
        r.choices = [MagicMock()]
        r.choices[0].message = MagicMock()
        r.choices[0].message.content = content
        r.choices[0].message.tool_calls = tool_calls or []
        r.usage = MagicMock()
        r.usage.prompt_tokens = 100
        r.usage.completion_tokens = 50
        return r

    @staticmethod
    def _mock_tool_call(call_id: str = "call_123", name: str = "get_greeting", arguments: str = '{"name": "World"}'):
        tc = MagicMock()
        tc.id = call_id
        tc.type = "function"
        tc.function = MagicMock()
        tc.function.name = name
        tc.function.arguments = arguments
        return tc
