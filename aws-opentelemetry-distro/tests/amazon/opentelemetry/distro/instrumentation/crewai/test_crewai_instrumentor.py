# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import unittest
from typing import Any, Dict, Sequence
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.instrumentation.crewai import CrewAIInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
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
    GEN_AI_TOOL_TYPE,
)


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
        """Test that errors during crew execution are properly recorded in spans."""
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
        self.assertEqual(error_span.attributes.get(ERROR_TYPE), "RuntimeError")

    def _run_crew_kickoff_test(self, model: str, provider: str, model_id: str):
        """Helper to test crew kickoff with different model providers."""
        mock_tool_call = MagicMock()
        mock_tool_call.id = "call_123"
        mock_tool_call.function = MagicMock()
        mock_tool_call.function.name = "get_greeting"
        mock_tool_call.function.arguments = '{"name": "World"}'

        mock_llm = MagicMock()
        mock_llm.provider = model.split("/")[0] if "/" in model else None
        mock_llm.model = model_id
        mock_llm.temperature = 0.7
        mock_llm.max_tokens = 1024
        mock_llm.call.side_effect = [
            [mock_tool_call],
            "Thought: I now know the final answer\nFinal Answer: Hello! Welcome!",
        ]

        with patch.object(self.LLM, "__new__", return_value=mock_llm):
            crew = self._create_test_crew(model)
            crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        crew_id = str(crew.id)
        agent_id = str(crew.agents[0].id)
        crew_span = next((s for s in spans if s.name == "crew_kickoff GreetingCrew"), None)

        self._assert_span_attributes(
            self,
            spans,
            "crew_kickoff GreetingCrew",
            {
                GEN_AI_OPERATION_NAME: "invoke_agent",
                GEN_AI_AGENT_NAME: "GreetingCrew",
                GEN_AI_AGENT_ID: crew_id,
            },
        )
        self.assertNotIn(GEN_AI_PROVIDER_NAME, crew_span.attributes)  # type: ignore
        self.assertNotIn(GEN_AI_REQUEST_MODEL, crew_span.attributes)  # type: ignore
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, crew_span.attributes)  # type: ignore
        self.assertIn("get_greeting", crew_span.attributes[GEN_AI_TOOL_DEFINITIONS])  # type: ignore
        self._assert_span_attributes(
            self,
            spans,
            "invoke_agent Greeter",
            {
                GEN_AI_OPERATION_NAME: "invoke_agent",
                GEN_AI_AGENT_NAME: "Greeter",
                GEN_AI_PROVIDER_NAME: provider,
                GEN_AI_REQUEST_MODEL: model_id,
                GEN_AI_AGENT_ID: agent_id,
                GEN_AI_AGENT_DESCRIPTION: "Greet the user",
                GEN_AI_REQUEST_TEMPERATURE: 0.7,
                GEN_AI_REQUEST_MAX_TOKENS: 1024,
                GEN_AI_SYSTEM_INSTRUCTIONS: "You are a friendly greeter.",
            },
        )
        self._assert_span_attributes(
            self,
            spans,
            "execute_tool get_greeting",
            {
                GEN_AI_OPERATION_NAME: "execute_tool",
                GEN_AI_TOOL_NAME: "get_greeting",
                GEN_AI_TOOL_TYPE: "function",
            },
        )
        tool_span = next((s for s in spans if s.name == "execute_tool get_greeting"), None)
        self.assertIsNotNone(tool_span)
        assert tool_span is not None and tool_span.attributes is not None
        self.assertIn("get_greeting", tool_span.attributes[GEN_AI_TOOL_DESCRIPTION])
        self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, tool_span.attributes)
        self.assertEqual('"Hello, World!"', tool_span.attributes[GEN_AI_TOOL_CALL_RESULT])

    def _create_test_crew(self, model: str):
        @self.tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        llm = self.LLM(model=model, temperature=0.7)

        agent = self.Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting],
            verbose=True,
        )

        task = self.Task(
            description="Greet the user warmly.",
            expected_output="A friendly greeting.",
            agent=agent,
        )

        return self.Crew(
            name="GreetingCrew",
            agents=[agent],
            tasks=[task],
            verbose=True,
        )

    @staticmethod
    def _assert_span_attributes(
        test: TestCase,
        spans: Sequence[ReadableSpan],
        expected_name: str,
        expected_attrs: Dict[str, Any],
    ) -> None:
        span: ReadableSpan | None = next((s for s in spans if s.name == expected_name), None)
        test.assertIsNotNone(span, f"Span '{expected_name}' not found")
        assert span is not None
        test.assertIsNotNone(span.attributes)
        assert span.attributes is not None

        for key, value in expected_attrs.items():
            test.assertIn(key, span.attributes, f"Attribute '{key}' missing from span '{expected_name}'")
            test.assertEqual(span.attributes.get(key), value)
