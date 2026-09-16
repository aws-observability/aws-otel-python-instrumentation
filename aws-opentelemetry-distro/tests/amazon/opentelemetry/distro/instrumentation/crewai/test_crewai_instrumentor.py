# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# flake8: noqa: E402
# pylint: disable=wrong-import-position
import asyncio
import json
import os
import sys
import threading
import unittest
from typing import Any, Dict, Optional, Sequence
from unittest import TestCase
from unittest.mock import MagicMock, patch

from conftest import call_mock_llm, validate_otel_genai_schema

if sys.version_info < (3, 10) or sys.version_info >= (3, 14):
    raise unittest.SkipTest("crewai requires >=3.10, <3.14")

from crewai import LLM, Agent, Crew, Task
from crewai.events import crewai_event_bus
from crewai.events.types.llm_events import LLMCallCompletedEvent, LLMCallStartedEvent, LLMCallType
from crewai.llms.base_llm import BaseLLM, llm_call_context
from crewai.tools import tool

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    GEN_AI_WORKFLOW_NAME,
    OPERATION_INVOKE_WORKFLOW,
)
from amazon.opentelemetry.distro.instrumentation.crewai import CrewAIInstrumentor
from opentelemetry.instrumentation.threading import ThreadingInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_DESCRIPTION,
    GEN_AI_AGENT_ID,
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
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE


# https://pypi.org/project/crewai/
class TestCrewAIInstrumentor(TestCase):
    def setUp(self):
        self._env_backup = {}
        self._set_env("CREWAI_DISABLE_TELEMETRY", "true")
        self._set_env("OPENAI_API_KEY", "fake-key")
        self._set_env("ANTHROPIC_API_KEY", "fake-key")
        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.instrumentor = CrewAIInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self.threading_instrumentor = ThreadingInstrumentor()
        self.threading_instrumentor.instrument()

    def tearDown(self):
        self.threading_instrumentor.uninstrument()
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
            GenAiProviderNameValues.AWS_BEDROCK.value,
            "anthropic.claude-3-haiku-20240307-v1:0",
        )

        self.span_exporter.clear()
        model = "anthropic.claude-fable-5"
        llm = LLM(
            model=f"bedrock/{model}",
            temperature=0.7,
            top_p=0.9,
            max_tokens=100,
            stream=False,
            additional_model_request_fields={"top_k": 40},
            region_name="us-east-1",
            aws_access_key_id="fake-key",
            aws_secret_access_key="fake-key",
        )
        llm.stop = "STOP"

        def invoke_llm(client):
            # CrewAI has no public seam for replacing the provider client with the mock transport.
            if hasattr(llm, "_client"):
                llm._client = client
            else:
                llm.client = client
            llm.call([{"role": "user", "content": "Hello"}])

        call_mock_llm("bedrock", invoke_llm_callback=invoke_llm)

        chat_span = self._find_span(f"chat {model}")
        self.assertIsNotNone(chat_span)
        self.assertEqual(chat_span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.AWS_BEDROCK.value)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.7)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_P], 0.9)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_K], 40)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("STOP",))
        self.assertIs(chat_span.attributes[GEN_AI_REQUEST_STREAM], False)

    def test_openai_crew_kickoff(self):
        self._run_crew_kickoff_test("openai/gpt-4", GenAiProviderNameValues.OPENAI.value, "gpt-4")

        from amazon.opentelemetry.distro.instrumentation.crewai._event_handler import OpenTelemetryEventHandler

        attributes = {}
        OpenTelemetryEventHandler._set_llm_request_span_attributes(
            attributes, LLM(model="openai/gpt-4", is_litellm=True)
        )
        self.assertNotIn(GEN_AI_REQUEST_STOP_SEQUENCES, attributes)

        model = "gpt-5.6-sol"

        for provider, is_litellm in (("openai", False), ("litellm", True)):
            with self.subTest(client=provider):
                self.span_exporter.clear()
                llm = LLM(
                    model=f"openai/{model}",
                    is_litellm=is_litellm,
                    temperature=1.0,
                    top_p=0.9,
                    max_completion_tokens=100,
                    frequency_penalty=0.5,
                    presence_penalty=0.3,
                    seed=42,
                    stream=False,
                    n=2,
                    stop=["STOP"],
                )

                def invoke_llm(client):
                    if client is not None:
                        # CrewAI has no public seam for replacing the provider client with the mock transport.
                        if hasattr(llm, "_client"):
                            llm._client = client
                        else:
                            llm.client = client
                    llm.call([{"role": "user", "content": "Hello"}])

                call_mock_llm(provider, invoke_llm_callback=invoke_llm)

                chat_span = self._find_span(f"chat {model}")
                self.assertIsNotNone(chat_span)
                self.assertEqual(chat_span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.OPENAI.value)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MODEL], model)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TEMPERATURE], 1.0)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_P], 0.9)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_FREQUENCY_PENALTY], 0.5)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_PRESENCE_PENALTY], 0.3)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("STOP",))
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_SEED], 42)
                self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_CHOICE_COUNT], 2)
                self.assertIs(chat_span.attributes[GEN_AI_REQUEST_STREAM], False)

    def test_anthropic_crew_kickoff(self):
        self._run_crew_kickoff_test(
            "anthropic/claude-3-sonnet-20240229",
            GenAiProviderNameValues.ANTHROPIC.value,
            "claude-3-sonnet-20240229",
        )

        self.span_exporter.clear()
        model = "claude-fable-5"
        llm = LLM(
            model=f"anthropic/{model}",
            temperature=1.0,
            top_p=0.9,
            top_k=40,
            max_tokens=100,
            stream=False,
        )
        llm.stop = ["STOP"]
        if hasattr(llm, "stop_sequences"):
            llm.stop_sequences = ["STOP"]

        def invoke_llm(client):
            # CrewAI has no public seam for replacing the provider client with the mock transport.
            if hasattr(llm, "_client"):
                llm._client = client
            else:
                llm.client = client
            llm.call([{"role": "user", "content": "Hello"}])

        call_mock_llm("anthropic", invoke_llm_callback=invoke_llm)

        chat_span = self._find_span(f"chat {model}")
        self.assertIsNotNone(chat_span)
        self.assertEqual(chat_span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.ANTHROPIC.value)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TEMPERATURE], 1.0)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_P], 0.9)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_K], 40)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("STOP",))
        self.assertIs(chat_span.attributes[GEN_AI_REQUEST_STREAM], False)

    def test_llm_call_uses_effective_request_attributes_from_started_event(self):
        if "temperature" not in LLMCallStartedEvent.model_fields:
            self.skipTest("crewai <1.11.0 does not report effective request attributes on LLMCallStartedEvent")

        llm = LLM(
            model="openai/gpt-4",
            is_litellm=True,
            temperature=0.7,
            top_p=0.9,
            max_tokens=200,
            frequency_penalty=0.5,
            presence_penalty=0.3,
            seed=42,
            stream=True,
            n=1,
            stop=["SOURCE_STOP"],
        )
        start_event = LLMCallStartedEvent(
            call_id="c1",
            messages=[{"role": "user", "content": "Hello"}],
            temperature=0.0,
            top_p=0.0,
            max_tokens=100,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            seed=0,
            stream=False,
            n=2,
            stop_sequences=["EVENT_STOP"],
        )
        crewai_event_bus.emit(llm, start_event)
        crewai_event_bus.emit(
            llm,
            LLMCallCompletedEvent(
                call_id="c1",
                response="Hello",
                call_type=LLMCallType.LLM_CALL,
                started_event_id=start_event.event_id,
                finish_reason="length",
                response_id="resp_123",
                usage={
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "cached_prompt_tokens": 3,
                    "cache_creation_tokens": 2,
                    "reasoning_tokens": 4,
                },
            ),
        )

        chat_span = self._find_span("chat gpt-4")
        self.assertIsNotNone(chat_span)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.0)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_P], 0.0)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_FREQUENCY_PENALTY], 0.0)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_PRESENCE_PENALTY], 0.0)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_SEED], 0)
        self.assertIs(chat_span.attributes[GEN_AI_REQUEST_STREAM], False)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_CHOICE_COUNT], 2)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("EVENT_STOP",))
        self.assertEqual(chat_span.attributes[GEN_AI_RESPONSE_FINISH_REASONS], ("length",))
        self.assertEqual(chat_span.attributes[GEN_AI_RESPONSE_ID], "resp_123")
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 5)
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS], 3)
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS], 2)
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_REASONING_OUTPUT_TOKENS], 4)

    def test_azure_crew_kickoff(self):
        self._run_crew_kickoff_test("azure/gpt-4", GenAiProviderNameValues.AZURE_AI_OPENAI.value, "gpt-4")

    def test_google_crew_kickoff(self):
        self._run_crew_kickoff_test("google/gemini-pro", GenAiProviderNameValues.GCP_GEN_AI.value, "gemini-pro")

    def test_groq_crew_kickoff(self):
        self._run_crew_kickoff_test("groq/llama-3", GenAiProviderNameValues.GROQ.value, "llama-3")

    def test_cohere_crew_kickoff(self):
        self._run_crew_kickoff_test("cohere/command-r", GenAiProviderNameValues.COHERE.value, "command-r")

    def test_mistral_crew_kickoff(self):
        self._run_crew_kickoff_test("mistral/mistral-large", GenAiProviderNameValues.MISTRAL_AI.value, "mistral-large")

    def test_deepseek_crew_kickoff(self):
        self._run_crew_kickoff_test("deepseek/deepseek-chat", GenAiProviderNameValues.DEEPSEEK.value, "deepseek-chat")

    def test_perplexity_crew_kickoff(self):
        self._run_crew_kickoff_test("perplexity/sonar-medium", GenAiProviderNameValues.PERPLEXITY.value, "sonar-medium")

    def test_multimodal_input_and_output_messages(self):
        models = [
            "bedrock/anthropic.claude-3-haiku-20240307-v1:0",
            "openai/gpt-4",
            "anthropic/claude-3-sonnet-20240229",
            "google/gemini-pro",
            "groq/llama-3",
            "cohere/command-r",
            "mistral/mistral-large",
            "deepseek/deepseek-chat",
            "perplexity/sonar-medium",
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

        for model in models:
            with self.subTest(model=model):
                self.span_exporter.clear()
                llm = LLM(model=model, is_litellm=True)
                start_event = LLMCallStartedEvent(call_id="c1", messages=[{"role": "user", "content": input_content}])
                crewai_event_bus.emit(llm, start_event)
                crewai_event_bus.emit(
                    llm,
                    LLMCallCompletedEvent(
                        call_id="c1",
                        response=[{"role": "assistant", "content": output_content}],
                        call_type=LLMCallType.LLM_CALL,
                        started_event_id=start_event.event_id,
                    ),
                )

                chat_span = self._find_span("chat")
                self.assertIsNotNone(chat_span)

                input_messages = json.loads(chat_span.attributes[GEN_AI_INPUT_MESSAGES])
                validate_otel_genai_schema(input_messages, "gen-ai-input-messages")
                self.assertEqual(input_messages[0]["parts"], expected_input_parts)

                output_messages = json.loads(chat_span.attributes[GEN_AI_OUTPUT_MESSAGES])
                validate_otel_genai_schema(output_messages, "gen-ai-output-messages")
                self.assertEqual(output_messages[0]["parts"], expected_output_parts)

                for parts in (input_messages[0]["parts"], output_messages[0]["parts"]):
                    for part in parts:
                        value = part.get("content", "")
                        if isinstance(value, str):
                            self.assertFalse(value.lstrip().startswith("[{") and "'type'" in value)

    def test_per_call_token_usage_prefers_event_usage(self):
        if "usage" not in LLMCallCompletedEvent.model_fields:
            self.skipTest("crewai <1.13.0 does not report per-call usage on LLMCallCompletedEvent")

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        # get_token_usage_summary would report a cumulative total; the event usage must win.
        llm.get_token_usage_summary = lambda: MagicMock(prompt_tokens=9999, completion_tokens=8888)

        start_event = LLMCallStartedEvent(call_id="c1", messages=[{"role": "user", "content": "hi"}])
        crewai_event_bus.emit(llm, start_event)
        crewai_event_bus.emit(
            llm,
            LLMCallCompletedEvent(
                call_id="c1",
                response="Hello",
                call_type=LLMCallType.LLM_CALL,
                started_event_id=start_event.event_id,
                usage={"prompt_tokens": 123, "completion_tokens": 45},
            ),
        )

        chat_span = self._find_span("chat")
        self.assertIsNotNone(chat_span)
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 123)
        self.assertEqual(chat_span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 45)

    def test_per_call_token_usage_provider_key_variants(self):
        if "usage" not in LLMCallCompletedEvent.model_fields:
            self.skipTest("crewai <1.13.0 does not report per-call usage on LLMCallCompletedEvent")

        variants = [
            ("openai", {"prompt_tokens": 11, "completion_tokens": 22}),
            ("bedrock", {"inputTokens": 33, "outputTokens": 44, "totalTokens": 77}),
            ("anthropic", {"input_tokens": 55, "output_tokens": 66}),
        ]
        for name, usage in variants:
            with self.subTest(provider=name):
                self.span_exporter.clear()
                llm = LLM(model="openai/gpt-4", is_litellm=True)
                start_event = LLMCallStartedEvent(call_id="c1", messages=[{"role": "user", "content": "hi"}])
                crewai_event_bus.emit(llm, start_event)
                crewai_event_bus.emit(
                    llm,
                    LLMCallCompletedEvent(
                        call_id="c1",
                        response="Hello",
                        call_type=LLMCallType.LLM_CALL,
                        started_event_id=start_event.event_id,
                        usage=usage,
                    ),
                )

                chat_span = self._find_span("chat")
                self.assertIsNotNone(chat_span)
                expected_input = usage.get("prompt_tokens") or usage.get("inputTokens") or usage.get("input_tokens")
                expected_output = (
                    usage.get("completion_tokens") or usage.get("outputTokens") or usage.get("output_tokens")
                )
                self.assertEqual(chat_span.attributes[GEN_AI_USAGE_INPUT_TOKENS], expected_input)
                self.assertEqual(chat_span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], expected_output)

    def test_crew_kickoff_error_handling(self):
        mock_llm = MagicMock(spec=LLM)
        mock_llm.provider = "openai"
        mock_llm.model = "gpt-4"
        mock_llm.temperature = 0.7
        mock_llm.max_tokens = 1024
        mock_llm.stop = []
        mock_llm.call.side_effect = RuntimeError("LLM call failed")

        with patch.object(LLM, "__new__", return_value=mock_llm):
            crew = self._create_test_crew("openai/gpt-4")
            with self.assertRaises(RuntimeError):
                crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        error_span = next((s for s in spans if s.status.status_code.name == "ERROR"), None)
        self.assertIsNotNone(error_span, "Expected at least one span with ERROR status")
        self.assertIn("LLM call failed", error_span.attributes.get(ERROR_TYPE, ""))

    def test_text_based_tool_calling(self):
        mock_llm = MagicMock(spec=LLM)
        mock_llm.provider = "openai"
        mock_llm.model = "gpt-4"
        mock_llm.temperature = 0.7
        mock_llm.max_tokens = 1024
        mock_llm.stop = []
        mock_llm.supports_function_calling.return_value = False
        mock_llm.supports_stop_words.return_value = True
        mock_llm.call.side_effect = [
            'Thought: I should greet the user.\nAction: get_greeting\nAction Input: {"name": "World"}',
            "Thought: I now know the final answer\nFinal Answer: Hello! Welcome!",
        ]

        with patch.object(LLM, "__new__", return_value=mock_llm):
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
        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="Simple", goal="Say hi", backstory="Simple agent.", llm=llm, tools=[])
        task = Task(description="Say hi.", expected_output="A greeting.", agent=agent)
        crew = Crew(name="SimpleCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", return_value=self._mock_response("Final Answer: Hello!")):
            crew.kickoff()

        crew_span = self._find_span("invoke_workflow SimpleCrew")
        agent_span = self._find_span("invoke_agent Simple")
        chat_span = self._find_span("chat gpt-4")
        self.assertIsNotNone(crew_span)
        self.assertIsNotNone(agent_span)
        self.assertIsNotNone(chat_span)
        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(chat_span, agent_span)
        self._assert_spans_all_ended()

    def test_single_agent_multiple_tool_calls(self):
        @tool
        def tool_a(value: str) -> str:
            """Tool A."""
            return f"A: {value}"

        @tool
        def tool_b(value: str) -> str:
            """Tool B."""
            return f"B: {value}"

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="Multi", goal="Use tools", backstory="Agent.", llm=llm, tools=[tool_a, tool_b])
        task = Task(description="Use tools.", expected_output="Results.", agent=agent)
        crew = Crew(name="MultiToolCrew", agents=[agent], tasks=[task])

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(tool_calls=[self._mock_tool_call("c1", "tool_a", '{"value": "1"}')]),
                self._mock_response(tool_calls=[self._mock_tool_call("c2", "tool_b", '{"value": "2"}')]),
                self._mock_response("Final Answer: Done!"),
            ],
        ):
            crew.kickoff()

        self.assertIsNotNone(self._find_span("execute_tool tool_a"))
        self.assertIsNotNone(self._find_span("execute_tool tool_b"))
        agent_span = self._find_span("invoke_agent Multi")
        self._assert_span_parent(self._find_span("execute_tool tool_a"), agent_span)
        self._assert_span_parent(self._find_span("execute_tool tool_b"), agent_span)
        self._assert_spans_all_ended()

    def test_async_task_parenting(self):
        llm_a = LLM(model="openai/gpt-4", is_litellm=True)
        llm_b = LLM(model="openai/gpt-4", is_litellm=True)
        llm_c = LLM(model="openai/gpt-4", is_litellm=True)
        agent_a = Agent(role="AsyncTaskA", goal="Complete A", backstory="Async A.", llm=llm_a, tools=[])
        agent_b = Agent(role="AsyncTaskB", goal="Complete B", backstory="Async B.", llm=llm_b, tools=[])
        agent_c = Agent(role="AsyncTaskC", goal="Combine results", backstory="Combine.", llm=llm_c, tools=[])
        task_a = Task(
            description="Complete async task A.",
            expected_output="A.",
            agent=agent_a,
            async_execution=True,
        )
        task_b = Task(
            description="Complete async task B.",
            expected_output="B.",
            agent=agent_b,
            async_execution=True,
        )
        task_c = Task(
            description="Combine async task results.",
            expected_output="Combined.",
            agent=agent_c,
            context=[task_a, task_b],
        )
        crew = Crew(
            name="AsyncTaskCrew",
            agents=[agent_a, agent_b, agent_c],
            tasks=[task_a, task_b, task_c],
        )

        with patch("litellm.completion", return_value=self._mock_response("Final Answer: Done")):
            crew.kickoff()

        crew_span = self._find_span("invoke_workflow AsyncTaskCrew")
        for role in ("AsyncTaskA", "AsyncTaskB", "AsyncTaskC"):
            self._assert_span_parent(self._find_span(f"invoke_agent {role}"), crew_span)
        self._assert_spans_all_ended()

    def test_multiple_tool_calls(self):
        @tool
        def tool_a(value: str) -> str:
            """Tool A."""
            return f"A: {value}"

        @tool
        def tool_b(value: str) -> str:
            """Tool B."""
            return f"B: {value}"

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="Parallel", goal="Use tools", backstory="Agent.", llm=llm, tools=[tool_a, tool_b])
        crew = Crew(
            name="ParallelToolCrew",
            agents=[agent],
            tasks=[Task(description="Use both tools.", expected_output="Results.", agent=agent)],
        )

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(
                    tool_calls=[
                        self._mock_tool_call("c1", "tool_a", '{"value": "1"}'),
                        self._mock_tool_call("c2", "tool_b", '{"value": "2"}'),
                    ]
                ),
                self._mock_response("Final Answer: Done!"),
            ],
        ):
            crew.kickoff()

        agent_span = self._find_span("invoke_agent Parallel")
        self._assert_span_parent(self._find_span("execute_tool tool_a"), agent_span)
        self._assert_span_parent(self._find_span("execute_tool tool_b"), agent_span)
        chat_spans = [span for span in self.span_exporter.get_finished_spans() if span.name == "chat gpt-4"]
        self.assertEqual(len(chat_spans), 2)
        self.assertTrue(all(span.parent.span_id == agent_span.context.span_id for span in chat_spans))
        tool_call_names = [
            part["name"]
            for span in chat_spans
            if GEN_AI_OUTPUT_MESSAGES in span.attributes
            for message in json.loads(span.attributes[GEN_AI_OUTPUT_MESSAGES])
            for part in message["parts"]
            if part["type"] == "tool_call"
        ]
        self.assertCountEqual(tool_call_names, ["tool_a", "tool_b"])
        self._assert_spans_all_ended()

    def test_native_tool_failure_does_not_end_agent_span(self):
        @tool
        def bad_native_tool(value: str) -> str:
            """A native-provider tool that fails."""
            raise ValueError(f"Tool failed: {value}")

        class NativeToolFailureLLM(BaseLLM):
            def supports_function_calling(self):
                return True

            def call(
                self,
                messages,
                tools=None,
                callbacks=None,
                available_functions=None,
                from_task=None,
                from_agent=None,
                response_model=None,
            ):
                with llm_call_context():
                    native_functions = {bad_native_tool.name: bad_native_tool.func}
                    self._emit_call_started_event(
                        messages,
                        tools,
                        callbacks,
                        native_functions,
                        from_task,
                        from_agent,
                    )
                    self._handle_tool_execution(
                        bad_native_tool.name,
                        {"value": "x"},
                        native_functions,
                        from_task,
                        from_agent,
                    )
                    return "Final Answer: Recovered from tool failure."

        llm = NativeToolFailureLLM(model="native-test", provider="openai")
        agent = Agent(
            role="NativeTool",
            goal="Use a tool",
            backstory="Agent.",
            llm=llm,
            tools=[bad_native_tool],
        )
        task = Task(description="Use the native tool.", expected_output="Result.", agent=agent)
        crew = Crew(name="NativeToolCrew", agents=[agent], tasks=[task])

        crew.kickoff()

        crew_span = self._find_span("invoke_workflow NativeToolCrew")
        agent_span = self._find_span("invoke_agent NativeTool")
        tool_span = self._find_span("execute_tool bad_native_tool")
        self.assertEqual(tool_span.status.status_code.name, "ERROR")
        self.assertEqual(agent_span.status.status_code.name, "OK")
        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(tool_span, agent_span)
        self._assert_spans_all_ended()

    def test_retry_cleanup(self):
        class RetryLLM(BaseLLM):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                object.__setattr__(self, "attempts", 0)

            def call(
                self,
                messages,
                tools=None,
                callbacks=None,
                available_functions=None,
                from_task=None,
                from_agent=None,
                response_model=None,
            ):
                with llm_call_context():
                    self._emit_call_started_event(
                        messages,
                        tools,
                        callbacks,
                        available_functions,
                        from_task,
                        from_agent,
                    )
                    object.__setattr__(self, "attempts", self.attempts + 1)
                    if self.attempts == 1:
                        self._emit_call_failed_event("Transient failure", from_task, from_agent)
                        raise ValueError("Transient failure")
                    response = "Final Answer: Retry succeeded."
                    self._emit_call_completed_event(
                        response,
                        LLMCallType.LLM_CALL,
                        from_task,
                        from_agent,
                        messages,
                    )
                    return response

        llm = RetryLLM(model="retry-test", provider="openai")
        agent = Agent(
            role="RetryAgent",
            goal="Retry once",
            backstory="Agent.",
            llm=llm,
            tools=[],
            max_retry_limit=1,
        )
        task = Task(description="Retry once.", expected_output="Success.", agent=agent)
        crew = Crew(name="RetryCrew", agents=[agent], tasks=[task])

        crew.kickoff()

        self.assertEqual(llm.attempts, 2)
        agent_spans = [
            span for span in self.span_exporter.get_finished_spans() if span.name == "invoke_agent RetryAgent"
        ]
        self.assertEqual(len(agent_spans), 2)
        self.assertTrue(any(span.status.status_code.name == "OK" for span in agent_spans))
        self._assert_spans_all_ended()

    def test_multiple_agents_sequential_tasks(self):
        @tool
        def t1(v: str) -> str:
            """T1."""
            return f"t1:{v}"

        @tool
        def t2(v: str) -> str:
            """T2."""
            return f"t2:{v}"

        llm1 = LLM(model="openai/gpt-4", is_litellm=True)
        llm1.supports_function_calling = lambda: True
        llm2 = LLM(model="openai/gpt-4", is_litellm=True)
        llm2.supports_function_calling = lambda: True

        a1 = Agent(role="A1", goal="Task1", backstory="First.", llm=llm1, tools=[t1])
        a2 = Agent(role="A2", goal="Task2", backstory="Second.", llm=llm2, tools=[t2])
        task1 = Task(description="Do task1.", expected_output="R1.", agent=a1)
        task2 = Task(description="Do task2.", expected_output="R2.", agent=a2)
        crew = Crew(name="MultiAgent", agents=[a1, a2], tasks=[task1, task2])

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(tool_calls=[self._mock_tool_call("c1", "t1", '{"v":"x"}')]),
                self._mock_response("Final Answer: R1"),
                self._mock_response(tool_calls=[self._mock_tool_call("c2", "t2", '{"v":"y"}')]),
                self._mock_response("Final Answer: R2"),
            ],
        ):
            crew.kickoff()

        crew_span = self._find_span("invoke_workflow MultiAgent")
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
        shared_llm = LLM(model="openai/gpt-4", is_litellm=True)
        shared_llm.supports_function_calling = lambda: True
        a1 = Agent(role="Shared1", goal="G1", backstory="S1.", llm=shared_llm, tools=[])
        a2 = Agent(role="Shared2", goal="G2", backstory="S2.", llm=shared_llm, tools=[])
        task1 = Task(description="T1.", expected_output="R1.", agent=a1)
        task2 = Task(description="T2.", expected_output="R2.", agent=a2)
        crew = Crew(name="SharedLLM", agents=[a1, a2], tasks=[task1, task2])

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response("Final Answer: R1"),
                self._mock_response("Final Answer: R2"),
            ],
        ):
            crew.kickoff()

        self.assertIsNotNone(self._find_span("invoke_agent Shared1"))
        self.assertIsNotNone(self._find_span("invoke_agent Shared2"))
        chat_spans = [s for s in self.span_exporter.get_finished_spans() if "chat" in s.name]
        self.assertGreaterEqual(len(chat_spans), 2)
        self._assert_spans_all_ended()

    def test_llm_call_failure(self):
        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="Failing", goal="Fail", backstory="Will fail.", llm=llm, tools=[])
        task = Task(description="Fail.", expected_output="N/A.", agent=agent)
        crew = Crew(name="FailCrew", agents=[agent], tasks=[task])

        with patch("litellm.completion", side_effect=RuntimeError("Service unavailable")):
            with self.assertRaises(RuntimeError):
                crew.kickoff()

        error_spans = [s for s in self.span_exporter.get_finished_spans() if s.status.status_code.name == "ERROR"]
        self.assertGreater(len(error_spans), 0)
        self.assertIn(ERROR_TYPE, error_spans[0].attributes)
        self._assert_spans_all_ended()

    def test_tool_execution_error(self):
        @tool
        def bad_tool(value: str) -> str:
            """A tool that fails."""
            raise ValueError(f"Tool failed: {value}")

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="ToolErr", goal="Handle errors", backstory="Agent.", llm=llm, tools=[bad_tool])
        task = Task(description="Use bad tool.", expected_output="Result.", agent=agent)
        crew = Crew(name="ToolErrCrew", agents=[agent], tasks=[task])

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(tool_calls=[self._mock_tool_call("c1", "bad_tool", '{"value": "x"}')]),
                self._mock_response("Final Answer: Handled error."),
            ],
        ):
            crew.kickoff()

        self.assertIsNotNone(self._find_span("invoke_workflow ToolErrCrew"))
        self.assertIsNotNone(self._find_span("invoke_agent ToolErr"))
        self._assert_spans_all_ended()

    def test_large_message_payloads(self):
        @tool
        def big_tool(size: int) -> str:
            """Returns large data."""
            return "X" * size

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        large_args = json.dumps({"size": 10000, "extra": "Y" * 5000})
        agent = Agent(role="BigData", goal="Handle big data", backstory="Agent.", llm=llm, tools=[big_tool])
        task = Task(description="Big data.", expected_output="Result.", agent=agent)
        crew = Crew(name="BigCrew", agents=[agent], tasks=[task])

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(tool_calls=[self._mock_tool_call("c1", "big_tool", large_args)]),
                self._mock_response("Final Answer: " + "Z" * 5000),
            ],
        ):
            crew.kickoff()

        tool_span = self._find_span("execute_tool big_tool")
        self.assertIsNotNone(tool_span)
        parsed_args = json.loads(tool_span.attributes[GEN_AI_TOOL_CALL_ARGUMENTS])
        self.assertEqual(parsed_args["size"], 10000)
        self._assert_spans_all_ended()

    def test_multiple_sequential_crew_kickoffs(self):
        @tool
        def seq_tool(v: str) -> str:
            """Sequential tool."""
            return f"seq:{v}"

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="Seq", goal="Run twice", backstory="Agent.", llm=llm, tools=[seq_tool])
        task = Task(description="Sequential.", expected_output="Result.", agent=agent)
        crew = Crew(name="SeqCrew", agents=[agent], tasks=[task])

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(tool_calls=[self._mock_tool_call("c1", "seq_tool", '{"v":"1"}')]),
                self._mock_response("Final Answer: Run 1"),
                self._mock_response(tool_calls=[self._mock_tool_call("c2", "seq_tool", '{"v":"2"}')]),
                self._mock_response("Final Answer: Run 2"),
            ],
        ):
            crew.kickoff()
            first_count = len(self.span_exporter.get_finished_spans())
            crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        self.assertGreater(len(spans), first_count)
        crew_spans = [s for s in spans if "invoke_workflow SeqCrew" in s.name]
        self.assertEqual(len(crew_spans), 2)
        tool_spans = [s for s in spans if "execute_tool seq_tool" in s.name]
        self.assertEqual(len(tool_spans), 2)
        self._assert_spans_all_ended()

    def test_async_crew_kickoff(self):
        @tool
        def async_tool(name: str) -> str:
            """Async tool."""
            return f"Hello, {name}!"

        llm = LLM(model="openai/gpt-4", is_litellm=True)
        llm.supports_function_calling = lambda: True
        agent = Agent(role="AsyncAgent", goal="Greet", backstory="Async.", llm=llm, tools=[async_tool])
        task = Task(description="Greet.", expected_output="Greeting.", agent=agent)
        crew = Crew(name="AsyncCrew", agents=[agent], tasks=[task])

        responses = iter(
            [
                self._mock_response(tool_calls=[self._mock_tool_call("c1", "async_tool", '{"name":"World"}')]),
                self._mock_response("Final Answer: Hi!"),
            ]
        )

        async def mock_acompletion(*args, **kwargs):
            return next(responses)

        def mock_completion(*args, **kwargs):
            return next(responses)

        async def run():
            # crewai routes akickoff through either litellm.acompletion or sync
            # litellm.completion depending on the executor, so patch both.
            with patch("litellm.acompletion", side_effect=mock_acompletion), patch(
                "litellm.completion", side_effect=mock_completion
            ):
                return await crew.akickoff()

        asyncio.run(run())

        crew_span = self._find_span("invoke_workflow AsyncCrew")
        agent_span = self._find_span("invoke_agent AsyncAgent")
        tool_span = self._find_span("execute_tool async_tool")
        self.assertIsNotNone(crew_span)
        self.assertIsNotNone(agent_span)
        self.assertIsNotNone(tool_span)
        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(tool_span, agent_span)
        self._assert_spans_all_ended()

    def test_async_multiple_agents(self):
        llm1 = LLM(model="openai/gpt-4", is_litellm=True)
        llm1.supports_function_calling = lambda: True
        llm2 = LLM(model="openai/gpt-4", is_litellm=True)
        llm2.supports_function_calling = lambda: True
        a1 = Agent(role="AsyncA1", goal="G1", backstory="A1.", llm=llm1, tools=[])
        a2 = Agent(role="AsyncA2", goal="G2", backstory="A2.", llm=llm2, tools=[])
        t1 = Task(description="T1.", expected_output="R1.", agent=a1)
        t2 = Task(description="T2.", expected_output="R2.", agent=a2)
        crew = Crew(name="AsyncMulti", agents=[a1, a2], tasks=[t1, t2])

        responses = iter(
            [
                self._mock_response("Final Answer: R1"),
                self._mock_response("Final Answer: R2"),
            ]
        )

        async def mock_acompletion(*args, **kwargs):
            return next(responses)

        def mock_completion(*args, **kwargs):
            return next(responses)

        async def run():
            # crewai routes akickoff through either litellm.acompletion or sync
            # litellm.completion depending on the executor, so patch both.
            with patch("litellm.acompletion", side_effect=mock_acompletion), patch(
                "litellm.completion", side_effect=mock_completion
            ):
                return await crew.akickoff()

        asyncio.run(run())

        crew_span = self._find_span("invoke_workflow AsyncMulti")
        a1_span = self._find_span("invoke_agent AsyncA1")
        a2_span = self._find_span("invoke_agent AsyncA2")
        self.assertIsNotNone(a1_span)
        self.assertIsNotNone(a2_span)
        self._assert_span_parent(a1_span, crew_span)
        self._assert_span_parent(a2_span, crew_span)
        self._assert_spans_all_ended()

    def test_concurrent_crews_have_isolated_span_cleanup(self):
        crew_a_entered_llm = threading.Event()
        crew_b_entered_llm = threading.Event()
        crew_a_completed = threading.Event()

        shared_llm = LLM(model="openai/gpt-4o-mini", is_litellm=True)
        agent_a = Agent(role="ConcurrentA", goal="Complete A", backstory="A.", llm=shared_llm, tools=[])
        agent_b = Agent(role="ConcurrentB", goal="Complete B", backstory="B.", llm=shared_llm, tools=[])
        crew_a = Crew(
            name="ConcurrentCrewA",
            agents=[agent_a],
            tasks=[Task(description="Run A.", expected_output="A.", agent=agent_a)],
        )
        crew_b = Crew(
            name="ConcurrentCrewB",
            agents=[agent_b],
            tasks=[Task(description="Run B.", expected_output="B.", agent=agent_b)],
        )

        def wait_for(event: threading.Event, description: str) -> None:
            if not event.wait(timeout=10):
                raise TimeoutError(f"Timed out waiting for {description}")

        def coordinated_response(messages):
            prompt = str(messages)
            if "Run A." in prompt:
                crew_a_entered_llm.set()
                wait_for(crew_b_entered_llm, "Crew B to enter its LLM call")
                return self._mock_response("Final Answer: A")
            if "Run B." in prompt:
                crew_b_entered_llm.set()
                wait_for(crew_a_entered_llm, "Crew A to enter its LLM call")
                wait_for(crew_a_completed, "Crew A to complete")
                return self._mock_response("Final Answer: B")
            raise AssertionError(f"Unexpected messages: {messages}")

        async def mock_acompletion(*args, **kwargs):
            return await asyncio.to_thread(coordinated_response, kwargs.get("messages"))

        def mock_completion(*args, **kwargs):
            return coordinated_response(kwargs.get("messages"))

        async def run():
            async def run_crew_a():
                result = await crew_a.akickoff()
                crew_a_completed.set()
                return result

            with patch("litellm.acompletion", side_effect=mock_acompletion), patch(
                "litellm.completion", side_effect=mock_completion
            ):
                await asyncio.wait_for(
                    asyncio.gather(run_crew_a(), crew_b.akickoff()),
                    timeout=20,
                )

        asyncio.run(run())

        workflow_a = self._find_span("invoke_workflow ConcurrentCrewA")
        workflow_b = self._find_span("invoke_workflow ConcurrentCrewB")
        agent_span_a = self._find_span("invoke_agent ConcurrentA")
        agent_span_b = self._find_span("invoke_agent ConcurrentB")
        self.assertIsNotNone(workflow_a)
        self.assertIsNotNone(workflow_b)
        self.assertIsNotNone(agent_span_a)
        self.assertIsNotNone(agent_span_b)
        self._assert_span_parent(agent_span_a, workflow_a)
        self._assert_span_parent(agent_span_b, workflow_b)
        self._assert_spans_all_ended()

    def test_concurrent_crew_trace_map(self):
        crew_a_entered_llm = threading.Event()
        crew_b_entered_llm = threading.Event()
        crew_a_completed = threading.Event()
        test_tracer = self.tracer_provider.get_tracer("test")

        shared_llm = LLM(model="openai/gpt-4o-mini", is_litellm=True)
        agent_a = Agent(role="ConcurrentA", goal="Complete A", backstory="A.", llm=shared_llm, tools=[])
        agent_b = Agent(role="ConcurrentB", goal="Complete B", backstory="B.", llm=shared_llm, tools=[])
        crew_a = Crew(
            name="ConcurrentCrewA",
            agents=[agent_a],
            tasks=[Task(description="Run A.", expected_output="A.", agent=agent_a)],
        )
        crew_b = Crew(
            name="ConcurrentCrewB",
            agents=[agent_b],
            tasks=[Task(description="Run B.", expected_output="B.", agent=agent_b)],
        )

        def wait_for(event: threading.Event, description: str) -> None:
            if not event.wait(timeout=10):
                raise TimeoutError(f"Timed out waiting for {description}")

        def coordinated_response(messages):
            prompt = str(messages)
            if "Run A." in prompt:
                crew_a_entered_llm.set()
                wait_for(crew_b_entered_llm, "Crew B to enter its LLM call")
                return self._mock_response("Final Answer: A")
            if "Run B." in prompt:
                crew_b_entered_llm.set()
                wait_for(crew_a_entered_llm, "Crew A to enter its LLM call")
                wait_for(crew_a_completed, "Crew A to complete")
                return self._mock_response("Final Answer: B")
            raise AssertionError(f"Unexpected messages: {messages}")

        async def mock_acompletion(*args, **kwargs):
            return await asyncio.to_thread(coordinated_response, kwargs.get("messages"))

        def mock_completion(*args, **kwargs):
            return coordinated_response(kwargs.get("messages"))

        async def run():
            async def run_crew_a():
                with test_tracer.start_as_current_span("concurrent_request_a"):
                    result = await crew_a.akickoff()
                    crew_a_completed.set()
                    return result

            async def run_crew_b():
                with test_tracer.start_as_current_span("concurrent_request_b"):
                    return await crew_b.akickoff()

            with patch("litellm.acompletion", side_effect=mock_acompletion), patch(
                "litellm.completion", side_effect=mock_completion
            ):
                await asyncio.wait_for(
                    asyncio.gather(run_crew_a(), run_crew_b()),
                    timeout=20,
                )

        asyncio.run(run())

        workflow_a = self._find_span("invoke_workflow ConcurrentCrewA")
        workflow_b = self._find_span("invoke_workflow ConcurrentCrewB")
        agent_span_a = self._find_span("invoke_agent ConcurrentA")
        agent_span_b = self._find_span("invoke_agent ConcurrentB")
        request_a = self._find_span("concurrent_request_a")
        request_b = self._find_span("concurrent_request_b")
        self.assertIsNotNone(workflow_a)
        self.assertIsNotNone(workflow_b)
        self.assertIsNotNone(agent_span_a)
        self.assertIsNotNone(agent_span_b)
        self._assert_span_parent(workflow_a, request_a)
        self._assert_span_parent(workflow_b, request_b)
        self._assert_span_parent(agent_span_a, workflow_a)
        self._assert_span_parent(agent_span_b, workflow_b)
        self.assertNotEqual(request_a.context.trace_id, request_b.context.trace_id)
        self._assert_trace_spans(
            request_a,
            [
                "concurrent_request_a",
                "invoke_workflow ConcurrentCrewA",
                "invoke_agent ConcurrentA",
                "chat gpt-4o-mini",
            ],
        )
        self._assert_trace_spans(
            request_b,
            [
                "concurrent_request_b",
                "invoke_workflow ConcurrentCrewB",
                "invoke_agent ConcurrentB",
                "chat gpt-4o-mini",
            ],
        )
        self._assert_spans_all_ended()

    def test_concurrent_crew_same_instance_isolation(self):
        first_call_entered = threading.Event()
        release_first_call = threading.Event()
        call_count_lock = threading.Lock()
        call_count = 0
        llm = LLM(model="openai/gpt-4o-mini", is_litellm=True)
        agent = Agent(
            role="SharedCrewAgent",
            goal="Complete task",
            backstory="Agent.",
            llm=llm,
            tools=[],
            max_retry_limit=0,
        )
        crew = Crew(
            name="SharedCrew",
            agents=[agent],
            tasks=[Task(description="Run shared crew.", expected_output="Done.", agent=agent)],
        )

        def concurrent_response(*args, **kwargs):
            nonlocal call_count
            with call_count_lock:
                call_count += 1
                current_call = call_count
            if current_call == 1:
                first_call_entered.set()
                if not release_first_call.wait(timeout=10):
                    raise TimeoutError("Timed out waiting to release the first LLM call")
            return self._mock_response("Final Answer: Done")

        def kickoff_result():
            try:
                return crew.kickoff()
            except Exception as error:  # pylint: disable=broad-except
                return error

        async def run():
            with patch("litellm.completion", side_effect=concurrent_response):
                first_kickoff = asyncio.create_task(asyncio.to_thread(kickoff_result))
                entered = await asyncio.to_thread(first_call_entered.wait, 10)
                if not entered:
                    raise TimeoutError("Timed out waiting for the first LLM call")
                try:
                    second_result = await asyncio.to_thread(kickoff_result)
                finally:
                    release_first_call.set()
                return await first_kickoff, second_result

        first_result, second_result = asyncio.run(asyncio.wait_for(run(), timeout=20))

        self.assertNotIsInstance(first_result, Exception)
        if isinstance(second_result, Exception):
            self.assertIsInstance(second_result, RuntimeError)
            self.assertIn("already running", str(second_result))
        workflow_spans = [
            span for span in self.span_exporter.get_finished_spans() if span.name == "invoke_workflow SharedCrew"
        ]
        agent_spans = [
            span for span in self.span_exporter.get_finished_spans() if span.name == "invoke_agent SharedCrewAgent"
        ]
        self.assertEqual(len(workflow_spans), 2)
        self.assertEqual(len(agent_spans), 2)
        self.assertIn("OK", [span.status.status_code.name for span in workflow_spans])
        self.assertCountEqual(
            [span.parent.span_id for span in agent_spans],
            [span.context.span_id for span in workflow_spans],
        )
        self._assert_spans_all_ended()

    def test_concurrent_crew_failure_isolation(self):
        crew_b_entered_llm = threading.Event()
        crew_a_failed = threading.Event()
        shared_llm = LLM(model="openai/gpt-4o-mini", is_litellm=True)
        agent_a = Agent(
            role="FailingConcurrentAgent",
            goal="Fail",
            backstory="Agent.",
            llm=shared_llm,
            tools=[],
            max_retry_limit=0,
        )
        agent_b = Agent(role="SuccessfulConcurrentAgent", goal="Complete", backstory="Agent.", llm=shared_llm, tools=[])
        crew_a = Crew(
            name="FailingConcurrentCrew",
            agents=[agent_a],
            tasks=[Task(description="Run failing crew.", expected_output="Failure.", agent=agent_a)],
        )
        crew_b = Crew(
            name="SuccessfulConcurrentCrew",
            agents=[agent_b],
            tasks=[Task(description="Run successful crew.", expected_output="Success.", agent=agent_b)],
        )

        def wait_for(event: threading.Event, description: str) -> None:
            if not event.wait(timeout=10):
                raise TimeoutError(f"Timed out waiting for {description}")

        def coordinated_response(*args, **kwargs):
            messages = str(kwargs.get("messages"))
            if "Run failing crew." in messages:
                wait_for(crew_b_entered_llm, "successful crew to enter its LLM call")
                raise RuntimeError("Crew A failed")
            if "Run successful crew." in messages:
                crew_b_entered_llm.set()
                wait_for(crew_a_failed, "failing crew to finish cleanup")
                return self._mock_response("Final Answer: Success")
            raise AssertionError(f"Unexpected messages: {kwargs.get('messages')}")

        def run_failing_crew():
            try:
                return crew_a.kickoff()
            finally:
                crew_a_failed.set()

        async def run():
            with patch("litellm.completion", side_effect=coordinated_response):
                return await asyncio.wait_for(
                    asyncio.gather(
                        asyncio.to_thread(run_failing_crew),
                        asyncio.to_thread(crew_b.kickoff),
                        return_exceptions=True,
                    ),
                    timeout=20,
                )

        failing_result, successful_result = asyncio.run(run())

        self.assertIsInstance(failing_result, RuntimeError)
        self.assertNotIsInstance(successful_result, Exception)
        failing_workflow = self._find_span("invoke_workflow FailingConcurrentCrew")
        successful_workflow = self._find_span("invoke_workflow SuccessfulConcurrentCrew")
        successful_agent = self._find_span("invoke_agent SuccessfulConcurrentAgent")
        self.assertEqual(failing_workflow.status.status_code.name, "ERROR")
        self.assertEqual(successful_workflow.status.status_code.name, "OK")
        self._assert_span_parent(successful_agent, successful_workflow)
        self._assert_spans_all_ended()

    def _run_crew_kickoff_test(self, model: str, provider: str, model_id: str):
        test_tracer = self.tracer_provider.get_tracer("test")
        tc = self._mock_tool_call()

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            with test_tracer.start_as_current_span("custom_downstream_span"):
                return f"Hello, {name}!"

        llm = LLM(
            model=model,
            is_litellm=True,
            temperature=0.7,
            top_p=0.9,
            max_tokens=1024,
            frequency_penalty=0.1,
            presence_penalty=0.2,
            stop=["STOP"],
        )
        llm.supports_function_calling = lambda: True
        agent = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting],
            verbose=True,
        )
        task = Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=agent)
        crew = Crew(name="GreetingCrew", agents=[agent], tasks=[task], verbose=True)

        with patch(
            "litellm.completion",
            side_effect=[
                self._mock_response(content="", tool_calls=[tc], prompt_tokens=100, completion_tokens=50),
                self._mock_response(
                    content="Thought: I now know the final answer\nFinal Answer: Hello! Welcome!",
                    prompt_tokens=200,
                    completion_tokens=80,
                ),
            ],
        ):
            crew.kickoff()

        spans = self.span_exporter.get_finished_spans()
        crew_span = next((s for s in spans if s.name == "invoke_workflow GreetingCrew"), None)
        agent_span = next((s for s in spans if s.name == "invoke_agent Greeter"), None)
        tool_span = next((s for s in spans if s.name == "execute_tool get_greeting"), None)

        self._assert_span_attributes(
            spans,
            "invoke_workflow GreetingCrew",
            {
                GEN_AI_OPERATION_NAME: OPERATION_INVOKE_WORKFLOW,
                GEN_AI_WORKFLOW_NAME: "GreetingCrew",
                GEN_AI_AGENT_ID: str(crew.id),
            },
        )
        self.assertNotIn(GEN_AI_PROVIDER_NAME, crew_span.attributes)
        self.assertNotIn(GEN_AI_REQUEST_MODEL, crew_span.attributes)
        self.assertIn(GEN_AI_TOOL_DEFINITIONS, crew_span.attributes)
        self.assertIn("get_greeting", crew_span.attributes[GEN_AI_TOOL_DEFINITIONS])

        self._assert_span_attributes(
            spans,
            "invoke_agent Greeter",
            {
                GEN_AI_OPERATION_NAME: "invoke_agent",
                GEN_AI_AGENT_NAME: "Greeter",
                GEN_AI_PROVIDER_NAME: provider,
                GEN_AI_REQUEST_MODEL: model_id,
                GEN_AI_AGENT_ID: str(crew.agents[0].id),
                GEN_AI_AGENT_DESCRIPTION: "Greet the user",
                GEN_AI_REQUEST_TEMPERATURE: 0.7,
                GEN_AI_REQUEST_TOP_P: 0.9,
                GEN_AI_REQUEST_MAX_TOKENS: 1024,
                GEN_AI_REQUEST_FREQUENCY_PENALTY: 0.1,
                GEN_AI_REQUEST_PRESENCE_PENALTY: 0.2,
            },
        )
        self.assertIn("STOP", agent_span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES])

        agent_input_messages = json.loads(agent_span.attributes[GEN_AI_INPUT_MESSAGES])
        validate_otel_genai_schema(agent_input_messages, "gen-ai-input-messages")
        self.assertTrue(
            any(
                part.get("type") == "text" and "Greet the user warmly." in part.get("content", "")
                for message in agent_input_messages
                if message.get("role") == "user"
                for part in message.get("parts", [])
            )
        )
        agent_output_messages = json.loads(agent_span.attributes[GEN_AI_OUTPUT_MESSAGES])
        validate_otel_genai_schema(agent_output_messages, "gen-ai-output-messages")
        self.assertTrue(any(message.get("role") == "assistant" for message in agent_output_messages))

        self._assert_span_attributes(
            spans,
            "execute_tool get_greeting",
            {
                GEN_AI_OPERATION_NAME: "execute_tool",
                GEN_AI_TOOL_NAME: "get_greeting",
                GEN_AI_TOOL_TYPE: "function",
            },
        )
        self.assertIsNotNone(tool_span)
        self.assertIsNotNone(tool_span.attributes)
        self.assertEqual(
            "Get a greeting message for the given name.",
            tool_span.attributes[GEN_AI_TOOL_DESCRIPTION],
        )
        self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, tool_span.attributes)
        self.assertEqual("Hello, World!", tool_span.attributes[GEN_AI_TOOL_CALL_RESULT])

        self._assert_span_parent(agent_span, crew_span)
        self._assert_span_parent(tool_span, agent_span)

        custom_span = next((s for s in spans if s.name == "custom_downstream_span"), None)
        self.assertIsNotNone(custom_span, "custom_downstream_span not found")
        self._assert_span_parent(custom_span, tool_span)

        chat_span = next(
            (s for s in spans if s.name == f"chat {model_id}" and s.attributes.get(GEN_AI_OUTPUT_MESSAGES)),
            None,
        )
        self.assertIsNotNone(chat_span, f"chat {model_id} span with output not found")
        input_messages = json.loads(chat_span.attributes[GEN_AI_INPUT_MESSAGES])
        validate_otel_genai_schema(input_messages, "gen-ai-input-messages")
        self.assertTrue(any(m["role"] == "user" for m in input_messages))
        system_instructions = json.loads(chat_span.attributes[GEN_AI_SYSTEM_INSTRUCTIONS])
        validate_otel_genai_schema(system_instructions, "gen-ai-system-instructions")
        self.assertTrue(any("friendly greeter" in i.get("content", "") for i in system_instructions))
        output_messages = json.loads(chat_span.attributes[GEN_AI_OUTPUT_MESSAGES])
        validate_otel_genai_schema(output_messages, "gen-ai-output-messages")
        self.assertEqual(chat_span.attributes.get(GEN_AI_RESPONSE_MODEL), model_id)
        self.assertIn(GEN_AI_RESPONSE_FINISH_REASONS, chat_span.attributes)

        usage_spans = [s for s in spans if GEN_AI_USAGE_INPUT_TOKENS in s.attributes]
        usage = sorted(
            (s.attributes[GEN_AI_USAGE_INPUT_TOKENS], s.attributes[GEN_AI_USAGE_OUTPUT_TOKENS]) for s in usage_spans
        )
        self.assertEqual(usage, [(100, 50), (200, 80)])

    def _create_test_crew(self, model: str):
        test_tracer = self.tracer_provider.get_tracer("test")

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            with test_tracer.start_as_current_span("custom_downstream_span"):
                return f"Hello, {name}!"

        llm = LLM(model=model, temperature=0.7)
        agent = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting],
            verbose=True,
        )
        task = Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=agent)
        return Crew(name="GreetingCrew", agents=[agent], tasks=[task], verbose=True)

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

    def _assert_trace_spans(self, trace_span: ReadableSpan, expected_names: Sequence[str]):
        trace_id = trace_span.context.trace_id
        trace_spans = [span for span in self.span_exporter.get_finished_spans() if span.context.trace_id == trace_id]
        self.assertCountEqual([span.name for span in trace_spans], expected_names)

    def _assert_spans_all_ended(self):
        for span in self.span_exporter.get_finished_spans():
            self.assertIsNotNone(span.end_time, f"Span {span.name} was not ended")
        self.assertEqual(
            len(self.instrumentor._handler._event_id_to_span._data), 0, "Leaked entries in event_id_to_span map"
        )
        self.assertEqual(
            len(self.instrumentor._handler._task_or_agent_id_to_started_llm_event_id._data),
            0,
            "Leaked entries in task_or_agent_id_to_started_llm_event_id map",
        )

    def _find_span(self, name_contains: str) -> Optional[ReadableSpan]:
        return next((s for s in self.span_exporter.get_finished_spans() if name_contains in s.name), None)

    @staticmethod
    def _mock_response(
        content: str = "", tool_calls: Optional[list] = None, prompt_tokens: int = 100, completion_tokens: int = 50
    ):
        from litellm.types.utils import Choices, Message, ModelResponse, Usage

        message = Message(content=content, role="assistant", tool_calls=tool_calls or None)
        choice = Choices(index=0, message=message, finish_reason="tool_calls" if tool_calls else "stop")
        usage = Usage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        )
        return ModelResponse(choices=[choice], usage=usage)

    @staticmethod
    def _mock_tool_call(call_id: str = "call_123", name: str = "get_greeting", arguments: str = '{"name": "World"}'):
        from litellm.types.utils import ChatCompletionMessageToolCall, Function

        return ChatCompletionMessageToolCall(
            id=call_id, type="function", function=Function(name=name, arguments=arguments)
        )
