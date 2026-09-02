# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import unittest
from importlib.metadata import entry_points
from types import SimpleNamespace
from unittest.mock import MagicMock

import litellm
from agents import Agent, ModelSettings, OpenAIChatCompletionsModel, RunConfig, Runner, function_tool, tracing
from agents.extensions.models.litellm_model import LitellmModel
from agents.tracing import processors
from agents.tracing.processors import BackendSpanExporter
from conftest import call_mock_llm, validate_otel_genai_schema
from openai import Omit
from pydantic import BaseModel

from amazon.opentelemetry.distro.instrumentation.openai_agents import OpenAIAgentsInstrumentor
from amazon.opentelemetry.distro.instrumentation.openai_agents._gen_ai_context_capture import GenAIContextCapture
from amazon.opentelemetry.distro.instrumentation.openai_agents._processor import (
    GEN_AI_REQUEST_REASONING_LEVEL,
    OpenTelemetryTracingProcessor,
    _GenAIMessageNormalizer,
)
from opentelemetry.instrumentation.httpx import HTTPX2ClientInstrumentor, HTTPXClientInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import Decision, SamplingResult
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPENAI_REQUEST_SERVICE_TIER,
    GEN_AI_OPENAI_RESPONSE_SERVICE_TIER,
    GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_OUTPUT_TYPE,
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
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_REASONING_OUTPUT_TOKENS,
    GEN_AI_WORKFLOW_NAME,
    GenAiOperationNameValues,
    GenAiOutputTypeValues,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.server_attributes import SERVER_ADDRESS, SERVER_PORT
from opentelemetry.trace import SpanKind, StatusCode


def _passthrough(*args, **kwargs):
    return "wrapped result"


def _record_tool_call(capture, name=None, call_id=None):
    return capture.record_tool_call(_passthrough, None, (), {"tool_call": SimpleNamespace(name=name, call_id=call_id)})


def _record_request(capture, base_url=None, **kwargs):
    instance = SimpleNamespace(_client=SimpleNamespace(base_url=base_url)) if base_url else None
    return capture.record_openai_request(_passthrough, instance, (), kwargs)


class TestOpenAIAgentsInstrumentor(unittest.TestCase):
    def setUp(self) -> None:
        self.instrumentor = OpenAIAgentsInstrumentor()
        if self.instrumentor.is_instrumented_by_opentelemetry:
            self.instrumentor.uninstrument()
        provider = tracing.get_trace_provider()
        self.previous_processors = tuple(provider._multi_processor._processors)  # pylint: disable=protected-access

    def tearDown(self) -> None:
        if self.instrumentor.is_instrumented_by_opentelemetry:
            self.instrumentor.uninstrument()
        tracing.set_trace_processors(list(self.previous_processors))

    def test_entry_point_resolves_to_native_instrumentor(self):
        eps = list(entry_points(group="opentelemetry_instrumentor", name="aws_openai_agents"))
        self.assertEqual(len(eps), 1)
        self.assertIs(eps[0].load(), OpenAIAgentsInstrumentor)

    def test_instrument_is_idempotent_and_additive(self):
        existing_processor = MagicMock()
        tracing.set_trace_processors([existing_processor])
        self.assertEqual(self.instrumentor.instrumentation_dependencies(), ("openai-agents >= 0.3.3",))

        self.instrumentor.instrument(skip_dep_check=True)
        first_processor = self.instrumentor._processor  # pylint: disable=protected-access
        self.instrumentor.instrument(skip_dep_check=True)
        self.instrumentor._instrument()  # pylint: disable=protected-access

        processors = tracing.get_trace_provider()._multi_processor._processors  # pylint: disable=protected-access
        self.assertEqual(processors, (existing_processor, first_processor))

        self.instrumentor.uninstrument()
        self.instrumentor._uninstrument()  # pylint: disable=protected-access
        processors = tracing.get_trace_provider()._multi_processor._processors  # pylint: disable=protected-access
        self.assertEqual(processors, (existing_processor,))
        self.assertIsNone(self.instrumentor._processor)  # pylint: disable=protected-access

    def test_disable_openai_trace_export_restores_previous_processors(self):
        existing_processor = MagicMock()
        tracing.set_trace_processors([existing_processor])

        self.instrumentor.instrument(disable_openai_trace_export=True, skip_dep_check=True)
        processor = self.instrumentor._processor  # pylint: disable=protected-access
        current = tracing.get_trace_provider()._multi_processor._processors  # pylint: disable=protected-access
        self.assertEqual(current, (processor,))

        self.instrumentor.uninstrument()
        current = tracing.get_trace_provider()._multi_processor._processors  # pylint: disable=protected-access
        self.assertEqual(current, (existing_processor,))
        self.assertIsNone(self.instrumentor._processor)  # pylint: disable=protected-access

    def test_openai_trace_export_produces_no_http_spans(self):
        exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        httpx_instrumentor = HTTPX2ClientInstrumentor() if hasattr(processors, "httpx2") else HTTPXClientInstrumentor()
        httpx_instrumentor.instrument(tracer_provider=tracer_provider)
        backend_exporter = BackendSpanExporter(
            api_key="test", endpoint="http://localhost:1/v1/traces/ingest", max_retries=1
        )
        items = [SimpleNamespace(tracing_api_key=None, export=lambda: {"object": "trace"})]
        try:
            backend_exporter.export(items)
            self.assertEqual(len(exporter.get_finished_spans()), 1)

            self.instrumentor.instrument(tracer_provider=tracer_provider, skip_dep_check=True)
            exporter.clear()
            backend_exporter.export(items)
            self.assertEqual(exporter.get_finished_spans(), tuple())
        finally:
            httpx_instrumentor.uninstrument()
            backend_exporter.close()
            tracer_provider.shutdown()

    def test_force_flush_delegates_to_tracer_span_processor(self):
        tracer_provider = TracerProvider()
        span_processor = MagicMock()
        tracer_provider.add_span_processor(span_processor)
        try:
            self.instrumentor.instrument(
                tracer_provider=tracer_provider,
                skip_dep_check=True,
            )

            self.instrumentor._processor.force_flush()  # pylint: disable=protected-access

            span_processor.force_flush.assert_called_once()
        finally:
            self.instrumentor.uninstrument()
            tracer_provider.shutdown()


class TestOpenAIAgentsTracingProcessor(unittest.TestCase):
    def setUp(self) -> None:
        self.exporter = InMemorySpanExporter()
        self.sampler = MagicMock()
        self.sampler.should_sample.side_effect = lambda *args, **kwargs: SamplingResult(
            Decision.RECORD_AND_SAMPLE,
            attributes=args[4] if len(args) > 4 else kwargs.get("attributes"),
        )
        self.tracer_provider = TracerProvider(sampler=self.sampler)
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        tracer = self.tracer_provider.get_tracer(
            "amazon.opentelemetry.distro.instrumentation.openai_agents",
            "test",
        )
        self.processor = OpenTelemetryTracingProcessor(tracer)
        self.capture = GenAIContextCapture(self.processor.get_otel_span)
        provider = tracing.get_trace_provider()
        self.previous_processors = tuple(provider._multi_processor._processors)  # pylint: disable=protected-access
        tracing.set_trace_processors([self.processor])

    def tearDown(self) -> None:
        self.processor.shutdown()
        tracing.set_trace_processors(list(self.previous_processors))
        self.tracer_provider.shutdown()

    def _spans_by_name(self) -> dict[str, object]:
        return {span.name: span for span in self.exporter.get_finished_spans()}

    def test_span_mapping_attributes_and_agent_rollup(self):
        first_input = [
            {"role": "system", "content": "Be helpful."},
            {"role": "user", "content": "Call the tool."},
        ]
        first_output = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "function": {"name": "lookup", "arguments": '{"query": "weather"}'},
                    }
                ],
            }
        ]
        final_output = [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Done"}]}]
        response = SimpleNamespace(
            id="resp_1",
            model="gpt-response",
            instructions="Response instructions",
            output=final_output,
            usage=SimpleNamespace(input_tokens=7, output_tokens=3),
        )

        with tracing.trace("Test workflow"):
            with tracing.agent_span("Test agent"):
                with tracing.generation_span(
                    input=first_input,
                    output=first_output,
                    model="gpt-generation",
                    model_config={
                        "temperature": 0.7,
                        "top_p": 0.8,
                        "top_k": 10,
                        "max_tokens": 100,
                        "frequency_penalty": 0.1,
                        "presence_penalty": 0.2,
                        "stop_sequences": ["STOP"],
                        "seed": 42,
                        "choice_count": 2,
                    },
                    usage={"prompt_tokens": 11, "completion_tokens": 5},
                ):
                    pass
                with tracing.function_span("lookup", input='{"query": "weather"}', output={"result": "sunny"}):
                    pass
                response_trace_span = tracing.response_span(response=response)
                response_trace_span.span_data.input = [
                    {"role": "user", "content": "Ignored because the first LLM input wins."}
                ]
                if hasattr(response_trace_span.span_data, "usage"):
                    response_trace_span.span_data.usage = {"input_tokens": 7, "output_tokens": 3}
                with response_trace_span:
                    pass

        spans = self._spans_by_name()
        workflow_span = spans["invoke_workflow Test workflow"]
        agent_span = spans["invoke_agent Test agent"]
        generation_span = spans["chat gpt-generation"]
        response_span = spans["chat gpt-response"]
        tool_span = spans["execute_tool lookup"]

        self.assertEqual(workflow_span.kind, SpanKind.INTERNAL)
        self.assertEqual(agent_span.kind, SpanKind.INTERNAL)
        self.assertEqual(tool_span.kind, SpanKind.INTERNAL)
        self.assertEqual(generation_span.kind, SpanKind.CLIENT)
        self.assertEqual(response_span.kind, SpanKind.CLIENT)

        self.assertEqual(
            workflow_span.attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.INVOKE_WORKFLOW.value,
        )
        self.assertEqual(workflow_span.attributes[GEN_AI_WORKFLOW_NAME], "Test workflow")
        self.assertEqual(
            workflow_span.attributes[GEN_AI_PROVIDER_NAME],
            GenAiProviderNameValues.OPENAI.value,
        )
        self.assertEqual(agent_span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.INVOKE_AGENT.value)
        self.assertEqual(agent_span.attributes[GEN_AI_AGENT_NAME], "Test agent")
        self.assertEqual(tool_span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.EXECUTE_TOOL.value)
        self.assertEqual(tool_span.attributes[GEN_AI_TOOL_NAME], "lookup")
        self.assertEqual(tool_span.attributes[GEN_AI_TOOL_TYPE], "function")
        self.assertEqual(tool_span.attributes[GEN_AI_TOOL_CALL_ARGUMENTS], '{"query": "weather"}')
        self.assertEqual(json.loads(tool_span.attributes[GEN_AI_TOOL_CALL_RESULT]), {"result": "sunny"})

        expected_request_attributes = {
            GEN_AI_REQUEST_MODEL: "gpt-generation",
            GEN_AI_REQUEST_TEMPERATURE: 0.7,
            GEN_AI_REQUEST_TOP_P: 0.8,
            GEN_AI_REQUEST_TOP_K: 10,
            GEN_AI_REQUEST_MAX_TOKENS: 100,
            GEN_AI_REQUEST_FREQUENCY_PENALTY: 0.1,
            GEN_AI_REQUEST_PRESENCE_PENALTY: 0.2,
            GEN_AI_REQUEST_STOP_SEQUENCES: ("STOP",),
            GEN_AI_REQUEST_SEED: 42,
            GEN_AI_REQUEST_CHOICE_COUNT: 2,
            GEN_AI_USAGE_INPUT_TOKENS: 11,
            GEN_AI_USAGE_OUTPUT_TOKENS: 5,
        }
        for key, value in expected_request_attributes.items():
            self.assertEqual(generation_span.attributes[key], value)
        self.assertEqual(
            generation_span.attributes[GEN_AI_RESPONSE_FINISH_REASONS],
            ("tool_call",),
        )

        self.assertEqual(response_span.attributes[GEN_AI_RESPONSE_ID], "resp_1")
        self.assertEqual(response_span.attributes[GEN_AI_RESPONSE_MODEL], "gpt-response")
        self.assertEqual(response_span.attributes[GEN_AI_REQUEST_MODEL], "gpt-response")
        self.assertEqual(response_span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 7)
        self.assertEqual(response_span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 3)

        self.assertEqual(agent_span.attributes[GEN_AI_REQUEST_MODEL], "gpt-generation")
        self.assertEqual(
            json.loads(agent_span.attributes[GEN_AI_INPUT_MESSAGES]),
            [{"role": "user", "parts": [{"type": "text", "content": "Call the tool."}]}],
        )
        self.assertEqual(
            json.loads(agent_span.attributes[GEN_AI_OUTPUT_MESSAGES]),
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "Done"}],
                    "finish_reason": "stop",
                }
            ],
        )
        self.assertEqual(
            json.loads(agent_span.attributes[GEN_AI_SYSTEM_INSTRUCTIONS]),
            [{"type": "text", "content": "Be helpful."}],
        )

    def test_input_output_usage_styles_and_sensitive_data_none(self):
        with tracing.trace("Generation variants"):
            with tracing.generation_span(
                input=[{"content": "plain prompt"}],
                output=[{"content": "plain completion", "finish_reason": "length"}],
                model="completion-model",
                usage={"input_tokens": 9, "output_tokens": 4},
            ):
                pass
            delayed_chat_span = tracing.generation_span(
                input=None,
                output=None,
                model="delayed-chat-model",
                usage={},
            )
            with delayed_chat_span:
                delayed_chat_span.span_data.input = [{"role": "user", "content": "Filled after start"}]
                delayed_chat_span.span_data.output = [{"role": "assistant", "content": "Completed"}]
            with tracing.generation_span(input=None, output=None, model="private-model", usage={}):
                pass

        spans = self._spans_by_name()
        completion_span = spans["text_completion completion-model"]
        self.assertEqual(
            completion_span.attributes[GEN_AI_OPERATION_NAME],
            GenAiOperationNameValues.TEXT_COMPLETION.value,
        )
        self.assertEqual(completion_span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 9)
        self.assertEqual(completion_span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 4)
        self.assertEqual(completion_span.attributes[GEN_AI_RESPONSE_FINISH_REASONS], ("length",))

        delayed_chat = spans["chat delayed-chat-model"]
        self.assertEqual(delayed_chat.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.CHAT.value)

        private_span = spans["text_completion private-model"]
        self.assertNotIn(GEN_AI_INPUT_MESSAGES, private_span.attributes)
        self.assertNotIn(GEN_AI_OUTPUT_MESSAGES, private_span.attributes)
        self.assertNotIn(GEN_AI_SYSTEM_INSTRUCTIONS, private_span.attributes)

    def test_handoff_span_uses_captured_sdk_tool_name(self):
        with tracing.trace("Handoff workflow"):
            handoff_span = tracing.handoff_span(from_agent="Triage agent", to_agent=None)
            with handoff_span:
                _record_tool_call(self.capture, name="beam_me_to_french", call_id="call_handoff")
                handoff_span.span_data.to_agent = "French agent"

        span = self._spans_by_name()["execute_tool beam_me_to_french"]
        self.assertEqual(span.kind, SpanKind.INTERNAL)
        self.assertEqual(span.attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.EXECUTE_TOOL.value)
        self.assertEqual(span.attributes[GEN_AI_TOOL_NAME], "beam_me_to_french")
        self.assertEqual(span.attributes[GEN_AI_TOOL_TYPE], "function")
        self.assertEqual(span.attributes[GEN_AI_TOOL_CALL_ID], "call_handoff")
        self.assertEqual(
            json.loads(span.attributes[GEN_AI_TOOL_CALL_ARGUMENTS]),
            {"from_agent": "Triage agent", "to_agent": "French agent"},
        )

    def test_handoff_span_derives_tool_name_without_capture(self):
        with tracing.trace("Derived handoff workflow"):
            derived_span = tracing.handoff_span(from_agent="Triage agent", to_agent=None)
            with derived_span:
                derived_span.span_data.to_agent = "Spanish Agent 2"
            with tracing.handoff_span(from_agent="Triage agent", to_agent=None):
                pass

        spans = self._spans_by_name()
        derived = spans["execute_tool transfer_to_spanish_agent_2"]
        self.assertEqual(derived.attributes[GEN_AI_TOOL_NAME], "transfer_to_spanish_agent_2")
        self.assertNotIn(GEN_AI_TOOL_CALL_ID, derived.attributes)
        unknown = spans["execute_tool handoff"]
        self.assertEqual(unknown.attributes[GEN_AI_TOOL_NAME], "handoff")

    def test_function_span_records_tool_call_id_and_resets_between_calls(self):
        with tracing.trace("Tool workflow"):
            with tracing.function_span("get_weather", input='{"city": "Seattle"}', output="sunny"):
                _record_tool_call(self.capture, name="get_weather", call_id="call_weather")
            with tracing.function_span("get_time", input="{}", output="noon"):
                pass

        spans = self._spans_by_name()
        self.assertEqual(spans["execute_tool get_weather"].attributes[GEN_AI_TOOL_CALL_ID], "call_weather")
        self.assertNotIn(GEN_AI_TOOL_CALL_ID, spans["execute_tool get_time"].attributes)

    def test_streamed_generation_response_envelope(self):
        envelope = {
            "object": "response",
            "id": "resp_streamed",
            "model": "gpt-streamed",
            "service_tier": "default",
            "system_fingerprint": "fp_streamed",
            "temperature": 0.5,
            "top_p": 0.6,
            "max_output_tokens": 32,
            "text": {"format": {"type": "json_schema"}},
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Streamed"}]}
            ],
            "usage": {
                "input_tokens": 21,
                "output_tokens": 5,
                "input_tokens_details": {"cached_tokens": 12, "cache_write_tokens": 3},
                "output_tokens_details": {"reasoning_tokens": 4},
            },
        }

        with tracing.trace("Streamed workflow"):
            with tracing.generation_span(
                input=[{"role": "user", "content": "Stream it"}],
                output=[envelope],
                model=None,
            ):
                pass

        span = self._spans_by_name()["chat gpt-streamed"]
        self.assertEqual(span.attributes[GEN_AI_REQUEST_STREAM], True)
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_ID], "resp_streamed")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_MODEL], "gpt-streamed")
        self.assertEqual(span.attributes[GEN_AI_OPENAI_RESPONSE_SERVICE_TIER], "default")
        self.assertEqual(span.attributes[GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT], "fp_streamed")
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.5)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TOP_P], 0.6)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 32)
        self.assertEqual(span.attributes[GEN_AI_OUTPUT_TYPE], GenAiOutputTypeValues.JSON.value)
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 21)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 5)
        self.assertEqual(span.attributes[GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS], 12)
        self.assertEqual(span.attributes[GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS], 3)
        self.assertEqual(span.attributes[GEN_AI_USAGE_REASONING_OUTPUT_TOKENS], 4)
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_FINISH_REASONS], ("length",))

        output_messages = json.loads(span.attributes[GEN_AI_OUTPUT_MESSAGES])
        validate_otel_genai_schema(output_messages, "gen-ai-output-messages")
        self.assertEqual(
            output_messages,
            [{"role": "assistant", "parts": [{"type": "text", "content": "Streamed"}], "finish_reason": "length"}],
        )

    def test_chat_completions_usage_aliases(self):
        with tracing.trace("Usage workflow"):
            with tracing.generation_span(
                input=[{"role": "user", "content": "Hi"}],
                output=[{"role": "assistant", "content": "Hello"}],
                model="gpt-usage",
                usage={
                    "prompt_tokens": 13,
                    "completion_tokens": 6,
                    "prompt_tokens_details": {"cached_tokens": 9},
                    "completion_tokens_details": {"reasoning_tokens": 2},
                    "cache_creation_input_tokens": 4,
                },
            ):
                pass

        span = self._spans_by_name()["chat gpt-usage"]
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 13)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 6)
        self.assertEqual(span.attributes[GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS], 9)
        self.assertEqual(span.attributes[GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS], 4)
        self.assertEqual(span.attributes[GEN_AI_USAGE_REASONING_OUTPUT_TOKENS], 2)

    def test_captured_request_params_set_provider_and_server_attributes(self):
        model = "gpt-5.6-sol"
        instrumentor = OpenAIAgentsInstrumentor()
        instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            disable_openai_trace_export=True,
            skip_dep_check=True,
        )
        self.addCleanup(instrumentor.uninstrument)

        def invoke_agent(client):
            Runner.run_sync(
                Agent(
                    name="Test agent",
                    instructions="You are a helpful assistant.",
                    model=OpenAIChatCompletionsModel(model=model, openai_client=client),
                    model_settings=ModelSettings(
                        temperature=0.7,
                        top_p=0.9,
                        max_tokens=100,
                        frequency_penalty=0.5,
                        presence_penalty=0.3,
                        extra_args={"stop": ["STOP"], "seed": 42, "n": 2},
                    ),
                ),
                "Hello",
            )

        call_mock_llm("openai", invoke_llm_callback=invoke_agent, is_async=True)

        chat_span = self._spans_by_name()[f"chat {model}"]
        self.assertEqual(chat_span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.OPENAI.value)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.7)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_TOP_P], 0.9)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 100)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_FREQUENCY_PENALTY], 0.5)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_PRESENCE_PENALTY], 0.3)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("STOP",))
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_SEED], 42)
        self.assertEqual(chat_span.attributes[GEN_AI_REQUEST_CHOICE_COUNT], 2)

        self.exporter.clear()
        with tracing.trace("Capture workflow"):
            captured_span = tracing.generation_span(
                input=[{"role": "user", "content": "Hi"}],
                output=None,
                model=None,
            )
            with captured_span:
                _record_request(
                    instrumentor._capture,  # pylint: disable=protected-access
                    base_url="https://bedrock-runtime.us-west-2.amazonaws.com:8443/v1",
                    model="bedrock/anthropic.claude-3-5-haiku",
                    temperature=0.3,
                    stream=True,
                    n=2,
                    service_tier="flex",
                    response_format={"type": "json_object"},
                    stop=["END"],
                    max_completion_tokens=64,
                    metadata={"dropped": "not a request parameter"},
                    top_p=None,
                )

        span = self._spans_by_name()["chat bedrock/anthropic.claude-3-5-haiku"]
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], "bedrock/anthropic.claude-3-5-haiku")
        self.assertEqual(span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.AWS_BEDROCK.value)
        self.assertEqual(span.attributes[SERVER_ADDRESS], "bedrock-runtime.us-west-2.amazonaws.com")
        self.assertEqual(span.attributes[SERVER_PORT], 8443)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.3)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_STREAM], True)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_CHOICE_COUNT], 2)
        self.assertEqual(span.attributes[GEN_AI_OPENAI_REQUEST_SERVICE_TIER], "flex")
        self.assertEqual(span.attributes[GEN_AI_REQUEST_STOP_SEQUENCES], ("END",))
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MAX_TOKENS], 64)
        self.assertEqual(span.attributes[GEN_AI_OUTPUT_TYPE], GenAiOutputTypeValues.JSON.value)
        self.assertNotIn(GEN_AI_REQUEST_TOP_P, span.attributes)

    def test_litellm_bedrock_model_captures_tools_and_response_metadata(self):
        model = "bedrock/anthropic.claude-3-haiku-20240307-v1:0"
        instrumentor = OpenAIAgentsInstrumentor()
        instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            disable_openai_trace_export=True,
            skip_dep_check=True,
        )
        self.addCleanup(instrumentor.uninstrument)

        @function_tool
        def lookup(city: str) -> str:
            """Look up a city."""
            return f"Sunny in {city}"

        def invoke_agent(_client):
            asyncio.run(
                Runner.run(
                    Agent(
                        name="LiteLLM Bedrock agent",
                        instructions="Answer directly.",
                        model=LitellmModel(
                            model=model,
                            base_url="https://bedrock-runtime.us-west-2.amazonaws.com",
                        ),
                        model_settings=ModelSettings(
                            temperature=0.2,
                            extra_args={
                                "aws_region_name": "us-west-2",
                                "aws_access_key_id": "fake-key",
                                "aws_secret_access_key": "fake-key",
                            },
                        ),
                        tools=[lookup],
                    ),
                    "What is the weather in Seattle?",
                ),
            )

        call_mock_llm("bedrock", invoke_llm_callback=invoke_agent, is_litellm=True)

        span = self._spans_by_name()[f"chat {model}"]
        sampling_call = next(
            call
            for call in self.sampler.should_sample.call_args_list
            if len(call.args) > 2 and call.args[2] == f"chat {model}"
        )
        sampling_attributes = sampling_call.args[4]
        self.assertEqual(sampling_attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.AWS_BEDROCK.value)
        self.assertEqual(sampling_attributes[GEN_AI_OPERATION_NAME], GenAiOperationNameValues.CHAT.value)
        self.assertEqual(sampling_attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(sampling_attributes[SERVER_ADDRESS], "bedrock-runtime.us-west-2.amazonaws.com")
        self.assertEqual(span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.AWS_BEDROCK.value)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.2)
        self.assertEqual(span.attributes[SERVER_ADDRESS], "bedrock-runtime.us-west-2.amazonaws.com")
        self.assertIn(GEN_AI_RESPONSE_ID, span.attributes)
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_MODEL], "anthropic.claude-3-haiku-20240307-v1:0")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_FINISH_REASONS], ("stop",))
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 14)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)
        self.assertEqual(span.attributes[GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS], 3)
        self.assertEqual(span.attributes[GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS], 1)
        self.assertEqual(
            json.loads(span.attributes[GEN_AI_TOOL_DEFINITIONS]),
            [
                {
                    "type": "function",
                    "name": "lookup",
                    "description": "Look up a city.",
                    "parameters": {
                        "type": "object",
                        "properties": {"city": {"title": "City", "type": "string"}},
                        "required": ["city"],
                        "title": "lookup_args",
                        "additionalProperties": False,
                    },
                }
            ],
        )

    def test_litellm_openai_sdk_call_preserves_litellm_attributes(self):
        model = "openai/test-model"
        instrumentor = OpenAIAgentsInstrumentor()
        instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            disable_openai_trace_export=True,
            skip_dep_check=True,
        )
        self.addCleanup(instrumentor.uninstrument)

        def invoke_agent(_client):
            asyncio.run(
                Runner.run(
                    Agent(
                        name="LiteLLM OpenAI agent",
                        model=LitellmModel(
                            model=model,
                            base_url="https://example.com/v1",
                            api_key="fake-key",
                        ),
                    ),
                    "Hello",
                ),
            )

        call_mock_llm("litellm", invoke_llm_callback=invoke_agent, model="test-model")

        span = self._spans_by_name()[f"chat {model}"]
        sampling_call = next(
            call
            for call in self.sampler.should_sample.call_args_list
            if len(call.args) > 2 and call.args[2] == f"chat {model}"
        )
        sampling_attributes = sampling_call.args[4]
        self.assertEqual(sampling_attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.OPENAI.value)
        self.assertEqual(sampling_attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(sampling_attributes[SERVER_ADDRESS], "example.com")
        self.assertEqual(span.attributes[GEN_AI_PROVIDER_NAME], GenAiProviderNameValues.OPENAI.value)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(span.attributes[SERVER_ADDRESS], "example.com")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_ID], "chatcmpl-mock")
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)

        self.exporter.clear()
        model = "openai/sync-model"

        def invoke_completion(_client):
            with tracing.trace("Synchronous LiteLLM workflow"):
                with tracing.generation_span(
                    input=[{"role": "user", "content": "Hello"}],
                    output=None,
                    model=model,
                    model_config={"model_impl": "litellm"},
                ):
                    litellm.completion(
                        model=model,
                        messages=[{"role": "user", "content": "Hello"}],
                        base_url="https://example.com/v1",
                        api_key="fake-key",
                        temperature=0.4,
                    )

        call_mock_llm("litellm", invoke_llm_callback=invoke_completion, model="sync-model")

        span = self._spans_by_name()[f"chat {model}"]
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TEMPERATURE], 0.4)
        self.assertEqual(span.attributes[SERVER_ADDRESS], "example.com")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_ID], "chatcmpl-mock")
        self.assertEqual(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 10)
        self.assertEqual(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 20)

        self.exporter.clear()
        model = "openai/sync-stream-model"
        with tracing.trace("Synchronous streaming LiteLLM workflow"):
            with tracing.generation_span(
                input=[{"role": "user", "content": "Hello"}],
                output=None,
                model=model,
                model_config={"model_impl": "litellm"},
            ):
                list(
                    litellm.completion(
                        model=model,
                        messages=[{"role": "user", "content": "Hello"}],
                        stream=True,
                        mock_response="Hello",
                    )
                )

        span = self._spans_by_name()[f"chat {model}"]
        self.assertEqual(span.attributes[GEN_AI_REQUEST_MODEL], model)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_STREAM], True)
        self.assertIn(GEN_AI_RESPONSE_ID, span.attributes)
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_MODEL], "sync-stream-model")
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_FINISH_REASONS], ("stop",))

    def test_litellm_run_streamed_captures_metadata_without_content(self):
        model = "moonshot/test-model"
        cancelled_model = "ollama/cancelled-model"
        no_usage_model = "no-usage-model"
        instrumentor = OpenAIAgentsInstrumentor()
        instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            disable_openai_trace_export=True,
            skip_dep_check=True,
        )
        self.addCleanup(instrumentor.uninstrument)

        async def run_streamed(stream_model, include_usage, extra_args=None, cancel=False):
            result = Runner.run_streamed(
                Agent(
                    name="Streaming LiteLLM agent",
                    model=LitellmModel(model=stream_model),
                    model_settings=ModelSettings(
                        include_usage=include_usage,
                        reasoning={"effort": "high"},
                        extra_args={"mock_response": "Hello", "drop_params": True, **(extra_args or {})},
                    ),
                ),
                "Hello",
                run_config=RunConfig(trace_include_sensitive_data=not include_usage),
            )
            async for event in result.stream_events():
                if cancel and event.type == "raw_response_event":
                    result.cancel()
                    cancel = False

        async def run():
            await run_streamed(model, True, {"modalities": ["text", "audio"]})
            await run_streamed(cancelled_model, True, cancel=True)
            await run_streamed(
                no_usage_model,
                False,
                {
                    "api_base": "https://openrouter.ai/api/v1",
                    "custom_llm_provider": "openrouter",
                    "functions": [{"name": "lookup", "description": "Look up a city."}],
                },
            )

        asyncio.run(run())

        spans = self._spans_by_name()
        span = spans[f"chat {model}"]
        self.assertEqual(span.attributes[GEN_AI_PROVIDER_NAME], "moonshot")
        self.assertIn(GEN_AI_RESPONSE_ID, span.attributes)
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_MODEL], model)
        self.assertEqual(span.attributes[GEN_AI_RESPONSE_FINISH_REASONS], ("stop",))
        self.assertGreater(span.attributes[GEN_AI_USAGE_INPUT_TOKENS], 0)
        self.assertGreater(span.attributes[GEN_AI_USAGE_OUTPUT_TOKENS], 0)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_REASONING_LEVEL], "high")
        self.assertEqual(span.attributes[GEN_AI_OUTPUT_TYPE], GenAiOutputTypeValues.SPEECH.value)
        self.assertNotIn(GEN_AI_OUTPUT_MESSAGES, span.attributes)

        self.assertIn(f"chat {cancelled_model}", spans)

        no_usage_span = spans[f"chat {no_usage_model}"]
        sampling_call = next(
            call
            for call in self.sampler.should_sample.call_args_list
            if len(call.args) > 2 and call.args[2] == f"chat {no_usage_model}"
        )
        sampling_attributes = sampling_call.args[4]
        self.assertEqual(sampling_attributes[GEN_AI_PROVIDER_NAME], "openrouter")
        self.assertEqual(sampling_attributes[SERVER_ADDRESS], "openrouter.ai")
        self.assertEqual(no_usage_span.attributes[GEN_AI_PROVIDER_NAME], "openrouter")
        self.assertEqual(no_usage_span.attributes[SERVER_ADDRESS], "openrouter.ai")
        self.assertEqual(
            json.loads(no_usage_span.attributes[GEN_AI_TOOL_DEFINITIONS]),
            [{"type": "function", "name": "lookup", "description": "Look up a city."}],
        )
        self.assertNotIn(GEN_AI_USAGE_INPUT_TOKENS, no_usage_span.attributes)
        self.assertNotIn(GEN_AI_USAGE_OUTPUT_TOKENS, no_usage_span.attributes)
        self.assertIn(GEN_AI_OUTPUT_MESSAGES, no_usage_span.attributes)

    def test_sentinel_request_values_are_not_exported(self):
        attributes: dict = {}
        self.processor._set_attribute(
            attributes, GEN_AI_REQUEST_TEMPERATURE, Omit()
        )  # pylint: disable=protected-access
        self.processor._set_attribute(attributes, GEN_AI_REQUEST_TOP_P, 0.4)  # pylint: disable=protected-access
        self.assertEqual(attributes, {GEN_AI_REQUEST_TOP_P: 0.4})

        with tracing.trace("Sentinel workflow"):
            sentinel_span = tracing.generation_span(
                input=[{"role": "user", "content": "Hi"}],
                output=None,
                model="gpt-sentinel",
            )
            with sentinel_span:
                _record_request(self.capture, temperature=Omit(), max_tokens=Omit(), top_p=0.4)

        span = self._spans_by_name()["chat gpt-sentinel"]
        self.assertNotIn(GEN_AI_REQUEST_TEMPERATURE, span.attributes)
        self.assertNotIn(GEN_AI_REQUEST_MAX_TOKENS, span.attributes)
        self.assertEqual(span.attributes[GEN_AI_REQUEST_TOP_P], 0.4)

    def test_parenting_skips_unhandled_task_and_turn_spans(self):
        with tracing.trace("Parent workflow"):
            if hasattr(tracing, "task_span"):
                with tracing.task_span("task") as task:
                    with tracing.turn_span(1, "agent", parent=task) as turn:
                        with tracing.agent_span("Nested agent", parent=turn):
                            pass
            else:
                with tracing.custom_span("task") as task:
                    with tracing.custom_span("turn", parent=task) as turn:
                        with tracing.agent_span("Nested agent", parent=turn):
                            pass

        spans = self._spans_by_name()
        self.assertEqual(set(spans), {"invoke_workflow Parent workflow", "invoke_agent Nested agent"})
        workflow_span = spans["invoke_workflow Parent workflow"]
        agent_span = spans["invoke_agent Nested agent"]
        self.assertEqual(agent_span.parent.span_id, workflow_span.context.span_id)

    def test_error_status_and_shutdown_close_leaked_spans(self):
        with tracing.trace("Error workflow"):
            failed_span = tracing.function_span("failing tool")
            failed_span.start()
            failed_span.set_error({"message": "Tool failed", "data": {"error": "boom"}})
            failed_span.finish()

        failed = self._spans_by_name()["execute_tool failing tool"]
        self.assertEqual(failed.status.status_code, StatusCode.ERROR)
        self.assertEqual(failed.status.description, "Tool failed: boom")
        self.assertEqual(failed.attributes[ERROR_TYPE], "_OTHER")

        leaked_trace = tracing.trace("Leaked workflow")
        leaked_trace.start()
        leaked_agent = tracing.agent_span("Leaked agent", parent=leaked_trace)
        leaked_agent.start()
        self.processor.shutdown()

        leaked_spans = self._spans_by_name()
        for name in ("invoke_workflow Leaked workflow", "invoke_agent Leaked agent"):
            self.assertEqual(leaked_spans[name].status.status_code, StatusCode.ERROR)
            self.assertEqual(leaked_spans[name].status.description, "Trace ended before span completion")
            self.assertEqual(leaked_spans[name].attributes[ERROR_TYPE], "_OTHER")

    def test_unhandled_span_data_produces_no_span(self):
        with tracing.trace("Unhandled workflow"):
            with tracing.custom_span("ignored"):
                pass
        self.assertEqual(
            [span.name for span in self.exporter.get_finished_spans()], ["invoke_workflow Unhandled workflow"]
        )


class TestOpenAIAgentsMessages(unittest.TestCase):
    def test_chat_completions_messages(self):
        system, conversation = _GenAIMessageNormalizer.normalize_input_messages(
            [
                {"role": "developer", "content": "Follow instructions."},
                {"role": "user", "content": [{"type": "input_text", "text": "Hello"}]},
                {
                    "role": "assistant",
                    "content": "Calling a tool",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "function": {"name": "lookup", "arguments": '{"city": "Seattle"}'},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "Sunny"},
            ]
        )
        self.assertEqual(system, [{"type": "text", "content": "Follow instructions."}])
        self.assertEqual(conversation[0]["parts"], [{"type": "text", "content": "Hello"}])
        self.assertEqual(
            conversation[1]["parts"][-1],
            {
                "type": "tool_call",
                "id": "call_1",
                "name": "lookup",
                "arguments": {"city": "Seattle"},
            },
        )
        self.assertEqual(
            conversation[2],
            {
                "role": "tool",
                "parts": [{"type": "tool_call_response", "id": "call_1", "response": "Sunny"}],
            },
        )

    def test_responses_api_messages_and_finish_reasons(self):
        inputs = _GenAIMessageNormalizer.normalize_input_messages(
            [
                {"type": "function_call", "call_id": "call_2", "name": "search", "arguments": '{"q": "otel"}'},
                {"type": "function_call_output", "call_id": "call_2", "output": "result"},
            ]
        )[1]
        self.assertEqual(inputs[0]["parts"][0]["arguments"], {"q": "otel"})
        self.assertEqual(inputs[1]["parts"][0]["response"], "result")

        outputs = _GenAIMessageNormalizer.normalize_output_messages(
            [
                {"type": "reasoning", "summary": [{"text": "Think first"}]},
                {"type": "function_call", "call_id": "call_3", "name": "lookup", "arguments": "not-json"},
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Final answer"}],
                    "finish_reason": "end_turn",
                },
                {"role": "assistant", "content": "Filtered", "finish_reason": "content_filter"},
            ]
        )
        self.assertEqual(outputs[0]["parts"], [{"type": "reasoning", "content": "Think first"}])
        self.assertEqual(outputs[1]["parts"][0]["arguments"], "not-json")
        self.assertEqual(outputs[1]["finish_reason"], "tool_call")
        self.assertEqual(outputs[2]["parts"], [{"type": "text", "content": "Final answer"}])
        self.assertEqual(outputs[2]["finish_reason"], "stop")
        self.assertEqual(outputs[3]["finish_reason"], "content_filter")

    def test_object_and_scalar_payloads(self):
        class ModelDumpPayload(BaseModel):
            role: str
            content: str

        _, object_messages = _GenAIMessageNormalizer.normalize_input_messages(
            ModelDumpPayload(role="user", content="From an object")
        )
        self.assertEqual(
            object_messages,
            [{"role": "user", "parts": [{"type": "text", "content": "From an object"}]}],
        )

        _, scalar_input = _GenAIMessageNormalizer.normalize_input_messages(123)
        self.assertEqual(scalar_input, [{"role": "user", "parts": [{"type": "text", "content": "123"}]}])
        self.assertEqual(
            _GenAIMessageNormalizer.normalize_output_messages("plain output"),
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "plain output"}],
                    "finish_reason": "stop",
                }
            ],
        )
        self.assertEqual(
            _GenAIMessageNormalizer.normalize_output_messages(
                {"type": "function_call_output", "call_id": "call_4", "output": "done"}
            )[0]["finish_reason"],
            "stop",
        )
        self.assertEqual(
            _GenAIMessageNormalizer.normalize_output_messages({"type": "reasoning", "content": "fallback reasoning"})[
                0
            ]["parts"],
            [{"type": "reasoning", "content": "fallback reasoning"}],
        )
