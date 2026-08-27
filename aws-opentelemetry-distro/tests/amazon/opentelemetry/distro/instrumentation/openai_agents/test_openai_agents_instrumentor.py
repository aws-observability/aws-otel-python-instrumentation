# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest
from importlib.metadata import entry_points
from types import SimpleNamespace
from unittest.mock import MagicMock

from agents import tracing
from pydantic import BaseModel

from amazon.opentelemetry.distro.instrumentation.openai_agents import OpenAIAgentsInstrumentor
from amazon.opentelemetry.distro.instrumentation.openai_agents._processor import (
    OpenTelemetryTracingProcessor,
    _GenAIMessageNormalizer,
)
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
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_K,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_WORKFLOW_NAME,
    GenAiOperationNameValues,
    GenAiProviderNameValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.trace import SpanKind, StatusCode


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

    def test_force_flush_delegates_to_tracer_provider(self):
        tracer_provider = TracerProvider()
        tracer_provider.force_flush = MagicMock(return_value=True)
        try:
            self.instrumentor.instrument(
                tracer_provider=tracer_provider,
                skip_dep_check=True,
            )

            self.instrumentor._processor.force_flush()  # pylint: disable=protected-access

            tracer_provider.force_flush.assert_called_once_with()
        finally:
            self.instrumentor.uninstrument()
            tracer_provider.shutdown()


class TestOpenAIAgentsTracingProcessor(unittest.TestCase):
    def setUp(self) -> None:
        self.exporter = InMemorySpanExporter()
        self.tracer_provider = TracerProvider()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        tracer = self.tracer_provider.get_tracer(
            "amazon.opentelemetry.distro.instrumentation.openai_agents",
            "test",
        )
        self.processor = OpenTelemetryTracingProcessor(tracer)
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
