# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from typing import Any, Dict, List, Optional

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase
from amazon.gen_ai.otel_schema import validate_otel_genai_schema
from opentelemetry.proto.common.v1.common_pb2 import AnyValue
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (  # noqa: F401
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_DEFINITIONS,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
)

AGENT_FINAL_OUTPUT = "Hello, World!"


class GenAITestBase(ContractTestBase):

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        pass

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        # Every recorded GenAI content attribute MUST contain data and follow its corresponding OTel JSON schema.
        self._assert_otel_gen_ai_attribute_formats(resource_scope_spans)
        invoke_agent_spans, execute_tool_spans, chat_spans = self._collect_gen_ai_spans(resource_scope_spans)
        expected_provider = kwargs.get("expected_provider")
        if expected_provider is not None:
            for span in invoke_agent_spans + chat_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, GEN_AI_PROVIDER_NAME, expected_provider)
        if "agent" in path:
            self._assert_invoke_agent_spans(invoke_agent_spans, kwargs.get("expected_agent_count", 1))
            self._assert_execute_tool_spans(execute_tool_spans, kwargs.get("expected_tool_count", 1))
        self._assert_chat_spans(chat_spans, kwargs.get("expected_chat_count", 1))

    def _collect_gen_ai_spans(self, resource_scope_spans: List[ResourceScopeSpan]):
        invoke_agent_spans = []
        execute_tool_spans = []
        chat_spans = []
        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            if "invoke_agent" in span.name:
                invoke_agent_spans.append(span)
            elif "execute_tool" in span.name:
                execute_tool_spans.append(span)
            elif "chat" in span.name.lower():
                chat_spans.append(span)
        return invoke_agent_spans, execute_tool_spans, chat_spans

    def _assert_invoke_agent_spans(self, invoke_agent_spans: list, expected_count: int = 1):
        self.assertEqual(len(invoke_agent_spans), expected_count)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
            self.assertIn(GEN_AI_AGENT_NAME, attrs)
            self.assertIn(GEN_AI_PROVIDER_NAME, attrs)
            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)

            # invoke_agent spans SHOULD include the initial user input and final agent output.
            self._assert_invoke_agent_content(attrs, span.name, AGENT_FINAL_OUTPUT)

    def _assert_execute_tool_spans(self, execute_tool_spans: list, expected_count: int = 1):
        self.assertGreaterEqual(len(execute_tool_spans), expected_count)
        for span in execute_tool_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)
            self.assertIn(GEN_AI_TOOL_NAME, attrs)
            self._assert_str_attribute(attrs, GEN_AI_TOOL_TYPE, "function")
            self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, attrs)
            self.assertIn(GEN_AI_TOOL_CALL_RESULT, attrs)

    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        self.assertGreaterEqual(len(chat_spans), expected_count)
        input_messages = []
        output_messages = []
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            self.assertIn(GEN_AI_PROVIDER_NAME, attrs)
            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)
            self.assertIn(GEN_AI_INPUT_MESSAGES, attrs)
            self.assertIn(GEN_AI_SYSTEM_INSTRUCTIONS, attrs)
            self.assertIn(GEN_AI_USAGE_INPUT_TOKENS, attrs)
            self.assertIn(GEN_AI_USAGE_OUTPUT_TOKENS, attrs)
            input_messages.extend(self._parse_json_attribute(attrs, GEN_AI_INPUT_MESSAGES, span.name))
            if GEN_AI_OUTPUT_MESSAGES in attrs:
                output_messages.extend(self._parse_json_attribute(attrs, GEN_AI_OUTPUT_MESSAGES, span.name))
        completed_spans = [s for s in chat_spans if GEN_AI_OUTPUT_MESSAGES in self._get_attributes_dict(s.attributes)]
        self.assertGreaterEqual(len(completed_spans), 1)

        self.assertTrue(self._text_parts(input_messages, "user"), "Expected a user input message across chat spans")
        self.assertTrue(
            self._text_parts(output_messages, "assistant"), "Expected an assistant output message across chat spans"
        )
        self.assertTrue(self._tool_call_parts(input_messages), "Expected a tool call in chat span input messages")

    def _assert_invoke_agent_content(
        self, attrs: Dict[str, AnyValue], span_name: str, expected_output: Optional[str] = None
    ) -> None:
        input_messages = self._parse_json_attribute(attrs, GEN_AI_INPUT_MESSAGES, span_name)
        output_messages = self._parse_json_attribute(attrs, GEN_AI_OUTPUT_MESSAGES, span_name)

        # gen_ai.input.messages MUST preserve message order, with the initial user input first.
        self.assertTrue(input_messages, f"{span_name}: expected the first user input on the invoke_agent span")
        self.assertEqual(
            input_messages[0].get("role"),
            "user",
            f"{span_name}: expected the first invoke_agent input message to be from the user",
        )
        first_user_input = self._text_parts(input_messages[:1], "user")
        self.assertTrue(first_user_input, f"{span_name}: expected non-empty text in the first user input")

        # gen_ai.output.messages SHOULD end with the final output returned by the agent invocation.
        self.assertTrue(output_messages, f"{span_name}: expected the last agent output on the invoke_agent span")
        self.assertEqual(
            output_messages[-1].get("role"),
            "assistant",
            f"{span_name}: expected the last invoke_agent output message to be from the assistant",
        )
        last_agent_output = self._text_parts(output_messages[-1:], "assistant")
        self.assertTrue(last_agent_output, f"{span_name}: expected non-empty text in the last agent output")
        if expected_output is not None:
            self.assertEqual(last_agent_output[-1], expected_output)

    def _assert_otel_gen_ai_attribute_formats(self, resource_scope_spans: List[ResourceScopeSpan]) -> None:
        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            for attribute, schema_name in (
                (GEN_AI_INPUT_MESSAGES, "gen-ai-input-messages"),
                (GEN_AI_OUTPUT_MESSAGES, "gen-ai-output-messages"),
                (GEN_AI_SYSTEM_INSTRUCTIONS, "gen-ai-system-instructions"),
                (GEN_AI_TOOL_DEFINITIONS, "gen-ai-tool-definitions"),
                (GEN_AI_TOOL_CALL_ARGUMENTS, "gen-ai-tool-call-arguments"),
                (GEN_AI_TOOL_CALL_RESULT, "gen-ai-tool-call-result"),
            ):
                if attribute in attrs:
                    schema_value = self._parse_json_attribute(attrs, attribute, span.name)
                    self.assertIsNotNone(schema_value, f"{span.name}: expected {attribute} to contain data")
                    if isinstance(schema_value, list):
                        self.assertTrue(schema_value, f"{span.name}: expected {attribute} to contain data")
                    validate_otel_genai_schema(
                        schema_value,
                        schema_name,
                    )

    def _parse_json_attribute(self, attrs: Dict[str, AnyValue], attribute: str, span_name: str) -> Any:
        self.assertIn(attribute, attrs, f"{span_name}: expected {attribute}")
        value = attrs[attribute]
        self.assertEqual(
            value.WhichOneof("value"),
            "string_value",
            f"{span_name}: {attribute} must be an OTLP string containing JSON",
        )
        try:
            return json.loads(value.string_value)
        except json.JSONDecodeError as error:
            self.fail(f"{span_name}: {attribute} must contain valid JSON: {error}")

    @staticmethod
    def _text_parts(messages: list, role: str) -> list:
        return [
            part["content"]
            for message in messages
            if message.get("role") == role
            for part in message.get("parts", [])
            if part.get("type") == "text" and part.get("content")
        ]

    @staticmethod
    def _tool_call_parts(messages: list) -> list:
        return [part for message in messages for part in message.get("parts", []) if part.get("type") == "tool_call"]
