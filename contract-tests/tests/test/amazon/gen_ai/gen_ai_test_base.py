# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
from typing import Any, Dict, List, Optional

from mock_collector_client import ResourceScopeSpan
from requests import Response, request
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
    GEN_AI_SYSTEM,
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
    GenAiProviderNameValues,
)
from opentelemetry.semconv.trace import SpanAttributes

AGENT_FINAL_OUTPUT = "Hello, World!"
SESSION_ID = "gen-ai-contract-test-session"


class GenAITestBase(ContractTestBase):

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {
            "AGENT_OBSERVABILITY_ENABLED": "true",
            "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false",
            "OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED": "false",
            "OTEL_AWS_SERVICE_EVENTS_ENABLED": "false",
            "OTEL_LOGS_EXPORTER": "none",
            "OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS": "false",
        }

    @override
    def get_application_wait_pattern(self) -> str:
        return "Uvicorn running on"

    @override
    def do_test_requests(
        self, path: str, method: str, status_code: int, expected_error: int, expected_fault: int, **kwargs
    ) -> None:
        response = self.send_request(method, path)
        self.assertEqual(status_code, response.status_code)

        resource_scope_spans = self.mock_collector_client.get_traces()
        self._assert_aws_span_attributes(resource_scope_spans, path, **kwargs)
        self._assert_semantic_conventions_span_attributes(resource_scope_spans, method, path, status_code, **kwargs)

    @override
    def send_request(self, method, path) -> Response:
        address = self.application.get_container_host_ip()
        port = self.application.get_exposed_port(self.get_application_port())
        url = f"http://{address}:{port}/{path}"
        return request(method, url, headers={"baggage": f"session.id={SESSION_ID}"}, timeout=20)

    def _do_test_for_each_llm(self, path: str, **kwargs) -> None:
        # Every model-backed scenario MUST run unchanged against both supported LLM providers.
        for llm, provider, model in (
            ("openai", GenAiProviderNameValues.OPENAI.value, "gpt-4"),
            (
                "bedrock",
                GenAiProviderNameValues.AWS_BEDROCK.value,
                "anthropic.claude-3-haiku-20240307-v1:0",
            ),
        ):
            with self.subTest(llm=llm):
                try:
                    self.do_test_requests(
                        f"{path}/{llm}",
                        "GET",
                        200,
                        0,
                        0,
                        expected_provider=provider,
                        expected_model=model,
                        **kwargs,
                    )
                finally:
                    self.mock_collector_client.clear_signals()

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        expected_s3_call_count = kwargs.get("expected_s3_call_count", 0)
        spans = [resource_scope_span.span for resource_scope_span in resource_scope_spans]

        # Every span created for the inbound request MUST retain the session identifier extracted from W3C baggage.
        for span in spans:
            self._assert_str_attribute(self._get_attributes_dict(span.attributes), "session.id", SESSION_ID)

        if expected_s3_call_count:
            store_tool_spans = []
            for span in spans:
                attrs = self._get_attributes_dict(span.attributes)
                if attrs.get(GEN_AI_TOOL_NAME) and attrs[GEN_AI_TOOL_NAME].string_value == "store_agent_output":
                    store_tool_spans.append(span)

            s3_spans = [span for span in spans if span.name == "S3.PutObject"]

            # Every tested agent MUST execute the shared storage tool and emit one downstream S3 span.
            self.assertEqual(len(store_tool_spans), expected_s3_call_count)
            self.assertEqual(len(s3_spans), expected_s3_call_count)

            # Each S3 call MUST be a direct child of the store_agent_output tool that initiated it.
            self.assertEqual(
                {span.parent_span_id for span in s3_spans},
                {span.span_id for span in store_tool_spans},
            )
            for span in s3_spans:
                attrs = self._get_attributes_dict(span.attributes)
                self._assert_str_attribute(attrs, SpanAttributes.AWS_S3_BUCKET, "agent-results")

    @override
    def _assert_metric_attributes(self, resource_scope_metrics, metric_name: str, expected_sum: int, **kwargs) -> None:
        pass

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        # Every recorded GenAI content attribute MUST contain data and use its OTel-defined representation.
        self._assert_otel_gen_ai_attribute_formats(resource_scope_spans)
        invoke_agent_spans, execute_tool_spans, chat_spans = self._collect_gen_ai_spans(resource_scope_spans)
        expected_provider = kwargs.get("expected_provider")
        if expected_provider is not None:
            # Every span declaring a GenAI provider MUST identify the LLM selected for this test run.
            self._assert_gen_ai_provider(resource_scope_spans, expected_provider)
        if "agent" in path:
            self._assert_invoke_agent_spans(invoke_agent_spans, kwargs.get("expected_agent_count", 1))
            self._assert_execute_tool_spans(execute_tool_spans, kwargs.get("expected_tool_count", 1))
        self._assert_chat_spans(chat_spans, kwargs.get("expected_chat_count", 1))

    def _assert_gen_ai_provider(self, resource_scope_spans: List[ResourceScopeSpan], expected_provider: str) -> None:
        providers = []
        for resource_scope_span in resource_scope_spans:
            attrs = self._get_attributes_dict(resource_scope_span.span.attributes)
            provider = self._get_gen_ai_provider(attrs)
            if provider is not None:
                providers.append(provider)

        # At least one span MUST identify the selected LLM provider using the current or legacy OTel attribute.
        self.assertIn(expected_provider, providers)

    def _collect_gen_ai_spans(self, resource_scope_spans: List[ResourceScopeSpan]):
        invoke_agent_spans = []
        execute_tool_spans = []
        chat_spans = []
        for resource_scope_span in resource_scope_spans:
            span = resource_scope_span.span
            attrs = self._get_attributes_dict(span.attributes)
            if "invoke_agent" in span.name:
                invoke_agent_spans.append(span)
            elif "execute_tool" in span.name:
                execute_tool_spans.append(span)
            elif (
                GEN_AI_OPERATION_NAME in attrs
                and attrs[GEN_AI_OPERATION_NAME].string_value == GenAiOperationNameValues.CHAT.value
            ):
                chat_spans.append(span)
        return invoke_agent_spans, execute_tool_spans, self._select_gen_ai_provider_spans(chat_spans)

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
            self.assertIsNotNone(self._get_gen_ai_provider(attrs))
            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)
            self.assertIn(GEN_AI_INPUT_MESSAGES, attrs)
            self.assertIn(GEN_AI_SYSTEM_INSTRUCTIONS, attrs)
            self.assertIn(GEN_AI_USAGE_INPUT_TOKENS, attrs)
            self.assertIn(GEN_AI_USAGE_OUTPUT_TOKENS, attrs)
            input_messages.extend(self._get_schema_value(attrs, GEN_AI_INPUT_MESSAGES, span.name))
            if GEN_AI_OUTPUT_MESSAGES in attrs:
                output_messages.extend(self._get_schema_value(attrs, GEN_AI_OUTPUT_MESSAGES, span.name))
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
        input_messages = self._get_schema_value(attrs, GEN_AI_INPUT_MESSAGES, span_name)
        output_messages = self._get_schema_value(attrs, GEN_AI_OUTPUT_MESSAGES, span_name)

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
            is_execute_tool_span = (
                GEN_AI_OPERATION_NAME in attrs
                and attrs[GEN_AI_OPERATION_NAME].string_value == GenAiOperationNameValues.EXECUTE_TOOL.value
            )

            for attribute, schema_name in (
                (GEN_AI_INPUT_MESSAGES, "gen-ai-input-messages"),
                (GEN_AI_OUTPUT_MESSAGES, "gen-ai-output-messages"),
                (GEN_AI_SYSTEM_INSTRUCTIONS, "gen-ai-system-instructions"),
                (GEN_AI_TOOL_DEFINITIONS, "gen-ai-tool-definitions"),
                (GEN_AI_TOOL_CALL_ARGUMENTS, "gen-ai-tool-call-arguments"),
                (GEN_AI_TOOL_CALL_RESULT, "gen-ai-tool-call-result"),
            ):
                # execute_tool spans MUST contain a result; structured content MUST follow its OTel JSON schema.
                if attribute in attrs or (is_execute_tool_span and attribute == GEN_AI_TOOL_CALL_RESULT):
                    is_tool_value = attribute in (GEN_AI_TOOL_CALL_ARGUMENTS, GEN_AI_TOOL_CALL_RESULT)
                    schema_value = self._get_schema_value(
                        attrs,
                        attribute,
                        span.name,
                        allow_raw_otlp_value=is_tool_value,
                    )
                    self.assertIsNotNone(schema_value, f"{span.name}: expected {attribute} to contain data")
                    if isinstance(schema_value, (dict, list, str, bytes)):
                        self.assertTrue(schema_value, f"{span.name}: expected {attribute} to contain data")
                    # Tool arguments and results have type `any`; validate them when emitted as structured JSON.
                    if not is_tool_value or isinstance(schema_value, dict):
                        validate_otel_genai_schema(
                            schema_value,
                            schema_name,
                        )

    def _get_schema_value(
        self,
        attrs: Dict[str, AnyValue],
        attribute: str,
        span_name: str,
        allow_raw_otlp_value: bool = False,
    ) -> Any:
        self.assertIn(attribute, attrs, f"{span_name}: expected {attribute}")
        value = attrs[attribute]
        value_kind = value.WhichOneof("value")
        if allow_raw_otlp_value and value_kind != "string_value":
            return self._any_value_to_python(value)
        self.assertEqual(
            value_kind,
            "string_value",
            f"{span_name}: {attribute} must be an OTLP string containing JSON",
        )
        try:
            schema_value = json.loads(value.string_value)
        except json.JSONDecodeError as error:
            if allow_raw_otlp_value:
                return value.string_value
            self.fail(f"{span_name}: {attribute} must contain valid JSON: {error}")
        if allow_raw_otlp_value and not isinstance(schema_value, (dict, list)):
            return value.string_value
        return schema_value

    @classmethod
    def _any_value_to_python(cls, value: AnyValue) -> Any:
        value_kind = value.WhichOneof("value")
        if value_kind == "array_value":
            return [cls._any_value_to_python(item) for item in value.array_value.values]
        if value_kind == "kvlist_value":
            return {item.key: cls._any_value_to_python(item.value) for item in value.kvlist_value.values}
        return getattr(value, value_kind)

    @staticmethod
    def _get_gen_ai_provider(attrs: Dict[str, AnyValue]) -> Optional[str]:
        provider = attrs.get(GEN_AI_PROVIDER_NAME) or attrs.get(GEN_AI_SYSTEM)
        return provider.string_value if provider is not None else None

    def _select_gen_ai_provider_spans(self, spans: list) -> list:
        current_spans = [span for span in spans if GEN_AI_PROVIDER_NAME in self._get_attributes_dict(span.attributes)]
        if current_spans:
            return current_spans
        return [span for span in spans if GEN_AI_SYSTEM in self._get_attributes_dict(span.attributes)]

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
