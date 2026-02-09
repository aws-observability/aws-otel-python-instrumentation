# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List

from mock_collector_client import ResourceScopeSpan
from typing_extensions import override

from amazon.base.contract_test_base import ContractTestBase

GEN_AI_OPERATION_NAME: str = "gen_ai.operation.name"
GEN_AI_AGENT_NAME: str = "gen_ai.agent.name"
GEN_AI_PROVIDER_NAME: str = "gen_ai.provider.name"
GEN_AI_REQUEST_MODEL: str = "gen_ai.request.model"
GEN_AI_REQUEST_TEMPERATURE: str = "gen_ai.request.temperature"
GEN_AI_RESPONSE_MODEL: str = "gen_ai.response.model"
GEN_AI_USAGE_INPUT_TOKENS: str = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS: str = "gen_ai.usage.output_tokens"
GEN_AI_TOOL_NAME: str = "gen_ai.tool.name"
GEN_AI_TOOL_TYPE: str = "gen_ai.tool.type"
GEN_AI_TOOL_DESCRIPTION: str = "gen_ai.tool.description"
GEN_AI_TOOL_CALL_ARGUMENTS: str = "gen_ai.tool.call.arguments"
GEN_AI_TOOL_CALL_RESULT: str = "gen_ai.tool.call.result"


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
        invoke_agent_spans, execute_tool_spans, chat_spans = self._collect_gen_ai_spans(resource_scope_spans)
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
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, "invoke_agent")
            self.assertIn(GEN_AI_AGENT_NAME, attrs)
            self.assertIn(GEN_AI_PROVIDER_NAME, attrs)
            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)

    def _assert_execute_tool_spans(self, execute_tool_spans: list, expected_count: int = 1):
        self.assertGreaterEqual(len(execute_tool_spans), expected_count)
        for span in execute_tool_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, "execute_tool")
            self.assertIn(GEN_AI_TOOL_NAME, attrs)
            self._assert_str_attribute(attrs, GEN_AI_TOOL_TYPE, "function")
            self.assertIn(GEN_AI_TOOL_DESCRIPTION, attrs)
            self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, attrs)
            self.assertIn(GEN_AI_TOOL_CALL_RESULT, attrs)

    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        self.assertGreaterEqual(len(chat_spans), expected_count)
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, "chat")
            self.assertIn(GEN_AI_PROVIDER_NAME, attrs)
            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)
            self.assertIn(GEN_AI_RESPONSE_MODEL, attrs)
            self.assertIn(GEN_AI_USAGE_INPUT_TOKENS, attrs)
            self.assertIn(GEN_AI_USAGE_OUTPUT_TOKENS, attrs)
