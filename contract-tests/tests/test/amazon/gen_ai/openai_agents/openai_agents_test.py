# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json

from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import (
    GEN_AI_AGENT_NAME,
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OPERATION_NAME,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_PROVIDER_NAME,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_SYSTEM_INSTRUCTIONS,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_NAME,
    GEN_AI_TOOL_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
    GenAITestBase,
)


class OpenAIAgentsTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-openai_agents-app"

    def test_openai_agents_single_agent(self):
        self.do_test_requests("openai_agents/agent", "GET", 200, 0, 0)

    def test_openai_agents_multi_agent(self):
        self.do_test_requests(
            "openai_agents/multiagent", "GET", 200, 0, 0, expected_agent_count=2, expected_tool_count=2
        )

    @override
    def _assert_invoke_agent_spans(self, invoke_agent_spans: list, expected_count: int = 1):
        self.assertEqual(len(invoke_agent_spans), expected_count)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
            self._assert_str_attribute(attrs, GEN_AI_PROVIDER_NAME, "openai")
            self.assertIn(GEN_AI_AGENT_NAME, attrs)
            self.assertIn(GEN_AI_REQUEST_MODEL, attrs)
            self.assertIn(GEN_AI_INPUT_MESSAGES, attrs)
            self.assertIn(GEN_AI_OUTPUT_MESSAGES, attrs)
            self.assertIn(GEN_AI_SYSTEM_INSTRUCTIONS, attrs)

    @override
    def _assert_execute_tool_spans(self, execute_tool_spans: list, expected_count: int = 1):
        self.assertGreaterEqual(len(execute_tool_spans), expected_count)
        for span in execute_tool_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)
            self._assert_str_attribute(attrs, GEN_AI_PROVIDER_NAME, "openai")
            self._assert_str_attribute(attrs, GEN_AI_TOOL_TYPE, "function")
            self.assertIn(GEN_AI_TOOL_NAME, attrs)
            self.assertIn(GEN_AI_TOOL_CALL_ARGUMENTS, attrs)
            self.assertIn(GEN_AI_TOOL_CALL_RESULT, attrs)

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        self.assertGreaterEqual(len(chat_spans), expected_count)
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.CHAT.value)
            self._assert_str_attribute(attrs, GEN_AI_PROVIDER_NAME, "openai")
            self._assert_str_attribute(attrs, GEN_AI_REQUEST_MODEL, "gpt-4")
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertEqual(attrs[GEN_AI_REQUEST_TEMPERATURE].double_value, 0.7)
            self.assertIn(GEN_AI_SYSTEM_INSTRUCTIONS, attrs)
            self.assertIn(GEN_AI_INPUT_MESSAGES, attrs)
            self.assertIn(GEN_AI_USAGE_INPUT_TOKENS, attrs)
            self.assertIn(GEN_AI_USAGE_OUTPUT_TOKENS, attrs)

            input_messages = json.loads(attrs[GEN_AI_INPUT_MESSAGES].string_value)
            self.assertIsInstance(input_messages, list)
            self.assertGreater(len(input_messages), 0)

        completed_spans = [s for s in chat_spans if GEN_AI_OUTPUT_MESSAGES in self._get_attributes_dict(s.attributes)]
        self.assertGreaterEqual(len(completed_spans), 1)
