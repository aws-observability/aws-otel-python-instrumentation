# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json

from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import (
    GEN_AI_INPUT_MESSAGES,
    GEN_AI_OUTPUT_MESSAGES,
    GEN_AI_REQUEST_TEMPERATURE,
    GenAITestBase,
)

_EXPECTED_USER_INPUTS = {"Greet the world", "Format: Hello World"}
_EXPECTED_FINAL_OUTPUT = "Hello, World!"


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
        super()._assert_invoke_agent_spans(invoke_agent_spans, expected_count)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            input_messages = json.loads(attrs[GEN_AI_INPUT_MESSAGES].string_value)
            output_messages = json.loads(attrs[GEN_AI_OUTPUT_MESSAGES].string_value)

            user_inputs = self._text_parts(input_messages, "user")
            self.assertTrue(user_inputs)
            self.assertIn(user_inputs[0], _EXPECTED_USER_INPUTS)

            agent_outputs = self._text_parts(output_messages, "assistant")
            self.assertTrue(agent_outputs)
            self.assertEqual(agent_outputs[-1], _EXPECTED_FINAL_OUTPUT)

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        super()._assert_chat_spans(chat_spans, expected_count)
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertEqual(attrs[GEN_AI_REQUEST_TEMPERATURE].double_value, 0.7)
