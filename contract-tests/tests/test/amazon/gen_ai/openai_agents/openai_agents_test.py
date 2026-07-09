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

            # The invoke_agent span aggregates the whole turn: the first user input must be visible.
            user_inputs = self._text_parts(input_messages, "user")
            self.assertTrue(user_inputs, "Expected a user input message on the invoke_agent span")
            self.assertTrue(user_inputs[0])

            # ...and the last assistant LLM output must be visible.
            agent_outputs = self._text_parts(output_messages, "assistant")
            self.assertTrue(agent_outputs, "Expected an assistant output message on the invoke_agent span")
            self.assertTrue(agent_outputs[-1])

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        super()._assert_chat_spans(chat_spans, expected_count)

        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertEqual(attrs[GEN_AI_REQUEST_TEMPERATURE].double_value, 0.7)

        # Across the chat turns we must be able to see the user input, the assistant output, and the
        # tool calls the model requested.
        input_messages = []
        output_messages = []
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            input_messages.extend(json.loads(attrs[GEN_AI_INPUT_MESSAGES].string_value))
            if GEN_AI_OUTPUT_MESSAGES in attrs:
                output_messages.extend(json.loads(attrs[GEN_AI_OUTPUT_MESSAGES].string_value))

        self.assertTrue(self._text_parts(input_messages, "user"), "Expected a user input message across chat spans")
        self.assertTrue(
            self._text_parts(output_messages, "assistant"), "Expected an assistant output message across chat spans"
        )
        self.assertTrue(self._tool_call_parts(input_messages), "Expected a tool call across chat spans")

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
        return [
            part
            for message in messages
            for part in message.get("parts", [])
            if part.get("type") == "tool_call"
        ]
