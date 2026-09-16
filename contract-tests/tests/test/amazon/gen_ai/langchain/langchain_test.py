# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import (
    AGENT_FINAL_OUTPUT,
    GEN_AI_AGENT_NAME,
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_TOOL_DESCRIPTION,
    GenAiOperationNameValues,
    GenAiProviderNameValues,
    GenAITestBase,
)


class LangChainTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-langchain-app"

    def test_langchain_single_agent(self):
        self._do_test_for_each_llm(
            "langchain/agent",
            expected_tool_count=4,
            expected_s3_call_count=1,
        )

    def test_langchain_multi_agent(self):
        self._do_test_for_each_llm(
            "langchain/multiagent",
            expected_agent_count=2,
            expected_tool_count=4,
            expected_s3_call_count=2,
        )

    @override
    def _assert_invoke_agent_spans(self, invoke_agent_spans: list, expected_count: int = 1):
        self.assertEqual(len(invoke_agent_spans), expected_count)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self._assert_str_attribute(attrs, GEN_AI_OPERATION_NAME, GenAiOperationNameValues.INVOKE_AGENT.value)
            self.assertIn(GEN_AI_AGENT_NAME, attrs)
            self._assert_invoke_agent_content(attrs, span.name, AGENT_FINAL_OUTPUT)

    @override
    def _assert_execute_tool_spans(self, execute_tool_spans: list, expected_count: int = 1):
        super()._assert_execute_tool_spans(execute_tool_spans, expected_count)
        for span in execute_tool_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_TOOL_DESCRIPTION, attrs)

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        super()._assert_chat_spans(chat_spans, expected_count)
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertIn(GEN_AI_RESPONSE_MODEL, attrs)
            # The OpenAI mock returns a response ID; the Bedrock Converse response does not expose one.
            if self._get_gen_ai_provider(attrs) == GenAiProviderNameValues.OPENAI.value:
                self.assertIn(GEN_AI_RESPONSE_ID, attrs)
