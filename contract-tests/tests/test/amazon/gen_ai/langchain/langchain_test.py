# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import (
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_RESPONSE_ID,
    GEN_AI_RESPONSE_MODEL,
    GEN_AI_TOOL_DESCRIPTION,
    GenAITestBase,
)


class LangChainTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-langchain-app"

    def test_langchain_single_agent(self):
        self._do_test_for_each_llm("langchain/agent")

    def test_langchain_multi_agent(self):
        self._do_test_for_each_llm("langchain/multiagent", expected_agent_count=2)

    @override
    def _assert_invoke_agent_spans(self, invoke_agent_spans: list, expected_count: int = 1):
        super()._assert_invoke_agent_spans(invoke_agent_spans, expected_count)
        for span in invoke_agent_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)

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
            self.assertIn(GEN_AI_RESPONSE_ID, attrs)
