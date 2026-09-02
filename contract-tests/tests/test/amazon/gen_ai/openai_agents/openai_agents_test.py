# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import GEN_AI_REQUEST_TEMPERATURE, GEN_AI_TOOL_DEFINITIONS, GenAITestBase


class OpenAIAgentsTest(GenAITestBase):
    _SINGLE_AGENT_TOOLS = frozenset({"build_greeting", "summarize_weather", "calculate_budget", "store_agent_output"})
    _MULTI_AGENT_TOOL_SETS = {
        frozenset({"build_greeting", "describe_audience", "store_agent_output"}),
        frozenset({"format_message", "add_delivery_metadata", "store_agent_output"}),
    }

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-openai_agents-app"

    def test_openai_agents_single_agent(self):
        self._do_test_for_each_llm(
            "openai_agents/agent",
            expected_tool_count=4,
            expected_s3_call_count=1,
        )

    def test_openai_agents_multi_agent(self):
        self._do_test_for_each_llm(
            "openai_agents/multiagent",
            expected_agent_count=2,
            expected_tool_count=6,
            expected_s3_call_count=2,
        )

    @override
    def _assert_chat_spans(self, chat_spans: list, expected_count: int = 1):
        super()._assert_chat_spans(chat_spans, expected_count)
        observed_tool_sets = set()
        for span in chat_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_REQUEST_TEMPERATURE, attrs)
            self.assertEqual(attrs[GEN_AI_REQUEST_TEMPERATURE].double_value, 0.7)
            tools = self._get_schema_value(attrs, GEN_AI_TOOL_DEFINITIONS, span.name)
            self.assertIsInstance(tools, list)
            self.assertTrue(tools)
            for tool in tools:
                self.assertEqual(tool["type"], "function")
                self.assertTrue(tool["description"])
                self.assertIsInstance(tool["parameters"], dict)
            observed_tool_sets.add(frozenset(tool["name"] for tool in tools))

        if self._SINGLE_AGENT_TOOLS in observed_tool_sets:
            self.assertEqual(observed_tool_sets, {self._SINGLE_AGENT_TOOLS})
        else:
            self.assertEqual(observed_tool_sets, self._MULTI_AGENT_TOOL_SETS)
