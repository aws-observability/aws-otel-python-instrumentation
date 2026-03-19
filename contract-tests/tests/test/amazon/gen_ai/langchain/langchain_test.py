# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import GEN_AI_RESPONSE_ID, GenAITestBase


class LangChainTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-langchain-app"

    def test_langchain_single_agent(self):
        self.do_test_requests("langchain/agent", "GET", 200, 0, 0)

    def test_langchain_multi_agent(self):
        self.do_test_requests("langchain/multiagent", "GET", 200, 0, 0, expected_agent_count=2)

    @override
    def _assert_invoke_model_spans(self, invoke_model_spans: list, expected_count: int = 1):
        super()._assert_invoke_model_spans(invoke_model_spans, expected_count)
        for span in invoke_model_spans:
            attrs = self._get_attributes_dict(span.attributes)
            self.assertIn(GEN_AI_RESPONSE_ID, attrs)
