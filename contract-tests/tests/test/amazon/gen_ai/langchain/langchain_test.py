# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.gen_ai.gen_ai_test_base import GenAITestBase


class LangChainTest(GenAITestBase):
    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-langchain-app"

    def test_langchain_agent(self):
        self.do_test_requests("langchain/agent", "GET", 200, 0, 0)

    def test_langchain_chat(self):
        self.do_test_requests("langchain/chat", "GET", 200, 0, 0)
