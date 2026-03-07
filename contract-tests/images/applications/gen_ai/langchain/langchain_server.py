# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from http.server import BaseHTTPRequestHandler

from langchain.agents import create_agent
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from mock_llm import MOCK_LLM_PORT, reset_llm_call_count, start_servers
from typing_extensions import override

os.environ["OPENAI_API_KEY"] = "fake-key"


class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if "langchain" in self.path:
            if "agent" in self.path:
                self._run_agent()
            elif "chat" in self.path:
                self._run_chat()
            else:
                RequestHandler.main_status = 404
        self.send_response_only(self.main_status)
        self.end_headers()

    def _run_agent(self) -> None:  # pylint: disable=no-self-use
        reset_llm_call_count()
        RequestHandler.main_status = 200

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        llm = ChatOpenAI(model="gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)
        agent = create_agent(llm, [get_greeting], name="TestAgent")
        agent.invoke({"messages": [("human", "Greet the world")]})

    def _run_chat(self) -> None:  # pylint: disable=no-self-use
        RequestHandler.main_status = 200
        llm = ChatOpenAI(model="gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1")
        llm.invoke("Say hello")


if __name__ == "__main__":
    start_servers(RequestHandler)
