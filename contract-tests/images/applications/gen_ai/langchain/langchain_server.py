# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from http.server import BaseHTTPRequestHandler

from langchain.agents import create_agent
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from mock_llm import MOCK_LLM_PORT, reset_llm_call_count, start_servers
from typing_extensions import override

from amazon.opentelemetry.distro.instrumentation.langchain import LangChainInstrumentor

os.environ["OPENAI_API_KEY"] = "fake-key"

LangChainInstrumentor().instrument()


class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if "langchain" in self.path:
            if "multiagent" in self.path:
                self._run_multi_agent()
            elif "agent" in self.path:
                self._run_single_agent()
            else:
                RequestHandler.main_status = 404
        self.send_response_only(self.main_status)
        self.end_headers()

    def _run_single_agent(self) -> None:  # pylint: disable=no-self-use
        reset_llm_call_count()
        RequestHandler.main_status = 200

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        @tool
        def get_weather(city: str) -> str:
            """Get the current weather for a city."""
            return f"Sunny, 72F in {city}"

        @tool
        def calculate(expression: str) -> str:
            """Evaluate a math expression and return the result."""
            return "42"

        llm = ChatOpenAI(model="gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)
        agent = create_agent(
            llm,
            [get_greeting, get_weather, calculate],
            name="TestAgent",
            system_prompt="You are a helpful assistant with access to greeting, weather, and calculator tools.",
        )
        agent.invoke({"messages": [("human", "Greet the world")]})

    def _run_multi_agent(self) -> None:  # pylint: disable=no-self-use
        reset_llm_call_count()
        RequestHandler.main_status = 200

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        @tool
        def format_message(message: str) -> str:
            """Format a message with decorations."""
            return f"*** {message} ***"

        llm = ChatOpenAI(model="gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)

        greeter = create_agent(
            llm,
            [get_greeting],
            name="GreeterAgent",
            system_prompt="You are a friendly greeter.",
        )
        formatter = create_agent(
            llm,
            [format_message],
            name="FormatterAgent",
            system_prompt="You are a message formatter.",
        )

        greeter.invoke({"messages": [("human", "Greet the world")]})
        reset_llm_call_count()
        formatter.invoke({"messages": [("human", "Format: Hello World")]})


if __name__ == "__main__":
    start_servers(RequestHandler)
