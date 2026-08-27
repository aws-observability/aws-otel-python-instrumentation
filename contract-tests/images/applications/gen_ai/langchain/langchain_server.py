# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from http.server import BaseHTTPRequestHandler
from typing import Union

from langchain.agents import create_agent
from langchain_aws import ChatBedrockConverse
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from mock_llm import MOCK_BEDROCK_PORT, MOCK_LLM_PORT, reset_bedrock_call_count, reset_llm_call_count, start_servers
from typing_extensions import override

os.environ["OPENAI_API_KEY"] = "fake-key"


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

    def _run_single_agent(self) -> None:
        self._reset_model_call_count()
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

        llm = self._create_llm()
        agent = create_agent(
            llm,
            [get_greeting, get_weather, calculate],
            name="TestAgent",
            system_prompt="You are a helpful assistant with access to greeting, weather, and calculator tools.",
        )
        agent.invoke({"messages": [("human", "Greet the world")]})

    def _run_multi_agent(self) -> None:
        self._reset_model_call_count()
        RequestHandler.main_status = 200

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        @tool
        def format_message(message: str) -> str:
            """Format a message with decorations."""
            return f"*** {message} ***"

        llm = self._create_llm()

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
        self._reset_model_call_count()
        formatter.invoke({"messages": [("human", "Format: Hello World")]})

    def _create_llm(self) -> Union[ChatBedrockConverse, ChatOpenAI]:
        if "bedrock" in self.path:
            return ChatBedrockConverse(
                model="anthropic.claude-3-haiku-20240307-v1:0",
                region_name="us-east-1",
                base_url=f"http://localhost:{MOCK_BEDROCK_PORT}",
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-key",
                temperature=0.7,
            )
        return ChatOpenAI(model="gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)

    def _reset_model_call_count(self) -> None:
        if "bedrock" in self.path:
            reset_bedrock_call_count()
        else:
            reset_llm_call_count()


if __name__ == "__main__":
    start_servers(RequestHandler)
