# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Union

from langchain.agents import create_agent
from langchain_aws import ChatBedrockConverse
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from mock_llm import (
    MOCK_BEDROCK_PORT,
    MOCK_LLM_PORT,
    reset_bedrock_call_count,
    reset_llm_call_count,
    start_servers,
    store_agent_output,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

os.environ["OPENAI_API_KEY"] = "fake-key"


class RequestHandler:
    def __init__(self, path: str) -> None:
        self.path = path

    def handle(self) -> Response:
        if "langchain" in self.path:
            if "multiagent" in self.path:
                self._run_multi_agent()
            elif "agent" in self.path:
                self._run_single_agent()
            else:
                return Response(status_code=404)
            return Response(status_code=200)
        return Response(status_code=404)

    def _run_single_agent(self) -> None:
        self._reset_model_call_count()

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
        store_agent_output_tool = tool(store_agent_output)
        agent = create_agent(
            llm,
            [get_greeting, get_weather, calculate, store_agent_output_tool],
            name="TestAgent",
            system_prompt=(
                "You are a helpful assistant with access to greeting, weather, calculator, and storage tools. "
                "Use every available tool before answering."
            ),
        )
        agent.invoke({"messages": [("human", "Greet the world")]})

    def _run_multi_agent(self) -> None:
        self._reset_model_call_count()

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        @tool
        def format_message(message: str) -> str:
            """Format a message with decorations."""
            return f"*** {message} ***"

        llm = self._create_llm()
        greeter_store_agent_output = tool(store_agent_output)
        formatter_store_agent_output = tool(store_agent_output)

        greeter = create_agent(
            llm,
            [get_greeting, greeter_store_agent_output],
            name="GreeterAgent",
            system_prompt="You are a friendly greeter. Use every available tool before answering.",
        )
        formatter = create_agent(
            llm,
            [format_message, formatter_store_agent_output],
            name="FormatterAgent",
            system_prompt="You are a message formatter. Use every available tool before answering.",
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


def handle_request(request: Request) -> Response:
    return RequestHandler(request.url.path).handle()


app = Starlette(routes=[Route("/{path:path}", handle_request, methods=["GET"])])


if __name__ == "__main__":
    start_servers(app)
