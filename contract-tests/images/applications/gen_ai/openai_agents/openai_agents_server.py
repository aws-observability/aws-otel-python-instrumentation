# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from http.server import BaseHTTPRequestHandler
from typing import Union

from agents import (
    Agent,
    Model,
    ModelSettings,
    Runner,
    function_tool,
    set_default_openai_api,
    set_default_openai_client,
    set_tracing_disabled,
)
from agents.extensions.models.litellm_model import LitellmModel
from mock_llm import MOCK_BEDROCK_PORT, MOCK_LLM_PORT, reset_bedrock_call_count, reset_llm_call_count, start_servers
from openai import AsyncOpenAI
from typing_extensions import override

os.environ["AWS_ACCESS_KEY_ID"] = "fake-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake-key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ENDPOINT_URL_BEDROCK_RUNTIME"] = f"http://localhost:{MOCK_BEDROCK_PORT}"
os.environ["OPENAI_API_KEY"] = "fake-key"

set_default_openai_client(
    AsyncOpenAI(base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", api_key="fake-key"),
    use_for_tracing=False,
)
set_default_openai_api("chat_completions")
set_tracing_disabled(False)


class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if "openai_agents" in self.path:
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

        @function_tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        agent = Agent(
            name="TestAgent",
            instructions="You are a helpful assistant that greets people.",
            model=self._create_model(),
            tools=[get_greeting],
            model_settings=ModelSettings(temperature=0.7),
        )
        Runner.run_sync(agent, "Greet the world")

    def _run_multi_agent(self) -> None:
        self._reset_model_call_count()
        RequestHandler.main_status = 200

        @function_tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        @function_tool
        def format_message(message: str) -> str:
            """Format a message with decorations."""
            return f"*** {message} ***"

        model = self._create_model()
        greeter = Agent(
            name="GreeterAgent",
            instructions="You are a friendly greeter.",
            model=model,
            tools=[get_greeting],
            model_settings=ModelSettings(temperature=0.7),
        )
        formatter = Agent(
            name="FormatterAgent",
            instructions="You are a message formatter.",
            model=model,
            tools=[format_message],
            model_settings=ModelSettings(temperature=0.7),
        )

        Runner.run_sync(greeter, "Greet the world")
        self._reset_model_call_count()
        Runner.run_sync(formatter, "Format: Hello World")

    def _create_model(self) -> Union[str, Model]:
        if "bedrock" in self.path:
            return LitellmModel(
                model="bedrock/anthropic.claude-3-haiku-20240307-v1:0",
                base_url=f"http://localhost:{MOCK_BEDROCK_PORT}",
            )
        return "gpt-4"

    def _reset_model_call_count(self) -> None:
        if "bedrock" in self.path:
            reset_bedrock_call_count()
        else:
            reset_llm_call_count()


if __name__ == "__main__":
    start_servers(RequestHandler)
