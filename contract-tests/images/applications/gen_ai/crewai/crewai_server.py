# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

from crewai import LLM, Agent, Crew, Task
from crewai.tools import tool
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

os.environ["AWS_ACCESS_KEY_ID"] = "fake-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake-key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ENDPOINT_URL_BEDROCK_RUNTIME"] = f"http://localhost:{MOCK_BEDROCK_PORT}"
os.environ["OPENAI_API_KEY"] = "fake-key"
os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"


class RequestHandler:
    def __init__(self, path: str) -> None:
        self.path = path

    def handle(self) -> Response:
        if "crewai" in self.path:
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

        llm = self._create_llm()
        store_agent_output_tool = tool(store_agent_output)
        agent = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting, store_agent_output_tool],
            verbose=True,
        )
        task = Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=agent)
        Crew(name="GreetingCrew", agents=[agent], tasks=[task], verbose=True).kickoff()

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

        greeter = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting, greeter_store_agent_output],
            verbose=True,
        )
        formatter = Agent(
            role="Formatter",
            goal="Format messages nicely",
            backstory="You are a message formatter.",
            llm=llm,
            tools=[format_message, formatter_store_agent_output],
            verbose=True,
        )

        greet_task = Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=greeter)
        format_task = Task(
            description="Format the greeting nicely.", expected_output="A formatted greeting.", agent=formatter
        )

        Crew(
            name="MultiAgentCrew", agents=[greeter, formatter], tasks=[greet_task, format_task], verbose=True
        ).kickoff()

    def _create_llm(self) -> LLM:
        if "bedrock" in self.path:
            return LLM(
                model="bedrock/anthropic.claude-3-haiku-20240307-v1:0",
                temperature=0.7,
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-key",
                region_name="us-east-1",
            )
        return LLM(model="openai/gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)

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
