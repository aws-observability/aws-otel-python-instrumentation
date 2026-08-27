# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from http.server import BaseHTTPRequestHandler

from crewai import LLM, Agent, Crew, Task
from crewai.tools import tool
from mock_llm import MOCK_BEDROCK_PORT, MOCK_LLM_PORT, reset_bedrock_call_count, reset_llm_call_count, start_servers
from typing_extensions import override

os.environ["AWS_ACCESS_KEY_ID"] = "fake-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake-key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ENDPOINT_URL_BEDROCK_RUNTIME"] = f"http://localhost:{MOCK_BEDROCK_PORT}"
os.environ["OPENAI_API_KEY"] = "fake-key"
os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"


class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if "crewai" in self.path:
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

        llm = self._create_llm()
        agent = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting],
            verbose=True,
        )
        task = Task(description="Greet the user warmly.", expected_output="A friendly greeting.", agent=agent)
        Crew(name="GreetingCrew", agents=[agent], tasks=[task], verbose=True).kickoff()

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

        greeter = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting],
            verbose=True,
        )
        formatter = Agent(
            role="Formatter",
            goal="Format messages nicely",
            backstory="You are a message formatter.",
            llm=llm,
            tools=[format_message],
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


if __name__ == "__main__":
    start_servers(RequestHandler)
