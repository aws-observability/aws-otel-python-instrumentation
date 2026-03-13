# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from http.server import BaseHTTPRequestHandler

from crewai import LLM, Agent, Crew, Task
from crewai.tools import tool
from mock_llm import MOCK_LLM_PORT, reset_llm_call_count, start_servers
from typing_extensions import override

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

    def _run_single_agent(self) -> None:  # pylint: disable=no-self-use
        reset_llm_call_count()
        RequestHandler.main_status = 200

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        llm = LLM(model="openai/gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)
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

        openai_llm = LLM(model="openai/gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.7)
        bedrock_llm = LLM(model="gpt-4", base_url=f"http://localhost:{MOCK_LLM_PORT}/v1", temperature=0.5)

        greeter = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=openai_llm,
            tools=[get_greeting],
            verbose=True,
        )
        formatter = Agent(
            role="Formatter",
            goal="Format messages nicely",
            backstory="You are a message formatter.",
            llm=bedrock_llm,
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


if __name__ == "__main__":
    start_servers(RequestHandler)
