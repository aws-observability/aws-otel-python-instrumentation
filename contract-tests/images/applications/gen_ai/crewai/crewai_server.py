# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple

from crewai import LLM, Agent, Crew, Task
from crewai.tools import tool
from typing_extensions import override

_PORT: int = 8080
_MOCK_LLM_PORT: int = 8081

os.environ["OPENAI_API_KEY"] = "fake-key"
os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"

_llm_call_count = 0


class MockLLMHandler(BaseHTTPRequestHandler):
    # pylint: disable=invalid-name
    def do_POST(self):
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count += 1

        if _llm_call_count % 2 == 1:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"call_{_llm_call_count}",
                        "type": "function",
                        "function": {
                            "name": "get_greeting",
                            "arguments": json.dumps({"name": "World"}),
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        else:
            message = {"role": "assistant", "content": "Hello, World!"}
            finish_reason = "stop"

        response = {
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4",
            "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
        }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        pass


class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if self.in_path("crewai"):
            self._handle_crewai_request()
        self._end_request(self.main_status)

    def in_path(self, sub_path: str) -> bool:
        return sub_path in self.path

    def _handle_crewai_request(self) -> None:
        if self.in_path("multiagent"):
            self._run_multi_agent()
        elif self.in_path("agent"):
            self._run_single_agent()
        else:
            set_main_status(404)

    def _run_single_agent(self) -> None:  # pylint: disable=no-self-use
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count = 0
        set_main_status(200)

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        llm = LLM(
            model="openai/gpt-4",
            base_url=f"http://localhost:{_MOCK_LLM_PORT}/v1",
            temperature=0.7,
        )

        agent = Agent(
            role="Greeter",
            goal="Greet the user",
            backstory="You are a friendly greeter.",
            llm=llm,
            tools=[get_greeting],
            verbose=True,
        )

        task = Task(
            description="Greet the user warmly.",
            expected_output="A friendly greeting.",
            agent=agent,
        )

        crew = Crew(
            name="GreetingCrew",
            agents=[agent],
            tasks=[task],
            verbose=True,
        )

        crew.kickoff()

    def _run_multi_agent(self) -> None:  # pylint: disable=no-self-use
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count = 0
        set_main_status(200)

        @tool
        def get_greeting(name: str) -> str:
            """Get a greeting message for the given name."""
            return f"Hello, {name}!"

        @tool
        def format_message(message: str) -> str:
            """Format a message with decorations."""
            return f"*** {message} ***"

        openai_llm = LLM(
            model="openai/gpt-4",
            base_url=f"http://localhost:{_MOCK_LLM_PORT}/v1",
            temperature=0.7,
        )

        bedrock_llm = LLM(
            model="gpt-4",
            base_url=f"http://localhost:{_MOCK_LLM_PORT}/v1",
            temperature=0.5,
        )

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

        greet_task = Task(
            description="Greet the user warmly.",
            expected_output="A friendly greeting.",
            agent=greeter,
        )

        format_task = Task(
            description="Format the greeting nicely.",
            expected_output="A formatted greeting.",
            agent=formatter,
        )

        crew = Crew(
            name="MultiAgentCrew",
            agents=[greeter, formatter],
            tasks=[greet_task, format_task],
            verbose=True,
        )

        crew.kickoff()

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


def main() -> None:
    mock_llm_server = ThreadingHTTPServer(("0.0.0.0", _MOCK_LLM_PORT), MockLLMHandler)
    mock_llm_thread = Thread(target=mock_llm_server.serve_forever, daemon=True)
    mock_llm_thread.start()

    server_address: Tuple[str, int] = ("0.0.0.0", _PORT)
    server = ThreadingHTTPServer(server_address, RequestHandler)
    atexit.register(server.shutdown)
    atexit.register(mock_llm_server.shutdown)
    server_thread = Thread(target=server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()


if __name__ == "__main__":
    main()
