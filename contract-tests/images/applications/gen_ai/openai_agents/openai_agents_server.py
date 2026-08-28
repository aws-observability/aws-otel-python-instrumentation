# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import List, Union

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
from mock_llm import (
    MOCK_BEDROCK_PORT,
    MOCK_LLM_PORT,
    reset_bedrock_call_count,
    reset_llm_call_count,
    start_servers,
    store_agent_output,
)
from openai import AsyncOpenAI
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

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


class RequestHandler:
    def __init__(self, path: str) -> None:
        self.path = path

    def handle(self) -> Response:
        if "openai_agents" in self.path:
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

        @function_tool
        def build_greeting(name: str, language: str, excited: bool) -> str:
            """Build a personalized greeting in the requested language and tone."""
            punctuation = "!" if excited else "."
            return f"{language} greeting for {name}{punctuation}"

        @function_tool
        def summarize_weather(city: str, temperatures: List[float], include_advice: bool) -> str:
            """Summarize temperatures for a city and optionally include travel advice."""
            average = sum(temperatures) / len(temperatures)
            advice = " Pack layers." if include_advice else ""
            return f"{city} average: {average:.1f}F.{advice}"

        @function_tool
        def calculate_budget(amounts: List[float], include_tax: bool, tax_rate: float) -> str:
            """Calculate a total budget from multiple amounts with optional tax."""
            subtotal = sum(amounts)
            total = subtotal * (1 + tax_rate) if include_tax else subtotal
            return f"Budget total: {total:.2f}"

        store_agent_output_tool = function_tool(store_agent_output)

        agent = Agent(
            name="TestAgent",
            instructions=(
                "You are a travel-planning assistant. Use every available tool to prepare a personalized greeting, "
                "weather summary, and budget estimate, then store the completed itinerary before answering."
            ),
            model=self._create_model(),
            tools=[build_greeting, summarize_weather, calculate_budget, store_agent_output_tool],
            model_settings=ModelSettings(temperature=0.7),
        )
        Runner.run_sync(agent, "Plan a cheerful trip to Seattle for World using the available tools.")

    def _run_multi_agent(self) -> None:
        self._reset_model_call_count()

        @function_tool
        def build_greeting(name: str, language: str, excited: bool) -> str:
            """Build a personalized greeting in the requested language and tone."""
            punctuation = "!" if excited else "."
            return f"{language} greeting for {name}{punctuation}"

        @function_tool
        def describe_audience(audience: str, interests: List[str], formal: bool) -> str:
            """Describe an audience using its interests and preferred tone."""
            tone = "formal" if formal else "casual"
            return f"{audience}: {', '.join(interests)} ({tone})"

        @function_tool
        def format_message(message: str, style: str, width: int) -> str:
            """Format a message using a named style and target display width."""
            return f"[{style}:{width}] {message}"

        @function_tool
        def add_delivery_metadata(channel: str, urgent: bool, tags: List[str]) -> str:
            """Add delivery-channel metadata, urgency, and classification tags."""
            return f"{channel}; urgent={urgent}; tags={','.join(tags)}"

        model = self._create_model()
        store_agent_output_tool = function_tool(store_agent_output)
        greeter = Agent(
            name="GreeterAgent",
            instructions="Use every available tool to prepare a greeting tailored to the audience.",
            model=model,
            tools=[build_greeting, describe_audience, store_agent_output_tool],
            model_settings=ModelSettings(temperature=0.7),
        )
        formatter = Agent(
            name="FormatterAgent",
            instructions="Use every available tool to format the message and prepare its delivery metadata.",
            model=model,
            tools=[format_message, add_delivery_metadata, store_agent_output_tool],
            model_settings=ModelSettings(temperature=0.7),
        )

        Runner.run_sync(greeter, "Create a cheerful greeting for a developer audience.")
        self._reset_model_call_count()
        Runner.run_sync(formatter, "Format and classify this message for email delivery: Hello, World!")

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


def handle_request(request: Request) -> Response:
    return RequestHandler(request.url.path).handle()


app = Starlette(routes=[Route("/{path:path}", handle_request, methods=["GET"])])


if __name__ == "__main__":
    start_servers(app)
