# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import List, Union

import boto3
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
from botocore.config import Config
from mock_llm import MOCK_BEDROCK_PORT, MOCK_LLM_PORT, reset_bedrock_call_count, reset_llm_call_count, start_servers
from openai import AsyncOpenAI
from typing_extensions import override

MOCK_AWS_PORT = 8083

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


class MockAWSS3Handler(BaseHTTPRequestHandler):
    @override
    # pylint: disable=invalid-name
    def do_PUT(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length:
            self.rfile.read(content_length)

        self.send_response(200)
        self.send_header("ETag", '"mock-etag"')
        self.send_header("x-amz-request-id", "mock-aws-request")
        self.send_header("Content-Length", "0")
        self.end_headers()

    @override
    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        pass


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

        @function_tool
        def store_trip_plan(bucket: str, key: str, itinerary: str, tags: List[str]) -> str:
            """Store the completed itinerary and its metadata in Amazon S3."""
            s3_client = boto3.client(
                "s3",
                endpoint_url=f"http://localhost:{MOCK_AWS_PORT}",
                region_name="us-east-1",
                config=Config(
                    retries={"max_attempts": 0},
                    connect_timeout=3,
                    read_timeout=3,
                    s3={"addressing_style": "path"},
                ),
            )
            response = s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=itinerary.encode(),
                Metadata={"tags": ",".join(tags)},
            )
            return f"Stored {key} in {bucket} with ETag {response['ETag']}"

        agent = Agent(
            name="TestAgent",
            instructions=(
                "You are a travel-planning assistant. Use every available tool to prepare a personalized greeting, "
                "weather summary, and budget estimate, then store the completed itinerary before answering."
            ),
            model=self._create_model(),
            tools=[build_greeting, summarize_weather, calculate_budget, store_trip_plan],
            model_settings=ModelSettings(temperature=0.7),
        )
        Runner.run_sync(agent, "Plan a cheerful trip to Seattle for World using the available tools.")

    def _run_multi_agent(self) -> None:
        self._reset_model_call_count()
        RequestHandler.main_status = 200

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
        greeter = Agent(
            name="GreeterAgent",
            instructions="Use every available tool to prepare a greeting tailored to the audience.",
            model=model,
            tools=[build_greeting, describe_audience],
            model_settings=ModelSettings(temperature=0.7),
        )
        formatter = Agent(
            name="FormatterAgent",
            instructions="Use every available tool to format the message and prepare its delivery metadata.",
            model=model,
            tools=[format_message, add_delivery_metadata],
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


if __name__ == "__main__":
    mock_aws_server = ThreadingHTTPServer(("0.0.0.0", MOCK_AWS_PORT), MockAWSS3Handler)
    atexit.register(mock_aws_server.shutdown)
    Thread(target=mock_aws_server.serve_forever, daemon=True).start()
    start_servers(RequestHandler)
