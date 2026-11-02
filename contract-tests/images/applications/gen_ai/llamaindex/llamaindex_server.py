# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import os
import traceback
from typing import Union

from llama_index.core import Document, Settings, VectorStoreIndex
from llama_index.core.agent.workflow import AgentWorkflow, FunctionAgent
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.embeddings import MockEmbedding
from llama_index.core.tools import FunctionTool
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.llms.openai import OpenAI
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
os.environ["OPENAI_API_KEY"] = "fake-key"


class RequestHandler:
    def __init__(self, path: str) -> None:
        self.path = path

    def handle(self) -> Response:
        if not self.in_path("llamaindex"):
            return Response(status_code=404)
        return self._handle_llamaindex_request()

    def in_path(self, sub_path: str) -> bool:
        return sub_path in self.path

    def _handle_llamaindex_request(self) -> Response:
        if self.in_path("workflow"):
            handler = self._run_workflow
        elif self.in_path("agent"):
            handler = self._run_agent
        elif self.in_path("chat"):
            handler = self._run_chat
        elif self.in_path("query"):
            handler = self._run_query
        elif self.in_path("embedding"):
            handler = self._run_embedding
        elif self.in_path("tool"):
            handler = self._run_tool_call
        else:
            return Response(status_code=404)

        handler()
        return Response(status_code=200)

    def _run_agent(self) -> None:
        self._reset_model_call_count()

        try:

            def get_greeting(name: str) -> str:
                return f"Hello, {name}!"

            def multiply(a: float, b: float) -> float:
                return a * b

            llm = self._create_llm()

            agent = FunctionAgent(
                tools=[multiply, get_greeting, store_agent_output],
                llm=llm,
                name="TestAgent",
                description="A test agent that greets and multiplies.",
                system_prompt="You are a helpful assistant.",
                streaming=False,
            )

            async def run_agent():
                response = await agent.run("Please greet the world")
                return response

            response = asyncio.run(run_agent())
            print(f"Agent response: {response}")

        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"Error in _run_agent: {exc}")
            traceback.print_exc()

    def _run_workflow(self) -> None:
        self._reset_model_call_count()

        try:

            def get_greeting(name: str) -> str:
                return f"Hello, {name}!"

            def multiply(a: float, b: float) -> float:
                return a * b

            llm = self._create_llm()

            greeter = FunctionAgent(
                tools=[get_greeting, store_agent_output],
                llm=llm,
                name="Greeter",
                description="Greets people by name.",
                system_prompt="You greet people.",
                can_handoff_to=["Calculator"],
                streaming=False,
            )
            calculator = FunctionAgent(
                tools=[multiply, store_agent_output],
                llm=llm,
                name="Calculator",
                description="Multiplies numbers.",
                system_prompt="You multiply numbers.",
                streaming=False,
            )
            workflow = AgentWorkflow(
                agents=[greeter, calculator], root_agent="Greeter", workflow_name="multi_agent_workflow"
            )

            async def run_workflow():
                workflow_response = await workflow.run(user_msg="Please greet the world")
                await calculator.run("Multiply 3.5 by 3.5 and store the result")
                return workflow_response

            response = asyncio.run(run_workflow())
            print(f"Workflow response: {response}")

        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"Error in _run_workflow: {exc}")
            traceback.print_exc()

    def _run_chat(self) -> None:
        self._reset_model_call_count()

        try:
            llm = self._create_llm()

            messages = [
                ChatMessage(role=MessageRole.SYSTEM, content="You are a helpful assistant."),
                ChatMessage(role=MessageRole.USER, content="Hello, how are you?"),
            ]

            response = llm.chat(messages)
            print(f"Chat response: {response}")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"Error in _run_chat: {exc}")
            traceback.print_exc()

    def _run_query(self) -> None:
        self._reset_model_call_count()

        try:
            llm = self._create_llm()
            embed_model = MockEmbedding(embed_dim=384)

            Settings.llm = llm
            Settings.embed_model = embed_model

            documents = [
                Document(text="The sky is blue."),
                Document(text="The grass is green."),
            ]

            index = VectorStoreIndex.from_documents(documents)
            query_engine = index.as_query_engine()

            response = query_engine.query("What color is the sky?")
            print(f"Query response: {response}")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"Error in _run_query: {exc}")
            traceback.print_exc()

    @staticmethod
    def _run_embedding() -> None:
        try:
            embed_model = MockEmbedding(embed_dim=384)

            texts = ["Hello world", "Test embedding"]
            embeddings = embed_model.get_text_embedding_batch(texts)
            print(f"Generated {len(embeddings)} embeddings")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"Error in _run_embedding: {exc}")
            traceback.print_exc()

    def _run_tool_call(self) -> None:
        self._reset_model_call_count()

        try:

            def calculate_sum(a: int, b: int) -> int:
                return a + b

            def multiply(a: float, b: float) -> float:
                return a * b

            sum_tool = FunctionTool.from_defaults(fn=calculate_sum)
            multiply_tool = FunctionTool.from_defaults(fn=multiply)

            llm = self._create_llm()

            messages = [
                ChatMessage(role=MessageRole.USER, content="What is 5 + 3?"),
            ]

            response = llm.chat_with_tools(tools=[sum_tool, multiply_tool], messages=messages)
            print(f"Chat with tools response: {response}")

        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"Error in _run_tool_call: {exc}")
            traceback.print_exc()

    def _create_llm(self) -> Union[BedrockConverse, OpenAI]:
        if "bedrock" in self.path:
            return BedrockConverse(
                model="anthropic.claude-3-haiku-20240307-v1:0",
                region_name="us-east-1",
                endpoint_url=f"http://localhost:{MOCK_BEDROCK_PORT}",
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-key",
                temperature=0.7,
                max_tokens=100,
            )
        return OpenAI(
            model="gpt-4",
            api_base=f"http://localhost:{MOCK_LLM_PORT}/v1",
            temperature=0.7,
            max_tokens=100,
        )

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
