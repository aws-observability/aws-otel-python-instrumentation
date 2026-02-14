# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple

from llama_index.core import Document, Settings, VectorStoreIndex
from llama_index.core.agent.workflow import FunctionAgent
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.embeddings import MockEmbedding
from llama_index.core.tools import FunctionTool
from llama_index.llms.openai import OpenAI
from typing_extensions import override

_PORT: int = 8080
_MOCK_LLM_PORT: int = 8081

os.environ["OPENAI_API_KEY"] = "fake-key"

_llm_call_count = 0


class MockOpenAIHandler(BaseHTTPRequestHandler):
    
    # pylint: disable=invalid-name
    def do_POST(self):
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count += 1
        
        content_length = int(self.headers.get('Content-Length', 0))
        request_body = self.rfile.read(content_length).decode('utf-8')
        request_data = json.loads(request_body) if content_length > 0 else {}
        
        has_tools = 'tools' in request_data and len(request_data.get('tools', [])) > 0
        
        if has_tools:
            if _llm_call_count % 2 == 1:
                content = (
                    "Thought: I need to calculate the sum of 5 and 3.\n"
                    "Action: calculate_sum\n"
                    'Action Input: {"a": 5, "b": 3}'
                )
            else:
                content = "Thought: I now know the final answer.\nFinal Answer: The sum of 5 and 3 is 8."
            
            response = {
                "id": "chatcmpl-mock-tool",
                "object": "chat.completion",
                "created": 1234567890,
                "model": "gpt-4",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": content
                    },
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70},
            }
        else:
            response = {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": 1234567890,
                "model": "gpt-4",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "This is a mock response from the fake OpenAI API."
                    },
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 25, "completion_tokens": 50, "total_tokens": 75},
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
        if self.in_path("llamaindex"):
            self._handle_llamaindex_request()
        self._end_request(self.main_status)

    def in_path(self, sub_path: str) -> bool:
        return sub_path in self.path

    def _handle_llamaindex_request(self) -> None:
        if self.in_path("agent"):
            self._run_agent()
        elif self.in_path("chat"):
            self._run_chat()
        elif self.in_path("query"):
            self._run_query()
        elif self.in_path("embedding"):
            self._run_embedding()
        elif self.in_path("tool"):
            self._run_tool_call()
        else:
            set_main_status(404)

    def _run_agent(self) -> None:  # pylint: disable=no-self-use
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count = 0
        set_main_status(200)

        try:
            import asyncio
            
            def get_greeting(name: str) -> str:
                return f"Hello, {name}!"

            def multiply(a: float, b: float) -> float:
                return a * b

            llm = OpenAI(
                model="gpt-4",
                api_base=f"http://localhost:{_MOCK_LLM_PORT}/v1",
                temperature=0.7,
                max_tokens=100,
            )
            
            agent = FunctionAgent(
                tools=[multiply, get_greeting],
                llm=llm,
                name="TestAgent",
                description="A test agent that greets and multiplies.",
                system_prompt="You are a helpful assistant.",
            )
            
            async def run_agent():
                response = await agent.run("Please greet the world")
                return response
            
            response = asyncio.run(run_agent())
            print(f"Agent response: {response}")
            
        except Exception as e:
            print(f"Error in _run_agent: {e}")
            import traceback
            traceback.print_exc()

    def _run_chat(self) -> None:  # pylint: disable=no-self-use
        set_main_status(200)

        try:
            llm = OpenAI(
                model="gpt-4",
                api_base=f"http://localhost:{_MOCK_LLM_PORT}/v1",
                temperature=0.7,
                max_tokens=100,
            )
            
            messages = [
                ChatMessage(role=MessageRole.SYSTEM, content="You are a helpful assistant."),
                ChatMessage(role=MessageRole.USER, content="Hello, how are you?"),
            ]
            
            response = llm.chat(messages)
            print(f"Chat response: {response}")
        except Exception as e:
            print(f"Error in _run_chat: {e}")
            import traceback
            traceback.print_exc()

    def _run_query(self) -> None:  # pylint: disable=no-self-use
        set_main_status(200)

        try:
            llm = OpenAI(
                model="gpt-4",
                api_base=f"http://localhost:{_MOCK_LLM_PORT}/v1",
                temperature=0.7,
                max_tokens=100,
            )
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
        except Exception as e:
            print(f"Error in _run_query: {e}")
            import traceback
            traceback.print_exc()

    def _run_embedding(self) -> None:  # pylint: disable=no-self-use
        set_main_status(200)

        try:
            embed_model = MockEmbedding(embed_dim=384)
            
            texts = ["Hello world", "Test embedding"]
            embeddings = embed_model.get_text_embedding_batch(texts)
            print(f"Generated {len(embeddings)} embeddings")
        except Exception as e:
            print(f"Error in _run_embedding: {e}")
            import traceback
            traceback.print_exc()

    def _run_tool_call(self) -> None:  # pylint: disable=no-self-use
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count = 0
        set_main_status(200)

        try:
            def calculate_sum(a: int, b: int) -> int:
                return a + b

            def multiply(a: float, b: float) -> float:
                return a * b

            sum_tool = FunctionTool.from_defaults(fn=calculate_sum)
            multiply_tool = FunctionTool.from_defaults(fn=multiply)
            
            llm = OpenAI(
                model="gpt-4",
                api_base=f"http://localhost:{_MOCK_LLM_PORT}/v1",
                temperature=0.7,
                max_tokens=100,
            )
            
            messages = [
                ChatMessage(role=MessageRole.USER, content="What is 5 + 3?"),
            ]
            
            response = llm.chat_with_tools(tools=[sum_tool, multiply_tool], messages=messages)
            print(f"Chat with tools response: {response}")
            
        except Exception as e:
            print(f"Error in _run_tool_call: {e}")
            import traceback
            traceback.print_exc()

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


def main() -> None:
    # Start mock OpenAI API server
    mock_llm_server = ThreadingHTTPServer(("0.0.0.0", _MOCK_LLM_PORT), MockOpenAIHandler)
    mock_llm_thread = Thread(target=mock_llm_server.serve_forever, daemon=True)
    mock_llm_thread.start()

    # Start main test server
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
