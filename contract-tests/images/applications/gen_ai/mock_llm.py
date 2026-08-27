# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Dict, Tuple

MOCK_LLM_PORT: int = 8081
MOCK_BEDROCK_PORT: int = 8082
APP_PORT: int = 8080

_llm_call_count = 0
_bedrock_call_count = 0


def _tool_arguments(properties: Dict[str, Any]) -> Dict[str, Any]:
    arguments: Dict[str, Any] = {}
    for name, schema in properties.items():
        value_type = schema.get("type")
        if value_type == "integer":
            arguments[name] = 3
        elif value_type == "number":
            arguments[name] = 3.0
        elif value_type == "boolean":
            arguments[name] = True
        elif value_type == "array":
            arguments[name] = []
        elif value_type == "object":
            arguments[name] = {}
        else:
            arguments[name] = "World"
    return arguments


def reset_llm_call_count():
    global _llm_call_count  # pylint: disable=global-statement
    _llm_call_count = 0


def reset_bedrock_call_count():
    global _bedrock_call_count  # pylint: disable=global-statement
    _bedrock_call_count = 0


class MockLLMHandler(BaseHTTPRequestHandler):

    # pylint: disable=invalid-name
    def do_POST(self):
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count += 1

        body = {}
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length:
            body = json.loads(self.rfile.read(content_length))

        tool_name, tool_args = self._extract_first_tool(body)

        if _llm_call_count % 2 == 1 and tool_name:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"call_{_llm_call_count}",
                        "type": "function",
                        "function": {"name": tool_name, "arguments": json.dumps(tool_args)},
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

    @staticmethod
    def _extract_first_tool(body):
        tools = body.get("tools", [])
        if not tools:
            return None, {}
        func = tools[0].get("function", {})
        name = func.get("name")
        props = func.get("parameters", {}).get("properties", {})
        args = _tool_arguments(props)
        return name, args

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        pass


class MockBedrockHandler(BaseHTTPRequestHandler):

    # pylint: disable=invalid-name
    def do_POST(self):
        global _bedrock_call_count  # pylint: disable=global-statement
        _bedrock_call_count += 1

        body = {}
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length:
            body = json.loads(self.rfile.read(content_length))

        tool_name, tool_args = self._extract_first_tool(body)

        if _bedrock_call_count % 2 == 1 and tool_name:
            content = [
                {
                    "toolUse": {
                        "toolUseId": f"call_{_bedrock_call_count}",
                        "name": tool_name,
                        "input": tool_args,
                    }
                }
            ]
            stop_reason = "tool_use"
        else:
            content = [{"text": "Hello, World!"}]
            stop_reason = "end_turn"

        response = {
            "output": {"message": {"role": "assistant", "content": content}},
            "stopReason": stop_reason,
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
            "metrics": {"latencyMs": 1},
        }
        response_body = json.dumps(response).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("x-amzn-RequestId", "mock-request")
        self.end_headers()
        self.wfile.write(response_body)

    @staticmethod
    def _extract_first_tool(body):
        tools = body.get("toolConfig", {}).get("tools", [])
        if not tools:
            return None, {}
        tool_spec = tools[0].get("toolSpec", {})
        name = tool_spec.get("name")
        properties = tool_spec.get("inputSchema", {}).get("json", {}).get("properties", {})
        args = _tool_arguments(properties)
        return name, args

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        pass


def start_servers(request_handler_class):
    """Start mock model servers and application server."""
    mock_llm_server = ThreadingHTTPServer(("0.0.0.0", MOCK_LLM_PORT), MockLLMHandler)
    mock_llm_thread = Thread(target=mock_llm_server.serve_forever, daemon=True)
    mock_llm_thread.start()

    mock_bedrock_server = ThreadingHTTPServer(("0.0.0.0", MOCK_BEDROCK_PORT), MockBedrockHandler)
    mock_bedrock_thread = Thread(target=mock_bedrock_server.serve_forever, daemon=True)
    mock_bedrock_thread.start()

    server_address: Tuple[str, int] = ("0.0.0.0", APP_PORT)
    server = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(server.shutdown)
    atexit.register(mock_llm_server.shutdown)
    atexit.register(mock_bedrock_server.shutdown)
    server_thread = Thread(target=server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()
