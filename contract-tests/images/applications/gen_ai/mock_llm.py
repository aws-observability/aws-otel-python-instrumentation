# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Dict, List, Tuple

MOCK_LLM_PORT: int = 8081
MOCK_BEDROCK_PORT: int = 8082
APP_PORT: int = 8080

_llm_call_count = 0
_bedrock_call_count = 0


def _tool_arguments(properties: Dict[str, Any]) -> Dict[str, Any]:
    return {name: _tool_argument(name, schema) for name, schema in properties.items()}


def _tool_argument(name: str, schema: Dict[str, Any]) -> Any:
    if "const" in schema:
        return schema["const"]
    if schema.get("enum"):
        return schema["enum"][0]

    value_type = schema.get("type")
    if isinstance(value_type, list):
        value_type = next((item for item in value_type if item != "null"), None)
    if value_type is None:
        for option in schema.get("anyOf", []):
            if option.get("type") != "null":
                return _tool_argument(name, option)

    if value_type == "integer":
        return 3
    if value_type == "number":
        return 3.5
    if value_type == "boolean":
        return True
    if value_type == "array":
        return [_tool_argument(name, schema.get("items", {}))]
    if value_type == "object":
        return _tool_arguments(schema.get("properties", {}))

    return {
        "audience": "developers",
        "channel": "email",
        "city": "Seattle",
        "language": "English",
        "message": "Hello, World!",
        "name": "World",
        "style": "celebratory",
    }.get(name, f"example-{name}")


def reset_llm_call_count():
    global _llm_call_count  # pylint: disable=global-statement
    _llm_call_count = 0


def reset_bedrock_call_count():
    global _bedrock_call_count  # pylint: disable=global-statement
    _bedrock_call_count = 0


class MockOpenAILLMHandler(BaseHTTPRequestHandler):

    # pylint: disable=invalid-name
    def do_POST(self):
        global _llm_call_count  # pylint: disable=global-statement
        _llm_call_count += 1

        body = {}
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length:
            body = json.loads(self.rfile.read(content_length))

        tools = self._extract_tools(body)

        if _llm_call_count % 2 == 1 and tools:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"call_{_llm_call_count}_{index}",
                        "type": "function",
                        "function": {"name": tool_name, "arguments": json.dumps(tool_args)},
                    }
                    for index, (tool_name, tool_args) in enumerate(tools, start=1)
                ],
            }
            finish_reason = "tool_calls"
        else:
            message = {"role": "assistant", "content": "Hello, World!"}
            finish_reason = "stop"

        response = {
            "id": f"chatcmpl-mock-{_llm_call_count}",
            "object": "chat.completion",
            "created": 1234567890,
            "model": body.get("model", "gpt-4"),
            "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30,
                "prompt_tokens_details": {"cached_tokens": 2},
                "completion_tokens_details": {"reasoning_tokens": 4},
            },
        }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    @staticmethod
    def _extract_tools(body) -> List[Tuple[str, Dict[str, Any]]]:
        tools = []
        for tool in body.get("tools", []):
            function = tool.get("function", {})
            if name := function.get("name"):
                properties = function.get("parameters", {}).get("properties", {})
                tools.append((name, _tool_arguments(properties)))
        return tools

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

        tools = self._extract_tools(body)

        if _bedrock_call_count % 2 == 1 and tools:
            content = [
                {
                    "toolUse": {
                        "toolUseId": f"call_{_bedrock_call_count}_{index}",
                        "name": tool_name,
                        "input": tool_args,
                    }
                }
                for index, (tool_name, tool_args) in enumerate(tools, start=1)
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
    def _extract_tools(body) -> List[Tuple[str, Dict[str, Any]]]:
        tools = []
        for tool in body.get("toolConfig", {}).get("tools", []):
            tool_spec = tool.get("toolSpec", {})
            if name := tool_spec.get("name"):
                properties = tool_spec.get("inputSchema", {}).get("json", {}).get("properties", {})
                tools.append((name, _tool_arguments(properties)))
        return tools

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        pass


def start_servers(request_handler_class):
    """Start mock model servers and application server."""
    mock_llm_server = ThreadingHTTPServer(("0.0.0.0", MOCK_LLM_PORT), MockOpenAILLMHandler)
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
