# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Dict, List, Tuple

import boto3
import uvicorn
from botocore.config import Config
from typing_extensions import override

MOCK_LLM_PORT: int = 8081
MOCK_BEDROCK_PORT: int = 8082
MOCK_AWS_PORT: int = 8083
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

    if value_type in ("integer", "number", "boolean"):
        return {"integer": 3, "number": 3.5, "boolean": True}[value_type]
    if value_type == "array":
        value = [_tool_argument(name, schema.get("items", {}))]
    elif value_type == "object":
        value = _tool_arguments(schema.get("properties", {}))
    else:
        value = {
            "audience": "developers",
            "bucket": "agent-results",
            "channel": "email",
            "city": "Seattle",
            "content": "Hello, World!",
            "key": "results/hello-world.txt",
            "language": "English",
            "message": "Hello, World!",
            "name": "World",
            "style": "celebratory",
        }.get(name, f"example-{name}")
    return value


def reset_llm_call_count():
    global _llm_call_count  # pylint: disable=global-statement
    _llm_call_count = 0


def reset_bedrock_call_count():
    global _bedrock_call_count  # pylint: disable=global-statement
    _bedrock_call_count = 0


def store_in_s3(bucket: str, key: str, content: str, metadata: Dict[str, str]) -> str:
    """Store content in the shared fake S3 service."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{MOCK_AWS_PORT}",
        region_name="us-east-1",
        aws_access_key_id="fake-key",
        aws_secret_access_key="fake-key",
        config=Config(
            retries={"max_attempts": 0},
            connect_timeout=3,
            read_timeout=3,
            s3={"addressing_style": "path"},
        ),
    )
    response = s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode(), Metadata=metadata)
    return f"Stored {key} in {bucket} with ETag {response['ETag']}"


def store_agent_output(bucket: str, key: str, content: str, tags: List[str]) -> str:
    """Store an agent's output and classification tags in Amazon S3."""
    return store_in_s3(bucket, key, content, {"tags": ",".join(tags)})


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


def start_servers(application: Any) -> None:
    """Start mock model, AWS, and application servers."""
    mock_llm_server = ThreadingHTTPServer(("0.0.0.0", MOCK_LLM_PORT), MockOpenAILLMHandler)
    mock_llm_thread = Thread(target=mock_llm_server.serve_forever, daemon=True)
    mock_llm_thread.start()

    mock_bedrock_server = ThreadingHTTPServer(("0.0.0.0", MOCK_BEDROCK_PORT), MockBedrockHandler)
    mock_bedrock_thread = Thread(target=mock_bedrock_server.serve_forever, daemon=True)
    mock_bedrock_thread.start()

    mock_aws_server = ThreadingHTTPServer(("0.0.0.0", MOCK_AWS_PORT), MockAWSS3Handler)
    mock_aws_thread = Thread(target=mock_aws_server.serve_forever, daemon=True)
    mock_aws_thread.start()

    atexit.register(mock_llm_server.shutdown)
    atexit.register(mock_bedrock_server.shutdown)
    atexit.register(mock_aws_server.shutdown)
    uvicorn.run(application, host="0.0.0.0", port=APP_PORT, log_level="info")
