# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple

MOCK_LLM_PORT: int = 8081
APP_PORT: int = 8080

_llm_call_count = 0


def reset_llm_call_count():
    global _llm_call_count  # pylint: disable=global-statement
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


def start_servers(request_handler_class):
    """Start mock LLM server and application server."""
    mock_llm_server = ThreadingHTTPServer(("0.0.0.0", MOCK_LLM_PORT), MockLLMHandler)
    mock_llm_thread = Thread(target=mock_llm_server.serve_forever, daemon=True)
    mock_llm_thread.start()

    server_address: Tuple[str, int] = ("0.0.0.0", APP_PORT)
    server = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(server.shutdown)
    atexit.register(mock_llm_server.shutdown)
    server_thread = Thread(target=server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()
