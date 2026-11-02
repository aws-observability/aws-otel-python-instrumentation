# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Mock DI API server that serves instrumentation configurations.

Runs alongside the FastAPI app inside the same container, providing
/list-instrumentation-configurations and /report-instrumentation-configuration-status
endpoints that the DI poller calls.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

PROBE_CONFIGS = []
BREAKPOINT_CONFIGS = []
STATUS_REPORTS = []


def set_probe_configs(configs):
    global PROBE_CONFIGS
    PROBE_CONFIGS = configs


def set_breakpoint_configs(configs):
    global BREAKPOINT_CONFIGS
    BREAKPOINT_CONFIGS = configs


def set_configs(configs):
    """Legacy: set all configs (treated as breakpoints for backward compatibility)."""
    set_breakpoint_configs(configs)


class MockDIHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b""

        if self.path == "/list-instrumentation-configurations":
            self._handle_list(body)
        elif self.path == "/report-instrumentation-configuration-status":
            self._handle_status_report(body)
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_list(self, body):
        payload = json.loads(body) if body else {}
        instrumentation_type = payload.get("InstrumentationType", "BREAKPOINT")

        if instrumentation_type == "PROBE":
            configs = PROBE_CONFIGS
        else:
            configs = BREAKPOINT_CONFIGS

        response = {
            "Changed": True,
            "Service": payload.get("Service", ""),
            "Environment": payload.get("Environment", ""),
            "LatestConfigurations": configs,
            "NextToken": None,
            "SyncInterval": 10,
        }
        self._send_json(200, response)

    def _handle_status_report(self, body):
        if body:
            STATUS_REPORTS.append(json.loads(body))
        self._send_json(200, {"message": "ok"})

    def _send_json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass  # Suppress request logs


def start_mock_api(port=3030):
    server = HTTPServer(("0.0.0.0", port), MockDIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
