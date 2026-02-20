# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Mock OTLP collector for testing distributed tracing."""

from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans


@dataclass
class Telemetry:
    traces: list = field(default_factory=list)

    def clear(self):
        self.traces.clear()


class OTLPServer(HTTPServer):
    def __init__(self, server_address, telemetry):
        super().__init__(server_address, OTLPHandler, True)
        self.telemetry = telemetry


class OTLPHandler(BaseHTTPRequestHandler):
    server: OTLPServer

    def log_message(self, format, *args):
        pass  # Suppress logging

    def do_POST(self):
        if self.path == "/v1/traces":
            content_length = int(self.headers["Content-Length"])
            body = self.rfile.read(content_length)
            request = ExportTraceServiceRequest()
            request.ParseFromString(body)
            self.server.telemetry.traces.extend(request.resource_spans)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()
