# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import gzip
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

from grpc import server
from mock_collector_logs_service import MockCollectorLogsService
from mock_collector_metrics_service import MockCollectorMetricsService
from mock_collector_service import MockCollectorService
from mock_collector_service_pb2_grpc import add_MockCollectorServiceServicer_to_server
from mock_collector_trace_service import MockCollectorTraceService

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest, ExportLogsServiceResponse
from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import add_LogsServiceServicer_to_server
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2_grpc import add_MetricsServiceServicer_to_server
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import add_TraceServiceServicer_to_server


def _create_http_handler(logs_collector: MockCollectorLogsService, metrics_collector: MockCollectorMetricsService):
    """Factory to inject collector instances into HTTP handler (avoids global state)."""

    def _read_body(self_handler) -> bytes:
        content_length = int(self_handler.headers.get("Content-Length", 0))
        raw = self_handler.rfile.read(content_length)
        encoding = (self_handler.headers.get("Content-Encoding") or "").lower()
        if encoding == "gzip":
            return gzip.decompress(raw)
        if encoding == "deflate":
            return zlib.decompress(raw)
        return raw

    class OtlpHttpHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path == "/v1/logs":
                body = _read_body(self)
                request = ExportLogsServiceRequest()
                request.ParseFromString(body)
                logs_collector._export_requests.put(request)
                resp_bytes = ExportLogsServiceResponse().SerializeToString()
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.end_headers()
                self.wfile.write(resp_bytes)
            elif self.path == "/v1/metrics":
                body = _read_body(self)
                request = ExportMetricsServiceRequest()
                request.ParseFromString(body)
                metrics_collector._export_requests.put(request)
                resp_bytes = ExportMetricsServiceResponse().SerializeToString()
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.end_headers()
                self.wfile.write(resp_bytes)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):  # pylint: disable=redefined-builtin
            # Override BaseHTTPRequestHandler's log_message to suppress per-request logging.
            # The `format` name matches the parent signature, so we keep it.
            pass

    return OtlpHttpHandler


def main() -> None:
    # gRPC server on port 4315 (traces, metrics, logs via gRPC + query service)
    mock_collector_server: server = server(thread_pool=ThreadPoolExecutor(max_workers=10))
    mock_collector_server.add_insecure_port("0.0.0.0:4315")

    trace_collector: MockCollectorTraceService = MockCollectorTraceService()
    metrics_collector: MockCollectorMetricsService = MockCollectorMetricsService()
    logs_collector: MockCollectorLogsService = MockCollectorLogsService()
    mock_collector: MockCollectorService = MockCollectorService(trace_collector, metrics_collector, logs_collector)

    add_TraceServiceServicer_to_server(trace_collector, mock_collector_server)
    add_MetricsServiceServicer_to_server(metrics_collector, mock_collector_server)
    add_LogsServiceServicer_to_server(logs_collector, mock_collector_server)
    add_MockCollectorServiceServicer_to_server(mock_collector, mock_collector_server)

    mock_collector_server.start()
    atexit.register(mock_collector_server.stop, None)

    # HTTP server on port 4316 (OTLP HTTP /v1/logs and /v1/metrics for the DI snapshot emitter)
    handler_class = _create_http_handler(logs_collector, metrics_collector)
    http_server = HTTPServer(("0.0.0.0", 4316), handler_class)
    http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    http_thread.start()

    print("Ready")
    mock_collector_server.wait_for_termination(None)


if __name__ == "__main__":
    main()
