# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Simulated Lambda function for lite SDK e2e testing.

Exposes an HTTP server that simulates Lambda invocations. Each request to
various endpoints triggers handlers that create spans for different test scenarios.
"""

import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from opentelemetry import trace
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.status import Status


def invoke_handler(event):
    tracer = trace.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")

    with tracer.start_as_current_span("my-function.handler", kind=SpanKind.SERVER) as span:
        span.set_attribute("faas.invocation_id", event.get("request_id", "test-id"))
        span.set_attribute("cloud.resource_id", "arn:aws:lambda:us-west-2:123456789012:function:my-function")

        botocore_tracer = trace.get_tracer("opentelemetry.instrumentation.botocore", "0.2.0")
        with botocore_tracer.start_as_current_span("S3.ListBuckets", kind=SpanKind.CLIENT) as client_span:
            client_span.set_attribute("rpc.service", "S3")
            client_span.set_attribute("rpc.system", "aws-api")
            client_span.set_attribute("rpc.method", "ListBuckets")
            client_span.set_attribute("http.status_code", 200)
            time.sleep(0.01)

    return {"statusCode": 200, "body": "ok"}


def invoke_with_error():
    tracer = trace.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")

    with tracer.start_as_current_span("my-function.handler", kind=SpanKind.SERVER) as span:
        try:
            raise ValueError("handler failed")
        except ValueError as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, "handler failed"))

    return {"statusCode": 500, "body": "error"}


def invoke_with_context(headers):
    ctx = get_global_textmap().extract(headers)
    tracer = trace.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")

    with tracer.start_as_current_span("my-function.handler", kind=SpanKind.SERVER, context=ctx) as span:
        span.set_attribute("faas.invocation_id", "ctx-test")

    return {"statusCode": 200, "body": "ok"}


def invoke_with_varied_attributes():
    tracer = trace.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")

    with tracer.start_as_current_span("my-function.handler", kind=SpanKind.SERVER) as span:
        span.set_attribute("int.positive", 42)
        span.set_attribute("int.negative", -1)
        span.set_attribute("int.zero", 0)
        span.set_attribute("float.value", 3.14)
        span.set_attribute("bool.true", True)
        span.set_attribute("bool.false", False)
        span.set_attribute("string.empty", "")
        span.set_attribute("string.value", "hello")

    return {"statusCode": 200, "body": "ok"}


def invoke_large_payload():
    tracer = trace.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")

    with tracer.start_as_current_span("my-function.handler", kind=SpanKind.SERVER) as span:
        for i in range(50):
            span.set_attribute(f"attr.key.{i}", f"value-{'x' * 100}-{i}")

    return {"statusCode": 200, "body": "ok"}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if self.path == "/invoke":
            event = {"request_id": f"req-{int(time.time_ns())}"}
            result = invoke_handler(event)
            self._flush_and_respond(result)
            return

        if self.path == "/invoke-error":
            result = invoke_with_error()
            self._flush_and_respond(result)
            return

        if self.path == "/invoke-attributes":
            result = invoke_with_varied_attributes()
            self._flush_and_respond(result)
            return

        if self.path == "/invoke-large":
            result = invoke_large_payload()
            self._flush_and_respond(result)
            return

        if self.path.startswith("/invoke-with-context"):
            headers = {}
            xray_header = self.headers.get("X-Amzn-Trace-Id")
            if xray_header:
                headers["X-Amzn-Trace-Id"] = xray_header
            traceparent = self.headers.get("traceparent")
            if traceparent:
                headers["traceparent"] = traceparent
            result = invoke_with_context(headers)
            self._flush_and_respond(result)
            return

        self.send_response(404)
        self.end_headers()

    def _flush_and_respond(self, result):
        provider = trace.get_tracer_provider()
        provider.force_flush()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    from amazon.opentelemetry.distro.opentelemetry_lite_sdk import configure_lite_mode

    configure_lite_mode()

    print("Ready", flush=True)
    server = HTTPServer(("0.0.0.0", 8080), Handler)
    server.serve_forever()
