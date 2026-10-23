# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.patches._starlette_patches import _apply_starlette_instrumentation_patches
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.instrumentation.utils import suppress_http_instrumentation
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind


class TestStarlettePatch(TestCase):
    """Test the Starlette instrumentation patches."""

    def setUp(self):
        self.addCleanup(setattr, OpenTelemetryMiddleware, "__init__", OpenTelemetryMiddleware.__init__)

    def test_apply_starlette_instrumentation_patches_excludes_asgi_event_spans(self):
        _apply_starlette_instrumentation_patches()
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        self.addCleanup(provider.shutdown)
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer(__name__)

        async def simple_asgi(scope, receive, send):
            with tracer.start_as_current_span("application"):
                await receive()
                await receive()
                await send({"type": "http.response.start", "status": 200, "headers": []})
                await send({"type": "http.response.body", "body": b"first", "more_body": True})
                await send({"type": "http.response.body", "body": b"last"})

        app = OpenTelemetryMiddleware(simple_asgi, tracer_provider=provider)

        async def run():
            sent = []
            messages = [
                {"type": "http.request", "body": b"first", "more_body": True},
                {"type": "http.request", "body": b"last", "more_body": False},
            ]

            async def receive():
                return messages.pop(0)

            async def send(message):
                sent.append(message)

            await app(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/",
                    "query_string": b"",
                    "headers": [],
                    "server": ("127.0.0.1", 80),
                    "scheme": "http",
                    "http_version": "1.1",
                    "client": ("127.0.0.1", 32767),
                },
                receive,
                send,
            )
            return sent

        sent = asyncio.run(run())
        spans = exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)
        request_span = next(span for span in spans if span.kind == SpanKind.SERVER)
        application_span = next(span for span in spans if span.name == "application")
        self.assertEqual(application_span.parent.span_id, request_span.context.span_id)
        self.assertEqual([message["body"] for message in sent[1:]], [b"first", b"last"])

    def test_asgi_suppress_http_instrumentation(self):
        original_call = OpenTelemetryMiddleware.__call__

        async def simple_asgi(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [],
            "server": ("127.0.0.1", 80),
            "scheme": "http",
            "http_version": "1.0",
            "client": ("127.0.0.1", 32767),
        }

        for case, apply_patch, expect_spans in [
            ("unpatched", False, False),
            ("patched", True, False),
        ]:
            with self.subTest(case=case):
                OpenTelemetryMiddleware.__call__ = original_call
                exporter = InMemorySpanExporter()
                provider = TracerProvider()
                provider.add_span_processor(SimpleSpanProcessor(exporter))

                if apply_patch:
                    with patch.dict("os.environ", {"AGENT_OBSERVABILITY_ENABLED": "true"}, clear=False):
                        _apply_starlette_instrumentation_patches()

                async def run():
                    app = OpenTelemetryMiddleware(simple_asgi, tracer_provider=provider)
                    messages = [{"type": "http.request", "body": b""}]

                    async def receive():
                        return messages.pop(0)

                    sent = []

                    async def send(msg):
                        sent.append(msg)

                    with suppress_http_instrumentation():
                        await app(scope, receive, send)

                asyncio.run(run())
                spans = exporter.get_finished_spans()

                if expect_spans:
                    self.assertGreater(len(spans), 0)
                else:
                    self.assertEqual(len(spans), 0)

        OpenTelemetryMiddleware.__call__ = original_call
