# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

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

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_applied_successfully(self, mock_logger):
        """Test that the Starlette ASGI middleware patch is applied successfully."""
        env_configs = [
            {"AGENT_OBSERVABILITY_ENABLED": "true"},
            {"AGENT_OBSERVABILITY_ENABLED": "false"},
            {},
        ]
        for env_vars in env_configs:
            with self.subTest(env_vars=env_vars):
                mock_logger.reset_mock()

                with patch.dict("os.environ", env_vars, clear=False):
                    if not env_vars:
                        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)

                    def create_middleware_class():
                        class MockMiddleware:
                            def __init__(self, app, **kwargs):
                                pass

                        return MockMiddleware

                    mock_middleware_class = create_middleware_class()

                    mock_asgi_module = MagicMock()
                    mock_asgi_module.OpenTelemetryMiddleware = mock_middleware_class

                    with patch.dict(
                        "sys.modules",
                        {
                            "opentelemetry.instrumentation.asgi": mock_asgi_module,
                        },
                    ):
                        # Apply the patch
                        _apply_starlette_instrumentation_patches()

                        mock_middleware_instance = MagicMock()
                        mock_middleware_instance.exclude_receive_span = False
                        mock_middleware_instance.exclude_send_span = False
                        mock_middleware_class.__init__(mock_middleware_instance, "app")

                        # Test middleware patching sets exclude flags
                        self.assertTrue(mock_middleware_instance.exclude_receive_span)
                        self.assertTrue(mock_middleware_instance.exclude_send_span)

                        # Verify logging
                        mock_logger.debug.assert_called_with("Successfully patched Starlette ASGI middleware")

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_handles_import_error(self, mock_logger):
        """Test that the patch handles import errors gracefully."""
        # Mock the import to fail by removing the module
        with patch.dict("sys.modules", {"opentelemetry.instrumentation.asgi": None}):
            # This should not raise an exception
            _apply_starlette_instrumentation_patches()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Starlette instrumentation patches", args[0])

    def test_asgi_event_spans_excluded_without_losing_request_or_application_spans(self):
        async def run(provider):
            tracer = provider.get_tracer(__name__)
            sent = []

            async def simple_asgi(scope, receive, send):
                with tracer.start_as_current_span("application"):
                    await receive()
                    await receive()
                    await send({"type": "http.response.start", "status": 200, "headers": []})
                    await send({"type": "http.response.body", "body": b"first", "more_body": True})
                    await send({"type": "http.response.body", "body": b"last"})

            app = OpenTelemetryMiddleware(simple_asgi, tracer_provider=provider)
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

        original_init = OpenTelemetryMiddleware.__init__
        for agent_enabled in ("true", "false", None):
            with self.subTest(agent_enabled=agent_enabled), patch.dict("os.environ", clear=False):
                if agent_enabled is None:
                    os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
                else:
                    os.environ["AGENT_OBSERVABILITY_ENABLED"] = agent_enabled
                OpenTelemetryMiddleware.__init__ = original_init
                _apply_starlette_instrumentation_patches()
                exporter = InMemorySpanExporter()
                provider = TracerProvider()
                self.addCleanup(provider.shutdown)
                provider.add_span_processor(SimpleSpanProcessor(exporter))

                sent = asyncio.run(run(provider))
                spans = exporter.get_finished_spans()
                self.assertEqual(len(spans), 2)
                request_span = next(span for span in spans if span.kind == SpanKind.SERVER)
                application_span = next(span for span in spans if span.name == "application")
                self.assertEqual(application_span.parent.span_id, request_span.context.span_id)
                self.assertEqual([message["body"] for message in sent[1:]], [b"first", b"last"])

    def test_asgi_suppress_http_instrumentation(self):
        original_init = OpenTelemetryMiddleware.__init__

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
                OpenTelemetryMiddleware.__init__ = original_init
                exporter = InMemorySpanExporter()
                provider = TracerProvider()
                provider.add_span_processor(SimpleSpanProcessor(exporter))

                if apply_patch:
                    with patch.dict("os.environ", {"AGENT_OBSERVABILITY_ENABLED": "true"}, clear=False):
                        _apply_starlette_instrumentation_patches()

                async def run(tracer_provider=provider):
                    app = OpenTelemetryMiddleware(simple_asgi, tracer_provider=tracer_provider)
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
