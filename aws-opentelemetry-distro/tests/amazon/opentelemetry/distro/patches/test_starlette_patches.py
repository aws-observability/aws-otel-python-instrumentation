# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import asyncio
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.patches._starlette_patches import _apply_starlette_instrumentation_patches
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.instrumentation.utils import suppress_http_instrumentation
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


class TestStarlettePatch(TestCase):
    """Test the Starlette instrumentation patches."""

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_applied_successfully(self, mock_logger):
        """Test that the Starlette ASGI middleware patch is applied successfully."""
        env_configs = [
            {"AGENT_OBSERVABILITY_ENABLED": "true"},
            {"AGENT_OBSERVABILITY_ENABLED": "false"},
        ]
        for env_vars in env_configs:
            agent_enabled = any(v == "true" for v in env_vars.values())
            with self.subTest(env_vars=env_vars):
                mock_logger.reset_mock()

                with patch.dict("os.environ", env_vars, clear=False):

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
                        if agent_enabled:
                            self.assertTrue(mock_middleware_instance.exclude_receive_span)
                            self.assertTrue(mock_middleware_instance.exclude_send_span)
                        else:
                            self.assertFalse(mock_middleware_instance.exclude_receive_span)
                            self.assertFalse(mock_middleware_instance.exclude_send_span)

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
