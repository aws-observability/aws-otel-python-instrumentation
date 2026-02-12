# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


"""OpenTelemetry MCP (Model Context Protocol) instrumentation."""

import logging
from typing import Any, Callable, Collection

from wrapt import register_post_import_hook, wrap_function_wrapper  # type: ignore[import-untyped]

from amazon.opentelemetry.distro.instrumentation.mcp._wrappers import ClientWrapper, ServerWrapper
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap

_LOG = logging.getLogger(__name__)


class McpInstrumentor(BaseInstrumentor):
    """
    Instrumentor for MCP (Model Context Protocol).

    Provides automatic tracing for MCP client and server operations,
    including distributed trace context propagation.

    See: https://modelcontextprotocol.io/overview
    """

    _SESSION_MODULE = "mcp.shared.session"
    _SERVER_MODULE = "mcp.server.lowlevel.server"
    _STDIO_MODULE = "mcp.client.stdio"
    _HTTP_MODULE = "mcp.client.streamable_http"
    _SSE_MODULE = "mcp.client.sse"

    def __init__(self, **kwargs: Any) -> None:
        _LOG.info("Initializing MCP instrumentor.")
        super().__init__()
        self._client_wrapper = ClientWrapper(**kwargs)
        self._server_wrapper = ServerWrapper(**kwargs)

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("mcp >= 1.8.1",)

    def _instrument(self, **kwargs: Any) -> None:
        _LOG.debug("Instrument MCP client-side session methods.")
        McpInstrumentor._register_hook(
            self._SESSION_MODULE,
            "BaseSession.send_request",
            self._client_wrapper.wrap_session_send,
        )
        McpInstrumentor._register_hook(
            self._SESSION_MODULE,
            "BaseSession.send_notification",
            self._client_wrapper.wrap_session_send,
        )

        _LOG.debug("Instrument MCP server-side session methods.")
        McpInstrumentor._register_hook(
            self._SERVER_MODULE,
            "Server._handle_request",
            self._server_wrapper._wrap_server_handle_request,
        )
        McpInstrumentor._register_hook(
            self._SERVER_MODULE,
            "Server._handle_notification",
            self._server_wrapper._wrap_server_handle_notification,
        )

        _LOG.debug("Instrument MCP transport layer.")
        McpInstrumentor._register_hook(self._STDIO_MODULE, "stdio_client", self._client_wrapper.wrap_stdio_client)
        McpInstrumentor._register_hook(
            self._HTTP_MODULE, "streamablehttp_client", self._client_wrapper.wrap_http_client
        )
        McpInstrumentor._register_hook(self._SSE_MODULE, "sse_client", self._client_wrapper.wrap_http_client)
        McpInstrumentor._register_hook(
            self._HTTP_MODULE,
            "StreamableHTTPTransport._maybe_extract_session_id_from_response",
            ClientWrapper.wrap_extract_session_id,
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(self._SESSION_MODULE, "BaseSession.send_request")
        unwrap(self._SESSION_MODULE, "BaseSession.send_notification")
        unwrap(self._SERVER_MODULE, "Server._handle_request")
        unwrap(self._SERVER_MODULE, "Server._handle_notification")
        unwrap(self._STDIO_MODULE, "stdio_client")
        unwrap(self._HTTP_MODULE, "streamablehttp_client")
        unwrap(self._SSE_MODULE, "sse_client")
        unwrap(self._HTTP_MODULE, "StreamableHTTPTransport._maybe_extract_session_id_from_response")

    @staticmethod
    def _register_hook(module: str, target: str, wrapper: Callable[..., Any]) -> None:
        """Register a post-import hook for wrapping a function."""

        def hook(_module: Any) -> None:
            wrap_function_wrapper(module, target, wrapper)

        register_post_import_hook(hook, module)
