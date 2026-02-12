# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Collection

from amazon.opentelemetry.distro.instrumentation.common.utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.instrumentation.mcp._wrappers import ClientWrapper, ServerWrapper
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

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
        self._client_wrapper: ClientWrapper | None = None
        self._server_wrapper: ServerWrapper | None = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("mcp >= 1.8.1",)

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)
        self._client_wrapper = ClientWrapper(tracer=tracer, **kwargs)
        self._server_wrapper = ServerWrapper(tracer=tracer, **kwargs)

        _LOG.debug("Instrument MCP client-side session methods.")

        try_wrap(
            self._SESSION_MODULE,
            "BaseSession.send_request",
            self._client_wrapper.wrap_session_send,
        )
        try_wrap(
            self._SESSION_MODULE,
            "BaseSession.send_notification",
            self._client_wrapper.wrap_session_send,
        )

        _LOG.debug("Instrument MCP server-side session methods.")

        try_wrap(
            self._SERVER_MODULE,
            "Server._handle_request",
            self._server_wrapper._wrap_server_handle_request,
        )
        try_wrap(
            self._SERVER_MODULE,
            "Server._handle_notification",
            self._server_wrapper._wrap_server_handle_notification,
        )

        _LOG.debug("Instrument MCP transport layer.")

        try_wrap(self._STDIO_MODULE, "stdio_client", self._client_wrapper.wrap_stdio_client)
        try_wrap(self._HTTP_MODULE, "streamablehttp_client", self._client_wrapper.wrap_http_client)  # deprecated
        try_wrap(self._HTTP_MODULE, "streamable_http_client", self._client_wrapper.wrap_http_client)
        try_wrap(self._SSE_MODULE, "sse_client", self._client_wrapper.wrap_http_client)
        try_wrap(
            self._HTTP_MODULE,
            "StreamableHTTPTransport._maybe_extract_session_id_from_response",
            ClientWrapper.wrap_extract_session_id,
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        # pylint: disable=import-outside-toplevel
        from mcp.client import sse, stdio, streamable_http
        from mcp.server.lowlevel import server
        from mcp.shared import session

        try_unwrap(session.BaseSession, "send_request")
        try_unwrap(session.BaseSession, "send_notification")
        try_unwrap(server.Server, "_handle_request")
        try_unwrap(server.Server, "_handle_notification")
        try_unwrap(stdio, "stdio_client")
        try_unwrap(streamable_http, "streamablehttp_client")
        try_unwrap(streamable_http, "streamable_http_client")
        try_unwrap(sse, "sse_client")
        try_unwrap(streamable_http.StreamableHTTPTransport, "_maybe_extract_session_id_from_response")
