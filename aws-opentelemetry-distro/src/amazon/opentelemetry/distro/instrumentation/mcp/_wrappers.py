# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from contextlib import asynccontextmanager
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple
from urllib.parse import urlparse

from amazon.opentelemetry.distro.instrumentation.common.utils import serialize_to_json
from amazon.opentelemetry.distro.instrumentation.mcp._transport import (
    TransportInfo,
    TransportType,
    clear_parent_context,
    clear_transport_info,
    get_parent_context,
    get_transport_info,
    set_parent_context,
    set_transport_info,
)
from amazon.opentelemetry.distro.semconv._incubating.attributes.gen_ai_attributes import (
    JSONRPC_REQUEST_ID,
    MCP_METHOD_NAME,
    MCP_PROTOCOL_VERSION,
    MCP_RESOURCE_URI,
    MCP_SESSION_ID,
    RPC_RESPONSE_STATUS_CODE,
    MCPMethodValue,
)
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.propagate import get_global_textmap
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROMPT,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.semconv.attributes.client_attributes import CLIENT_ADDRESS, CLIENT_PORT
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.network_attributes import NETWORK_TRANSPORT
from opentelemetry.semconv.attributes.server_attributes import SERVER_ADDRESS, SERVER_PORT
from opentelemetry.trace import SpanKind, Status, StatusCode

_LOG = logging.getLogger(__name__)


class McpWrapper:
    """Base wrapper class for MCP operations."""

    _CLIENT_SPAN_NAME = "mcp.client"
    _SERVER_SPAN_NAME = "mcp.server"
    _SESSION_ID_HEADER = "mcp-session-id"

    def __init__(self, **kwargs: Any) -> None:
        self.propagators = kwargs.get("propagators") or get_global_textmap()
        self.tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider=kwargs.get("tracer_provider"),
        )

    @staticmethod
    def _generate_mcp_message_attrs(span: trace.Span, message: Any, request_id: Optional[int]) -> None:
        """
        Populate span with MCP semantic convention attributes.

        Based on v1.39.0: https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/
        Note: These conventions are currently unstable and may change.

        Args:
            span: Span to enrich with attributes
            message: MCP message (ClientRequest, ServerRequest, etc.)
            request_id: Request ID if available (None for notifications)
        """
        from mcp import types  # pylint: disable=import-outside-toplevel

        if hasattr(message, "root"):
            message = message.root

        if request_id is not None:
            span.set_attribute(JSONRPC_REQUEST_ID, str(request_id))

        span.set_attribute(MCP_METHOD_NAME, message.method)
        span.set_attribute(MCP_PROTOCOL_VERSION, types.LATEST_PROTOCOL_VERSION)

        if isinstance(message, types.CallToolRequest):
            tool_name = message.params.name
            span.update_name(f"{MCPMethodValue.TOOLS_CALL} {tool_name}")
            span.set_attribute(GEN_AI_TOOL_NAME, tool_name)
            span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)

            if message.params.arguments:
                span.set_attribute(
                    GEN_AI_TOOL_CALL_ARGUMENTS,
                    serialize_to_json(message.params.arguments),
                )

        elif isinstance(message, types.GetPromptRequest):
            prompt_name = message.params.name
            span.update_name(f"{MCPMethodValue.PROMPTS_GET} {prompt_name}")
            span.set_attribute(GEN_AI_PROMPT, prompt_name)

        elif isinstance(
            message,
            (
                types.ReadResourceRequest,
                types.SubscribeRequest,
                types.UnsubscribeRequest,
                types.ResourceUpdatedNotification,
            ),
        ):
            resource_uri = str(message.params.uri)
            span.update_name(f"{message.method} {resource_uri}")
            span.set_attribute(MCP_RESOURCE_URI, resource_uri)

        else:
            span.update_name(message.method)

    @staticmethod
    def _set_tool_result(span: trace.Span, message: Any, result: Any) -> None:
        from mcp import types  # pylint: disable=import-outside-toplevel

        if hasattr(message, "root"):
            message = message.root

        if isinstance(message, types.CallToolRequest) and result is not None:
            try:
                span.set_attribute(GEN_AI_TOOL_CALL_RESULT, serialize_to_json(result))
            except Exception:  # pylint: disable=broad-exception-caught
                pass

    @staticmethod
    def _set_error_attrs(span: trace.Span, exc: Exception) -> None:
        """Set error attributes on span from exception."""
        span.set_status(Status(StatusCode.ERROR, str(exc)))
        span.record_exception(exc)
        span.set_attribute(ERROR_TYPE, type(exc).__name__)

        try:
            from mcp.shared.exceptions import McpError  # pylint: disable=import-outside-toplevel

            if isinstance(exc, McpError):
                span.set_attribute(RPC_RESPONSE_STATUS_CODE, str(exc.error.code))
        except ImportError:
            pass


class ClientWrapper(McpWrapper):
    """
    Wrapper for MCP client-side operations.
    """

    def wrap_session_send(
        self,
        wrapped: Callable[..., Coroutine[Any, Any, Any]],
        instance: Any,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> Coroutine[Any, Any, Any]:
        """
        Wrap BaseSession.send_request and send_notification methods.

        Instruments outgoing MCP messages by:
        1. Creating a span for the operation
        2. Injecting trace context into the message
        3. Recording message attributes
        """

        async def async_wrapper() -> Any:
            message = args[0] if args else None
            if not message:
                return await wrapped(*args, **kwargs)

            try:
                message_json = message.model_dump(by_alias=True, mode="json", exclude_none=True)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                _LOG.warning("Failed to serialize message for tracing: %s", exc)
                return await wrapped(*args, **kwargs)

            message_json.setdefault("params", {}).setdefault("_meta", {})

            # Use stored parent context
            parent_ctx = get_parent_context()
            with self.tracer.start_as_current_span(
                name=self._CLIENT_SPAN_NAME, kind=SpanKind.CLIENT, context=parent_ctx
            ) as span:
                ctx = trace.set_span_in_context(span)
                carrier: Dict[str, Any] = {}
                self.propagators.inject(carrier=carrier, context=ctx)
                message_json["params"]["_meta"].update(carrier)

                request_id = getattr(instance, "_request_id", None)
                self._generate_mcp_message_attrs(span, message, request_id)
                self._set_client_transport_attrs(span)

                try:
                    modified_message = message.model_validate(message_json)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    _LOG.warning("Failed to reconstruct message for tracing: %s", exc)
                    return await wrapped(*args, **kwargs)
                new_args = (modified_message,) + args[1:]

                try:
                    result = await wrapped(*new_args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    self._set_tool_result(span, message, result)
                    return result
                except Exception as exc:
                    self._set_error_attrs(span, exc)
                    raise

        return async_wrapper()

    def wrap_stdio_client(self, wrapped: Callable[..., Any], _instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap stdio_client to set transport context and capture parent context."""

        @asynccontextmanager
        async def wrapper():
            set_transport_info(TransportInfo(transport=TransportType.PIPE))
            # Create a parent span and store its context for child spans
            with self.tracer.start_as_current_span("mcp.session", kind=SpanKind.INTERNAL) as span:
                set_parent_context(trace.set_span_in_context(span))
                try:
                    async with wrapped(*args, **kwargs) as streams:
                        yield streams
                finally:
                    clear_transport_info()
                    clear_parent_context()

        return wrapper()

    def wrap_http_client(self, wrapped: Callable[..., Any], _instance: Any, args: Any, kwargs: Any) -> Any:
        """Wrap streamable_http_client/sse_client to set transport context and capture parent context."""

        @asynccontextmanager
        async def wrapper():
            url = args[0] if args else kwargs.get("url", "")
            parsed = urlparse(url)
            set_transport_info(
                TransportInfo(
                    transport=TransportType.TCP,
                    server_address=parsed.hostname,
                    server_port=parsed.port or (443 if parsed.scheme == "https" else 80),
                )
            )
            # Create a parent span and store its context for child spans
            with self.tracer.start_as_current_span("mcp.session", kind=SpanKind.INTERNAL) as span:
                set_parent_context(trace.set_span_in_context(span))
                try:
                    async with wrapped(*args, **kwargs) as streams:
                        yield streams
                finally:
                    clear_transport_info()
                    clear_parent_context()

        return wrapper()

    @staticmethod
    def wrap_extract_session_id(wrapped: Callable[..., Any], instance: Any, args: Any, kwargs: Any) -> Any:
        result = wrapped(*args, **kwargs)
        if instance.session_id:
            transport_info = get_transport_info()
            if transport_info:
                transport_info.session_id = instance.session_id
        return result

    @staticmethod
    def _set_client_transport_attrs(span: trace.Span) -> None:
        """Set transport attributes from context."""
        transport_info = get_transport_info()
        if transport_info:
            span.set_attribute(NETWORK_TRANSPORT, transport_info.transport.value)
            if transport_info.server_address:
                span.set_attribute(SERVER_ADDRESS, transport_info.server_address)
            if transport_info.server_port:
                span.set_attribute(SERVER_PORT, transport_info.server_port)
            if transport_info.session_id:
                span.set_attribute(MCP_SESSION_ID, transport_info.session_id)


class ServerWrapper(McpWrapper):
    """
    Wrapper for MCP server-side operations.
    """

    async def _wrap_server_handle_request(
        self,
        wrapped: Callable[..., Coroutine[Any, Any, Any]],
        instance: Any,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> Any:
        """
        Wrap Server._handle_request method.

        Args:
            wrapped: Original method
            instance: Server instance
            args: (session, request)
            kwargs: Keyword arguments
        """
        # https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp/server/lowlevel/server.py
        incoming_req = args[1] if len(args) > 1 else None
        return await self._wrap_server_message_handler(wrapped, instance, args, kwargs, incoming_msg=incoming_req)

    async def _wrap_server_handle_notification(
        self,
        wrapped: Callable[..., Coroutine[Any, Any, Any]],
        instance: Any,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> Any:
        """
        Wrap Server._handle_notification method.

        Args:
            wrapped: Original method
            instance: Server instance
            args: (notification,)
            kwargs: Keyword arguments
        """
        # https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp/server/lowlevel/server.py
        incoming_notif = args[0] if args else None
        return await self._wrap_server_message_handler(wrapped, instance, args, kwargs, incoming_msg=incoming_notif)

    async def _wrap_server_message_handler(
        self,
        wrapped: Callable[..., Coroutine[Any, Any, Any]],
        instance: Any,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        incoming_msg: Optional[Any],
    ) -> Any:
        """
        Common handler for server-side request and notification processing.

        Instruments incoming MCP messages by:
        1. Extracting trace context from the message
        2. Creating a linked server span
        3. Recording message attributes

        Args:
            wrapped: Original method
            instance: Server instance
            args: Method arguments
            kwargs: Keyword arguments
            incoming_msg: The incoming request or notification

        Returns:
            Result from the wrapped method
        """
        if not incoming_msg:
            return await wrapped(*args, **kwargs)

        request_id = getattr(incoming_msg, "id", None)

        carrier = self._extract_trace_context(incoming_msg)
        parent_ctx = self.propagators.extract(carrier=carrier)

        with self.tracer.start_as_current_span(
            self._SERVER_SPAN_NAME,
            kind=SpanKind.SERVER,
            context=parent_ctx,
        ) as span:
            is_http, session_id, client_address, client_port = self._extract_transport_info(args)
            if is_http:
                span.set_attribute(NETWORK_TRANSPORT, "tcp")
                if session_id:
                    span.set_attribute(MCP_SESSION_ID, session_id)
                if client_address:
                    span.set_attribute(CLIENT_ADDRESS, client_address)
                if client_port:
                    span.set_attribute(CLIENT_PORT, client_port)
            else:
                span.set_attribute(NETWORK_TRANSPORT, "pipe")

            self._generate_mcp_message_attrs(span, incoming_msg, request_id)

            try:
                result = await wrapped(*args, **kwargs)
                span.set_status(Status(StatusCode.OK))
                self._set_tool_result(span, incoming_msg, result)
                return result
            except Exception as exc:
                self._set_error_attrs(span, exc)
                raise

    def _extract_trace_context(self, message: Any) -> Dict[str, Any]:  # pylint: disable=no-self-use
        """
        Extract trace context carrier from message metadata.

        Args:
            message: Incoming MCP message

        Returns:
            Dictionary containing trace context or empty dict
        """
        try:
            if hasattr(message, "params") and hasattr(message.params, "meta") and message.params.meta:
                return message.params.meta.model_dump()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("Failed to extract trace context: %s", exc)
        return {}

    def _extract_transport_info(
        self, args: Tuple[Any, ...]
    ) -> Tuple[bool, Optional[str], Optional[str], Optional[int]]:  # pylint: disable=no-self-use
        """
        Extract transport info from HTTP request context.

        Args:
            args: Server method arguments

        Returns:
            Tuple of (is_http, session_id, client_address, client_port)
        """
        try:
            # pylint: disable=import-outside-toplevel
            from mcp.shared.message import ServerMessageMetadata
            from mcp.shared.session import RequestResponder

            if not args:
                return False, None, None, None

            message = args[0]
            if not isinstance(message, RequestResponder):
                return False, None, None, None

            metadata = message.message_metadata
            if not isinstance(metadata, ServerMessageMetadata):
                return False, None, None, None

            request_context = metadata.request_context
            session_id = None
            client_address = None
            client_port = None

            if request_context:
                headers = getattr(request_context, "headers", None)
                if headers:
                    session_id = headers.get(self._SESSION_ID_HEADER)

                client = getattr(request_context, "client", None)
                if client:
                    client_address = client[0] if len(client) > 0 else None
                    client_port = client[1] if len(client) > 1 else None

            return True, session_id, client_address, client_port

        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("Failed to extract transport info: %s", exc)

        return False, None, None, None
