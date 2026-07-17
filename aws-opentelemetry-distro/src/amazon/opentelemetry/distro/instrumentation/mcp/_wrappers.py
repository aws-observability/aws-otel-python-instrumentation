# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import inspect
import logging
import os
from contextlib import asynccontextmanager
from contextvars import Token
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple
from urllib.parse import urlparse

from amazon.opentelemetry.distro._utils import is_agent_observability_enabled
from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import to_tool_attribute_value
from opentelemetry import context, trace
from opentelemetry.instrumentation.utils import suppress_http_instrumentation
from opentelemetry.propagate import get_global_textmap
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_PROMPT_NAME,
    GEN_AI_TOOL_CALL_ARGUMENTS,
    GEN_AI_TOOL_CALL_RESULT,
    GEN_AI_TOOL_NAME,
    GenAiOperationNameValues,
)
from opentelemetry.semconv._incubating.attributes.jsonrpc_attributes import JSONRPC_REQUEST_ID
from opentelemetry.semconv._incubating.attributes.mcp_attributes import (
    MCP_METHOD_NAME,
    MCP_PROTOCOL_VERSION,
    MCP_RESOURCE_URI,
    MCP_SESSION_ID,
    McpMethodNameValues,
)
from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_RESPONSE_STATUS_CODE
from opentelemetry.semconv.attributes.client_attributes import CLIENT_ADDRESS, CLIENT_PORT
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.network_attributes import NETWORK_TRANSPORT, NetworkTransportValues
from opentelemetry.semconv.attributes.server_attributes import SERVER_ADDRESS, SERVER_PORT
from opentelemetry.trace import SpanKind, Status, StatusCode

_LOG = logging.getLogger(__name__)

OTEL_MCP_SUPPRESS_HTTP_INSTRUMENTATION = "OTEL_MCP_SUPPRESS_HTTP_INSTRUMENTATION"

# Context key for storing client transport metadata alongside the session span.
_TRANSPORT_KEY = context.create_key("mcp_client_transport")


def _get_parameter_names(func: Callable[..., Any]) -> Tuple[str, ...]:
    """Return the parameter names of ``func`` (best-effort, empty on failure)."""
    try:
        return tuple(inspect.signature(func).parameters.keys())
    except (TypeError, ValueError):
        return ()


class McpWrapper:
    """Base wrapper class for MCP operations."""

    # Temporary placeholder client and server span names.
    # These are set initially but updated once we know the actual MCP operation.
    _CLIENT_SPAN_NAME = "mcp.client"
    _SERVER_SPAN_NAME = "mcp.server"

    _SESSION_SPAN_NAME = "mcp.session"
    _SESSION_ID_HEADER = "mcp-session-id"

    def __init__(self, tracer: trace.Tracer, **kwargs: Any) -> None:
        self._tracer = tracer
        self._propagators = kwargs.get("propagators") or get_global_textmap()
        self._should_suppress_http_spans = (
            os.environ.get(OTEL_MCP_SUPPRESS_HTTP_INSTRUMENTATION, "true").lower() == "true"
        )
        self._agent_observability_enabled = is_agent_observability_enabled()

    def _should_suppress_mcp_span(self, message: Any) -> bool:
        from mcp import types  # pylint: disable=import-outside-toplevel

        if isinstance(
            message, (types.ClientRequest, types.ClientNotification, types.ServerRequest, types.ServerNotification)
        ):
            message = message.root
        # noisy spans most of the time
        if isinstance(message, (types.InitializeRequest, types.InitializedNotification)):
            return True
        # MCP servers hosted on AgentCore get frequent /ping health checks
        if self._agent_observability_enabled and isinstance(message, types.PingRequest):
            return True
        return False

    @staticmethod
    def _set_mcp_attributes(span: trace.Span, message: Any, request_id: Optional[int]) -> None:
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

        def create_mcp_span_name(method, target=None):
            return f"mcp {method} {target}" if target else f"mcp {method}"

        is_notification = isinstance(message, (types.ClientNotification, types.ServerNotification))

        if hasattr(message, "root"):
            message = message.root

        # notifications do not have request id per:
        # https://www.jsonrpc.org/specification#notification
        if request_id is not None and not is_notification:
            span.set_attribute(JSONRPC_REQUEST_ID, str(request_id))

        span.set_attribute(MCP_METHOD_NAME, message.method)
        span.set_attribute(MCP_PROTOCOL_VERSION, types.LATEST_PROTOCOL_VERSION)

        if isinstance(message, types.CallToolRequest):
            tool_name = message.params.name
            span.update_name(create_mcp_span_name(str(McpMethodNameValues.TOOLS_CALL.value), str(tool_name)))
            span.set_attribute(GEN_AI_TOOL_NAME, tool_name)
            span.set_attribute(GEN_AI_OPERATION_NAME, GenAiOperationNameValues.EXECUTE_TOOL.value)

            if message.params.arguments:
                span.set_attribute(
                    GEN_AI_TOOL_CALL_ARGUMENTS,
                    to_tool_attribute_value(message.params.arguments),
                )

        elif isinstance(message, types.GetPromptRequest):
            prompt_name = message.params.name
            span.update_name(create_mcp_span_name(str(McpMethodNameValues.PROMPTS_GET.value), str(prompt_name)))
            span.set_attribute(GEN_AI_PROMPT_NAME, prompt_name)

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
            span.update_name(create_mcp_span_name(str(message.method), resource_uri))
            span.set_attribute(MCP_RESOURCE_URI, resource_uri)

        else:
            span.update_name(create_mcp_span_name(str(message.method)))

        transport_info = context.get_value(_TRANSPORT_KEY)
        if isinstance(transport_info, dict):
            for key, value in transport_info.items():
                span.set_attribute(key, value)

    @staticmethod
    def _set_tool_result(span: trace.Span, message: Any, result: Any) -> None:
        from mcp import types  # pylint: disable=import-outside-toplevel

        if hasattr(message, "root"):
            message = message.root

        if isinstance(message, types.CallToolRequest) and result is not None:
            try:
                span.set_attribute(GEN_AI_TOOL_CALL_RESULT, to_tool_attribute_value(result))
                if hasattr(result, "isError") and result.isError:
                    span.set_attribute(ERROR_TYPE, "tool_error")
                    span.set_status(Status(StatusCode.ERROR))
                    return
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        span.set_status(Status(StatusCode.OK))

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
        Wrap BaseSession.send_request and send_notification methods. Which is responsible for
        both requests/notifications sent by both the client and server.
        """

        async def async_wrapper() -> Any:
            message = args[0] if args else None
            if not message:
                return await wrapped(*args, **kwargs)

            if self._should_suppress_http_spans and self._should_suppress_mcp_span(message):
                return await wrapped(*args, **kwargs)

            span = self._tracer.start_span(name=self._CLIENT_SPAN_NAME, kind=SpanKind.CLIENT)
            ctx = trace.set_span_in_context(span)
            token = context.attach(ctx)

            try:
                message_json = message.model_dump(by_alias=True, mode="json", exclude_none=True)
                message_json.setdefault("params", {}).setdefault("_meta", {})

                carrier: Dict[str, Any] = {}
                # as defined by OTel semantic conventions: trace context between
                # client and server must be injected in the _meta field of the
                # JSON RPC call:
                # {
                #   "jsonrpc": "2.0",
                #   "method": "tools/call",
                #   "params": {
                #     "name": "get-weather",
                #     "_meta": {
                #       "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                #       "tracestate": "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"
                #     }
                #   },
                #   "id": 1,
                # }
                self._propagators.inject(carrier=carrier, context=ctx)
                message_json["params"]["_meta"].update(carrier)

                request_id = getattr(instance, "_request_id", None)
                self._set_mcp_attributes(span, message, request_id)

                modified_message = message.model_validate(message_json)
                new_args = (modified_message,) + args[1:]

                result = await wrapped(*new_args, **kwargs)
                self._set_tool_result(span, message, result)
                return result
            except Exception as exc:
                self._set_error_attrs(span, exc)
                raise
            finally:
                span.end()
                context.detach(token)

        return async_wrapper()

    def wrap_stdio_client(self, wrapped: Callable[..., Any], _instance: Any, args: Any, kwargs: Any) -> Any:

        @asynccontextmanager
        async def wrapper():
            session_span, token = self._start_mcp_session_span({NETWORK_TRANSPORT: NetworkTransportValues.PIPE.value})
            try:
                async with wrapped(*args, **kwargs) as streams:
                    yield streams
            finally:
                session_span.end()
                context.detach(token)

        return wrapper()

    def wrap_http_client(self, wrapped: Callable[..., Any], _instance: Any, args: Any, kwargs: Any) -> Any:

        @asynccontextmanager
        async def wrapper():
            url = args[0] if args else kwargs.get("url", "")
            parsed = urlparse(url)
            session_span, token = self._start_mcp_session_span(
                {
                    NETWORK_TRANSPORT: NetworkTransportValues.TCP.value,
                    SERVER_ADDRESS: parsed.hostname,
                    SERVER_PORT: parsed.port or (443 if parsed.scheme == "https" else 80),
                }
            )

            # Ensure W3C trace context is injected into outbound HTTP request
            # headers so MCP servers that read context from HTTP headers (rather
            # than the JSON-RPC ``_meta`` body) can still join the caller's
            # trace. ``suppress_http_instrumentation()`` — which we use to
            # deduplicate the redundant HTTP CLIENT span — also short-circuits
            # the HTTP client instrumentation's own header injection, so we
            # inject ourselves via an httpx request event hook attached at
            # client-construction time.
            patched_args, patched_kwargs = self._install_trace_header_injection(wrapped, args, kwargs)

            try:
                if self._should_suppress_http_spans:
                    with suppress_http_instrumentation():
                        async with wrapped(*patched_args, **patched_kwargs) as streams:
                            yield streams
                else:
                    async with wrapped(*patched_args, **patched_kwargs) as streams:
                        yield streams
            finally:
                session_span.end()
                context.detach(token)

        return wrapper()

    def _install_trace_header_injection(  # pylint: disable=too-many-return-statements
        self, wrapped: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        """Return ``(args, kwargs)`` with an httpx event hook installed for W3C header injection.

        Wraps the caller's ``httpx_client_factory`` (or the transport's default
        factory) so every ``httpx.AsyncClient`` the MCP transport uses has a
        request event hook that calls the configured propagator against the
        outgoing request's headers. The hook takes the OTel context *at
        request-send time*, so per-tool-call parents are honored correctly
        across a shared MCP session.

        The newer ``streamable_http_client`` (mcp>=1.16) takes a pre-built
        ``http_client`` instead of a factory; the caller owns that client, so
        we install the event hook directly on it.

        Silently no-ops on any unexpected shape so instrumentation never
        breaks the request path.
        """
        # pylint: disable=import-outside-toplevel
        try:
            import httpx
        except ImportError:
            return args, kwargs

        propagators = self._propagators

        async def _inject_trace_context(request: "httpx.Request") -> None:
            propagators.inject(carrier=request.headers)

        def _attach_hook(client: "httpx.AsyncClient") -> None:
            try:
                request_hooks = client.event_hooks.setdefault("request", [])
                if _inject_trace_context not in request_hooks:
                    request_hooks.append(_inject_trace_context)
            except Exception:  # pylint: disable=broad-exception-caught
                _LOG.debug("Failed to attach MCP trace-context injection hook", exc_info=True)

        try:
            wrapped_params = _get_parameter_names(wrapped)
        except Exception:  # pylint: disable=broad-exception-caught
            wrapped_params = ()

        # streamable_http_client(url, http_client=...) — user may pass a
        # pre-built client. Attach the hook directly to it. If they didn't
        # pass one, MCP would create one internally via create_mcp_http_client;
        # we intercept by supplying our own pre-built client instead.
        if "http_client" in wrapped_params:
            pre_built_client = kwargs.get("http_client")
            if isinstance(pre_built_client, httpx.AsyncClient):
                _attach_hook(pre_built_client)
                return args, kwargs

            try:
                from mcp.client.streamable_http import create_mcp_http_client
            except ImportError:
                return args, kwargs

            new_client = create_mcp_http_client()
            _attach_hook(new_client)
            new_kwargs = dict(kwargs)
            new_kwargs["http_client"] = new_client
            return args, new_kwargs

        # streamablehttp_client / sse_client accept ``httpx_client_factory``.
        # Wrap it (or install a default that MCP would otherwise create).
        if "httpx_client_factory" not in wrapped_params:
            return args, kwargs

        original_factory = kwargs.get("httpx_client_factory")
        if original_factory is None:
            try:
                from mcp.client.streamable_http import create_mcp_http_client

                original_factory = create_mcp_http_client
            except ImportError:
                return args, kwargs

        def wrapped_factory(*f_args: Any, **f_kwargs: Any) -> "httpx.AsyncClient":
            client = original_factory(*f_args, **f_kwargs)
            _attach_hook(client)
            return client

        new_kwargs = dict(kwargs)
        new_kwargs["httpx_client_factory"] = wrapped_factory
        return args, new_kwargs

    def wrap_handle_post_request(
        self,
        wrapped: Callable[..., Any],
        instance: Any,
        args: Any,
        kwargs: Any,
    ) -> Any:
        """
        Wrap StreamableHTTPTransport._handle_post_request to restore
        trace context. Extracts the trace context sent to MCP server
        via JSON RPC so existing HTTP spans are correctly parented
        under the MCP request span.
        """

        async def async_wrapper():
            ctx = None
            try:
                from mcp.client.streamable_http import RequestContext  # pylint: disable=import-outside-toplevel

                request_ctx = args[0] if args else None
                if isinstance(request_ctx, RequestContext):
                    message = request_ctx.session_message.message
                    message_dict = message.model_dump(by_alias=True, mode="json", exclude_none=True)
                    meta = message_dict.get("params", {}).get("_meta", {})
                    if meta:
                        ctx = self._propagators.extract(carrier=meta)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            if ctx:
                token = context.attach(ctx)
                try:
                    if self._should_suppress_http_spans:
                        with suppress_http_instrumentation():
                            return await wrapped(*args, **kwargs)
                    else:
                        return await wrapped(*args, **kwargs)
                finally:
                    context.detach(token)
            else:
                return await wrapped(*args, **kwargs)

        return async_wrapper()

    @staticmethod
    def wrap_extract_session_id(wrapped: Callable[..., Any], instance: Any, args: Any, kwargs: Any) -> Any:
        result = wrapped(*args, **kwargs)
        if instance.session_id:
            transport_info = context.get_value(_TRANSPORT_KEY)
            if isinstance(transport_info, dict):
                transport_info[MCP_SESSION_ID] = instance.session_id
        return result

    def _start_mcp_session_span(self, transport_info: Dict[str, Any]) -> Tuple[trace.Span, Token]:
        # A bit strange and does not follow any existing OTel semantic
        # conventions, but we need an overarching parent span to capture
        # the MCP session lifetime. All server and client MCP operations
        # happen within this session context. Otherwise we get a bunch
        # of disjointed traces without a common ancestor.
        span = self._tracer.start_span(self._SESSION_SPAN_NAME, kind=SpanKind.INTERNAL)
        ctx = trace.set_span_in_context(span)
        ctx = context.set_value(_TRANSPORT_KEY, transport_info, ctx)

        return span, context.attach(ctx)


class ServerWrapper(McpWrapper):
    """
    Wrapper for MCP server-side operations.
    """

    # TODO: Wrap ServerSession._received_request and _received_notification to capture server-side
    # initialize and notifications/initialized spans, which are handled before Server._handle_request.
    # Though we can opt out of doing so as well as believe those spans provide minimum value with added
    # complexity.

    def wrap_mcp_http_sse_app_factory(self, wrapped: Callable[..., Any], instance: Any, args: Any, kwargs: Any) -> Any:
        if not self._should_suppress_http_spans:
            return wrapped(*args, **kwargs)

        class _HttpSuppressionMiddleware:
            def __init__(self, inner_app: Any) -> None:
                self.app = inner_app

            async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
                with suppress_http_instrumentation():
                    await self.app(scope, receive, send)

        app = wrapped(*args, **kwargs)
        try:
            app.add_middleware(_HttpSuppressionMiddleware)
        except Exception:  # pylint: disable=broad-exception-caught
            _LOG.debug("Failed to apply ASGI suppression to MCP server app", exc_info=True)
        return app

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

        if self._should_suppress_http_spans and self._should_suppress_mcp_span(incoming_msg):
            return await wrapped(*args, **kwargs)

        request_id = getattr(incoming_msg, "id", None)

        carrier = self._extract_trace_context(incoming_msg)
        parent_ctx = self._propagators.extract(carrier=carrier)

        with self._tracer.start_as_current_span(
            self._SERVER_SPAN_NAME,
            kind=SpanKind.SERVER,
            context=parent_ctx,
        ) as span:
            # Note: Notifications do not have request_context, so network.transport is only set for requests
            is_request, is_http, session_id, client_address, client_port = self._extract_server_transport_info(args)
            if is_request:
                if is_http:
                    span.set_attribute(NETWORK_TRANSPORT, NetworkTransportValues.TCP.value)
                    if session_id:
                        span.set_attribute(MCP_SESSION_ID, session_id)
                    if client_address:
                        span.set_attribute(CLIENT_ADDRESS, client_address)
                    if client_port:
                        span.set_attribute(CLIENT_PORT, client_port)
                else:
                    span.set_attribute(NETWORK_TRANSPORT, NetworkTransportValues.PIPE.value)

            self._set_mcp_attributes(span, incoming_msg, request_id)

            try:
                result = await wrapped(*args, **kwargs)
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

    def _extract_server_transport_info(
        self, args: Tuple[Any, ...]
    ) -> Tuple[bool, bool, Optional[str], Optional[str], Optional[int]]:  # pylint: disable=no-self-use
        """
        Extract transport info from the incoming request on the server side.
        Unlike the client which stores transport metadata in the OTel context, the server
        must inspect each incoming request to determine transport type.

        Args:
            args: Server method arguments

        Returns:
            Tuple of (is_request, is_http, session_id, client_address, client_port)
        """
        try:
            # pylint: disable=import-outside-toplevel
            from mcp.shared.message import ServerMessageMetadata
            from mcp.shared.session import RequestResponder

            if not args:
                return False, False, None, None, None

            message: RequestResponder = args[0]
            if not isinstance(message, RequestResponder):
                return False, False, None, None, None

            metadata = message.message_metadata
            if not isinstance(metadata, ServerMessageMetadata):
                return True, False, None, None, None  # is_request but no metadata

            request_context = metadata.request_context
            if not request_context:
                return True, False, None, None, None  # is_request but stdio

            session_id = None
            client_address = None
            client_port = None

            # streamable HTTP has session_id in headers
            headers = getattr(request_context, "headers", None)
            if headers:
                session_id = headers.get(self._SESSION_ID_HEADER)

            # HTTP with SSE has session_id in query parameters
            if not session_id:
                query_params = getattr(request_context, "query_params", None)
                if query_params:
                    session_id = query_params.get("session_id")

            client = getattr(request_context, "client", None)
            if client:
                client_address = client[0] if len(client) > 0 else None
                client_port = client[1] if len(client) > 1 else None

            return True, True, session_id, client_address, client_port

        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("Failed to extract transport info: %s", exc)

        return False, False, None, None, None
