# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
import json
from typing import Any, AsyncGenerator, Callable, Collection, Dict, Optional, Tuple, Union, cast

from wrapt import register_post_import_hook, wrap_function_wrapper

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.propagate import get_global_textmap

from .version import __version__

from .semconv import (
    MCPSpanAttributes,
    MCPMethodValue,
)


class McpInstrumentor(BaseInstrumentor):
    """
    An instrumentation class for MCP: https://modelcontextprotocol.io/overview
    """

    _DEFAULT_CLIENT_SPAN_NAME = "span.mcp.client"
    _DEFAULT_SERVER_SPAN_NAME = "span.mcp.server"

    def __init__(self, **kwargs):
        super().__init__()
        self.propagators = kwargs.get("propagators") or get_global_textmap()
        self.tracer = trace.get_tracer(__name__, __version__, tracer_provider=kwargs.get("tracer_provider", None))

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("mcp >= 1.8.1",)

    def _instrument(self, **kwargs: Any) -> None:
        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.shared.session",
                "BaseSession.send_request",
                self._wrap_session_send,
            ),
            "mcp.shared.session",
        )
        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.shared.session",
                "BaseSession.send_notification",
                self._wrap_session_send,
            ),
            "mcp.shared.session",
        )
        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.server.lowlevel.server",
                "Server._handle_request",
                self._wrap_server_handle_request,
            ),
            "mcp.server.lowlevel.server",
        )
        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.server.lowlevel.server",
                "Server._handle_notification",
                self._wrap_server_handle_notification,
            ),
            "mcp.server.lowlevel.server",
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap("mcp.shared.session", "BaseSession.send_request")
        unwrap("mcp.shared.session", "BaseSession.send_notification")
        unwrap("mcp.server.lowlevel.server", "Server._handle_request")
        unwrap("mcp.server.lowlevel.server", "Server._handle_notification")

    def _wrap_session_send(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Callable:
        """
         Instruments MCP client and server request/notification sending for both stdio and Streamable HTTP transport,
         see: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports

         See:
         - https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/shared/session.py#L220
         - https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/shared/session.py#L296

         This instrumentation intercepts the requests/notification messages sent between client and server to obtain attributes for creating span, injects
         the current trace context, and embeds it into the request's params._meta field before forwarding the request to the MCP server.

        Args:
             wrapped: The original BaseSession.send_request/send_notification method
             instance: The BaseSession instance
             args: Positional arguments passed to the original send_request/send_notification method
             kwargs: Keyword arguments passed to the original send_request/send_notification method
        """
        from mcp.types import ClientRequest, ClientNotification, ServerRequest, ServerNotification

        async def async_wrapper():
            message: Optional[Union[ClientRequest, ClientNotification, ServerRequest, ServerNotification]] = (
                args[0] if len(args) > 0 else None
            )

            if not message:
                return await wrapped(*args, **kwargs)

            request_id: Optional[int] = getattr(instance, "_request_id", None)
            span_name = self._DEFAULT_SERVER_SPAN_NAME
            span_kind = SpanKind.SERVER

            if isinstance(message, (ClientRequest, ClientNotification)):
                span_name = self._DEFAULT_CLIENT_SPAN_NAME
                span_kind = SpanKind.CLIENT

            message_json = message.model_dump(by_alias=True, mode="json", exclude_none=True)

            if "params" not in message_json:
                message_json["params"] = {}
            if "_meta" not in message_json["params"]:
                message_json["params"]["_meta"] = {}

            with self.tracer.start_as_current_span(name=span_name, kind=span_kind) as span:
                ctx = trace.set_span_in_context(span)
                carrier = {}
                self.propagators.inject(carrier=carrier, context=ctx)
                message_json["params"]["_meta"].update(carrier)

                McpInstrumentor._generate_mcp_message_attrs(span, message, request_id)

                modified_message = message.model_validate(message_json)
                new_args = (modified_message,) + args[1:]

                try:
                    result = await wrapped(*new_args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise

        return async_wrapper()

    async def _wrap_server_handle_request(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Any:
        """
        Instruments MCP server-side request handling for both stdio and Streamable HTTP transport,
        see: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports

        This is the core function responsible for processing incoming requests on the MCP server.
        See:
        https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/server/lowlevel/server.py#L616

        Args:
            wrapped: The original Server._handle_request method being instrumented
            instance: The MCP Server instance processing the stdio communication
            args: Positional arguments passed to the original _handle_request method, containing the incoming request
            kwargs: Keyword arguments passed to the original _handle_request method
        """
        incoming_req = args[1] if len(args) > 1 else None
        return await self._wrap_server_message_handler(wrapped, instance, args, kwargs, incoming_msg=incoming_req)

    async def _wrap_server_handle_notification(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Any:
        """
        Instruments MCP server-side notification handling for both stdio and Streamable HTTP transport,
        This is the core function responsible for processing incoming notifications on the MCP server instance.
        See:
        https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/server/lowlevel/server.py#L616

        Args:
            wrapped: The original Server._handle_notification method being instrumented
            instance: The MCP Server instance processing the stdio communication
            args: Positional arguments passed to the original _handle_request method, containing the incoming request
            kwargs: Keyword arguments passed to the original _handle_request method
        """
        incoming_notif = args[0] if len(args) > 0 else None
        return await self._wrap_server_message_handler(wrapped, instance, args, kwargs, incoming_msg=incoming_notif)

    async def _wrap_server_message_handler(
        self,
        wrapped: Callable,
        instance: Any,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        incoming_msg: Optional[Any],
    ) -> Any:
        """
        Instruments MCP server-side request/notification handling for both stdio and Streamable HTTP transport,
        see: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports

        See:
        https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/server/lowlevel/server.py#L616

        The instrumented MCP server intercepts incoming requests/notification messages from the client to extract tracing context from
        the messages's params._meta field and creates server-side spans linked to the originating client spans.

        Args:
            wrapped: The original Server._handle_notification/_handle_request method being instrumented
            instance: The Server instance
            args: Positional arguments passed to the original _handle_request/ method, containing the incoming request
            kwargs: Keyword arguments passed to the original _handle_request method
            incoming_msg: The incoming message from the client, can be one of: ClientRequest or ClientNotification
        """
        if not incoming_msg:
            return await wrapped(*args, **kwargs)

        request_id = None
        carrier = {}

        # Request IDs are only present in Request messages not Notifications.
        if hasattr(incoming_msg, "id") and incoming_msg.id:
            request_id = incoming_msg.id

        # If the client is instrumented then params._meta field will contain the trace context.
        if hasattr(incoming_msg, "params") and hasattr(incoming_msg.params, "meta") and incoming_msg.params.meta:
            carrier = incoming_msg.params.meta.model_dump()

        parent_ctx = self.propagators.extract(carrier=carrier)

        with self.tracer.start_as_current_span(
            self._DEFAULT_SERVER_SPAN_NAME, kind=SpanKind.SERVER, context=parent_ctx
        ) as server_span:

            server_span.set_attribute(SpanAttributes.RPC_SERVICE, instance.name)
            self._generate_mcp_message_attrs(server_span, incoming_msg, request_id)

            try:
                result = await wrapped(*args, **kwargs)
                server_span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                server_span.set_status(Status(StatusCode.ERROR, str(e)))
                server_span.record_exception(e)
                raise

    @staticmethod
    def _generate_mcp_message_attrs(span: trace.Span, message, request_id: Optional[int]) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        """
        Populates the given span with MCP semantic convention attributes based on the message type.
        These semantic conventions are based off: https://github.com/open-telemetry/semantic-conventions/pull/2083
        which are currently in development and are considered unstable.

        Args:
            span: The MCP span to be enriched with MCP attributes
            message: The MCP message object, from client side it is of type ClientRequestModel/ClientNotificationModel and from server side it gets passed as type RootModel
            request_id: Unique identifier for the request or None if the message is a notification.
        """

        # Client-side request type will be ClientRequest which has root as field
        # Server-side: request type will be the root object passed from ClientRequest
        # See: https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/types.py#L1220
        if hasattr(message, "root"):
            message = message.root

        if request_id:
            span.set_attribute(MCPSpanAttributes.MCP_REQUEST_ID, request_id)

        span.set_attribute(MCPSpanAttributes.MCP_METHOD_NAME, message.method)

        if isinstance(message, types.CallToolRequest):
            tool_name = message.params.name
            span.update_name(f"{MCPMethodValue.TOOLS_CALL} {tool_name}")
            span.set_attribute(MCPSpanAttributes.MCP_TOOL_NAME, tool_name)
            if message.params.arguments:
                for arg_name, arg_val in message.params.arguments.items():
                    span.set_attribute(
                        f"{MCPSpanAttributes.MCP_REQUEST_ARGUMENT}.{arg_name}", McpInstrumentor.serialize(arg_val)
                    )
            return
        if isinstance(message, types.GetPromptRequest):
            prompt_name = message.params.name
            span.update_name(f"{MCPMethodValue.PROMPTS_GET} {prompt_name}")
            span.set_attribute(MCPSpanAttributes.MCP_PROMPT_NAME, prompt_name)
            return
        if isinstance(
            message,
            (
                types.ReadResourceRequest,
                types.SubscribeRequest,
                types.UnsubscribeRequest,
                types.ResourceUpdatedNotification,
            ),
        ):
            resource_uri = str(message.params.uri)
            span.update_name(f"{MCPSpanAttributes.MCP_RESOURCE_URI} {resource_uri}")
            span.set_attribute(MCPSpanAttributes.MCP_RESOURCE_URI, resource_uri)
            return

        span.update_name(message.method)

    @staticmethod
    def serialize(args: dict[str, Any]) -> str:
        try:
            return json.dumps(args)
        except Exception:
            return ""
