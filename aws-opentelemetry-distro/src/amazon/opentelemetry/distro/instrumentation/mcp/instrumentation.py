# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
import json
from typing import Any, AsyncGenerator, Callable, Collection, Dict, Optional, Tuple, cast

from wrapt import ObjectProxy, register_post_import_hook, wrap_function_wrapper

from opentelemetry import context, trace
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
    _DEFAULT_CLIENT_SPAN_NAME = "span.mcp.client"
    _DEFAULT_SERVER_SPAN_NAME = "span.mcp.server"

    """
    An instrumentation class for MCP: https://modelcontextprotocol.io/overview
    """

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

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap("mcp.shared.session", "BaseSession.send_request")
        unwrap("mcp.server.lowlevel.server", "Server._handle_request")

    def _wrap_session_send(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Callable:
        import mcp.types as types

        async def async_wrapper():
            message = args[0] if len(args) > 0 else None
            if not message:
                return await wrapped(*args, **kwargs)

            is_client = isinstance(message, (types.ClientRequest, types.ClientNotification))
            request_id: Optional[int] = getattr(instance, "_request_id", None)
            span_name = self._DEFAULT_SERVER_SPAN_NAME
            span_kind = SpanKind.SERVER

            if is_client:
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

                McpInstrumentor._generate_mcp_req_attrs(span, message, request_id)

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

        The instrumented MCP server intercepts incoming requests to extract tracing context from
        the request's params._meta field, creates server-side spans linked to the originating client spans,
        and processes the request while maintaining trace continuity.

        Args:
            wrapped: The original Server._handle_request method being instrumented
            instance: The MCP Server instance processing the stdio communication
            args: Positional arguments passed to the original _handle_request method, containing the incoming request
            kwargs: Keyword arguments passed to the original _handle_request method
        """
        incoming_req = args[1] if len(args) > 1 else None

        if not incoming_req:
            return await wrapped(*args, **kwargs)

        request_id = None
        carrier = {}

        if hasattr(incoming_req, "id") and incoming_req.id:
            request_id = incoming_req.id
        if hasattr(incoming_req, "params") and hasattr(incoming_req.params, "meta") and incoming_req.meta:
            carrier = incoming_req.params.meta.model_dump()

        # If MCP client is instrumented then params._meta field will contain the
        # parent trace context.
        parent_ctx = self.propagators.extract(carrier=carrier)

        with self.tracer.start_as_current_span(
            "span.mcp.server", kind=trace.SpanKind.SERVER, context=parent_ctx
        ) as server_span:

            self._generate_mcp_req_attrs(server_span, incoming_req, request_id)

            try:
                result = await wrapped(*args, **kwargs)
                server_span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                server_span.set_status(Status(StatusCode.ERROR, str(e)))
                server_span.record_exception(e)
                raise

    @staticmethod
    def _generate_mcp_req_attrs(span: trace.Span, request, request_id: Optional[int]) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        """
        Populates the given span with MCP semantic convention attributes based on the request type.
        These semantic conventions are based off: https://github.com/open-telemetry/semantic-conventions/pull/2083
        which are currently in development and are considered unstable.

        Args:
            span: The MCP span to be enriched with MCP attributes
            request: The MCP request object, from Client Side it is of type ClientRequestModel and from server side it's of type RootModel
            request_id: Unique identifier for the request. In theory, this should never be Optional since all requests made from MCP client to server will contain a request id.
        """

        # Client-side request type will be ClientRequest which has root as field
        # Server-side: request type will be the root object passed from ClientRequest
        # See: https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/types.py#L1220
        if hasattr(request, "root"):
            request = request.root

        if request_id:
            span.set_attribute(MCPSpanAttributes.MCP_REQUEST_ID, request_id)

        span.set_attribute(MCPSpanAttributes.MCP_METHOD_NAME, request.method)

        if isinstance(request, types.CallToolRequest):
            tool_name = request.params.name
            span.update_name(f"{MCPMethodValue.TOOLS_CALL} {tool_name}")
            span.set_attribute(MCPSpanAttributes.MCP_TOOL_NAME, tool_name)
            request.params.arguments
            if request.params.arguments:
                for arg_name, arg_val in request.params.arguments.items():
                    span.set_attribute(
                        f"{MCPSpanAttributes.MCP_REQUEST_ARGUMENT}.{arg_name}", McpInstrumentor.serialize(arg_val)
                    )
            return
        if isinstance(request, types.GetPromptRequest):
            prompt_name = request.params.name
            span.update_name(f"{MCPMethodValue.PROMPTS_GET} {prompt_name}")
            span.set_attribute(MCPSpanAttributes.MCP_PROMPT_NAME, prompt_name)
            return
        if isinstance(request, (types.ReadResourceRequest, types.SubscribeRequest, types.UnsubscribeRequest)):
            resource_uri = str(request.params.uri)
            span.update_name(f"{MCPSpanAttributes.MCP_RESOURCE_URI} {resource_uri}")
            span.set_attribute(MCPSpanAttributes.MCP_RESOURCE_URI, resource_uri)
            return

        span.update_name(request.method)

    @staticmethod
    def serialize(args: dict[str, Any]) -> str:
        try:
            return json.dumps(args)
        except Exception:
            return ""
