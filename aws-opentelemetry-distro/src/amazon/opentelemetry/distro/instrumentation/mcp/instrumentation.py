# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
import json
from typing import Any, AsyncGenerator, Callable, Collection, Dict, Optional, Tuple, cast

from wrapt import ObjectProxy, register_post_import_hook, wrap_function_wrapper

from opentelemetry import context, trace
from opentelemetry.trace.status import Status, StatusCode
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

    def __init__(self, **kwargs):
        super().__init__()
        self.propagators = kwargs.get("propagators") or get_global_textmap()
        self.tracer = trace.get_tracer(__name__, __version__, tracer_provider=kwargs.get("tracer_provider", None))

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("mcp >= 1.8.1",)

    def _instrument(self, **kwargs: Any) -> None:
        # TODO: add instrumentation for Streamable Http transport
        # See: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports

        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.shared.session",
                "BaseSession.send_request",
                self._wrap_session_send_request,
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

    def _wrap_session_send_request(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Callable:
        import mcp.types as types

        """ 
        Instruments MCP client-side request sending for both stdio and Streamable HTTP transport, 
        see: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports

        This is the master function responsible for sending requests from the client to the MCP server. 
        See:
        - https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/shared/session.py#L220
        - https://github.com/modelcontextprotocol/python-sdk/blob/e68e513b428243057f9c4693e10162eb3bb52897/src/mcp/client/session_group.py#L233

        The instrumented MCP client intercepts the request to obtain attributes for creating client-side span, extracts
        the current trace context, and embeds it into the request's params._meta field
        before forwarding the request to the MCP server.

       Args:
            wrapped: The original BaseSession.send_request method being instrumented
            instance: The BaseSession instance handling the stdio communication
            args: Positional arguments passed to the original send_request method, containing the ClientRequest
            kwargs: Keyword arguments passed to the original send_request method
        """

        async def async_wrapper():
            request: Optional[types.ClientRequest] = args[0] if len(args) > 0 else None

            if not request:
                return await wrapped(*args, **kwargs)

            request_id = None

            if hasattr(instance, "_request_id"):
                request_id = instance._request_id

            request_as_json = request.model_dump(by_alias=True, mode="json", exclude_none=True)

            if "params" not in request_as_json:
                request_as_json["params"] = {}
            if "_meta" not in request_as_json["params"]:
                request_as_json["params"]["_meta"] = {}

            with self.tracer.start_as_current_span("span.mcp.client", kind=trace.SpanKind.CLIENT) as client_span:

                span_ctx = trace.set_span_in_context(client_span)
                parent_span = {}
                self.propagators.inject(carrier=parent_span, context=span_ctx)

                McpInstrumentor._generate_mcp_span_attrs(client_span, request, request_id)
                request_as_json["params"]["_meta"].update(parent_span)

                # Reconstruct request object with injected trace context
                modified_request = request.model_validate(request_as_json)
                new_args = (modified_request,) + args[1:]

                try:
                    result = await wrapped(*new_args, **kwargs)
                    client_span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    client_span.set_status(Status(StatusCode.ERROR, str(e)))
                    client_span.record_exception(e)
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
        request_id = None
        carrier = {}

        if incoming_req and hasattr(incoming_req, "id"):
            request_id = incoming_req.id
        if incoming_req and hasattr(incoming_req, "params") and hasattr(incoming_req.params, "meta"):
            carrier = incoming_req.params.meta.model_dump()

        parent_ctx = self.propagators.extract(carrier=carrier)

        if parent_ctx:
            with self.tracer.start_as_current_span(
                "span.mcp.server", kind=trace.SpanKind.SERVER, context=parent_ctx
            ) as server_span:

                self._generate_mcp_span_attrs(server_span, incoming_req, request_id)

                try:
                    result = await wrapped(*args, **kwargs)
                    server_span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    server_span.set_status(Status(StatusCode.ERROR, str(e)))
                    server_span.record_exception(e)
                    raise

    @staticmethod
    def _generate_mcp_span_attrs(span: trace.Span, request, request_id: Optional[str]) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        # Client-side: request is of type ClientRequest which contains the Union of different RootModel types
        # Server-side: request is passed the RootModel
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
    def serialize(args):
        try:
            return json.dumps(args)
        except Exception:
            return str(args)
