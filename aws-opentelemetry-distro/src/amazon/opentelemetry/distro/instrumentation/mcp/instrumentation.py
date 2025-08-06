# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, AsyncGenerator, Callable, Collection, Dict, Optional, Tuple, cast

from wrapt import register_post_import_hook, wrap_function_wrapper

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.propagate import get_global_textmap

from .version import __version__

from .semconv import (
    CLIENT_INITIALIZED,
    MCP_METHOD_NAME,
    TOOLS_CALL,
    TOOLS_LIST,
    MCPAttributes,
    MCPOperations,
    MCPSpanNames,
)


class McpInstrumentor(BaseInstrumentor):
    """
    An instrumentor class for MCP.
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.propagators = kwargs.get("propagators") or get_global_textmap()
        self.tracer = trace.get_tracer(__name__, __version__, tracer_provider=kwargs.get("tracer_provider", None))

    def instrumentation_dependencies(self) -> Collection[str]:
        return "mcp >= 1.6.0"

    def _instrument(self, **kwargs: Any) -> None:

        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.shared.session",
                "BaseSession.send_request",
                self._wrap_send_request,
            ),
            "mcp.shared.session",
        )
        register_post_import_hook(
            lambda _: wrap_function_wrapper(
                "mcp.server.lowlevel.server",
                "Server._handle_request",
                self._wrap_handle_request,
            ),
            "mcp.server.lowlevel.server",
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap("mcp.shared.session", "BaseSession.send_request")
        unwrap("mcp.server.lowlevel.server", "Server._handle_request")

    def _wrap_send_request(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Callable:
        import mcp.types as types

        """ 
        Patches BaseSession.send_request which is responsible for sending requests from the client to the MCP server.
        This patched MCP client intercepts the request to obtain attributes for creating client-side span, extracts
        the current trace context, and embeds it into the request's params._meta.traceparent field
        before forwarding the request to the MCP server.
        """

        async def async_wrapper():
            request: Optional[types.ClientRequest] = args[0] if len(args) > 0 else None

            if not request:
                return await wrapped(*args, **kwargs)

            request_as_json = request.model_dump(by_alias=True, mode="json", exclude_none=True)

            if "params" not in request_as_json:
                request_as_json["params"] = {}

            if "_meta" not in request_as_json["params"]:
                request_as_json["params"]["_meta"] = {}

            with self.tracer.start_as_current_span(
                MCPSpanNames.SPAN_MCP_CLIENT, kind=trace.SpanKind.CLIENT
            ) as mcp_client_span:

                if request:
                    span_ctx = trace.set_span_in_context(mcp_client_span)
                    parent_span = {}
                    self.propagators.inject(carrier=parent_span, context=span_ctx)

                    McpInstrumentor._set_mcp_client_attributes(mcp_client_span, request)

                    request_as_json["params"]["_meta"].update(parent_span)

                    # Reconstruct request object with injected trace context
                    modified_request = request.model_validate(request_as_json)
                    new_args = (modified_request,) + args[1:]

                    return await wrapped(*new_args, **kwargs)

        return async_wrapper

    # Handle Request Wrapper
    async def _wrap_handle_request(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Any:
        """
        Patches Server._handle_request which is responsible for processing requests on the MCP server.
        This patched MCP server intercepts incoming requests to extract tracing context from
        the request's params._meta field and creates server-side spans linked to the client spans.
        """
        req = args[1] if len(args) > 1 else None
        carrier = {}

        if req and hasattr(req, "params") and req.params and hasattr(req.params, "meta") and req.params.meta:
            carrier = req.params.meta.__dict__

        parent_ctx = self.propagators.extract(carrier=carrier)

        if parent_ctx:
            with self.tracer.start_as_current_span(
                MCPSpanNames.SPAN_MCP_SERVER, kind=trace.SpanKind.SERVER, context=parent_ctx
            ) as mcp_server_span:
                self._set_mcp_server_attributes(mcp_server_span, req)

            return await wrapped(*args, **kwargs)

    @staticmethod
    def _set_mcp_client_attributes(span: trace.Span, request: Any) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        if isinstance(request, types.ListToolsRequest):
            span.set_attribute(MCP_METHOD_NAME, TOOLS_LIST)
        if isinstance(request, types.CallToolRequest):
            tool_name = request.params.name
            span.update_name(f"{TOOLS_CALL} {tool_name}")
            span.set_attribute(MCP_METHOD_NAME, TOOLS_CALL)
            span.set_attribute(MCPAttributes.MCP_TOOL_NAME, tool_name)
        if isinstance(request, types.InitializeRequest):
            span.set_attribute(MCP_METHOD_NAME, CLIENT_INITIALIZED)

    @staticmethod
    def _set_mcp_server_attributes(span: trace.Span, request: Any) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        if isinstance(span, types.ListToolsRequest):
            span.set_attribute(MCP_METHOD_NAME, TOOLS_LIST)
        if isinstance(span, types.CallToolRequest):
            tool_name = request.params.name
            span.update_name(f"{TOOLS_CALL} {tool_name}")
            span.set_attribute(MCP_METHOD_NAME, TOOLS_CALL)
            span.set_attribute(MCPAttributes.MCP_TOOL_NAME, tool_name)
