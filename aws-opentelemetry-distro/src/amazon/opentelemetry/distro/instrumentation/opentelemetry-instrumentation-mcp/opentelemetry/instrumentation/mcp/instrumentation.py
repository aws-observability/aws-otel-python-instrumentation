# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Collection, Dict, Optional, Tuple

from wrapt import register_post_import_hook, wrap_function_wrapper

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.instrumentation.mcp.version import __version__
from opentelemetry.propagate import get_global_textmap

from .semconv import CLIENT_INITIALIZED, MCP_METHOD_NAME, TOOLS_CALL, TOOLS_LIST, MCPAttributes, MCPOperations, MCPSpanNames


class McpInstrumentor(BaseInstrumentor):
    """
    An instrumenter for MCP.
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
        
        Args:
            wrapped: The original BaseSession.send_request function
            instance: The BaseSession instance
            args: Positional arguments, where args[0] is typically the request object
            kwargs: Keyword arguments, may contain 'request' parameter
            
        Returns:
            Callable: Async wrapper function that handles trace context injection
        """

        async def async_wrapper():
            request: Optional[types.ClientRequest] = args[0] if len(args) > 0 else None

            if not request:
                return await wrapped(*args, **kwargs)

            with self.tracer.start_as_current_span(
                MCPSpanNames.CLIENT_SEND_REQUEST, kind=trace.SpanKind.CLIENT
            ) as span:
                
                if request:
                    span_ctx = trace.set_span_in_context(span)
                    parent_span = {}
                    self.propagators.inject(carrier=parent_span, context=span_ctx)
                    
                    request_data = request.model_dump(by_alias=True, mode="json", exclude_none=True)

                    if "params" not in request_data:
                        request_data["params"] = {}
                    if "_meta" not in request_data["params"]:
                        request_data["params"]["_meta"] = {}
                    request_data["params"]["_meta"].update(parent_span)

                    # Reconstruct request object with injected trace context
                    modified_request = request.model_validate(request_data)
                    new_args = (modified_request,) + args[1:]
                    
                    return await wrapped(*new_args, **kwargs)

        return async_wrapper

    # Handle Request Wrapper
    async def _wrap_handle_request(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Any:
        """
        Changes made:
        This wrapper intercepts requests before processing, extracts distributed tracing context from
        the request's params._meta field, and creates server-side OpenTelemetry spans linked to the client spans.
        The wrapper also does not change the original function's behavior by calling it with identical parameters
        ensuring no breaking changes to the MCP server functionality.

        request (args[1]) is typically an instance of CallToolRequest or ListToolsRequest
        and should have the structure:
        request.params.meta.traceparent -> "00-<trace_id>-<span_id>-01"
        """
        req = args[1] if len(args) > 1 else None
        traceparent = None

        if req and hasattr(req, "params") and req.params and hasattr(req.params, "meta") and req.params.meta:
            traceparent = None
        span_context = self._extract_span_context_from_traceparent(traceparent) if traceparent else None
        if span_context:
            span_name = self._get_mcp_operation(req)
            with self.tracer.start_as_current_span(
                span_name,
                kind=trace.SpanKind.SERVER,
                context=trace.set_span_in_context(trace.NonRecordingSpan(span_context)),
            ) as span:
                self._generate_mcp_attributes(span, req, False)
                result = await wrapped(*args, **kwargs)
                return result
        else:
            return await wrapped(*args, **kwargs)

    @staticmethod
    def _generate_mcp_attributes(span: trace.Span, request: Any, is_client: bool) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        if isinstance(request, types.ListToolsRequest):
            span.set_attribute(MCP_METHOD_NAME, TOOLS_LIST)
            if is_client:
                span.update_name(MCPSpanNames.CLIENT_LIST_TOOLS)
        elif isinstance(request, types.CallToolRequest):
            span.set_attribute(MCP_METHOD_NAME, TOOLS_CALL)
            if is_client:
                span.update_name(MCPSpanNames.client_call_tool(request.params.name))
        elif isinstance(request, types.InitializeRequest):
            span.set_attribute(MCP_METHOD_NAME, CLIENT_INITIALIZED)

        # Additional attributes can be added here if needed

    @staticmethod
    def _extract_span_context_from_traceparent(traceparent: str):
        parts = traceparent.split("-")
        if len(parts) == 4:
            try:
                trace_id = int(parts[1], 16)
                span_id = int(parts[2], 16)
                return trace.SpanContext(
                    trace_id=trace_id,
                    span_id=span_id,
                    is_remote=True,
                    trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
                    trace_state=trace.TraceState(),
                )
            except ValueError:
                return None
        return None

    @staticmethod
    def _get_mcp_operation(req: Any) -> str:

        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        span_name = "unknown"

        if isinstance(req, types.ListToolsRequest):
            span_name = MCPSpanNames.TOOLS_LIST
        elif isinstance(req, types.CallToolRequest):
            span_name = MCPSpanNames.tools_call(req.params.name)
        return span_name
