# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Collection, Dict, Tuple

from mcp import ClientRequest
from wrapt import register_post_import_hook, wrap_function_wrapper

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap

_instruments = ("mcp >= 1.6.0",)

class MCPInstrumentor(BaseInstrumentor):
    """
    An instrumenter for MCP.
    """

    def __init__(self):
        super().__init__()
        self.tracer = None

    @staticmethod
    def instrumentation_dependencies() -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        if tracer_provider:
            self.tracer = tracer_provider.get_tracer("mcp")
        else:
            self.tracer = trace.get_tracer("mcp")
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

    @staticmethod
    def _uninstrument(**kwargs: Any) -> None:
        unwrap("mcp.shared.session", "BaseSession.send_request")
        unwrap("mcp.server.lowlevel.server", "Server._handle_request")

    # Send Request Wrapper
    def _wrap_send_request(
        self, wrapped: Callable, instance: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Callable:
        """
        Changes made:
            The wrapper intercepts the request before sending, injects distributed tracing context into the
            request's params._meta field and creates OpenTelemetry spans. The wrapper does not change anything
            else from the original function's behavior because it reconstructs the request object with the same
            type and calling the original function with identical parameters.
        """

        async def async_wrapper():
            with self.tracer.start_as_current_span("client.send_request", kind=trace.SpanKind.CLIENT) as span:
                span_ctx = span.get_span_context()
                request = args[0] if len(args) > 0 else kwargs.get("request")
                if request:
                    req_root = request.root if hasattr(request, "root") else request

                    self._generate_mcp_attributes(span, req_root, is_client=True)
                    request_data = request.model_dump(by_alias=True, mode="json", exclude_none=True)
                    self._inject_trace_context(request_data, span_ctx)
                    # Reconstruct request object with injected trace context
                    modified_request = type(request).model_validate(request_data)
                    if len(args) > 0:
                        new_args = (modified_request,) + args[1:]
                        result = await wrapped(*new_args, **kwargs)
                    else:
                        kwargs["request"] = modified_request
                        result = await wrapped(*args, **kwargs)
                else:
                    result = await wrapped(*args, **kwargs)
                return result

        return async_wrapper()

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
            traceparent = getattr(req.params.meta, "traceparent", None)
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

    def _generate_mcp_attributes(self, span: trace.Span, request: ClientRequest, is_client: bool) -> None:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        operation = "UnknownOperation"
        if isinstance(request, types.ListToolsRequest):
            operation = "ListTool"
            span.set_attribute("mcp.list_tools", True)
        elif isinstance(request, types.CallToolRequest):
            operation = request.params.name
            span.set_attribute("mcp.call_tool", True)
        elif isinstance(request, types.InitializeRequest):
            operation = "Initialize"
            span.set_attribute("mcp.initialize", True)
        if is_client:
            self._add_client_attributes(span, operation, request)
        else:
            self._add_server_attributes(span, operation, request)

    @staticmethod
    def _inject_trace_context(request_data: Dict[str, Any], span_ctx) -> None:
        if "params" not in request_data:
            request_data["params"] = {}
        if "_meta" not in request_data["params"]:
            request_data["params"]["_meta"] = {}
        trace_id_hex = f"{span_ctx.trace_id:032x}"
        span_id_hex = f"{span_ctx.span_id:016x}"
        trace_flags = "01"
        traceparent = f"00-{trace_id_hex}-{span_id_hex}-{trace_flags}"
        request_data["params"]["_meta"]["traceparent"] = traceparent

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
    def _get_mcp_operation(req: ClientRequest) -> str:
        import mcp.types as types  # pylint: disable=import-outside-toplevel,consider-using-from-import

        span_name = "unknown"

        if isinstance(req, types.ListToolsRequest):
            span_name = "tools/list"
        elif isinstance(req, types.CallToolRequest):
            span_name = f"tools/{req.params.name}"
        elif isinstance(req, types.InitializeRequest):
            span_name = "tools/initialize"
        return span_name

    @staticmethod
    def _add_client_attributes(span: trace.Span, operation: str, request: ClientRequest) -> None:
        import os  # pylint: disable=import-outside-toplevel

        service_name = os.environ.get("MCP_INSTRUMENTATION_SERVER_NAME", "mcp server")
        span.set_attribute("aws.remote.service", service_name)
        span.set_attribute("aws.remote.operation", operation)
        if hasattr(request, "params") and request.params and hasattr(request.params, "name"):
            span.set_attribute("tool.name", request.params.name)

    @staticmethod
    def _add_server_attributes(span: trace.Span, operation: str, request: ClientRequest) -> None:
        if hasattr(request, "params") and request.params and hasattr(request.params, "name"):
            span.set_attribute("tool.name", request.params.name)
