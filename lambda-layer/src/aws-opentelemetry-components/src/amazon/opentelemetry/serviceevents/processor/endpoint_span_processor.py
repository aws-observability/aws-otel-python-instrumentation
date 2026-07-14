# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Framework-agnostic endpoint span processor for ServiceEvents.

This is the Python port of the Java agent's ``ServiceEventsSpanProcessor``. Instead of
installing per-framework request hooks (Flask/FastAPI/Django), it reads the request-boundary
span that OpenTelemetry's own instrumentation already produces and derives the same endpoint
metric + incident telemetry from span attributes alone. Any framework OTel instruments for
free (Starlette, Tornado, aiohttp, ...) is then covered without bespoke hook code.

Mapping to the Java design (``ServiceEventsSpanProcessor.java``):

* ``on_start`` fires the *begin signal* (``begin_investigation``). Java's ``onStart`` is a
  no-op because its bytecode servlet advice seeds ``InvestigationData``; Python has no such
  advice, so the processor itself seeds the per-request investigation context here. This is
  mandatory for exception attribution on handler-swallowed 5xx (FastAPI/DRF global handlers
  convert an exception to a 500 *before* the span records an ``exception`` event), where the
  AST function-monitor's captured call-path is the only surviving record of the error.
* ``on_end`` filters to the request boundary (SERVER or local-root), derives the operation
  with the SHARED App Signals ``get_ingress_operation`` (span-name primary, first-path-segment
  fallback) — byte-identical to Java line 202 and to the per-framework hooks for matched
  routes — then drives the unchanged ``EndpointMetricCollector`` and ``IncidentSnapshotCollector``.

The collectors are reused verbatim: both rebuild ``operation = f"{method} {route}"`` internally
and hold the fault-only (``status >= 500 && error_info``) breakdown gate, so this processor
passes scalar ``route``/``method`` and lets that shared layer do the rest.
"""

import logging
import re
from typing import Optional

from amazon.opentelemetry.application_signals.internal.aws_span_processing_util import (
    INTERNAL_OPERATION,
    UNKNOWN_OPERATION,
    get_ingress_operation,
    is_local_root,
)
from amazon.opentelemetry.serviceevents.python_monitor import (
    _ServiceEventsMonitorState,
    clear_current_operation,
    set_current_operation,
)
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

logger = logging.getLogger(__name__)


def _is_request_boundary(span) -> bool:
    """True for the span that delimits an inbound request.

    Matches Java's filter (``getKind() != SERVER && !isLocalRoot`` → skip): a SERVER span,
    or any local-root span (parent is absent or remote). Internal/CLIENT/DB child spans —
    the bulk of spans per request — are excluded.
    """
    return span.kind == SpanKind.SERVER or is_local_root(span)


def _get_http_method(span: ReadableSpan) -> Optional[str]:
    """HTTP method from the span: stable ``http.request.method`` first, legacy ``http.method``."""
    attributes = span.attributes or {}
    method = attributes.get(SpanAttributes.HTTP_REQUEST_METHOD)
    if method is None:
        method = attributes.get(SpanAttributes.HTTP_METHOD)
    return method


_PYTHON_FRAME_RE = re.compile(r'^\s*File\s+"[^"]*",\s+line\s+\d+,\s+in\s+(\S+)', re.MULTILINE)


def _extract_function_from_stacktrace(stacktrace: Optional[str]) -> str:
    """Extract the origin function name from a Python traceback string.

    Python tracebacks list the throw site LAST (most recent call last), so we take the last
    ``in <function_name>`` match. Returns "unknown" if unparsable — matching Java's fallback.
    """
    if not stacktrace:
        return "unknown"
    matches = _PYTHON_FRAME_RE.findall(stacktrace)
    if matches:
        return matches[-1]
    return "unknown"


def _exception_from_span_event(span: ReadableSpan) -> Optional[dict]:
    """Recover an exception from the span's own OTel ``exception`` event.

    The AST function-monitor only captures an exception when the throw unwinds through an
    instrumented frame. A 5xx raised in uninstrumented library code, or swallowed by a framework
    global handler that converts it to a 500 *before* it reaches any instrumented frame, leaves the
    investigation data empty — yet OTel's own instrumentation still records an ``exception`` event
    on the span (``span.record_exception``). Java's ``ServiceEventsSpanProcessor`` reads that event
    as its exception source; this is the Python equivalent.

    Returns a dict shaped like the monitor's captured exception (``name``/``message``/
    ``traceback_info``/``function_name``) so it can seed the investigation data and flow through the
    unchanged breakdown + snapshot recovery paths, or None when the span has no exception event.
    """
    events = getattr(span, "events", None)
    if not events:
        return None
    # First exception event wins — the original root-cause exception, matching Java.
    for event in events:
        if getattr(event, "name", None) != "exception":
            continue
        attributes = event.attributes or {}
        exc_type = attributes.get(SpanAttributes.EXCEPTION_TYPE)
        if not exc_type:
            continue
        stacktrace = attributes.get(SpanAttributes.EXCEPTION_STACKTRACE) or ""
        return {
            "name": exc_type,
            "message": attributes.get(SpanAttributes.EXCEPTION_MESSAGE) or "",
            "traceback_info": stacktrace,
            "function_name": _extract_function_from_stacktrace(stacktrace),
        }
    return None


def _extract_error_from_call_path(route, method) -> Optional[dict]:
    """Extract the primary error type + origin function from the monitor's investigation data.

    Reads (PEEKS — does not clear) the per-request investigation data the AST monitor accumulates,
    so the data remains available for the incident snapshot collector. Returns ``{error_type,
    function_name}`` or None when no error type was captured.

    Returns None when no real error type was captured — neither a monitor-recorded exception nor a
    span-seeded one — so callers omit the error breakdown entirely, matching Java (whose gate is
    ``statusCode >= 500 && errorType != null``). A 5xx with no captured exception (e.g. a handler
    that returns a 500 status without raising) must NOT synthesize an "UnknownError" breakdown entry.

    ``route``/``method`` are accepted for call-site symmetry with the collectors but are not used:
    the error type/origin come entirely from the investigation data.
    """
    inv_data = _ServiceEventsMonitorState.get_instance().peek_investigation_data()
    exc_data = inv_data.get("exception") if inv_data else None
    if isinstance(exc_data, dict) and exc_data.get("name"):
        error_type = exc_data["name"]
    else:
        return None

    # Find the origin function. Prefer the function the monitor recorded as the actual thrower;
    # call_path[0] is the innermost frame entered, which is not necessarily where the exception was
    # raised, so the captured exception origin is authoritative when present.
    function_name = "unknown"
    if isinstance(exc_data, dict) and exc_data.get("function_name"):
        function_name = exc_data["function_name"]
    elif inv_data and inv_data.get("call_path"):
        call_path = inv_data["call_path"]
        if call_path:
            first_entry = call_path[0]
            if isinstance(first_entry, dict):
                function_name = first_entry.get("function_name", "unknown")

    return {"error_type": error_type, "function_name": function_name}


def _get_status_code(span: ReadableSpan) -> int:
    """HTTP status from the span: stable ``http.response.status_code`` first, legacy ``http.status_code``.

    Returns 0 when neither is present (mirrors Java's ``statusCode = 0`` default), which the
    collectors treat as a non-fault, non-error.
    """
    attributes = span.attributes or {}
    status = attributes.get(SpanAttributes.HTTP_RESPONSE_STATUS_CODE)
    if status is None:
        status = attributes.get(SpanAttributes.HTTP_STATUS_CODE)
    try:
        return int(status) if status is not None else 0
    except (TypeError, ValueError):
        return 0


class ServiceEventsSpanProcessor(SpanProcessor):
    """SpanProcessor that produces ServiceEvents endpoint + incident telemetry from spans.

    Holds references to the same two collectors the per-framework hooks use, plus the
    ServiceEvents config (for endpoint include/exclude filtering). Crash-safe by contract:
    a telemetry failure must never disrupt application tracing, so ``on_start``/``on_end``
    swallow every exception.
    """

    def __init__(self, endpoint_collector, incident_snapshot_collector, config):
        self._endpoint_collector = endpoint_collector
        self._incident_snapshot_collector = incident_snapshot_collector
        self._config = config

    # -- SpanProcessor interface ------------------------------------------------------------

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:  # pylint: disable=no-self-use
        """Begin investigation for the request-boundary span only.

        This is the generic begin hook (the Python analogue of Java's bytecode servlet
        advice). Gated to the request boundary because ``begin_investigation`` resets the
        per-request call-path: firing it on a nested child span would wipe the call-path the
        AST monitor is accumulating for exception attribution.
        """
        try:
            if not _is_request_boundary(span):
                return
            _ServiceEventsMonitorState.get_instance().begin_investigation()
        except Exception:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
            logger.warning("ServiceEvents span processor on_start failed", exc_info=True)

    def on_end(self, span: ReadableSpan) -> None:
        """Record endpoint metric + potential incident from the request-boundary span."""
        try:
            if not _is_request_boundary(span):
                return
            # The request is ending. Whatever happens below (including the early returns),
            # clear the per-request context so it can't leak onto the next request that
            # reuses this worker thread — mirrors Java onEnd's finally block.
            try:
                self._process_request_span(span)
            finally:
                clear_current_operation()
                _ServiceEventsMonitorState.get_instance().clear_investigation_data()
        except Exception:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
            logger.warning("ServiceEvents span processor on_end failed", exc_info=True)

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=unused-argument,no-self-use
        return True

    def shutdown(self) -> None:  # pylint: disable=no-self-use
        return None

    # -- internals --------------------------------------------------------------------------

    @staticmethod
    def _route_from_operation(operation: Optional[str], method: str) -> Optional[str]:
        """Back the route out of the App Signals operation so the unchanged collector
        rebuilds the identical ``f"{method} {route}"`` operation.

        Handles the three shapes ``get_ingress_operation`` can return for a request boundary:

        * ``"{method} {route}"`` — the common case (span name is the HTTP semconv name, or the
          first-segment generator prepended the method). Strip the ``"{method} "`` prefix.
        * ``"{route}"`` — a bare path with no method prefix. ``_generate_ingress_operation``
          only prepends the method from the *legacy* ``http.method`` key, so a span carrying
          only the *stable* ``http.request.method`` plus an unmatched path yields a bare
          ``/segment``. Use it verbatim as the route (the collector re-adds the method).
        * ``InternalOperation`` / ``UnknownOperation`` / ``<lambda>/FunctionHandler`` / a bare
          method — no resolvable route. Return None so the caller skips, matching Java.
        """
        if not operation:
            return None
        if operation in (INTERNAL_OPERATION, UNKNOWN_OPERATION):
            return None
        if operation == method:
            # span name was just the bare HTTP method — no route.
            return None
        prefix = f"{method} "
        if operation.startswith(prefix):
            route = operation[len(prefix) :]
            return route or None
        if operation.startswith("/"):
            # Bare path (stable-method + unmatched-path case): the collector re-prepends method.
            return operation
        # Anything else (e.g. a lambda "name/FunctionHandler") is not an HTTP route.
        return None

    @staticmethod
    def _seed_exception_from_span(span: ReadableSpan) -> None:
        """Seed the span's recorded exception into the investigation data (first-writer-wins).

        Only fills the exception when the AST monitor captured none, so an instrumented throw's
        origin function (which the span event lacks) is always preferred. No-ops when the span has
        no exception event or no investigation context exists.
        """
        monitor_state = _ServiceEventsMonitorState.get_instance()
        inv_data = monitor_state.peek_investigation_data()
        if inv_data is None or inv_data.get("exception") is not None:
            return
        span_exception = _exception_from_span_event(span)
        if span_exception is not None:
            inv_data["exception"] = span_exception

    def _process_request_span(self, span: ReadableSpan) -> None:  # pylint: disable=too-many-locals
        method = _get_http_method(span)
        if not method:
            # No HTTP method → not an inbound HTTP request boundary (e.g. a messaging
            # consumer local-root). Java skips these too (method == null early return).
            return

        # Derive the operation exactly as Application Signals / the Java processor do:
        # span name when valid, else "{method} {first-path-segment}". This is the single
        # source of truth — we back the route out of it so the unchanged collectors rebuild
        # the identical operation string (verified equal to the per-framework hooks for
        # matched routes, and to App Signals' first-segment collapse for unmatched 404s).
        operation = get_ingress_operation(None, span)
        route = self._route_from_operation(operation, method)
        if not route:
            # operation is InternalOperation / UnknownOperation / a lambda handler / a bare
            # method — no resolvable HTTP route on this span. Java skips it (route == null).
            return

        # Apply the user-configured endpoint include/exclude filters before recording —
        # same gate the per-framework hooks and Java's EndpointFilter apply.
        if self._config and not self._config.should_track_endpoint(route, method):
            return

        status_code = _get_status_code(span)

        start_ns = span.start_time or 0
        end_ns = span.end_time or start_ns
        duration_ns = max(0, end_ns - start_ns)
        duration_ms = duration_ns / 1_000_000.0

        # Trace correlation is best-effort and sampling-conditional. Under reduced sampling,
        # AlwaysRecordSampler keeps this span recording (so App Signals metrics see every request)
        # even though its trace was dropped before export — a RECORD_ONLY span whose context is
        # valid but whose SAMPLED flag is unset. Capturing its ids would emit a correlation link to
        # a trace the backend never received. Gate on the real SAMPLED flag in addition to validity
        # so only sampled (backend-present) traces are correlated; an unsampled request still emits a
        # complete (self-contained) IncidentSnapshot, just without trace_id/span_id.
        span_context = span.get_span_context()
        sampled = bool(span_context and span_context.is_valid and span_context.trace_flags.sampled)
        trace_id = span_context.trace_id if sampled else None
        span_id = span_context.span_id if sampled else None

        # Fault recovery from the span's own exception event. When a 5xx unwound through code the
        # AST monitor never instrumented (library internals, or a global handler that converted the
        # error to a 500 before any instrumented frame saw it), the investigation data holds no
        # exception. OTel still recorded an `exception` event on the span, so seed it into the
        # investigation data here — first-writer-wins, so a real AST-captured exception is never
        # overwritten. Both the breakdown and the snapshot recover the exception from that same
        # investigation data, exactly as they do for an instrumented throw, matching Java which
        # reads the span event directly.
        if status_code >= 500:
            self._seed_exception_from_span(span)

        # Error info from the AST monitor's captured call-path (now also seeded from the span event
        # for uninstrumented faults). _extract_error_from_call_path PEEKS (does not clear) so the
        # incident collector can still consume the investigation data below. Like Java, the original
        # exception object is gone by span end; the captured type/origin live in the investigation
        # data.
        error_info = None
        if status_code >= 400:
            error_info = _extract_error_from_call_path(route, method)

        # 1. Endpoint metric.
        if self._endpoint_collector:
            try:
                self._endpoint_collector.record_request(
                    route=route,
                    method=method,
                    status_code=status_code,
                    duration_ns=duration_ns,
                    error_info=error_info,
                )
            except Exception:  # pylint: disable=broad-exception-caught  # best-effort telemetry
                logger.warning("ServiceEvents record_request failed", exc_info=True)

        # Set current operation before the incident path so exemplar correlation matches
        # the recorded aggregation key (mirrors Java onEnd line 241).
        set_current_operation(operation)

        # 2. Potential incident. exception=None: the trigger gate uses status_code >= 500,
        # and exception detail is recovered from investigation data inside the collector
        # (process_potential_incident → _collect_exception_info), exactly as Java does.
        if self._incident_snapshot_collector:
            try:
                exemplar = self._incident_snapshot_collector.process_potential_incident(
                    route=route,
                    method=method,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    exception=None,
                    # trace_id/span_id are already gated on the real SAMPLED flag above (fix #1):
                    # set iff the trace was sampled, else None. The collector uses them directly and
                    # never re-derives correlation from headers or the current span, so no request
                    # headers are threaded here.
                    request_data={
                        "trace_id": trace_id,
                        "span_id": span_id,
                    },
                )
                if exemplar and self._endpoint_collector:
                    self._endpoint_collector.record_incident_exemplar(exemplar["operation"], exemplar)
            except Exception:  # pylint: disable=broad-exception-caught  # best-effort telemetry
                logger.warning("ServiceEvents process_potential_incident failed", exc_info=True)
