# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
FastAPI instrumentation for ServiceEvents EndpointMetric and IncidentSnapshot events.

This module installs hooks into FastAPI applications to:
- Track endpoint metrics (requests, duration, status codes)
- Trigger incident snapshots on errors or slow requests
- Propagate endpoint context to function monitors

Note: FastAPI is async/ASGI-based, so this uses middleware instead of decorators.
"""

import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qs

from amazon.opentelemetry.serviceevents.instrumentation._constants import unmatched_route_label
from amazon.opentelemetry.serviceevents.instrumentation.flask_instrumentation import (
    _capture_active_trace_context,
    _extract_error_from_call_path,
)
from amazon.opentelemetry.serviceevents.python_monitor import (
    _ServiceEventsMonitorState,
    clear_current_operation,
    set_current_operation,
)

logger = logging.getLogger(__name__)

# Global reference to collectors and config (set during initialization)
_endpoint_collector = None
_incident_snapshot_collector = None
_serviceevents_config = None


async def _get_request_body(request) -> Optional[Any]:
    """
    Safely extract request body from FastAPI request with fallback chain.

    Note: This is async because FastAPI body reading is async.
    The body is cached after first read, so this is safe to call multiple times.

    Args:
        request: FastAPI Request object

    Returns:
        Request body as JSON dict, form dict, raw string, or None
    """
    try:
        # Try JSON first (most common for APIs)
        body = await request.json()
        if body is not None:
            return body
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        # Try form data
        form = await request.form()
        if form:
            return dict(form)
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        # Try raw data (with 10KB limit)
        data = await request.body()
        if data:
            if len(data) <= 10240:  # 10KB limit
                return data.decode("utf-8", errors="replace")
            return f"<payload too large: {len(data)} bytes>"
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    return None


def _get_route_pattern(scope) -> str:
    """
    Get the route pattern (e.g. /users/{id}) from the ASGI scope.

    Args:
        scope: ASGI scope dictionary

    Returns:
        Route pattern string (e.g., "/api/users/{id}")
    """
    # Try to get the route pattern from the scope
    route = scope.get("route")
    if route and hasattr(route, "path"):
        return route.path  # e.g., "/api/users/{id}"

    # Not yet routed: try to pre-resolve the template against the app's routes.
    resolved = _resolve_route_template(scope)
    if resolved is not None:
        return resolved

    # No route matched (404 / scanner traffic). Collapse to the first path segment rather
    # than the raw path so probed URLs can't explode metric cardinality. Matches
    # Flask/Django and Application Signals' unmatched-route handling.
    return unmatched_route_label(scope.get("path"))


def _resolve_route_template(scope) -> Optional[str]:
    """Pre-resolve the matching route template before Starlette routing runs.

    At middleware entry ``scope["route"]`` is unset (routing happens inside the inner
    app), so the raw URL path is all that's available. But the per-request *operation*
    (used for incident correlation) and the endpoint aggregation are both keyed on the
    route *template* (e.g. ``/users/{id}``). Without resolving the template here, every
    distinct param value (``/users/1``, ``/users/2``) would be a different operation, so
    incident correlation and endpoint aggregation would shatter into per-value cardinality.
    (Sampling does not depend on the operation: the default ``always`` mode samples every
    call, and ``auto`` keys its tier counters on the function, not the route.)

    Starlette sets ``scope["app"]`` before the middleware stack runs, so its routes are
    available. We match the scope against them (the same matching the router does moments
    later) and return the matched template, or None if nothing matches / anything goes
    wrong. Crash-safety: telemetry must never break the request, so this is fully guarded.
    """
    try:
        app = scope.get("app")
        routes = getattr(app, "routes", None)
        if not routes:
            return None
        # Lazy import: Starlette is an optional dependency, resolved on the request path.
        from starlette.routing import Match  # pylint: disable=import-outside-toplevel

        for route in routes:
            matches = getattr(route, "matches", None)
            path = getattr(route, "path", None)
            if matches is None or path is None:
                continue
            match, _child_scope = matches(scope)
            if match == Match.FULL:
                return path
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    return None


def _parse_query_string(query_string: bytes) -> Dict:
    """
    Parse ASGI query string bytes into a dictionary.

    Args:
        query_string: Raw query string bytes from scope

    Returns:
        Dictionary of query parameters
    """
    if not query_string:
        return {}

    try:
        # parse_qs returns lists as values, flatten to single values
        parsed = parse_qs(query_string.decode("utf-8"))
        return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    except Exception:  # pylint: disable=broad-exception-caught
        return {}


class ServiceEventsFastAPIMiddleware:
    """
    ASGI middleware for FastAPI instrumentation.

    This middleware:
    1. Tracks request timing and status codes
    2. Captures request body for incident snapshots
    3. Records endpoint metrics
    4. Triggers incident snapshots on errors/slow requests
    5. Propagates endpoint context to function monitors
    """

    def __init__(self, app):
        self.app = app

    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    async def __call__(self, scope, receive, send):
        """ASGI middleware entry point."""
        # Only instrument HTTP requests
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # Before request: capture timing and route info
        method = scope["method"]
        path = scope["path"]
        route = _get_route_pattern(scope)

        # Check endpoint filter - skip tracking if endpoint is filtered out
        if _serviceevents_config and not _serviceevents_config.should_track_endpoint(route, method):
            return await self.app(scope, receive, send)

        # Track this endpoint.
        # Crash-safety: this pre-await setup runs on the request path before the
        # app is invoked. A telemetry failure here must not stop the request, so
        # the whole block is guarded; on failure we run the app plainly (with the
        # original unwrapped receive/send) and emit no telemetry for this request.
        try:
            start_time = time.perf_counter_ns()

            # Set operation context for function monitors
            operation = f"{method} {route}"
            set_current_operation(operation)

            # Begin investigation tracking for this request
            monitor_state = _ServiceEventsMonitorState.get_instance()
            monitor_state.begin_investigation()
        except Exception:  # pylint: disable=broad-exception-caught
            return await self.app(scope, receive, send)

        # Cache request body for incident snapshots
        # This reads and stores the body so it can be accessed later
        body_bytes = b""
        body_received = False

        async def receive_wrapper():
            """Intercept receive to cache request body.

            Crash-safety: this sits in the customer's ASGI receive chain, so the
            message must always be returned even if body caching fails.
            """
            nonlocal body_bytes, body_received
            message = await receive()

            try:
                if message["type"] == "http.request":
                    chunk = message.get("body", b"")
                    if chunk:
                        # Respect 10KB limit for body caching
                        if len(body_bytes) + len(chunk) <= 10240:
                            body_bytes += chunk
                    body_received = message.get("more_body", False) is False
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            return message

        # Track status code and exception
        status_code = 200
        exception = None
        response_started = False
        # Pre-captured trace correlation (captured while span is active)
        captured_trace_id = None
        captured_span_id = None

        async def send_wrapper(message):
            """Intercept send to capture response status code and trace correlation."""
            nonlocal status_code, response_started, captured_trace_id, captured_span_id

            if message["type"] == "http.response.start":
                status_code = message.get("status", 200)
                response_started = True

                # Capture trace_id and span_id NOW while span is still active
                # By the time we reach the finally block, the OTel span context will be cleared
                trace_id, span_id = _capture_active_trace_context()
                if trace_id is not None:
                    captured_trace_id = trace_id
                    captured_span_id = span_id

            await send(message)

        try:
            # Call the app with wrapped receive/send
            await self.app(scope, receive_wrapper, send_wrapper)
        except Exception as exc:
            exception = exc
            status_code = 500

            # If response hasn't started, send a 500 error response. Crash-safety: these
            # sends are best-effort and guarded — if send() itself raises (client
            # disconnect, broken pipe, ASGI shutdown mid-error), that must NOT replace the
            # original application exception, so the failure is swallowed and the original
            # exc still propagates via the bare `raise` below. Telemetry must never alter
            # which exception the host application sees.
            if not response_started:
                try:
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 500,
                            "headers": [(b"content-type", b"text/plain")],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"Internal Server Error",
                        }
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

            # Re-raise to maintain error propagation
            raise
        finally:
            # After request: record metrics and check for incidents
            end_time = time.perf_counter_ns()
            duration_ns = end_time - start_time
            duration_ms = duration_ns / 1_000_000

            # Re-resolve the route now that the app has run. At middleware entry
            # scope["route"] is unset, so `route` was whatever _get_route_pattern could
            # determine pre-routing: a pre-resolved template, or the first-segment
            # unmatched label when nothing matched. Starlette/FastAPI routing populates scope["route"] in
            # place during dispatch, so by here it resolves to the template (/users/{id}).
            # Use the template for the exported endpoint telemetry below to avoid a
            # per-URL metric cardinality explosion. (The pre-await operation context
            # can't see the template yet — routing hasn't happened — so the FunctionCall
            # dimension keeps the entry-time value.)
            route = _get_route_pattern(scope)

            # Fallback trace correlation for the error path: send_wrapper captures
            # trace_id/span_id at http.response.start, but an unhandled exception never
            # starts a response, so nothing is captured there. If the OTel server span is
            # still active here (depends on middleware nesting order), recover it so
            # IncidentSnapshot stays joinable on the error path.
            if captured_trace_id is None:
                trace_id, span_id = _capture_active_trace_context()
                if trace_id is not None:
                    captured_trace_id = trace_id
                    captured_span_id = span_id

            # Extract error info if error occurred
            error_info = None
            if status_code >= 400:
                try:
                    error_info = _extract_error_from_call_path(exception, route, method)
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.debug("Error extracting error info from call path", exc_info=True)

            # Record endpoint metric
            if _endpoint_collector:
                try:
                    _endpoint_collector.record_request(
                        route=route,
                        method=method,
                        status_code=status_code,
                        duration_ns=duration_ns,
                        error_info=error_info,
                    )
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.error("Error recording endpoint metric: %s", exc, exc_info=True)

            # Check if incident snapshot should be triggered
            if _incident_snapshot_collector:
                try:
                    # Parse headers
                    headers_dict = {
                        k.decode("utf-8", errors="replace"): v.decode("utf-8", errors="replace")
                        for k, v in scope.get("headers", [])
                    }

                    # Parse query parameters
                    query_params = _parse_query_string(scope.get("query_string", b""))

                    # Path parameters
                    path_params = scope.get("path_params", {})

                    # Parse cached body (if any)
                    cached_body = None
                    if body_bytes:
                        try:
                            # Try to decode as UTF-8
                            cached_body = body_bytes.decode("utf-8", errors="replace")
                        except Exception:  # pylint: disable=broad-exception-caught
                            cached_body = f"<binary data: {len(body_bytes)} bytes>"

                    exemplar = _incident_snapshot_collector.process_potential_incident(
                        route=route,
                        method=method,
                        status_code=status_code,
                        duration_ms=duration_ms,
                        exception=exception,
                        request_data={
                            "path": path,
                            "endpoint": route,  # FastAPI doesn't have separate endpoint name
                            "method": method,
                            "headers": headers_dict,
                            "args": query_params,  # Query parameters
                            "view_args": path_params,  # Path parameters
                            "cached_body": cached_body,  # Pre-read body
                            # Pre-captured trace correlation (captured in send_wrapper while span was active)
                            "trace_id": captured_trace_id,
                            "span_id": captured_span_id,
                        },
                    )
                    if exemplar and _endpoint_collector:
                        _endpoint_collector.record_incident_exemplar(exemplar["operation"], exemplar)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.error("Error processing incident snapshot: %s", exc, exc_info=True)

            # Always clear operation context
            clear_current_operation()
            # Drop request-scoped investigation data. The incident path clears it via
            # get_investigation_data(), but the normal path returns early and never does, so
            # clear it unconditionally here to avoid leaking a stale dict (and any captured
            # traceback) onto pooled worker threads.
            _ServiceEventsMonitorState.get_instance().clear_investigation_data()


def install_fastapi_hooks(endpoint_collector=None, incident_snapshot_collector=None, config=None):
    """
    Install FastAPI instrumentation hooks.

    This monkey-patches FastAPI.__init__ to automatically add ServiceEvents middleware
    to all FastAPI application instances.

    Args:
        endpoint_collector: EndpointMetricCollector instance
        incident_snapshot_collector: IncidentSnapshotCollector instance
        config: ServiceEventsConfig instance for endpoint filtering
    """
    # global-statement: module-level singletons set once during instrumentation install.
    # pylint: disable=global-statement
    global _endpoint_collector, _incident_snapshot_collector, _serviceevents_config

    try:
        # Lazy import: defer optional heavy dependency (FastAPI) until install time.
        # pylint: disable=import-outside-toplevel
        from fastapi import FastAPI
    except ImportError:
        logger.warning("FastAPI not installed, skipping FastAPI instrumentation")
        return

    _endpoint_collector = endpoint_collector
    _incident_snapshot_collector = incident_snapshot_collector
    _serviceevents_config = config

    # Store original FastAPI.__init__ to wrap it
    original_init = FastAPI.__init__

    def instrumented_init(self, *args, **kwargs):
        """Wrap FastAPI.__init__ to install middleware after app creation."""
        # Call original init first and outside the guard below: this is the real
        # FastAPI constructor and must always run. Only OUR added work is guarded,
        # so a telemetry failure can never break FastAPI app construction.
        original_init(self, *args, **kwargs)

        # Crash-safety: telemetry must never break the host app. Installing the
        # middleware runs inside the customer's FastAPI.__init__, so any failure
        # here is swallowed and the app is constructed as if uninstrumented.
        try:
            # Add ServiceEvents middleware to the FastAPI app.
            # Known limitation: middleware is installed during FastAPI.__init__, the
            # earliest possible point. Under Starlette's add_middleware semantics
            # (insert(0, ...) + reversed-wrap when the stack is built), the last
            # middleware added becomes the OUTERMOST. So any user middleware added
            # after construction wraps ours, leaving ServiceEvents INNERMOST relative
            # to user middleware. Consequence: end-to-end duration may exclude time
            # spent in outer user middleware, and status/response changes made by
            # outer user middleware are not observed. We do not rework this to be
            # outermost because doing so reliably requires framework-internal hooks
            # that are version-fragile.
            self.add_middleware(ServiceEventsFastAPIMiddleware)

            logger.info("ServiceEvents FastAPI hooks installed on app: %s", self.title)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to install ServiceEvents FastAPI middleware", exc_info=True)

    # Replace FastAPI.__init__ with instrumented version
    FastAPI.__init__ = instrumented_init

    logger.info("ServiceEvents FastAPI instrumentation installed")
