# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Flask instrumentation for ServiceEvents EndpointMetric and IncidentSnapshot events.

This module installs hooks into Flask applications to:
- Track endpoint metrics (requests, duration, status codes)
- Trigger incident snapshots on errors or slow requests
- Propagate endpoint context to function monitors
"""

import logging
import time
from typing import Any, Dict, Optional

from amazon.opentelemetry.distro.serviceevents.instrumentation._safety import never_raises
from amazon.opentelemetry.distro.serviceevents.python_monitor import (
    _ServiceEventsMonitorState,
    clear_current_operation,
    set_current_operation,
)

logger = logging.getLogger(__name__)

# Global reference to collectors and config (set during initialization)
_endpoint_collector = None
_incident_snapshot_collector = None
_serviceevents_config = None


def _get_request_body(request) -> Optional[Any]:
    """
    Safely extract request body from Flask request with fallback chain.

    Flask caches these values after first access, so reading is cheap if
    the application code has already accessed the request body.

    Args:
        request: Flask request object

    Returns:
        Request body as JSON dict, form dict, raw string, or None
    """
    try:
        # Try JSON first (most common for APIs)
        body = request.get_json()
        if body is not None:
            return body
    # Telemetry must never crash the host app; any body-read failure is non-fatal.
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        # Try form data
        if request.form:
            return request.form.to_dict()
    # Telemetry must never crash the host app; any body-read failure is non-fatal.
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        # Try raw data (with 10KB limit)
        data = request.get_data()
        if data:
            if len(data) <= 10240:  # 10KB limit
                return data.decode("utf-8", errors="replace")
            return f"<payload too large: {len(data)} bytes>"
    # Telemetry must never crash the host app; any body-read failure is non-fatal.
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    return None


def install_flask_hooks(endpoint_collector=None, incident_snapshot_collector=None, config=None):
    """
    Install Flask instrumentation hooks.

    Args:
        endpoint_collector: EndpointMetricCollector instance
        incident_snapshot_collector: IncidentSnapshotCollector instance
        config: ServiceEventsConfig instance for endpoint filtering
    """
    # Singleton instrumentation state shared across Flask request hooks.
    # pylint: disable-next=global-statement
    global _endpoint_collector, _incident_snapshot_collector, _serviceevents_config

    # Lazy import: flask is an optional heavy dependency, imported only when hooks install.
    try:
        from flask import Flask  # pylint: disable=import-outside-toplevel
    except ImportError:
        logger.warning("Flask not installed, skipping Flask instrumentation")
        return

    _endpoint_collector = endpoint_collector
    _incident_snapshot_collector = incident_snapshot_collector
    _serviceevents_config = config

    # Store original Flask.__init__ to wrap it
    original_init = Flask.__init__

    def instrumented_init(self, *args, **kwargs):
        """Wrap Flask.__init__ to install hooks after app creation."""
        # Call original init
        original_init(self, *args, **kwargs)

        # Install before_request and after_request hooks
        self.before_request(_before_request_hook)
        self.after_request(_after_request_hook)
        self.teardown_request(_teardown_request_hook)

        logger.info("ServiceEvents Flask hooks installed on app: %s", self.name)

    # Replace Flask.__init__ with instrumented version
    Flask.__init__ = instrumented_init

    logger.info("ServiceEvents Flask instrumentation installed")


@never_raises()
def _before_request_hook():
    """Hook called before each request.

    Guarded by @never_raises: a telemetry failure here must not turn into a 500
    before the customer's view runs. On failure the request proceeds untracked.
    """
    # Lazy import: flask is an optional heavy dependency, resolved at request time.
    from flask import g, request  # pylint: disable=import-outside-toplevel

    # Store request info for later
    g.serviceevents_method = request.method
    g.serviceevents_path = request.path
    g.serviceevents_endpoint = request.endpoint or "unknown"
    g.serviceevents_route = _get_route_pattern(request)

    # Check endpoint filter - skip tracking if endpoint is filtered out
    if _serviceevents_config and not _serviceevents_config.should_track_endpoint(
        g.serviceevents_route, g.serviceevents_method
    ):
        g.serviceevents_skip = True
        return

    # Store request start time (only for tracked endpoints)
    g.serviceevents_start_time = time.perf_counter_ns()
    g.serviceevents_skip = False

    # Set operation context for function monitors
    operation = f"{g.serviceevents_method} {g.serviceevents_route}"
    set_current_operation(operation)
    g.serviceevents_operation = operation

    # Begin investigation tracking for this request (always-on mode for incident capture)
    # This enables call_path capture for potential incident snapshots
    monitor_state = _ServiceEventsMonitorState.get_instance()
    monitor_state.begin_investigation()


def _after_request_hook(response):
    """Hook called after each request (before response is sent).

    Crash-safety: the response must always pass through unchanged even if
    telemetry fails, so the body is guarded and ``return response`` sits outside
    the try.
    """
    # Lazy import: flask is an optional heavy dependency, resolved at request time.
    from flask import g  # pylint: disable=import-outside-toplevel

    try:
        # Skip if endpoint is filtered out
        if getattr(g, "serviceevents_skip", False):
            return response

        # Calculate duration
        if hasattr(g, "serviceevents_start_time"):
            end_time = time.perf_counter_ns()
            duration_ms = (end_time - g.serviceevents_start_time) / 1_000_000

            # Store in g for teardown hook
            g.serviceevents_duration_ms = duration_ms
            g.serviceevents_status_code = response.status_code

        # Capture trace_id and span_id NOW while span is still active
        # By teardown_request, the OTel span context will have been cleared
        trace_id, span_id = _capture_active_trace_context()
        if trace_id is not None:
            g.serviceevents_trace_id = trace_id
            g.serviceevents_span_id = span_id
    # Never let telemetry replace the customer's response with a 500.
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    return response


def _teardown_request_hook(exception=None):  # pylint: disable=too-many-branches
    """Hook called at the end of request (even if there's an exception)."""
    # Lazy import: flask is an optional heavy dependency, resolved at request time.
    from flask import g, request  # pylint: disable=import-outside-toplevel

    try:
        # Skip if endpoint is filtered out
        if getattr(g, "serviceevents_skip", False):
            return

        # Check if we have the required data
        if not hasattr(g, "serviceevents_start_time"):
            return

        # Prevent duplicate incident processing (Flask may call teardown multiple times)
        if hasattr(g, "serviceevents_incident_processed"):
            return

        # Fallback trace correlation for the error path. _after_request_hook normally
        # captures trace_id/span_id, but Flask skips after_request when an unhandled
        # exception propagates with PROPAGATE_EXCEPTIONS=True (debug/testing). The OTel
        # server span is still active in teardown, so recover it here if unset — keeps
        # IncidentSnapshot joinable on the error path.
        if not hasattr(g, "serviceevents_trace_id"):
            trace_id, span_id = _capture_active_trace_context()
            if trace_id is not None:
                g.serviceevents_trace_id = trace_id
                g.serviceevents_span_id = span_id

        # Get status code (may be set in after_request or need to infer from exception)
        status_code = getattr(g, "serviceevents_status_code", None)

        if status_code is None:
            # Infer from exception
            if exception is not None:
                status_code = 500
            else:
                status_code = 200  # Default

        # Get duration
        if not hasattr(g, "serviceevents_duration_ms"):
            end_time = time.perf_counter_ns()
            duration_ms = (end_time - g.serviceevents_start_time) / 1_000_000
        else:
            duration_ms = g.serviceevents_duration_ms

        # Convert duration to nanoseconds
        duration_ns = int(duration_ms * 1_000_000)

        # Extract error info if error occurred
        error_info = None
        if status_code >= 400:
            error_info = _extract_error_from_call_path(exception, g.serviceevents_route, g.serviceevents_method)

        # Record endpoint metric
        if _endpoint_collector:
            try:
                _endpoint_collector.record_request(
                    route=g.serviceevents_route,
                    method=g.serviceevents_method,
                    status_code=status_code,
                    duration_ns=duration_ns,
                    error_info=error_info,
                )
            # Telemetry must never crash the host app; metric recording is best-effort.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("Error recording endpoint metric: %s", exc, exc_info=True)

        # Check if incident snapshot should be triggered
        if _incident_snapshot_collector:
            try:
                exemplar = _incident_snapshot_collector.process_potential_incident(
                    route=g.serviceevents_route,
                    method=g.serviceevents_method,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    exception=exception,
                    request_data={
                        "path": g.serviceevents_path,
                        "endpoint": g.serviceevents_endpoint,
                        "method": g.serviceevents_method,
                        "headers": dict(request.headers),
                        "args": dict(request.args),
                        "view_args": request.view_args,
                        "flask_request": request,
                        # Pre-captured trace correlation (captured in after_request while span was active)
                        "trace_id": getattr(g, "serviceevents_trace_id", None),
                        "span_id": getattr(g, "serviceevents_span_id", None),
                    },
                )
                if exemplar and _endpoint_collector:
                    _endpoint_collector.record_incident_exemplar(exemplar["operation"], exemplar)
                # Mark incident as processed to prevent duplicate snapshots
                g.serviceevents_incident_processed = True
            # Telemetry must never crash the host app; snapshot processing is best-effort.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("Error processing incident snapshot: %s", exc, exc_info=True)
    finally:
        # Always clear operation context, even if there's an error
        clear_current_operation()


def _capture_active_trace_context():
    """Return (trace_id, span_id) of the active OTel span, or (None, None).

    Both are raw OTel integers. Returns (None, None) when there is no valid span
    or the OTel API is unavailable. Telemetry must never crash the host app, so
    any failure is swallowed.
    """
    try:
        # Lazy import: defer the OTel trace API until span correlation is needed.
        from opentelemetry import trace  # pylint: disable=import-outside-toplevel

        current_span = trace.get_current_span()
        if current_span:
            span_context = current_span.get_span_context()
            if span_context.is_valid:
                return span_context.trace_id, span_context.span_id
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    return None, None


def _get_route_pattern(request) -> str:
    """
    Get the route pattern (e.g. /users/<id>) from the request.

    Args:
        request: Flask request object

    Returns:
        Route pattern string (e.g., "/users/<id>")
    """
    # Try to get the route pattern from the URL rule
    if request.url_rule:
        return request.url_rule.rule

    # Fallback to endpoint name
    if request.endpoint:
        return f"/{request.endpoint}"

    # Last resort: use path
    return request.path


def _extract_error_from_call_path(exception, route, method) -> Optional[Dict]:
    """
    Extract primary error and origin function_name from investigation data.

    Track LAST/PRIMARY error only (the one that caused the HTTP error status),
    not all errors from the call path, to avoid noise in telemetry.

    IMPORTANT: This function only READS the investigation data without clearing it,
    so that it remains available for the incident snapshot collector.

    Args:
        exception: The exception that was raised (or None)
        route: Route pattern
        method: HTTP method

    Returns:
        Dictionary with {error_type, function_name} or None if no error
    """
    # Get investigation data WITHOUT clearing it
    # Use peek_investigation_data() instead of get_investigation_data()
    # which would clear the data before incident_snapshot_collector can use it
    monitor_state = _ServiceEventsMonitorState.get_instance()
    inv_data = monitor_state.peek_investigation_data()

    # Resolve the error type. Prefer the passed-in exception; otherwise recover the type the
    # monitor captured. This matters for FastAPI/Starlette: a global exception_handler
    # converts the error to a 500 response *before* it reaches our middleware, so `exception`
    # is None there even though a real error occurred — without this recovery the breakdown
    # would mislabel every such error as "UnknownError".
    exc_data = inv_data.get("exception") if inv_data else None
    if exception is not None:
        error_type = exception.__class__.__name__
    elif isinstance(exc_data, dict) and exc_data.get("name"):
        error_type = exc_data["name"]
    else:
        error_type = "UnknownError"

    # Find the origin function_name.
    function_name = "unknown"

    # Prefer the function the monitor recorded as the actual thrower. call_path[0] is the
    # innermost frame the monitor *entered*, which is not necessarily where the exception
    # was raised (e.g. it caught-and-reraised, or the deepest frame was uninstrumented), so
    # the captured exception origin is the authoritative source when present.
    if isinstance(exc_data, dict) and exc_data.get("function_name"):
        function_name = exc_data["function_name"]

    # Fall back to the innermost call_path entry when no exception origin was captured.
    if function_name == "unknown" and inv_data and inv_data.get("call_path"):
        call_path = inv_data["call_path"]
        if call_path and len(call_path) > 0:
            # call_path is ordered from innermost to outermost.
            first_entry = call_path[0]
            if isinstance(first_entry, dict):
                function_name = first_entry.get("function_name", "unknown")

    return {"error_type": error_type, "function_name": function_name}
