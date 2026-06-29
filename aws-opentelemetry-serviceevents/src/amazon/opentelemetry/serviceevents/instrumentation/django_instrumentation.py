# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Django instrumentation for ServiceEvents EndpointMetric and IncidentSnapshot events.

This module installs hooks into Django applications to:
- Track endpoint metrics (requests, duration, status codes)
- Trigger incident snapshots on errors or slow requests
- Propagate endpoint context to function monitors

Note: Django uses middleware for request processing hooks.
"""

import json
import logging
import time
from typing import Any, Optional

from amazon.opentelemetry.serviceevents.instrumentation._constants import unmatched_route_label
from amazon.opentelemetry.serviceevents.instrumentation.flask_instrumentation import (
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

# Django only calls process_view after a URL resolves to a view, so an unmatched 404 never
# gets a route stored and _finalize_request falls back to the shared first-segment unmatched
# label derived from the raw path (see _constants for the cardinality rationale).


def _get_request_body(request) -> Optional[Any]:
    """
    Safely extract request body from Django request with fallback chain.

    Args:
        request: Django HttpRequest object

    Returns:
        Request body as JSON dict, form dict, raw string, or None
    """
    try:
        # Try JSON first (most common for APIs)
        content_type = request.content_type or ""
        if "json" in content_type:
            body = json.loads(request.body)
            if body is not None:
                return body
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        # Try form data
        if request.POST:
            return request.POST.dict()
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    try:
        # Try raw data (with 10KB limit)
        data = request.body
        if data:
            if len(data) <= 10240:  # 10KB limit
                return data.decode("utf-8", errors="replace")
            return f"<payload too large: {len(data)} bytes>"
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    return None


def _get_route_pattern(request) -> str:
    """
    Get the route pattern (e.g. users/<int:id>/) from the Django request.

    Args:
        request: Django HttpRequest object

    Returns:
        Route pattern string (e.g., "users/<int:id>/")
    """
    # Try to get the route pattern from resolver_match (available after URL resolution)
    if hasattr(request, "resolver_match") and request.resolver_match is not None:
        # resolver_match.route is the URL pattern (e.g., "users/<int:id>/"). Django stores
        # it without a leading slash by convention; return it verbatim so the operation
        # label matches Application Signals, which derives the same value from span.name
        # (the upstream OTel Django instrumentation also leaves it slash-less). Flask and
        # FastAPI route patterns already carry a leading slash natively, so all three
        # frameworks agree with App Signals without further normalization here.
        route = getattr(request.resolver_match, "route", None)
        if route:
            return route

    # Fallback to path_info
    path = getattr(request, "path_info", None) or getattr(request, "path", "/unknown")
    return path


def _get_endpoint_name(request) -> str:
    """
    Get the endpoint/view name from the Django request.

    Args:
        request: Django HttpRequest object

    Returns:
        View name string (e.g., "myapp.views.user_detail") or "unknown"
    """
    if hasattr(request, "resolver_match") and request.resolver_match is not None:
        view_name = getattr(request.resolver_match, "view_name", None)
        if view_name:
            return view_name
    return "unknown"


def _finalize_request(request, response, exception):  # pylint: disable=too-many-branches,too-many-locals
    """
    Finalize request processing: record metrics and check for incidents.

    Called from the middleware __call__ finally block after the request
    has been fully processed (or errored).

    Args:
        request: Django HttpRequest object
        response: Django HttpResponse object (may be None if exception occurred)
        exception: Exception that was raised (or None)
    """
    try:
        # Skip if endpoint is filtered out or middleware __call__ was not entered
        if getattr(request, "_serviceevents_skip", False):
            return

        # Skip if no start time was recorded
        if not hasattr(request, "_serviceevents_start_time"):
            return

        # Prefer exception stored by process_exception (innermost exception)
        exc = getattr(request, "_serviceevents_exception", None) or exception

        # Determine status code
        if response is not None:
            status_code = response.status_code
        elif exc is not None:
            status_code = 500
        else:
            status_code = 200

        # Calculate duration
        end_time = time.perf_counter_ns()
        duration_ns = end_time - request._serviceevents_start_time
        duration_ms = duration_ns / 1_000_000

        # Get route and method from stored values (set in process_view). A missing
        # _serviceevents_route means process_view never ran — i.e. the URL matched no
        # urlpattern (unmatched 404). Collapse those to the first path segment instead of
        # the raw path so scanner/bot traffic to nonexistent URLs can't explode metric
        # cardinality, matching Application Signals' unmatched-route handling.
        route = getattr(request, "_serviceevents_route", None)
        if route is None:
            raw_path = getattr(request, "path_info", None) or getattr(request, "path", None)
            route = unmatched_route_label(raw_path)
        method = getattr(request, "_serviceevents_method", request.method)

        # Extract error info if error occurred
        error_info = None
        if status_code >= 400:
            error_info = _extract_error_from_call_path(exc, route, method)

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
            except Exception as record_err:  # pylint: disable=broad-exception-caught
                # Use a distinct name: `exc` holds the request exception still needed below.
                logger.error("Error recording endpoint metric: %s", record_err, exc_info=True)

        # Check if incident snapshot should be triggered
        if _incident_snapshot_collector:
            try:
                # Build request data for incident snapshot
                resolver_match = getattr(request, "resolver_match", None)
                request_data = {
                    "path": getattr(request, "_serviceevents_path", request.path),
                    "endpoint": getattr(request, "_serviceevents_endpoint", "unknown"),
                    "method": getattr(request, "_serviceevents_method", request.method),
                    "headers": dict(request.headers) if hasattr(request, "headers") else {},
                    "args": request.GET.dict() if hasattr(request, "GET") else {},
                    "view_args": resolver_match.kwargs if resolver_match else {},
                    "cached_body": _get_request_body(request),
                    # Pre-captured trace correlation (captured in process_view while span was active)
                    "trace_id": getattr(request, "_serviceevents_trace_id", None),
                    "span_id": getattr(request, "_serviceevents_span_id", None),
                }

                exemplar = _incident_snapshot_collector.process_potential_incident(
                    route=route,
                    method=method,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    exception=exc,
                    request_data=request_data,
                )
                if exemplar and _endpoint_collector:
                    _endpoint_collector.record_incident_exemplar(exemplar["operation"], exemplar)
            except Exception as snapshot_err:  # pylint: disable=broad-exception-caught
                logger.error("Error processing incident snapshot: %s", snapshot_err, exc_info=True)
    finally:
        # Always clear operation context, even if there's an error
        clear_current_operation()
        # Drop request-scoped investigation data. The incident path clears it via
        # get_investigation_data(), but the normal path returns early and never does, so
        # clear it unconditionally here to avoid leaking a stale dict (and any captured
        # traceback) onto pooled worker threads.
        _ServiceEventsMonitorState.get_instance().clear_investigation_data()


class ServiceEventsDjangoMiddleware:
    """
    Django middleware for ServiceEvents instrumentation.

    This middleware:
    1. Tracks request timing and status codes
    2. Captures request context for incident snapshots
    3. Records endpoint metrics
    4. Triggers incident snapshots on errors/slow requests
    5. Propagates endpoint context to function monitors

    Middleware ordering: This should be prepended to the BEGINNING of
    settings.MIDDLEWARE (outermost) so that __call__ wraps the entire
    request lifecycle including all other middleware.

    Hook execution order:
    - __call__: Starts timing (before any middleware runs)
    - process_view: Sets route context (after URL resolution, OTel span is active)
    - process_exception: Captures exceptions
    - __call__ finally: Finalizes metrics and snapshots (after everything)
    """

    def __init__(self, get_response):
        """
        Standard Django middleware init.

        Args:
            get_response: The next middleware or view in the chain
        """
        self.get_response = get_response

    def __call__(self, request):
        """
        Process the request through the middleware chain.

        Wraps the entire request lifecycle with timing and error capture.
        """
        # Start timing before any other middleware runs
        request._serviceevents_start_time = time.perf_counter_ns()
        request._serviceevents_skip = False
        request._serviceevents_exception = None

        response = None
        try:
            response = self.get_response(request)
        except Exception as exc:
            request._serviceevents_exception = exc
            raise
        finally:
            _finalize_request(request, response, request._serviceevents_exception)

        return response

    # no-self-use: Django middleware hook called as a bound method on the instance.
    def process_view(self, request, view_func, view_args, view_kwargs):  # pylint: disable=no-self-use
        """
        Called after URL resolution, before the view is called.

        This is where we know the resolved route pattern. The OTel middleware
        has already created the span at this point, so we can capture trace
        correlation IDs.

        Args:
            request: Django HttpRequest object
            view_func: The view function that will be called
            view_args: Positional arguments to the view
            view_kwargs: Keyword arguments to the view

        Returns:
            None to continue normal processing
        """
        # Crash-safety: process_view runs on the request path before the view.
        # A telemetry failure here must not abort the request, so the setup block
        # is guarded; on failure we mark the request skipped and return None so
        # the view runs untracked.
        try:
            # Extract route pattern (resolver_match is available here)
            route = _get_route_pattern(request)
            method = request.method

            # Check endpoint filter - skip tracking if endpoint is filtered out
            if _serviceevents_config and not _serviceevents_config.should_track_endpoint(route, method):
                request._serviceevents_skip = True
                return None

            # Store request context for finalization
            request._serviceevents_route = route
            request._serviceevents_method = method
            request._serviceevents_path = request.path
            request._serviceevents_endpoint = _get_endpoint_name(request)

            # Set operation context for function monitors
            operation = f"{method} {route}"
            set_current_operation(operation)

            # Begin investigation tracking for this request (always-on mode for incident capture)
            # This enables call_path capture for potential incident snapshots
            monitor_state = _ServiceEventsMonitorState.get_instance()
            monitor_state.begin_investigation()
        except Exception:  # pylint: disable=broad-exception-caught
            request._serviceevents_skip = True
            return None

        # Capture trace_id and span_id NOW while OTel span is active
        # The OTel middleware runs before our process_view, so the span context is available
        try:
            # Lazy import: defer OTel import to avoid import-time coupling on the request path.
            # pylint: disable=import-outside-toplevel
            from opentelemetry import trace

            span_context = None

            # PRIMARY: Try OTel context API
            current_span = trace.get_current_span()
            if current_span:
                sc = current_span.get_span_context()
                if sc and sc.is_valid:
                    span_context = sc

            # FALLBACK: Read span directly from OTel Django middleware's request.META storage
            # The OTel middleware stores the span at this key during process_request()
            # and removes it in process_response(). During process_view, it's always present.
            if span_context is None:
                otel_span = request.META.get("opentelemetry-instrumentor-django.span_key")
                if otel_span:
                    sc = otel_span.get_span_context()
                    if sc and sc.is_valid:
                        span_context = sc

            if span_context is not None:
                request._serviceevents_trace_id = span_context.trace_id
                request._serviceevents_span_id = span_context.span_id
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        return None

    # no-self-use: Django middleware hook called as a bound method on the instance.
    def process_exception(self, request, exception):  # pylint: disable=useless-return,no-self-use
        """
        Called when a view raises an exception.

        Args:
            request: Django HttpRequest object
            exception: The exception that was raised

        Returns:
            None to let Django handle the exception normally
        """
        request._serviceevents_exception = exception
        return None


def install_django_hooks(endpoint_collector=None, incident_snapshot_collector=None, config=None):
    """
    Install Django instrumentation hooks.

    This monkey-patches Django's BaseHandler.load_middleware() to automatically
    prepend ServiceEvents middleware to all Django application instances.

    Args:
        endpoint_collector: EndpointMetricCollector instance
        incident_snapshot_collector: IncidentSnapshotCollector instance
        config: ServiceEventsConfig instance for endpoint filtering
    """
    # global-statement: module-level singletons set once during instrumentation install.
    # pylint: disable=global-statement
    global _endpoint_collector, _incident_snapshot_collector, _serviceevents_config

    try:
        # Lazy import: defer optional heavy dependency (Django) until install time.
        # pylint: disable=import-outside-toplevel
        from django.core.handlers.base import BaseHandler
    except ImportError:
        logger.warning("Django not installed, skipping Django instrumentation")
        return

    _endpoint_collector = endpoint_collector
    _incident_snapshot_collector = incident_snapshot_collector
    _serviceevents_config = config

    # Middleware dotted path for Django settings
    # invalid-name: module/contract constant, not a regular local variable.
    _SERVICE_EVENTS_MIDDLEWARE_PATH = (  # pylint: disable=invalid-name
        "amazon.opentelemetry.serviceevents.instrumentation.django_instrumentation.ServiceEventsDjangoMiddleware"
    )

    # Store original load_middleware to wrap it
    original_load_middleware = BaseHandler.load_middleware

    def instrumented_load_middleware(self, *args, **kwargs):
        """Wrap BaseHandler.load_middleware to prepend ServiceEvents middleware for the build.

        Django reads settings.MIDDLEWARE only here, to materialize the middleware chain (and
        register each middleware's process_view/process_exception hooks) into the handler; it
        does not consult settings.MIDDLEWARE again per request. So we prepend our path at the
        front for the duration of this single build, then restore the customer's original
        MIDDLEWARE in the finally. Net result: ServiceEvents is wired outermost, but
        settings.MIDDLEWARE is left exactly as the customer defined it — no persistent global
        side effect for code/introspection that reads it, and apps that validate/freeze
        settings post-build aren't tripped.

        The stack is still built exactly once. load_middleware re-instantiates every
        middleware on each call, so a second build would double-init every third-party
        middleware on first startup (duplicate signal handlers, threads, connections, etc.).
        """
        settings = None
        original_middleware = None
        injected = False
        try:
            # Lazy import: defer optional heavy dependency (Django) until install time.
            # pylint: disable=import-outside-toplevel
            from django.conf import settings  # pylint: disable=redefined-outer-name

            original_middleware = list(getattr(settings, "MIDDLEWARE", []) or [])
            if _SERVICE_EVENTS_MIDDLEWARE_PATH not in original_middleware:
                # Prepend to the front (outermost) just for this build.
                settings.MIDDLEWARE = [_SERVICE_EVENTS_MIDDLEWARE_PATH, *original_middleware]
                injected = True
                logger.info("ServiceEvents Django middleware injected for the middleware-stack build")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error injecting ServiceEvents Django middleware: %s", exc, exc_info=True)

        try:
            # Build the middleware stack exactly once, now that settings.MIDDLEWARE includes ours.
            # Called unconditionally so the handler still builds a chain when SE middleware was
            # already present (e.g. pre-configured in user settings) or when injection failed.
            original_load_middleware(self, *args, **kwargs)
        finally:
            # Restore the customer's MIDDLEWARE so our injection leaves no persistent global
            # change. Only when we actually injected; the chain is already materialized above,
            # so removing our entry from settings here does not unwire the middleware.
            if injected and settings is not None:
                try:
                    settings.MIDDLEWARE = original_middleware
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.debug("Failed to restore settings.MIDDLEWARE after ServiceEvents injection", exc_info=True)

    # Replace BaseHandler.load_middleware with instrumented version
    BaseHandler.load_middleware = instrumented_load_middleware

    logger.info("ServiceEvents Django instrumentation installed")
