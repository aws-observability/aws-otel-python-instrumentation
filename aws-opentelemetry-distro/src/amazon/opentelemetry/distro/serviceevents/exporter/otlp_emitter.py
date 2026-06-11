# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
OTLP emitter for ServiceEvents signals.

Maps collector data models to OTLP LogRecords and OTel Metrics.
Uses dedicated LoggerProvider and MeterProvider isolated from
application telemetry.

Signals:
  - EndpointSummary → OTLP LogRecord (event.name = aws.service_events.endpoint_summary)
  - FunctionCall → OTel Exponential Histogram (name = service.function.duration)
  - IncidentSnapshot → OTLP LogRecord (event.name = aws.service_events.incident_snapshot)
  - DeploymentEvent → OTLP LogRecord (event.name = aws.service_events.deployment_event)
  - EndpointErrorMetrics → OTel Counter (name = count, unit = Count)
"""

import logging
import threading
import time
from typing import Any, Dict, Optional

from opentelemetry._logs import LogRecord, SeverityNumber

logger = logging.getLogger(__name__)

INSTRUMENTATION_SCOPE = "serviceevents"
INSTRUMENTATION_VERSION = "1.0"


class ServiceEventsOtlpEmitter:
    """Emits ServiceEvents signals as OTLP LogRecords and OTel Metrics."""

    def __init__(
        self,
        logger_provider,
        meter_provider,
        deployment_id: str = "",
        git_commit_sha: str = "",
        git_repo_url: str = "",
    ):
        self._logger_provider = logger_provider
        self._meter_provider = meter_provider
        self._deployment_id = deployment_id or ""
        self._git_commit_sha = git_commit_sha or ""
        self._git_repo_url = git_repo_url or ""

        self._otel_logger = None
        self._error_counter = None
        self._init_lock = threading.Lock()
        self._initialized = False

    def _ensure_initialized(self):
        """Lazy initialization of OTel logger and meter instruments."""
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            try:
                self._otel_logger = self._logger_provider.get_logger(INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION)
                meter = self._meter_provider.get_meter(INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION)
                self._error_counter = meter.create_counter("count", unit="Count")
                self._initialized = True
            # Telemetry must never crash the host app; swallow any init failure.
            except Exception:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to initialize OTLP emitter", exc_info=True)

    # ─── EndpointSummary ────────────────────────────────────────────────

    def emit_endpoint_summary(self, event) -> None:
        """Emit EndpointSummary as OTLP LogRecord."""
        self._ensure_initialized()
        if not self._otel_logger:
            return

        attrs = {
            "http.request.method": event.method or "",
            "url.route": event.route or "",
            "aws.service_events.operation": event.operation or "",
            "aws.service_events.request.count": event.count or 0,
            "aws.service_events.request.faults": event.faults or 0,
            "aws.service_events.request.errors": event.errors or 0,
            "aws.service_events.incident.count": getattr(event, "incident_count", 0) or 0,
        }
        self._put_vcs_and_deployment_attrs(attrs)

        body = {}
        if event.duration:
            body["duration"] = self._duration_to_dict(event.duration)
        body["exception_breakdown"] = self._error_breakdown_to_list(getattr(event, "error_breakdown", None))
        body["incidents_exemplar"] = self._incidents_exemplar_to_list(getattr(event, "incidents_exemplar", None))

        self._emit_log("aws.service_events.endpoint_summary", attrs, body)

    # ─── IncidentSnapshot ───────────────────────────────────────────────

    def emit_incident_snapshot(self, snapshot_dict: Dict[str, Any]) -> None:
        """Emit IncidentSnapshot as OTLP LogRecord with trace context.

        Args:
            snapshot_dict: The full incident snapshot dict (from IncidentSnapshot.to_dict()).
                          Must have telemetry_correlation, exception_info, request_context, etc.
        """
        self._ensure_initialized()
        if not self._otel_logger:
            return

        attrs = {
            "aws.service_events.snapshot_id": snapshot_dict.get("snapshot_id", ""),
            "aws.service_events.trigger_type": snapshot_dict.get("trigger_type", ""),
            "aws.service_events.operation": snapshot_dict.get("operation", ""),
            "aws.service_events.duration_ms": snapshot_dict.get("duration_ms", 0),
            "aws.service_events.is_partial": snapshot_dict.get("is_partial", False),
        }

        # HTTP context
        method = snapshot_dict.get("method", "")
        route = snapshot_dict.get("route", "")
        if not method:
            operation = snapshot_dict.get("operation", "")
            parts = operation.split(" ", 1) if operation else []
            method = parts[0] if len(parts) > 0 else ""
            route = parts[1] if len(parts) > 1 else ""
        attrs["http.request.method"] = method
        attrs["url.route"] = route

        request_context = snapshot_dict.get("request_context", {})
        if isinstance(request_context, dict):
            status_code = request_context.get("status_code")
            if status_code is not None:
                attrs["http.response.status_code"] = int(status_code)
            attrs["aws.service_events.request.type"] = request_context.get("type", "http")

        self._put_vcs_and_deployment_attrs(attrs)

        # Body: exception_info + request_context.
        body = {}
        exception_info = snapshot_dict.get("exception_info")
        if exception_info is not None:
            body["exception_info"] = exception_info
        if request_context:
            body["request_context"] = request_context

        # Trace context from telemetry_correlation
        trace_context = None
        correlation = snapshot_dict.get("telemetry_correlation", {})
        if isinstance(correlation, dict):
            trace_id_str = correlation.get("trace_id")
            span_id_str = correlation.get("span_id")
            if trace_id_str and span_id_str:
                trace_context = {"trace_id": trace_id_str, "span_id": span_id_str}

        self._emit_log("aws.service_events.incident_snapshot", attrs, body, trace_context)

    # ─── DeploymentEvent ────────────────────────────────────────────────

    def emit_deployment_event(self, event, trigger: str = "periodic") -> None:
        """Emit DeploymentEvent as OTLP LogRecord (no body)."""
        self._ensure_initialized()
        if not self._otel_logger:
            return

        attrs = {}
        # Read from DeploymentEventTelemetry model. Unset DeploymentContext fields
        # default to empty, so a plain truthiness guard omits them from the wire.
        ctx = getattr(event, "deployment_context", None)
        if ctx:
            if getattr(ctx, "git_commit_sha", None):
                attrs["vcs.ref.head.revision"] = ctx.git_commit_sha
            if getattr(ctx, "git_repo_url", None):
                attrs["vcs.repository.url.full"] = ctx.git_repo_url
            if getattr(ctx, "deployment_id", None):
                attrs["aws.service_events.deployment.id"] = ctx.deployment_id
            if getattr(ctx, "deployment_url", None):
                attrs["aws.service_events.deployment.url"] = ctx.deployment_url
            if getattr(ctx, "deployment_timestamp", None):
                attrs["aws.service_events.deployment.timestamp"] = ctx.deployment_timestamp
        else:
            # Fallback to emitter-level config
            self._put_vcs_and_deployment_attrs(attrs)

        attrs["aws.service_events.deployment.trigger"] = trigger

        # DeploymentEvent has no body
        self._emit_log("aws.service_events.deployment_event", attrs, None)

    # ─── EndpointErrorMetrics ───────────────────────────────────────────

    def emit_endpoint_error_metrics(self, metrics: list) -> None:
        """Emit EndpointErrorMetrics as OTel Counter data points."""
        self._ensure_initialized()
        if not self._error_counter:
            return

        for metric in metrics:
            count = getattr(metric, "count", 0) or 0
            if count <= 0:
                continue
            self._error_counter.add(
                count,
                {
                    "Telemetry.Source": "ServiceEvents",
                    # `environment` is Optional and is None when unset — coalesce every
                    # value with `or ""` so a None never reaches the OTel attribute API
                    # (which rejects None values and drops the dimension).
                    "service_name": getattr(metric, "service_name", "") or "",
                    "environment": getattr(metric, "environment", "") or "",
                    "operation": getattr(metric, "operation", "") or "",
                    "exception": getattr(metric, "exception", "") or "",
                },
            )

    # ─── Shutdown ───────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Flush and shutdown owned providers."""
        try:
            if self._logger_provider:
                self._logger_provider.force_flush()
                self._logger_provider.shutdown()
        # Telemetry must never crash the host app; swallow any shutdown failure.
        except Exception:  # pylint: disable=broad-exception-caught
            logger.warning("Error shutting down logger provider", exc_info=True)
        try:
            if self._meter_provider:
                self._meter_provider.force_flush()
                self._meter_provider.shutdown()
        # Telemetry must never crash the host app; swallow any shutdown failure.
        except Exception:  # pylint: disable=broad-exception-caught
            logger.warning("Error shutting down meter provider", exc_info=True)

    # ─── Private helpers ────────────────────────────────────────────────

    def _emit_log(
        self,
        event_name: str,
        attributes: Dict[str, Any],
        body: Optional[Dict[str, Any]] = None,
        trace_context: Optional[Dict[str, str]] = None,
    ) -> None:
        """Core log emission method."""
        # CloudWatch workaround: event.name as explicit attribute
        attributes["event.name"] = event_name

        timestamp_ns = int(time.time() * 1e9)

        # Trace context (IncidentSnapshot only)
        trace_id = 0
        span_id = 0
        trace_flags = 0
        if trace_context:
            try:
                tid = trace_context.get("trace_id", "")
                sid = trace_context.get("span_id", "")
                if tid:
                    trace_id = int(tid, 16) if isinstance(tid, str) else int(tid)
                if sid:
                    span_id = int(sid, 16) if isinstance(sid, str) else int(sid)
                if trace_id and span_id:
                    trace_flags = 1  # SAMPLED
            except (ValueError, TypeError):
                pass

        log_record = LogRecord(
            timestamp=timestamp_ns,
            event_name=event_name,
            trace_id=trace_id,
            span_id=span_id,
            trace_flags=trace_flags,
            severity_number=SeverityNumber.UNSPECIFIED,
            attributes=attributes,
            body=body if body is not None else "",
        )

        try:
            self._otel_logger.emit(log_record)
        # Telemetry must never crash the host app; swallow any emit failure.
        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to emit OTLP log: %s", event_name, exc_info=True)

    def _put_vcs_and_deployment_attrs(self, attrs: Dict[str, Any]) -> None:
        """Add VCS and deployment attributes if set."""
        if self._git_commit_sha:
            attrs["vcs.ref.head.revision"] = self._git_commit_sha
        if self._git_repo_url:
            attrs["vcs.repository.url.full"] = self._git_repo_url
        if self._deployment_id:
            attrs["aws.service_events.deployment.id"] = self._deployment_id

    @staticmethod
    def _duration_to_dict(duration) -> Dict[str, Any]:
        """Convert DurationMetrics to CamelCase dict for OTLP body."""
        return {
            "Values": list(getattr(duration, "values", []) or []),
            "Counts": list(getattr(duration, "counts", []) or []),
            "Max": getattr(duration, "max", 0) or 0,
            "Min": getattr(duration, "min", 0) or 0,
            "Count": getattr(duration, "count", 0) or 0,
            "Sum": getattr(duration, "sum", 0) or 0,
        }

    @staticmethod
    def _error_breakdown_to_list(error_breakdown) -> list:
        """Convert error_breakdown entries to the emitted body format."""
        if not error_breakdown:
            return []
        result = []
        for entry in error_breakdown:
            item = {
                "count": getattr(entry, "count", 0),
                "failure_type": getattr(entry, "failure_type", ""),
            }
            errors = getattr(entry, "errors", [])
            if errors:
                item["exceptions"] = [
                    {
                        "exception_type": getattr(e, "error_type", ""),
                        "function_name": getattr(e, "function_name", "") or getattr(e, "origin_function", ""),
                    }
                    for e in errors
                ]
            result.append(item)
        return result

    @staticmethod
    def _incidents_exemplar_to_list(exemplars) -> list:
        """Convert incidents_exemplar entries to the emitted body format."""
        if not exemplars:
            return []
        return [
            {
                "snapshot_id": getattr(ex, "snapshot_id", ""),
                "trigger_type": getattr(ex, "trigger_type", ""),
                "timestamp": getattr(ex, "timestamp", 0),
            }
            for ex in exemplars
        ]
