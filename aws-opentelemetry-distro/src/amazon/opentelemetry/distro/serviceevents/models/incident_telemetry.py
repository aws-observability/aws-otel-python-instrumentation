# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data models for incident snapshot telemetry.

Defines structured schemas for IncidentSnapshot events that capture
comprehensive context when errors, timeouts, or anomalies occur.
Uses custom JSON format (not CloudWatch EMF).
"""

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from amazon.opentelemetry.distro.serviceevents.models.resource_attributes import ResourceAttributes


@dataclass
class CallPathEntry:
    """
    Single function call in the execution path.

    Captures timing and caller information for each function invocation
    during request processing.
    """

    function_name: str  # Composite function identifier (e.g., "module/path.func")
    caller_function_name: Optional[str]  # Function that called this one (None if entry point)
    duration_ns: int  # Duration of function execution in nanoseconds
    error: bool = False  # True if this function threw the exception
    is_async: bool = False  # True if this is an async function


@dataclass
class ExceptionInfo:
    """
    Exception details with full call path.

    Captures exception type, message, stack trace, and the sequence of
    function calls that led to the exception.
    """

    exception_type: str  # Exception class name (e.g., "ValueError")
    exception_message: str  # Exception message
    stack_trace: str  # Formatted stack trace as single string
    call_path: List[CallPathEntry]  # Execution path leading to this exception


@dataclass
class RequestContext:
    """
    HTTP request context information.

    Captures details about the HTTP request that triggered the incident,
    including custom business context and request payload data.
    """

    type: str  # Request type (e.g., "http")
    timestamp: int  # Milliseconds since epoch when request started
    status_code: int  # HTTP status code
    custom_context: Dict[str, Any] = field(default_factory=dict)  # User-defined context

    # Request payload fields (captured only on incident triggers with lazy loading)
    request_body: Optional[Any] = None  # Request payload (JSON, form, or raw string)
    query_params: Optional[Dict[str, Any]] = None  # URL query parameters
    path_params: Optional[Dict[str, Any]] = None  # URL path parameters
    request_headers: Optional[Dict[str, str]] = None  # HTTP headers


@dataclass
class TelemetryCorrelation:
    """
    APM trace and business correlation identifiers.

    Links incidents to distributed traces, sessions, and business transactions
    for cross-system correlation.
    """

    trace_id: Optional[str] = None  # Distributed trace ID (OpenTelemetry, X-Ray, etc.)
    session_id: Optional[str] = None  # User session identifier
    span_id: Optional[str] = None  # Span ID within the trace
    request_id: Optional[str] = None  # Unique request identifier
    correlation_ids: Optional[Dict[str, str]] = None  # Business IDs


@dataclass
class IncidentSnapshot:
    """
    Incident snapshot telemetry event.

    Captures comprehensive context when errors, timeouts, or anomalies occur.
    Includes execution flow, exception details, request context, and correlation IDs.

    Example JSON output:
    {
      "snapshot_id": "snap_7f3e9a2c-4b1d-4e8f-9c5a-2d6b8e4f1a3c",
      "timestamp": 1706745600000,
      "severity": "critical",
      "trigger_type": "exception",
      "service": "user-service",
      "environment": "production",
      "instance_id": "ip-172-31-42-123",
      "operation": "GET /api/users/profile",
      "sdk_version": "0.14.2",
      "git_commit_sha": "abc123def456",
      "deployment_id": "12345",
      "pid": 25464,
      "duration_ms": 1250.5,
      "exception_info": [{...}],
      "request_context": {...},
      "telemetry_correlation": {...}
    }
    """

    # Non-default fields first
    snapshot_id: str  # Unique identifier for this incident (UUID)
    timestamp: int  # Milliseconds since epoch when incident occurred
    severity: str  # "critical", "high", "medium", "low"
    trigger_type: str  # "exception", "latency"
    service: str  # Service name
    environment: Optional[str]  # Deployment environment; None/empty when unset (omitted from output)
    instance_id: str  # Host/instance identifier (hostname or container ID)
    operation: str  # HTTP method + route (e.g., "GET /api/users/<id>")
    sdk_version: str  # ServiceEvents SDK version
    pid: int  # Process ID
    duration_ms: float  # Total request duration in milliseconds
    exception_info: List[ExceptionInfo]  # Exception details (may be multiple)
    request_context: RequestContext  # HTTP request context
    telemetry_correlation: TelemetryCorrelation  # APM and business correlation IDs

    # Default fields
    telemetry_type: str = "IncidentSnapshot"  # Static telemetry type identifier
    sdk_lang: str = "python"  # SDK language identifier

    # Deployment Metadata
    git_commit_sha: Optional[str] = None  # Git commit SHA from deployment
    deployment_id: Optional[str] = None  # CI/CD deployment identifier

    # True when call_path timing data is missing (first incident on endpoint, sampling was off)
    # Only present in serialized JSON when True (sparse pattern)
    is_partial: bool = False

    # AWS platform resource attributes (cloud, host, container, k8s)
    resource_attributes: Optional["ResourceAttributes"] = None

    def to_dict(self) -> Dict:
        """
        Convert to dictionary for JSON serialization.

        Applies sparse pattern: is_async is omitted from call_path entries when False.
        Serializes resource_attributes using OTel dot-notation keys.

        Returns:
            Dictionary representation suitable for JSON export
        """
        result = asdict(self)
        # Omit environment entirely when unset (no sentinel value)
        if not self.environment:
            result.pop("environment", None)
        # Strip is_async=False from call_path entries (sparse pattern)
        for exc_info in result.get("exception_info", []):
            for entry in exc_info.get("call_path", []):
                if not entry.get("is_async"):
                    entry.pop("is_async", None)

        # Strip only the misleading zero durations from a partial snapshot. A partial
        # snapshot mixes sampled frames (real duration_ns) with unsampled ones (duration_ns
        # == 0, which reads as "instantaneous" and is misleading). Drop only the zeros so the
        # genuine per-frame timings that WERE captured are preserved.
        if self.is_partial:
            for exc_info in result.get("exception_info", []):
                for entry in exc_info.get("call_path", []):
                    if not entry.get("duration_ns"):
                        entry.pop("duration_ns", None)

        # Strip correlation_ids from telemetry_correlation when None
        tc = result.get("telemetry_correlation", {})
        if tc.get("correlation_ids") is None:
            tc.pop("correlation_ids", None)

        # Serialize resource_attributes using OTel dot-notation keys
        if self.resource_attributes is not None:
            result["resource_attributes"] = self.resource_attributes.to_dict()
        else:
            result["resource_attributes"] = {}

        return result
