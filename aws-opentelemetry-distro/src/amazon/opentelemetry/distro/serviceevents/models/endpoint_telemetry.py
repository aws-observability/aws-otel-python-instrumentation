# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data models for endpoint telemetry.

Defines structured schemas for EndpointMetricEvent that captures
aggregated HTTP endpoint metrics including error breakdown, duration histograms,
fault/error counts, and incident exemplars.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

from amazon.opentelemetry.distro.serviceevents.models.function_telemetry import DurationMetrics

if TYPE_CHECKING:
    from amazon.opentelemetry.distro.serviceevents.models.resource_attributes import ResourceAttributes


@dataclass
class ErrorDetail:
    """
    Single error detail with type and origin function.

    Links an error type to the specific function where it originated.
    """

    error_type: str  # Exception class name (e.g., "ValueError", "TimeoutException")
    function_name: str  # Composite name of function where error originated


@dataclass
class ErrorBreakdownEntry:
    """
    Error breakdown entry grouping errors by HTTP status code.

    Represents a specific error pattern (error type + function + status code)
    and how many times it occurred during the aggregation period.

    Note: Each entry contains ONE primary error (not all errors from call path)
    to avoid noise in telemetry data.
    """

    errors: List[ErrorDetail]  # List with single error (primary/last error only)
    count: int  # Number of occurrences of this error pattern
    failure_type: str  # HTTP status code as string (e.g., "500", "404")


@dataclass
class IncidentExemplar:
    """
    Lightweight reference to an incident snapshot.

    Embedded in EndpointSummary telemetry to link endpoint metrics
    to incidents that occurred during the collection period.
    """

    snapshot_id: str  # Links to full IncidentSnapshot for detail lookup
    trigger_type: str  # "exception", "latency"
    severity: str  # "critical", "high", "medium", "low"
    timestamp: int  # Epoch milliseconds when incident occurred


@dataclass
class EndpointMetricEvent:
    """
    Endpoint metric telemetry event.

    Represents aggregated metrics for an HTTP endpoint over a collection period,
    emitted as an EndpointSummary OTLP LogRecord (plus per-error-type counter
    data points via to_error_type_metrics).

    Example JSON output:
    {
      "telemetry_type": "EndpointSummary",
      "operation": "GET /api/users/profile",
      "count": 1523,
      "faults": 5,
      "errors": 12,
      "incident_count": 2,
      "environment": "production",
      "service_name": "user-service",
      "instance_id": "ip-172-31-42-123",
      "sdk_version": "0.14.2",
      "git_commit_sha": "abc123def456",
      "deployment_id": "12345",
      "pid": 23165,
      "timestamp": "2026-01-21T00:02:37.189806+00:00",
      "duration": {
        "Values": [100.5, 200.3],
        "Counts": [10, 5],
        "Max": 500.0,
        "Min": 10.0,
        "Count": 1523,
        "Sum": 150000.0
      },
      "error_breakdown": [...],
      "incidents_exemplar": [
        {"snapshot_id": "snap_abc123", "trigger_type": "exception", "severity": "critical", "timestamp": 1706745600000}
      ]
    }
    """

    # Metadata (non-default fields first)
    environment: Optional[str]  # Deployment environment; None/empty when unset (omitted from output)
    service_name: str  # Service name from OTEL_SERVICE_NAME
    sdk_version: str  # ServiceEvents SDK version
    instance_id: str  # Host/instance identifier (hostname or container ID)

    # Endpoint Identification
    operation: str  # HTTP method + route (e.g., "GET /api/users/<id>")

    # Process and Timing
    pid: int  # Process ID
    timestamp: str  # ISO 8601 timestamp of collection

    # Core Metrics
    count: int  # Total number of requests to this endpoint

    # Metadata (default fields)
    telemetry_type: str = "EndpointSummary"  # Static telemetry type identifier
    sdk_lang: str = "python"  # SDK language identifier
    method: Optional[str] = None  # HTTP method (e.g., "GET", "POST")
    route: Optional[str] = None  # Route pattern (e.g., "/api/users/<id>")

    # Deployment Metadata
    git_commit_sha: Optional[str] = None  # Git commit SHA from deployment
    deployment_id: Optional[str] = None  # CI/CD deployment identifier

    faults: int = 0  # Count of HTTP 5xx responses
    errors: int = 0  # Count of HTTP 4xx responses
    incident_count: int = 0  # Number of incidents during this collection period
    error_breakdown: List[ErrorBreakdownEntry] = field(default_factory=list)
    incidents_exemplar: List[IncidentExemplar] = field(default_factory=list)  # Incident references

    # Duration histogram
    duration: Optional[DurationMetrics] = None

    # AWS platform resource attributes (cloud, host, container, k8s)
    resource_attributes: Optional["ResourceAttributes"] = None

    def to_error_type_metrics(self) -> List["EndpointErrorMetric"]:
        """Generate EndpointErrorMetric instances from error_breakdown.

        Returns:
            List of EndpointErrorMetric, one per error type. Empty list if no errors.
        """
        results = []
        for entry in self.error_breakdown:
            for error_detail in entry.errors:
                results.append(
                    EndpointErrorMetric(
                        environment=self.environment,
                        service_name=self.service_name,
                        operation=self.operation,
                        instance_id=self.instance_id,
                        pid=self.pid,
                        exception=error_detail.error_type,
                        count=entry.count,
                    )
                )
        return results


@dataclass
class EndpointErrorMetric:
    """
    Per-error-type metric event.

    Each instance represents a single error type observed at an endpoint
    during a collection period. Emitted as OTel Counter data points via
    ServiceEventsOtlpEmitter.emit_endpoint_error_metrics.
    """

    environment: Optional[str]
    service_name: str
    operation: str
    instance_id: str
    pid: int
    exception: str
    count: int
    telemetry_type: str = "EndpointErrorMetric"
    sdk_lang: str = "python"  # SDK language identifier
