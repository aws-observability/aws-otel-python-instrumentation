# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
EndpointMetricCollector - Collects and exports HTTP endpoint metrics.
"""

import logging
import os
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from amazon.opentelemetry.distro.serviceevents.collectors.base_collector import BaseCollector
from amazon.opentelemetry.distro.serviceevents.models import (
    DurationMetrics,
    EndpointMetricEvent,
    ErrorBreakdownEntry,
    ErrorDetail,
    IncidentExemplar,
    ResourceAttributes,
)
from amazon.opentelemetry.distro.serviceevents.utils import get_instance_id
from amazon.opentelemetry.distro.serviceevents.utils.seh_histogram import SEHHistogram

logger = logging.getLogger(__name__)


class EndpointMetricCollector(BaseCollector):
    """
    Collector for HTTP endpoint metrics using CloudWatch EMF format.

    Aggregates metrics by operation (method + route) and periodically exports.
    ServiceEvents endpoint telemetry format for CloudWatch integration.
    """

    def __init__(
        self,
        flush_interval_ms: int,
        environment: Optional[str] = None,
        service_name: Optional[str] = None,
        sdk_version: str = "0.14.2",
        resource_attributes: Optional[ResourceAttributes] = None,
        otlp_emitter=None,
        suppress_endpoint_summary: bool = False,
    ):
        """
        Initialize the endpoint metric collector.

        Args:
            flush_interval_ms: How often to collect and export data (milliseconds)
            environment: Deployment environment (e.g., "production", "staging")
            service_name: Service name from OTEL_SERVICE_NAME
            sdk_version: ServiceEvents SDK version
            resource_attributes: AWS platform resource attributes from OTel Resource detectors
            otlp_emitter: Optional ServiceEventsOtlpEmitter for OTLP export
            suppress_endpoint_summary: When True, skip emitting EndpointSummary LogRecords
                (Application Signals already carries equivalent per-endpoint metrics).
                The collector still runs to feed per-endpoint latency histograms into
                IncidentSnapshot's threshold triggering and to honor endpoint
                include/exclude filters.
        """
        super().__init__(flush_interval_ms, "EndpointMetricCollector", otlp_emitter)
        self.suppress_endpoint_summary = suppress_endpoint_summary

        # Environment from config (None/empty when unset — omitted from emitted signals)
        self.environment = environment

        # Get service name from config or env var
        self.service_name = service_name or os.getenv("OTEL_SERVICE_NAME", "UnknownService")

        self.sdk_version = sdk_version
        self.git_commit_sha = os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA")
        self.deployment_id = os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID")
        self.pid = os.getpid()
        self.resource_attributes = resource_attributes or ResourceAttributes()
        self.instance_id = get_instance_id()

        # Enhance instance_id: prefer host.id from resource attributes (e.g., EC2 instance ID)
        if self.resource_attributes.host_id:
            self.instance_id = self.resource_attributes.host_id

        # Aggregations: operation -> metrics
        # Structure: {operation: {route, method, count, sketch, error_breakdown}}
        self._aggregations: Dict[str, Dict] = {}
        self._aggregations_lock = threading.Lock()

    def _reset_for_fork(self):
        """Reset collector state after fork.

        The child inherits the parent's accumulated aggregations; left in place the child
        would re-emit the parent's metrics, double-counting. The aggregations lock may also
        have been held by a parent daemon thread at fork time (those threads do not survive
        fork), so it is recreated to avoid an unreleasable-lock deadlock. Safe to mutate
        without holding the old lock: os.register_at_fork's after_in_child hook runs
        single-threaded in the child.
        """
        super()._reset_for_fork()
        self.pid = os.getpid()
        self._aggregations = {}
        self._aggregations_lock = threading.Lock()

    def record_request(
        self,
        route: str,
        method: str,
        status_code: int,
        duration_ns: int,
        error_info: Optional[Dict] = None,
    ):
        """
        Record an HTTP request with optional error information.

        Args:
            route: Route pattern (e.g., "/users/<id>")
            method: HTTP method (e.g., "GET")
            status_code: HTTP status code (e.g., 200, 500)
            duration_ns: Request duration in nanoseconds
            error_info: Optional dict with {error_type, function_name} for errors
        """
        # Generate operation key (method + route)
        operation = f"{method} {route}"

        with self._aggregations_lock:
            if operation not in self._aggregations:
                self._aggregations[operation] = {
                    "route": route,
                    "method": method,
                    "count": 0,
                    "faults": 0,
                    "errors": 0,
                    "sum_duration": 0,
                    "seh_histogram": SEHHistogram(max_buckets=100),
                    "error_breakdown": defaultdict(
                        lambda: defaultdict(lambda: {"error_type": "", "function_name": "", "count": 0})
                    ),
                    "incidents_exemplar": [],
                }

            agg = self._aggregations[operation]
            agg["count"] += 1
            agg["sum_duration"] += duration_ns
            agg["seh_histogram"].record(float(duration_ns))

            # Track faults (5xx) and errors (4xx)
            if status_code >= 500:
                agg["faults"] += 1
            elif status_code >= 400:
                agg["errors"] += 1

            # Track error_breakdown if error occurred
            if status_code >= 400 and error_info:
                error_type = error_info.get("error_type", "UnknownError")
                function_name = error_info.get("function_name", "unknown")

                # Create unique key for this error pattern
                error_key = f"{error_type}:{function_name}"
                failure_type = str(status_code)

                # Initialize or increment error count
                if agg["error_breakdown"][failure_type][error_key]["count"] == 0:
                    # First time seeing this error pattern
                    agg["error_breakdown"][failure_type][error_key]["error_type"] = error_type
                    agg["error_breakdown"][failure_type][error_key]["function_name"] = function_name

                agg["error_breakdown"][failure_type][error_key]["count"] += 1

    def record_incident_exemplar(self, operation: str, exemplar: Dict):
        """
        Record an incident exemplar for an endpoint.

        Called from framework hooks when an incident snapshot is created,
        linking the incident to the endpoint's aggregation window.

        Args:
            operation: Operation string (e.g., "GET /api/users")
            exemplar: Dict with snapshot_id, trigger_type, severity, timestamp
        """
        with self._aggregations_lock:
            if operation in self._aggregations:
                self._aggregations[operation]["incidents_exemplar"].append(exemplar)
            else:
                logger.debug("No aggregation entry for operation %s, skipping exemplar", operation)

    def collect(self):
        """Collect aggregated endpoint metrics and emit them via the OTLP emitter."""
        # Get and swap aggregations (atomically retrieves and clears)
        aggregations = self._get_and_swap_aggregations()

        if not aggregations:
            logger.debug("No endpoint metrics to export")
            return

        if not self.otlp_emitter:
            return

        # Format and export
        endpoint_events = self._format_endpoint_metrics(aggregations)

        if endpoint_events:
            for event in endpoint_events:
                # Suppress EndpointSummary when Application Signals is enabled —
                # App Signals emits equivalent per-endpoint duration + error metrics,
                # so emitting both would duplicate data on the backend. Error metrics
                # (EndpointErrorMetric) carry ServiceEvents-specific per-exception-type
                # breakdown that App Signals doesn't, so those still emit.
                if not self.suppress_endpoint_summary:
                    self.otlp_emitter.emit_endpoint_summary(event)
                error_metrics = event.to_error_type_metrics()
                if error_metrics:
                    self.otlp_emitter.emit_endpoint_error_metrics(error_metrics)
            logger.info("Exported %d endpoint metrics", len(endpoint_events))

    def _get_and_swap_aggregations(self) -> Dict:
        """
        Atomically get current aggregations and replace with empty dict.

        Returns:
            Current aggregations dictionary
        """
        with self._aggregations_lock:
            aggregations = self._aggregations
            self._aggregations = {}
            return aggregations

    # pylint: disable-next=too-many-locals
    def _format_endpoint_metrics(self, aggregations: Dict) -> List[EndpointMetricEvent]:
        """
        Format aggregation data into EndpointMetricEvent objects.

        Args:
            aggregations: Dictionary of operation -> aggregation data

        Returns:
            List of EndpointMetricEvent objects
        """
        events = []
        timestamp = datetime.now(timezone.utc).isoformat()

        for operation, agg in aggregations.items():
            count = agg.get("count", 0)
            if count == 0:
                continue

            # Convert error_breakdown nested dict to list of ErrorBreakdownEntry
            error_breakdown_list = []
            for failure_type, error_dict in agg["error_breakdown"].items():
                for _error_key, error_data in error_dict.items():
                    if error_data["count"] > 0:
                        error_breakdown_list.append(
                            ErrorBreakdownEntry(
                                errors=[
                                    ErrorDetail(
                                        error_type=error_data["error_type"], function_name=error_data["function_name"]
                                    )
                                ],
                                count=error_data["count"],
                                failure_type=failure_type,
                            )
                        )

            # Convert SEH histogram to duration metrics
            duration_metrics = self._convert_to_emf_histogram(
                agg.get("seh_histogram"), agg.get("sum_duration", 0), count
            )

            # Convert raw exemplar dicts to IncidentExemplar objects
            raw_exemplars = agg.get("incidents_exemplar", [])
            exemplar_objects = [
                IncidentExemplar(
                    snapshot_id=ex["snapshot_id"],
                    trigger_type=ex["trigger_type"],
                    severity=ex["severity"],
                    timestamp=ex["timestamp"],
                )
                for ex in raw_exemplars
            ]

            # Create EndpointMetricEvent object
            event = EndpointMetricEvent(
                environment=self.environment,
                service_name=self.service_name,
                sdk_version=self.sdk_version,
                instance_id=self.instance_id,
                operation=operation,
                method=agg.get("method"),
                route=agg.get("route"),
                pid=self.pid,
                timestamp=timestamp,
                count=count,
                git_commit_sha=self.git_commit_sha,
                deployment_id=self.deployment_id,
                faults=agg.get("faults", 0),
                errors=agg.get("errors", 0),
                incident_count=len(exemplar_objects),
                error_breakdown=error_breakdown_list,
                incidents_exemplar=exemplar_objects,
                duration=duration_metrics,
                resource_attributes=self.resource_attributes,
            )

            events.append(event)

        return events

    @staticmethod
    def _convert_to_emf_histogram(seh_histogram, sum_duration: int, count: int) -> DurationMetrics:
        """
        Convert SEH histogram to EMF histogram format.

        Args:
            seh_histogram: SEHHistogram object containing aggregated durations
            sum_duration: Sum of all durations in nanoseconds (converted to microseconds)
            count: Total number of invocations

        Returns:
            DurationMetrics object with EMF histogram data (values in microseconds)
        """
        if not seh_histogram or seh_histogram.is_empty():
            return DurationMetrics(
                values=[],
                counts=[],
                max=0,
                min=0,
                count=0,
                sum=0,
            )

        # Get aggregated buckets from SEH histogram
        values, counts_list = seh_histogram.get_values_and_counts()
        stats = seh_histogram.get_statistics()

        # Convert from nanoseconds to microseconds
        values_us = [v / 1000.0 for v in values]
        max_us = stats["max"] / 1000.0
        min_us = stats["min"] / 1000.0
        sum_us = sum_duration / 1000.0

        return DurationMetrics(
            values=values_us,
            counts=counts_list,
            max=max_us,
            min=min_us,
            count=count,
            sum=sum_us,
        )
