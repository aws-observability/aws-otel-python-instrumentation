# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data models for ServiceEvents telemetry events.

This module re-exports all data models from the sub-modules for backward compatibility.
Existing code can continue to import from `serviceevents.models` without changes.
"""

# Deployment event telemetry models
from amazon.opentelemetry.distro.serviceevents.models.deployment_telemetry import (
    DeploymentContext,
    DeploymentEventTelemetry,
)

# Endpoint telemetry models
from amazon.opentelemetry.distro.serviceevents.models.endpoint_telemetry import (
    EndpointErrorMetric,
    EndpointMetricEvent,
    ErrorBreakdownEntry,
    ErrorDetail,
    IncidentExemplar,
)

# Function call duration model
from amazon.opentelemetry.distro.serviceevents.models.function_telemetry import DurationMetrics

# Incident snapshot telemetry models
from amazon.opentelemetry.distro.serviceevents.models.incident_telemetry import (
    CallPathEntry,
    ExceptionInfo,
    IncidentSnapshot,
    RequestContext,
    TelemetryCorrelation,
)

# Resource attributes model
from amazon.opentelemetry.distro.serviceevents.models.resource_attributes import ResourceAttributes

__all__ = [
    # Function call duration model
    "DurationMetrics",
    "ErrorDetail",
    "ErrorBreakdownEntry",
    "IncidentExemplar",
    "EndpointMetricEvent",
    "EndpointErrorMetric",
    # Incident snapshot models
    "CallPathEntry",
    "ExceptionInfo",
    "RequestContext",
    "TelemetryCorrelation",
    "IncidentSnapshot",
    # Deployment event models
    "DeploymentContext",
    "DeploymentEventTelemetry",
    # Resource attributes
    "ResourceAttributes",
]
