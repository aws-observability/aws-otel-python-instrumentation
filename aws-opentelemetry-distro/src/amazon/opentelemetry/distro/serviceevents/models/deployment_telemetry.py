# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data models for DeploymentEvent telemetry.

DeploymentEvent telemetry captures deployment metadata (git commit, CI/CD info)
for the instrumented service. Emitted once at startup and whenever deployment
context changes.
"""

import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from amazon.opentelemetry.distro.serviceevents.models.resource_attributes import ResourceAttributes


@dataclass
class DeploymentContext:
    """
    Git repository context for the deployment event.

    Provides traceability from instrumented functions back to source code
    in version control.
    """

    # Default to empty (not a sentinel): an unset field is absent, so downstream
    # emitters omit it rather than shipping a placeholder value onto the wire.
    git_repo_url: str = ""  # Repository URL (e.g., "https://github.com/org/repo")
    git_commit_sha: str = ""  # Commit SHA hash
    deployment_url: str = ""  # CI/CD workflow run URL
    deployment_timestamp: str = ""  # CI/CD workflow run timestamp
    deployment_id: str = ""  # CI/CD deployment identifier (e.g., run ID)

    @classmethod
    def from_environment(cls) -> "DeploymentContext":
        """
        Create DeploymentContext from environment variables.

        Reads from standard Git/CI environment variables:
        - OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL
        - OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA
        - OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_URL
        - OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_TIMESTAMP
        - OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID
        """
        return cls(
            git_repo_url=os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL", ""),
            git_commit_sha=os.getenv("OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA", ""),
            deployment_url=os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_URL", ""),
            deployment_timestamp=os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_TIMESTAMP", ""),
            deployment_id=os.getenv("OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID", ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class DeploymentEventTelemetry:
    """
    DeploymentEvent telemetry event.

    Captures deployment metadata (git commit, CI/CD info, SDK version) for the
    instrumented service. Emitted once at startup.

    Example output:
    {
        "telemetry_type": "DeploymentEvent",
        "timestamp": "2026-02-04T00:10:00.000000+00:00",
        "service_name": "serviceevents-demo",
        "environment": "prod",
        "instance_id": "ip-172-31-42-123",
        "sdk_version": "0.14.2.dev0",
        "pid": 12956,
        "deployment_context": {
            "git_repo_url": "https://github.com/org/repo",
            "git_commit_sha": "abc123",
            "deployment_url": "https://github.com/org/repo/actions/runs/12345",
            "deployment_timestamp": "2026-02-04T00:00:00Z",
            "deployment_id": "12345"
        }
    }
    """

    # Required fields
    service_name: str  # Service name from OTEL_SERVICE_NAME
    environment: Optional[str]  # Deployment environment; None/empty when unset (omitted from output)
    instance_id: str  # Host/instance identifier (hostname or container ID)
    sdk_version: str  # ServiceEvents SDK version
    pid: int  # Process ID

    # Optional fields with defaults
    telemetry_type: str = "DeploymentEvent"  # Static telemetry type identifier
    sdk_lang: str = "python"  # SDK language identifier
    timestamp: str = ""  # ISO 8601 timestamp (set in __post_init__ if empty)
    deployment_context: Optional[DeploymentContext] = None  # Git repository context

    # AWS platform resource attributes (cloud, host, container, k8s)
    resource_attributes: Optional["ResourceAttributes"] = None

    def __post_init__(self):
        """Set timestamp if not provided."""
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

    @classmethod
    def create(
        cls,
        service_name: str = "unknown-service",
        environment: Optional[str] = None,
        instance_id: Optional[str] = None,
        sdk_version: str = "0.14.2",
        pid: Optional[int] = None,
        include_deployment_context: bool = True,
        resource_attributes: Optional["ResourceAttributes"] = None,
    ) -> "DeploymentEventTelemetry":
        """
        Create a DeploymentEventTelemetry instance.

        Args:
            service_name: Service name for the telemetry metadata.
            environment: Environment name for the telemetry metadata.
            instance_id: Host/instance identifier (defaults to get_instance_id()).
            sdk_version: SDK version string.
            pid: Process ID (defaults to current process).
            include_deployment_context: Whether to include deployment context from environment.
            resource_attributes: Optional ResourceAttributes from OTel Resource detectors.

        Returns:
            DeploymentEventTelemetry instance.
        """
        # Lazy import to break the circular dependency between models and utils.
        from amazon.opentelemetry.distro.serviceevents.utils import (  # pylint: disable=import-outside-toplevel
            get_instance_id,
        )

        return cls(
            service_name=service_name,
            environment=environment,
            instance_id=instance_id if instance_id is not None else get_instance_id(),
            sdk_version=sdk_version,
            pid=pid if pid is not None else os.getpid(),
            deployment_context=DeploymentContext.from_environment() if include_deployment_context else None,
            resource_attributes=resource_attributes,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation suitable for telemetry export.
        """
        result = {
            "telemetry_type": self.telemetry_type,
            "sdk_lang": self.sdk_lang,
            "timestamp": self.timestamp,
            "service_name": self.service_name,
            "instance_id": self.instance_id,
            "sdk_version": self.sdk_version,
            "pid": self.pid,
        }

        # Omit environment entirely when unset (no sentinel value)
        if self.environment:
            result["environment"] = self.environment

        if self.deployment_context:
            result["deployment_context"] = self.deployment_context.to_dict()

        if self.resource_attributes is not None and not self.resource_attributes.is_empty():
            result["resource_attributes"] = self.resource_attributes.to_dict()

        return result
