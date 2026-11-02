# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Data model for AWS platform resource attributes.

Encapsulates a curated set of OTel resource attributes from AWS detectors
(EC2, ECS, EKS) that provide platform context for serviceevents telemetry.
"""

from dataclasses import dataclass, fields
from typing import Dict, Optional

# Mapping from OTel semantic convention keys to dataclass field names
_OTEL_KEY_MAP = {
    "cloud.provider": "cloud_provider",
    "cloud.platform": "cloud_platform",
    "cloud.region": "cloud_region",
    "cloud.account.id": "cloud_account_id",
    "cloud.availability_zone": "cloud_availability_zone",
    "host.id": "host_id",
    "host.type": "host_type",
    "container.id": "container_id",
    "k8s.cluster.name": "k8s_cluster_name",
    "k8s.pod.name": "k8s_pod_name",
    "k8s.namespace.name": "k8s_namespace_name",
}

# Reverse mapping: field name -> OTel key
_FIELD_TO_OTEL_KEY = {v: k for k, v in _OTEL_KEY_MAP.items()}


@dataclass
class ResourceAttributes:
    """
    AWS platform resource attributes from OTel Resource detectors.

    Contains a curated set of cloud, host, container, and Kubernetes attributes
    that provide platform context in serviceevents telemetry output.

    Serialization uses OTel semantic convention dot-notation keys (e.g., "cloud.region")
    and is sparse (only non-None values are included).
    """

    cloud_provider: Optional[str] = None  # e.g., "aws"
    cloud_platform: Optional[str] = None  # e.g., "aws_ec2", "aws_ecs", "aws_eks"
    cloud_region: Optional[str] = None  # e.g., "us-east-1"
    cloud_account_id: Optional[str] = None  # e.g., "123456789012"
    cloud_availability_zone: Optional[str] = None  # e.g., "us-east-1a"
    host_id: Optional[str] = None  # e.g., "i-0abc123def456"
    host_type: Optional[str] = None  # e.g., "t3.medium"
    container_id: Optional[str] = None  # e.g., "abcdef123..."
    k8s_cluster_name: Optional[str] = None  # e.g., "my-cluster"
    k8s_pod_name: Optional[str] = None  # e.g., "my-pod-xyz"
    k8s_namespace_name: Optional[str] = None  # e.g., "default"

    @classmethod
    def from_otel_resource(cls, resource) -> "ResourceAttributes":
        """
        Create from OTel Resource object, extracting known attributes.

        Args:
            resource: OTel Resource object (opentelemetry.sdk.resources.Resource).
                      Only attributes in the allowlist are extracted.

        Returns:
            ResourceAttributes instance with detected values.
        """
        if resource is None:
            return cls()

        kwargs = {}
        for otel_key, field_name in _OTEL_KEY_MAP.items():
            value = resource.attributes.get(otel_key)
            if value is not None and str(value).strip():
                kwargs[field_name] = str(value)
        return cls(**kwargs)

    def to_dict(self) -> Dict[str, str]:
        """
        Serialize to dict using OTel dot-notation keys.

        Only includes non-None values (sparse serialization).

        Returns:
            Dict mapping OTel attribute keys to string values.
            Example: {"cloud.region": "us-east-1", "host.id": "i-0abc123"}
        """
        result = {}
        for field_info in fields(self):
            value = getattr(self, field_info.name)
            if value is not None and field_info.name in _FIELD_TO_OTEL_KEY:
                result[_FIELD_TO_OTEL_KEY[field_info.name]] = value
        return result

    def is_empty(self) -> bool:
        """Return True if no attributes are set."""
        return all(getattr(self, f.name) is None for f in fields(self))
