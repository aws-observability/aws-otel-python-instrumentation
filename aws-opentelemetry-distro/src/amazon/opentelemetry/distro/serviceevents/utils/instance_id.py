# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Instance ID utility for ServiceEvents telemetry.

Provides a consistent host identifier that works across different deployment
environments (on-premise, containers, VMs, cloud instances).
"""

import os
import socket
from typing import Optional

# Cache the instance ID to avoid repeated lookups
_cached_instance_id: Optional[str] = None


def get_instance_id() -> str:
    """
    Get the host/instance identifier for telemetry.

    Works across different environments:
    - Containers: Returns container hostname (usually container ID or pod name)
    - VMs/EC2: Returns the VM hostname
    - On-premise: Returns the machine hostname

    Priority:
    1. INSTANCE_ID environment variable (common in cloud environments)
    2. HOSTNAME environment variable (set in many container runtimes)
    3. socket.gethostname() (fallback)

    Returns:
        String identifier for the current host/instance.
    """
    # Module-level cache for the resolved instance ID.
    global _cached_instance_id  # pylint: disable=global-statement

    if _cached_instance_id is not None:
        return _cached_instance_id

    # Try environment variables first (allows explicit override)
    instance_id = os.getenv("INSTANCE_ID") or os.getenv("HOSTNAME")

    if not instance_id:
        # Fall back to socket hostname
        try:
            instance_id = socket.gethostname()
        except Exception:  # pylint: disable=broad-exception-caught
            # Telemetry must never crash the customer app; fall back safely.
            instance_id = "unknown"

    # Cache the result
    _cached_instance_id = instance_id
    return instance_id


def clear_instance_id_cache():
    """Clear the cached instance ID (mainly for testing)."""
    # Module-level cache for the resolved instance ID.
    global _cached_instance_id  # pylint: disable=global-statement
    _cached_instance_id = None
