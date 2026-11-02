# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
ServiceEvents instrumentation for AWS OpenTelemetry Python.

This package provides deep observability capabilities including:
- Function-level invocation metrics
- HTTP endpoint performance tracking
- Automated error investigation on failures
"""

# Names are resolved by the lazy __getattr__ below; the E0603 here is a false positive.
# pylint: disable=undefined-all-variable
__all__ = [
    "ServiceEventsConfig",
    "ServiceEventsInstrumentation",
    "get_serviceevents_instrumentation",
]
# pylint: enable=undefined-all-variable


def __getattr__(name):
    """Lazy import to avoid circular dependencies."""
    # Lazy imports below intentionally live inside __getattr__ to avoid circular dependencies.
    if name == "ServiceEventsConfig":
        from amazon.opentelemetry.distro.serviceevents.config import (  # pylint: disable=import-outside-toplevel
            ServiceEventsConfig,
        )

        return ServiceEventsConfig
    if name == "ServiceEventsInstrumentation":
        # pylint: disable-next=import-outside-toplevel
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import ServiceEventsInstrumentation

        return ServiceEventsInstrumentation
    if name == "get_serviceevents_instrumentation":
        # pylint: disable-next=import-outside-toplevel
        from amazon.opentelemetry.distro.serviceevents.serviceevents_instrumentation import (
            get_serviceevents_instrumentation,
        )

        return get_serviceevents_instrumentation
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
