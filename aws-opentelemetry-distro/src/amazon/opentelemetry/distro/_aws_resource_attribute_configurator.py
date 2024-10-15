# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from amazon.opentelemetry.distro._aws_span_processing_util import UNKNOWN_SERVICE
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

# As per https://opentelemetry.io/docs/specs/semconv/resource/#service, if service name is not specified, SDK defaults
# the service name to unknown_service:<process name> or just unknown_service.
_OTEL_UNKNOWN_SERVICE_PREFIX: str = "unknown_service"


def get_service_attribute(resource: Resource) -> (str, bool):
    """Service is always derived from SERVICE_NAME"""
    service: str = resource.attributes.get(SERVICE_NAME)

    # In practice the service name is never None, but we can be defensive here.
    if service is None or service.startswith(_OTEL_UNKNOWN_SERVICE_PREFIX):
        return UNKNOWN_SERVICE, True

    return service, False
