# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import sys
from logging import Logger, getLogger

import pkg_resources

from opentelemetry.distro import OpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION

_logger: Logger = getLogger(__name__)

patch_libraries = [
    {
        "library": "botocore ~= 1.0",
        "instrumentation": "opentelemetry-instrumentation-botocore==0.44b0.dev",
    },
]


class AwsOpenTelemetryDistro(OpenTelemetryDistro):
    def _configure(self, **kwargs):
        """
        kwargs:
            apply_patches: bool - apply patches to upstream instrumentation. Default is True.

        TODO:
         1. OTLPMetricExporterMixin is using hard coded histogram_aggregation_type, which reads
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION environment variable. Need to work with upstream to
            make it to be configurable.
        """
        super(AwsOpenTelemetryDistro, self)._configure()
        os.environ.setdefault(
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION, "base2_exponential_bucket_histogram"
        )
        os.environ.setdefault(OTEL_PROPAGATORS, "xray,tracecontext,b3,b3multi")
        os.environ.setdefault(OTEL_PYTHON_ID_GENERATOR, "xray")

        # Apply patches to upstream instrumentation - usually stopgap measures until we can contribute long-term changes
        if kwargs.get("apply_patches", True) and _check_patches():
            # pylint: disable=import-outside-toplevel
            # Delay import to only occur if patches are detected from the system.
            from amazon.opentelemetry.distro._instrumentation_patch import apply_instrumentation_patches

            apply_instrumentation_patches()


def _check_patches():
    for lib in patch_libraries:
        if not _is_installed(lib["library"]):
            return False
    return True


def _is_installed(req):
    if req in sys.modules:
        return True

    try:
        pkg_resources.get_distribution(req)
    except pkg_resources.DistributionNotFound:
        return False
    except pkg_resources.VersionConflict as exc:
        _logger.warning(
            "instrumentation for package %s is available but version %s is installed. Skipping.",
            exc.req,
            exc.dist.as_requirement(),  # pylint: disable=no-member
        )
        return False
    return True
