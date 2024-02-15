# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

from amazon.opentelemetry.distro._instrumentation_patch import apply_instrumentation_patches
from opentelemetry.distro import OpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION


class AwsOpenTelemetryDistro(OpenTelemetryDistro):
    APPLY_PATCHES: str = "apply_patches"

    def _configure(self, **kwargs):
        """
        kwargs:
            apply_patches: bool - apply patches to upstream instrumentation. Default is True.

        TODO:
         1. Unlike opentelemetry Java, "otel.aws.imds.endpointOverride" is not configured on python. Need to figure out
            if we rely on ec2 and eks resource to get context about the platform for python and if we need endpoint
            override support.
         2. OTLPMetricExporterMixin is using hard coded histogram_aggregation_type, which reads
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
        if kwargs.get("apply_patches", True):
            apply_instrumentation_patches()
