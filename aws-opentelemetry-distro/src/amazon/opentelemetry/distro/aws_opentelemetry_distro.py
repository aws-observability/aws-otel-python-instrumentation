# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

from amazon.opentelemetry.distro.patches._instrumentation_patch import apply_instrumentation_patches
from opentelemetry.distro import OpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
    OTEL_EXPORTER_OTLP_PROTOCOL,
)


class AwsOpenTelemetryDistro(OpenTelemetryDistro):
    def _configure(self, **kwargs):
        """Sets up default environment variables and apply patches

        Set default OTEL_EXPORTER_OTLP_PROTOCOL to be HTTP. This must be run before super(), which attempts to set the
        default to gRPC. If we run afterwards, we don't know if the default was set by base OpenTelemetryDistro or if it
        was set by the user. We are setting to HTTP as gRPC does not work out of the box for the vended docker image,
        due to gRPC having a strict dependency on the Python version the artifact was built for (OTEL observed this:
        https://github.com/open-telemetry/opentelemetry-operator/blob/461ba68e80e8ac6bf2603eb353547cd026119ed2/autoinstrumentation/python/requirements.txt#L2-L3)

        Also sets default OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR, and
        OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION to ensure good compatibility with X-Ray and Application
        Signals.

        Also applies patches to upstream instrumentation - usually these are stopgap measures until we can contribute
        long-term changes to upstream.

        kwargs:
            apply_patches: bool - apply patches to upstream instrumentation. Default is True.

        TODO:
         1. OTLPMetricExporterMixin is using hard coded histogram_aggregation_type, which reads
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION environment variable. Need to work with upstream to
            make it to be configurable.
        """
        os.environ.setdefault(OTEL_EXPORTER_OTLP_PROTOCOL, "http/protobuf")

        super(AwsOpenTelemetryDistro, self)._configure()

        os.environ.setdefault(OTEL_PROPAGATORS, "xray,tracecontext,b3,b3multi")
        os.environ.setdefault(OTEL_PYTHON_ID_GENERATOR, "xray")
        os.environ.setdefault(
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION, "base2_exponential_bucket_histogram"
        )

        if kwargs.get("apply_patches", True):
            apply_instrumentation_patches()
