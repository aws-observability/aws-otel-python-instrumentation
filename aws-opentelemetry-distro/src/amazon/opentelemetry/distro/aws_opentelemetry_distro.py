# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from logging import getLogger

from opentelemetry.distro import OpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION

logger = getLogger(__name__)


class AwsOpenTelemetryDistro(OpenTelemetryDistro):
    def _configure(self, **kwargs):
        super(AwsOpenTelemetryDistro, self)._configure()
        os.environ.setdefault(
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION, "base2_exponential_bucket_histogram"
        )
        os.environ.setdefault(OTEL_PROPAGATORS, "xray,tracecontext,b3,b3multi")
        os.environ.setdefault(OTEL_PYTHON_ID_GENERATOR, "xray")
        # TODO:
        #  1. Verify if id generator and propagators work as expected.
        #  2. Unlike opentelemetry Java, "otel.aws.imds.endpointOverride" is not configured on python.
        #  Need to figure out if we rely on ec2 and eks resource to get context about the platform for python
        #  and if we need endpoint override support.
        #  3. Verify OTLPMetricExporter is using the ExponentialBucketHistogramAggregation.
        #  4. OTLPMetricExporterMixin is using harded coded histogram_aggregation_type,
        #  which reads OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION environment variable. Need to
        #  work with upstream to make it to be configurable.
