# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import importlib
import os
import sys
from logging import Logger, getLogger

from amazon.opentelemetry.distro._utils import get_aws_region, is_agent_observability_enabled
from amazon.opentelemetry.distro.aws_opentelemetry_configurator import (
    APPLICATION_SIGNALS_ENABLED_CONFIG,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT,
    OTEL_LOGS_EXPORTER,
    OTEL_METRICS_EXPORTER,
    OTEL_PYTHON_DISABLED_INSTRUMENTATIONS,
    OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED,
    OTEL_TRACES_EXPORTER,
    OTEL_TRACES_SAMPLER,
)
from amazon.opentelemetry.distro.patches._instrumentation_patch import apply_instrumentation_patches
from opentelemetry import propagate
from opentelemetry.distro import OpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
    OTEL_EXPORTER_OTLP_PROTOCOL,
)

_logger: Logger = getLogger(__name__)


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

        # Issue: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/2495
        # mimicking what is done here: https://tinyurl.com/54mvzmte
        # For handling applications like django running in containers, we are setting the current working directory
        # to the sys.path for the django application to find its executables.
        #
        # Note that we are updating the sys.path and not the PYTHONPATH env var, because once sys.path is
        # loaded upon process start, it doesn't refresh from the PYTHONPATH value.
        #
        # To be removed once the issue has been fixed in https://github.com/open-telemetry/opentelemetry-python-contrib
        cwd_path = os.getcwd()
        _logger.debug("Current working directory path: %s", cwd_path)
        if cwd_path not in sys.path:
            sys.path.insert(0, cwd_path)

        os.environ.setdefault(OTEL_EXPORTER_OTLP_PROTOCOL, "http/protobuf")

        if os.environ.get(OTEL_PROPAGATORS, None) is None:
            # xray is set after baggage in case xray propagator depends on the result of the baggage header extraction.
            os.environ.setdefault(OTEL_PROPAGATORS, "baggage,xray,tracecontext")
            # Issue: https://github.com/open-telemetry/opentelemetry-python/issues/4679
            # We need to explicitly reload the opentelemetry.propagate module here
            # because this module initializes the default propagators when it loads very early in the chain.
            # Without reloading the OTEL_PROPAGATOR config from this distro won't take any effect.
            # It's a hack from our end until OpenTelemetry fixes this behavior for distros to
            # override the default propagators.
            importlib.reload(propagate)

        os.environ.setdefault(OTEL_PYTHON_ID_GENERATOR, "xray")
        os.environ.setdefault(
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION, "base2_exponential_bucket_histogram"
        )

        if is_agent_observability_enabled():
            # "otlp" is already native OTel default, but we set them here to be explicit
            # about intended configuration for agent observability
            os.environ.setdefault(OTEL_TRACES_EXPORTER, "otlp")
            os.environ.setdefault(OTEL_LOGS_EXPORTER, "otlp")
            os.environ.setdefault(OTEL_METRICS_EXPORTER, "awsemf")

            # Set GenAI capture content default
            os.environ.setdefault(OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT, "true")

            region = get_aws_region()

            # Set OTLP endpoints with AWS region if not already set
            if region:
                os.environ.setdefault(
                    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, f"https://xray.{region}.amazonaws.com/v1/traces"
                )
                os.environ.setdefault(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, f"https://logs.{region}.amazonaws.com/v1/logs")
            else:
                _logger.warning(
                    "AWS region could not be determined. OTLP endpoints will not be automatically configured. "
                    "Please set AWS_REGION environment variable or configure OTLP endpoints manually."
                )

            # Set sampler default
            os.environ.setdefault(OTEL_TRACES_SAMPLER, "parentbased_always_on")

            # Set disabled instrumentations default
            os.environ.setdefault(
                OTEL_PYTHON_DISABLED_INSTRUMENTATIONS,
                "http,sqlalchemy,psycopg2,pymysql,sqlite3,aiopg,asyncpg,mysql_connector,"
                "urllib3,requests,system_metrics,google-genai",
            )

            # Set logging auto instrumentation default
            os.environ.setdefault(OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED, "true")

            # Disable AWS Application Signals by default
            os.environ.setdefault(APPLICATION_SIGNALS_ENABLED_CONFIG, "false")

        super(AwsOpenTelemetryDistro, self)._configure()

        if kwargs.get("apply_patches", True):
            apply_instrumentation_patches()
