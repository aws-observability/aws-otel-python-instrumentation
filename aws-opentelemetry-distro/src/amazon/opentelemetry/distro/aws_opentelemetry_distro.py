# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# flake8: noqa: E402
# pylint: disable=wrong-import-position
# ========================================================================
# Apply the Gevent's patching as the very first step in the distro.
# IMPORTANT: Do not put any imports before the following 2 lines.
# Read the comments in the _gevent_patches.py for details.
from amazon.opentelemetry.distro.patches._gevent_patches import apply_gevent_monkey_patch

apply_gevent_monkey_patch()
# ========================================================================
import importlib
import os
import sys
from logging import ERROR, Logger, getLogger

from amazon.opentelemetry.distro._utils import (
    OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS,
    get_aws_region,
    is_agent_observability_enabled,
    is_installed,
)
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
from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.logging import LEVELS
from opentelemetry.instrumentation.logging.environment_variables import OTEL_PYTHON_LOG_LEVEL
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
    OTEL_EXPORTER_OTLP_PROTOCOL,
)

_logger: Logger = getLogger(__name__)
# Suppress configurator warnings from auto-instrumentation
_load._logger.setLevel(LEVELS.get(os.environ.get(OTEL_PYTHON_LOG_LEVEL, "error").lower(), ERROR))


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

        # Check if Django is installed and determine if Django instrumentation should be enabled
        if is_installed("django"):
            # Django instrumentation is allowed when DJANGO_SETTINGS_MODULE is set
            if not os.getenv("DJANGO_SETTINGS_MODULE"):
                # DJANGO_SETTINGS_MODULE is not set, disable Django instrumentation
                disabled_instrumentations = os.getenv("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "")
                os.environ["OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"] = disabled_instrumentations + ",django"
                _logger.warning(
                    "Django is installed but DJANGO_SETTINGS_MODULE is not set. Disabling django instrumentation."
                )
            else:
                _logger.debug(
                    "Django instrumentation enabled: DJANGO_SETTINGS_MODULE=%s", os.getenv("DJANGO_SETTINGS_MODULE")
                )

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
            os.environ.setdefault(OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT, "true")
            os.environ.setdefault(OTEL_TRACES_SAMPLER, "parentbased_always_on")
            os.environ.setdefault(
                OTEL_PYTHON_DISABLED_INSTRUMENTATIONS,
                "http,sqlalchemy,psycopg2,pymysql,sqlite3,aiopg,asyncpg,mysql_connector,"
                "urllib3,requests,system_metrics,google-genai",
            )
            os.environ.setdefault(OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED, "true")
            os.environ.setdefault(APPLICATION_SIGNALS_ENABLED_CONFIG, "false")
            os.environ.setdefault(OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS, "false")

            version = os.environ.get("AGENT_OBSERVABILITY_VERSION", "1")
            if version == "1":
                _configure_agent_observability_v1(get_aws_region())
            else:
                _configure_agent_observability_v2()

        super(AwsOpenTelemetryDistro, self)._configure()

        if kwargs.get("apply_patches", True):
            apply_instrumentation_patches()


def _configure_agent_observability_v1(region: str) -> None:
    # "otlp" is already native OTel default, but we set them here to be explicit
    # about intended configuration for agent observability
    os.environ.setdefault(OTEL_TRACES_EXPORTER, "otlp")
    os.environ.setdefault(OTEL_LOGS_EXPORTER, "otlp")
    os.environ.setdefault(OTEL_METRICS_EXPORTER, "awsemf")
    if region:
        os.environ.setdefault(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, f"https://xray.{region}.amazonaws.com/v1/traces")
        os.environ.setdefault(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, f"https://logs.{region}.amazonaws.com/v1/logs")
    else:
        _logger.warning(
            "AWS region could not be determined. OTLP endpoints will not be automatically configured. "
            "Please set AWS_REGION environment variable or configure OTLP endpoints manually."
        )


def _configure_agent_observability_v2() -> None:
    os.environ.setdefault(OTEL_TRACES_EXPORTER, "otlp")
    os.environ.setdefault(OTEL_LOGS_EXPORTER, "otlp")
    os.environ.setdefault(OTEL_METRICS_EXPORTER, "otlp")
    os.environ.setdefault(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, "http://localhost:4318/v1/traces")
    os.environ.setdefault(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, "http://localhost:4318/v1/logs")
    os.environ.setdefault(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, "http://localhost:4318/v1/metrics")
