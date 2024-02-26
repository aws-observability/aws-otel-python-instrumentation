# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from logging import Logger, getLogger
from typing import ClassVar, Dict, Type

from importlib_metadata import version
from typing_extensions import override

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter_builder import (
    AwsMetricAttributesSpanExporterBuilder,
)
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as OTLPGrpcOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as OTLPHttpOTLPMetricExporter
from opentelemetry.sdk._configuration import (
    _get_exporter_names,
    _get_id_generator,
    _get_sampler,
    _import_exporters,
    _import_id_generator,
    _import_sampler,
    _init_logging,
    _init_metrics,
    _OTelSDKConfigurator,
)
from opentelemetry.sdk.environment_variables import (
    _OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED,
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_TRACES_SAMPLER_ARG,
)
from opentelemetry.sdk.extension.aws.resource.ec2 import AwsEc2ResourceDetector
from opentelemetry.sdk.extension.aws.resource.ecs import AwsEcsResourceDetector
from opentelemetry.sdk.extension.aws.resource.eks import AwsEksResourceDetector
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.instrument import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import AggregationTemporality, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, get_aggregated_resources
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import set_tracer_provider

OTEL_AWS_APP_SIGNALS_ENABLED = "OTEL_AWS_APP_SIGNALS_ENABLED"
OTEL_METRIC_EXPORT_INTERVAL = "OTEL_METRIC_EXPORT_INTERVAL"
OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT = "OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT"
OTEL_AWS_SMP_EXPORTER_ENDPOINT = "OTEL_AWS_SMP_EXPORTER_ENDPOINT"
DEFAULT_METRIC_EXPORT_INTERVAL = 60000.0

_logger: Logger = getLogger(__name__)


class AwsOpenTelemetryConfigurator(_OTelSDKConfigurator):
    """
    This AwsOpenTelemetryConfigurator extend _OTelSDKConfigurator configuration with the following change:

    - Use AlwaysRecordSampler to record all spans.
    - Add SpanMetricsProcessor to create metrics.
    - Add AttributePropagatingSpanProcessor to propagate span attributes from parent to child spans.
    - Add AwsMetricAttributesSpanExporter to add more attributes to all spans.

    You can control when these customizations are applied using the environment variable OTEL_AWS_APP_SIGNALS_ENABLED.
    This flag is disabled by default.
    """

    # pylint: disable=no-self-use
    @override
    def _configure(self, **kwargs):
        _initialize_components(kwargs.get("auto_instrumentation_version"))


def _initialize_components(auto_instrumentation_version):
    trace_exporters, metric_exporters, log_exporters = _import_exporters(
        _get_exporter_names("traces"),
        _get_exporter_names("metrics"),
        _get_exporter_names("logs"),
    )

    id_generator_name = _get_id_generator()
    id_generator = _import_id_generator(id_generator_name)
    # if env var OTEL_RESOURCE_ATTRIBUTES is given, it will read the service_name
    # from the env variable else defaults to "unknown_service"

    auto_resource: Dict[str, any] = {}
    auto_resource = _customize_versions(auto_resource, auto_instrumentation_version)
    resource = get_aggregated_resources(
        [
            AwsEc2ResourceDetector(),
            AwsEksResourceDetector(),
            AwsEcsResourceDetector(),
        ]
    ).merge(Resource.create(auto_resource))

    sampler_name = _get_sampler()
    sampler = _custom_import_sampler(sampler_name, resource)

    _init_tracing(
        exporters=trace_exporters,
        id_generator=id_generator,
        sampler=sampler,
        resource=resource,
    )
    _init_metrics(metric_exporters, resource)
    logging_enabled = os.getenv(_OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED, "false")
    if logging_enabled.strip().lower() == "true":
        _init_logging(log_exporters, resource)


def _init_tracing(
    exporters: Dict[str, Type[SpanExporter]],
    id_generator: IdGenerator = None,
    sampler: Sampler = None,
    resource: Resource = None,
):
    sampler = _customize_sampler(sampler)

    trace_provider: TracerProvider = TracerProvider(
        id_generator=id_generator,
        sampler=sampler,
        resource=resource,
    )

    for _, exporter_class in exporters.items():
        exporter_args: Dict[str, any] = {}
        span_exporter: SpanExporter = exporter_class(**exporter_args)
        span_exporter = _customize_exporter(span_exporter, resource)
        trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))

    _customize_span_processors(trace_provider, resource)

    set_tracer_provider(trace_provider)


def _custom_import_sampler(sampler_name: str, resource: Resource) -> Sampler:
    if sampler_name == "xray":
        # Example env var value
        # OTEL_TRACES_SAMPLER_ARG=endpoint=http://localhost:2000,polling_interval=360
        sampler_argument_env: str = os.getenv(OTEL_TRACES_SAMPLER_ARG, None)
        endpoint: str = None
        polling_interval: int = None

        if sampler_argument_env is not None:
            args = sampler_argument_env.split(",")
            for arg in args:
                key_value = arg.split("=", 1)
                if len(key_value) != 2:
                    continue
                if key_value[0] == "endpoint":
                    endpoint = key_value[1]
                elif key_value[0] == "polling_interval":
                    try:
                        polling_interval = int(key_value[1])
                    except ValueError as error:
                        _logger.error("polling_interval in OTEL_TRACES_SAMPLER_ARG must be a number: %s", error)

        _logger.debug("XRay Sampler Endpoint: %s", str(endpoint))
        _logger.debug("XRay Sampler Polling Interval: %s", str(polling_interval))
        return AwsXRayRemoteSampler(resource=resource, endpoint=endpoint, polling_interval=polling_interval)
    return _import_sampler(sampler_name)


def _customize_sampler(sampler: Sampler) -> Sampler:
    if not is_app_signals_enabled():
        return sampler
    return AlwaysRecordSampler(sampler)


def _customize_exporter(span_exporter: SpanExporter, resource: Resource) -> SpanExporter:
    if not is_app_signals_enabled():
        return span_exporter
    return AwsMetricAttributesSpanExporterBuilder(span_exporter, resource).build()


def _customize_span_processors(provider: TracerProvider, resource: Resource) -> None:
    if not is_app_signals_enabled():
        return

    # Construct and set local and remote attributes span processor
    provider.add_span_processor(AttributePropagatingSpanProcessorBuilder().build())

    # Construct meterProvider
    _logger.info("AWS AppSignals enabled")
    otel_metric_exporter = AppSignalsExporterProvider().create_exporter()
    export_interval_millis = float(os.environ.get(OTEL_METRIC_EXPORT_INTERVAL, DEFAULT_METRIC_EXPORT_INTERVAL))
    _logger.debug("Span Metrics export interval: %s", export_interval_millis)
    # Cap export interval to 60 seconds. This is currently required for metrics-trace correlation to work correctly.
    if export_interval_millis > DEFAULT_METRIC_EXPORT_INTERVAL:
        export_interval_millis = DEFAULT_METRIC_EXPORT_INTERVAL
        _logger.info("AWS AppSignals metrics export interval capped to %s", export_interval_millis)
    periodic_exporting_metric_reader = PeriodicExportingMetricReader(
        exporter=otel_metric_exporter, export_interval_millis=export_interval_millis
    )
    meter_provider: MeterProvider = MeterProvider(resource=resource, metric_readers=[periodic_exporting_metric_reader])
    # Construct and set AppSignals metrics processor
    provider.add_span_processor(AwsSpanMetricsProcessorBuilder(meter_provider, resource).build())

    return


def _customize_versions(auto_resource: Dict[str, any], auto_instrumentation_version: str) -> Dict[str, any]:
    # populate version if using auto-instrumentation
    if auto_instrumentation_version:
        auto_resource[ResourceAttributes.TELEMETRY_AUTO_VERSION] = auto_instrumentation_version
    distro_version = version("aws-opentelemetry-distro")
    auto_resource[ResourceAttributes.TELEMETRY_SDK_VERSION] = distro_version
    _logger.debug("aws-opentelementry-distro - version: %s", distro_version)
    return auto_resource


def is_app_signals_enabled():
    return os.environ.get(OTEL_AWS_APP_SIGNALS_ENABLED, False)


class AppSignalsExporterProvider:
    _instance: ClassVar["AppSignalsExporterProvider"] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # pylint: disable=no-self-use
    def create_exporter(self):
        protocol = os.environ.get(
            OTEL_EXPORTER_OTLP_METRICS_PROTOCOL, os.environ.get(OTEL_EXPORTER_OTLP_PROTOCOL, "grpc")
        )
        _logger.debug("AppSignals export protocol: %s", protocol)

        app_signals_endpoint = os.environ.get(
            OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT,
            os.environ.get(OTEL_AWS_SMP_EXPORTER_ENDPOINT, "http://localhost:4315"),
        )

        _logger.debug("AppSignals export endpoint: %s", app_signals_endpoint)

        temporality_dict: Dict[type, AggregationTemporality] = {}
        for typ in [
            Counter,
            UpDownCounter,
            ObservableCounter,
            ObservableCounter,
            ObservableUpDownCounter,
            ObservableGauge,
            Histogram,
        ]:
            temporality_dict[typ] = AggregationTemporality.DELTA

        if protocol == "http/protobuf":
            return OTLPHttpOTLPMetricExporter(endpoint=app_signals_endpoint, preferred_temporality=temporality_dict)
        if protocol == "grpc":
            return OTLPGrpcOTLPMetricExporter(endpoint=app_signals_endpoint, preferred_temporality=temporality_dict)

        raise RuntimeError(f"Unsupported AppSignals export protocol: {protocol} ")
