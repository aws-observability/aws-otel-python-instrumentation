# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from logging import Logger, getLogger
from typing import Dict, Type

from typing_extensions import override

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter_builder import (
    AwsMetricAttributesSpanExporterBuilder,
)
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
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
from opentelemetry.sdk.environment_variables import _OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED
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
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import set_tracer_provider

OTEL_SMP_ENABLED = "OTEL_SMP_ENABLED"
OTEL_METRIC_EXPORT_INTERVAL = "OTEL_METRIC_EXPORT_INTERVAL"
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

    You can control when these customizations are applied using the environment variable OTEL_SMP_ENABLED.
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
    sampler_name = _get_sampler()
    sampler = _import_sampler(sampler_name)
    id_generator_name = _get_id_generator()
    id_generator = _import_id_generator(id_generator_name)
    # if env var OTEL_RESOURCE_ATTRIBUTES is given, it will read the service_name
    # from the env variable else defaults to "unknown_service"
    auto_resource = {}
    # populate version if using auto-instrumentation
    if auto_instrumentation_version:
        auto_resource[ResourceAttributes.TELEMETRY_AUTO_VERSION] = auto_instrumentation_version
    resource = Resource.create(auto_resource)

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


def _customize_sampler(sampler: Sampler) -> Sampler:
    if not is_smp_enabled():
        return sampler
    return AlwaysRecordSampler(sampler)


def _customize_exporter(span_exporter: SpanExporter, resource: Resource) -> SpanExporter:
    if not is_smp_enabled():
        return span_exporter
    return AwsMetricAttributesSpanExporterBuilder(span_exporter, resource).build()


def _customize_span_processors(provider: TracerProvider, resource: Resource) -> None:
    if not is_smp_enabled():
        return

    # Construct and set local and remote attributes span processor
    provider.add_span_processor(AttributePropagatingSpanProcessorBuilder().build())

    # Construct meterProvider
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
    _logger.info("Span Metrics Processor enabled")
    smp_endpoint = os.environ.get(OTEL_AWS_SMP_EXPORTER_ENDPOINT, "http://cloudwatch-agent.amazon-cloudwatch:4317")
    otel_metric_exporter = OTLPMetricExporter(endpoint=smp_endpoint, preferred_temporality=temporality_dict)
    export_interval_millis = float(os.environ.get(OTEL_METRIC_EXPORT_INTERVAL, DEFAULT_METRIC_EXPORT_INTERVAL))
    _logger.debug("Span Metrics endpoint: %s", smp_endpoint)
    _logger.debug("Span Metrics export interval: %s", export_interval_millis)
    # Cap export interval to 60 seconds. This is currently required for metrics-trace correlation to work correctly.
    if export_interval_millis > DEFAULT_METRIC_EXPORT_INTERVAL:
        export_interval_millis = DEFAULT_METRIC_EXPORT_INTERVAL
        _logger.info("AWS AppSignals metrics export interval capped to %s", export_interval_millis)
    periodic_exporting_metric_reader = PeriodicExportingMetricReader(
        exporter=otel_metric_exporter, export_interval_millis=export_interval_millis
    )
    meter_provider: MeterProvider = MeterProvider(resource=resource, metric_readers=[periodic_exporting_metric_reader])
    # Construct and set span metrics processor
    provider.add_span_processor(AwsSpanMetricsProcessorBuilder(meter_provider, resource).build())

    return


def is_smp_enabled():
    return os.environ.get(OTEL_SMP_ENABLED, False)
