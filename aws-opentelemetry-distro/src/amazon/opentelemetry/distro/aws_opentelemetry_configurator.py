# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Dict, Optional, Type, Union

from typing_extensions import override

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import get_meter_provider, set_meter_provider
from opentelemetry.sdk._configuration import (
    _BaseConfigurator,
    _get_exporter_names,
    _get_id_generator,
    _get_sampler,
    _import_exporters,
    _import_id_generator,
    _import_sampler,
    _init_logging,
)
from opentelemetry.sdk._logs.export import LogExporter
from opentelemetry.sdk.environment_variables import _OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED
from opentelemetry.sdk.extension.aws.trace.aws_xray_id_generator import AwsXRayIdGenerator
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.aggregation import (
    Aggregation,
    DefaultAggregation,
    ExponentialBucketHistogramAggregation,
)
from opentelemetry.sdk.metrics._internal.instrument import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    MetricExporter,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import set_tracer_provider


class AwsOpenTelemetryConfigurator(_BaseConfigurator):
    def __init__(self):
        self.trace_provider = None

    @override
    def _configure(self, **kwargs):
        self._initialize_components(kwargs.get("auto_instrumentation_version"))

    def get_trace_provider(self):
        return self.trace_provider

    def _initialize_components(self, auto_instrumentation_version: str):
        trace_exporters: Dict[str, Type[SpanExporter]]
        metric_exporters: Dict[str, Union[Type[MetricExporter], Type[MetricReader]]]
        log_exporters: Dict[str, Type[LogExporter]]
        trace_exporters, metric_exporters, log_exporters = _import_exporters(
            _get_exporter_names("traces"),
            _get_exporter_names("metrics"),
            _get_exporter_names("logs"),
        )
        sampler_name: Optional[str] = _get_sampler()
        sampler: Optional[Sampler] = _import_sampler(sampler_name)
        id_generator_name: str = _get_id_generator()
        id_generator: IdGenerator = _import_id_generator(id_generator_name)

        auto_resource: Dict[str, str] = {}
        # populate version if using auto-instrumentation
        if auto_instrumentation_version:
            auto_resource[
                ResourceAttributes.TELEMETRY_AUTO_VERSION
            ] = auto_instrumentation_version
        resource: Resource = Resource.create(auto_resource)

        self._init_tracing(
            trace_exporters=trace_exporters,
            id_generator=id_generator,
            sampler=sampler,
            resource=resource,
        )

        logging_enabled: str = os.getenv(_OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED, "false")
        if logging_enabled.strip().lower() == "true":
            _init_logging(log_exporters, resource)
        self._init_metrics(metric_exporters, resource)

    def _init_tracing(
        self,
        trace_exporters: Dict[str, Type[SpanExporter]],
        id_generator: IdGenerator = None,
        sampler: Sampler = None,
        resource: Resource = None,
    ):
        if is_smp_enabled():
            sampler: Sampler = AlwaysRecordSampler(sampler)
            id_generator: IdGenerator = AwsXRayIdGenerator()
        self.trace_provider: TracerProvider = TracerProvider(
            id_generator=id_generator,
            sampler=sampler,
            resource=resource,
        )
        set_tracer_provider(self.trace_provider)
        exporter_args: Dict[str, any] = {}
        if is_smp_enabled():
            for _, exporter_class in trace_exporters.items():
                # span_exporter: SpanExporter = AttributeSpanExporter(exporter_class(**exporter_args))
                span_exporter: SpanExporter = exporter_class(**exporter_args)
                self.trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
            self.trace_provider.add_span_processor(AttributePropagatingSpanProcessorBuilder().build())
            meter_provider: MeterProvider = get_meter_provider()
            self.trace_provider.add_span_processor(AwsSpanMetricsProcessorBuilder(meter_provider, resource).build())
        else:
            for _, exporter_class in trace_exporters.items():
                span_exporter: SpanExporter = exporter_class(**exporter_args)
                self.trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))

        # TODO: Remove BatchSpanProcessor(ConsoleSpanExporter())) and update testing instructions
        self.trace_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # pylint: disable=no-self-use
    def _init_metrics(
        self,
        exporters_or_readers: Dict[str, Union[Type[MetricExporter], Type[MetricReader]]],
        resource: Resource = None,
    ):
        metric_readers = []
        for _, exporter_or_reader_class in exporters_or_readers.items():
            exporter_args = {}

            if issubclass(exporter_or_reader_class, MetricReader):
                metric_readers.append(exporter_or_reader_class(**exporter_args))
            elif issubclass(exporter_or_reader_class, OTLPMetricExporter) and is_smp_enabled():
                continue
            else:
                metric_readers.append(PeriodicExportingMetricReader(exporter_or_reader_class(**exporter_args)))

        if is_smp_enabled():
            aggregation_dict: Dict[type, Aggregation] = {}
            temporality_dict: Dict[type, AggregationTemporality] = {}
            for typ in [
                Counter,
                UpDownCounter,
                ObservableCounter,
                ObservableCounter,
                ObservableUpDownCounter,
                ObservableGauge,
            ]:
                aggregation_dict[typ] = DefaultAggregation()
                temporality_dict[typ] = AggregationTemporality.DELTA
            aggregation_dict[Histogram] = ExponentialBucketHistogramAggregation()
            temporality_dict[Histogram] = AggregationTemporality.DELTA
            export_endpoint = os.environ.get(
                "OTEL_AWS_SMP_EXPORTER_ENDPOINT", "http://cloudwatch-agent.amazon-cloudwatch:4317"
            )
            otel_metric_exporter = OTLPMetricExporter(
                endpoint=export_endpoint, preferred_aggregation=aggregation_dict, preferred_temporality=temporality_dict
            )
            export_interval_millis = float(os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", 60000))
            periodic_exporting_metric_reader = PeriodicExportingMetricReader(
                exporter=otel_metric_exporter, export_interval_millis=export_interval_millis
            )
            metric_readers.append(periodic_exporting_metric_reader)
        provider = MeterProvider(resource=resource, metric_readers=metric_readers)
        set_meter_provider(provider)

def is_smp_enabled():
    return os.environ.get("OTEL_SMP_ENABLED", False)
