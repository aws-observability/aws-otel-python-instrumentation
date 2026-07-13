# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from typing import ClassVar, Dict, Optional

from amazon.opentelemetry.application_signals.environment_variables import (
    OTEL_AWS_APP_SIGNALS_ENABLED,
    OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT,
    OTEL_AWS_APPLICATION_SIGNALS_ENABLED,
    OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT,
)
from amazon.opentelemetry.application_signals.processor.attribute_propagating_span_processor import (
    AttributePropagatingSpanProcessor,
)
from amazon.opentelemetry.application_signals.processor.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.application_signals.processor.aws_metric_attributes_span_processor import (
    AwsMetricAttributesSpanProcessor,
)
from amazon.opentelemetry.application_signals.processor.aws_span_metrics_processor_builder import (
    AwsSpanMetricsProcessorBuilder,
)
from amazon.opentelemetry.application_signals.sampler.always_record_sampler import AlwaysRecordSampler
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as OTLPHttpOTLPMetricExporter
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_METRIC_EXPORT_INTERVAL,
)
from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    MeterProvider,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import AggregationTemporality, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import Sampler

_logger = logging.getLogger(__name__)

DEFAULT_METRIC_EXPORT_INTERVAL = 60000.0
AWS_LAMBDA_FUNCTION_NAME_CONFIG = "AWS_LAMBDA_FUNCTION_NAME"


def configure_application_signals(
    tracer_provider: TracerProvider,
    resource: Resource,
    sampler: Optional[Sampler] = None,
    should_enable: bool = False,
) -> bool:
    """Configure the Application Signals pipeline on the given TracerProvider.

    Sets up:
    - AlwaysRecordSampler wrapping for metric derivation
    - AttributePropagatingSpanProcessor for attribute propagation
    - AwsMetricAttributesSpanProcessor to add metric-correlation attributes to spans
    - AwsSpanMetricsProcessor with a dedicated MeterProvider for span metrics
    - On Lambda: skips the metrics MeterProvider (caller handles unsampled export)

    Returns True if the pipeline was configured, False if already configured or disabled.
    """
    if is_application_signals_already_enabled(tracer_provider):
        _logger.debug("Application Signals pipeline already configured, skipping")
        return False
    if not should_enable and not is_application_signals_enabled():
        return False

    resolved_sampler: Optional[Sampler] = sampler or getattr(tracer_provider, "sampler", None)
    if resolved_sampler and not isinstance(resolved_sampler, AlwaysRecordSampler):
        tracer_provider._sampler = AlwaysRecordSampler(resolved_sampler)  # type: ignore[attr-defined]

    os.environ.setdefault(
        "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION", "base2_exponential_bucket_histogram"
    )

    # Construct and set local and remote attributes span processor
    tracer_provider.add_span_processor(AttributePropagatingSpanProcessorBuilder().build())

    # Add metric attributes to spans (aws.local.service, aws.local.operation, aws.span.kind, etc.)
    tracer_provider.add_span_processor(AwsMetricAttributesSpanProcessor(resource))

    # Export 100% spans and not export Application-Signals metrics if on Lambda.
    if _is_lambda_environment():
        return True

    # Construct meterProvider
    _logger.info("AWS Application Signals enabled")
    exporter_provider = ApplicationSignalsExporterProvider()
    otel_metric_exporter = exporter_provider.create_exporter()

    periodic_exporting_metric_reader = PeriodicExportingMetricReader(
        exporter=otel_metric_exporter, export_interval_millis=get_metric_export_interval()
    )
    meter_provider: MeterProvider = MeterProvider(resource=resource, metric_readers=[periodic_exporting_metric_reader])

    # Construct and set application signals metrics processor
    builder = AwsSpanMetricsProcessorBuilder(meter_provider, resource)
    if resolved_sampler is not None:
        builder.set_sampler(resolved_sampler)
    tracer_provider.add_span_processor(builder.build())

    return True


def is_application_signals_enabled() -> bool:
    """Check if Application Signals is enabled via environment variables."""
    return (
        os.environ.get(
            OTEL_AWS_APPLICATION_SIGNALS_ENABLED, os.environ.get(OTEL_AWS_APP_SIGNALS_ENABLED, "false")
        ).lower()
        == "true"
    )


def is_application_signals_already_enabled(tracer_provider: TracerProvider) -> bool:
    """Check if the Application Signals pipeline is already attached to the TracerProvider."""
    try:
        processors = tracer_provider._active_span_processor._span_processors
        return any(isinstance(p, AttributePropagatingSpanProcessor) for p in processors)
    except AttributeError:
        return False


def get_metric_export_interval() -> float:
    """Get the metric export interval, capped at 60 seconds.

    Cap export interval to 60 seconds. This is currently required for metrics-trace correlation to work correctly.
    """
    export_interval_millis = float(os.environ.get(OTEL_METRIC_EXPORT_INTERVAL, DEFAULT_METRIC_EXPORT_INTERVAL))
    _logger.debug("Span Metrics export interval: %s", export_interval_millis)
    # Cap export interval to 60 seconds. This is currently required for metrics-trace correlation to work correctly.
    if export_interval_millis > DEFAULT_METRIC_EXPORT_INTERVAL:
        export_interval_millis = DEFAULT_METRIC_EXPORT_INTERVAL
        _logger.info("AWS Application Signals metrics export interval capped to %s", export_interval_millis)
    return export_interval_millis


class ApplicationSignalsExporterProvider:
    _instance: ClassVar["ApplicationSignalsExporterProvider"] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def create_exporter(self):
        protocol = os.environ.get(
            OTEL_EXPORTER_OTLP_METRICS_PROTOCOL, os.environ.get(OTEL_EXPORTER_OTLP_PROTOCOL, "http/protobuf")
        )
        _logger.debug("AWS Application Signals export protocol: %s", protocol)

        temporality_dict: Dict[type, AggregationTemporality] = {}
        for typ in [
            Counter,
            UpDownCounter,
            ObservableCounter,
            ObservableUpDownCounter,
            ObservableGauge,
            Histogram,
        ]:
            temporality_dict[typ] = AggregationTemporality.DELTA

        if protocol == "http/protobuf":
            application_signals_endpoint = os.environ.get(
                OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT,
                os.environ.get(OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT, "http://localhost:4316/v1/metrics"),
            )
            _logger.debug("AWS Application Signals export endpoint: %s", application_signals_endpoint)
            return OTLPHttpOTLPMetricExporter(
                endpoint=application_signals_endpoint, preferred_temporality=temporality_dict
            )
        if protocol == "grpc":
            # Delay import to only occur if gRPC specifically requested. Vended Docker image will not have gRPC
            # bundled, so importing it at the class level can cause runtime failures.
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter as OTLPGrpcOTLPMetricExporter,
            )

            application_signals_endpoint = os.environ.get(
                OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT,
                os.environ.get(OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT, "localhost:4315"),
            )
            _logger.debug("AWS Application Signals export endpoint: %s", application_signals_endpoint)
            return OTLPGrpcOTLPMetricExporter(
                endpoint=application_signals_endpoint, preferred_temporality=temporality_dict
            )

        raise RuntimeError(f"Unsupported AWS Application Signals export protocol: {protocol} ")


def create_always_record_sampler(sampler: Sampler) -> Sampler:
    if not is_application_signals_enabled():
        return sampler
    return AlwaysRecordSampler(sampler)


def _is_lambda_environment() -> bool:
    return AWS_LAMBDA_FUNCTION_NAME_CONFIG in os.environ
