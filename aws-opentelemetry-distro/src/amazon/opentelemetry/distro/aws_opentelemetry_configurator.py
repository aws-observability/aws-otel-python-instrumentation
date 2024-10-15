# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import os
from logging import Logger, getLogger
from typing import ClassVar, Dict, List, Type, Union

from importlib_metadata import version
from typing_extensions import override

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_LOCAL_SERVICE
from amazon.opentelemetry.distro._aws_resource_attribute_configurator import get_service_attribute
from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter_builder import (
    AwsMetricAttributesSpanExporterBuilder,
)
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from amazon.opentelemetry.distro.otlp_udp_exporter import OTLPUdpSpanExporter
from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from amazon.opentelemetry.distro.scope_based_exporter import ScopeBasedPeriodicExportingMetricReader
from amazon.opentelemetry.distro.scope_based_filtering_view import ScopeBasedRetainingView
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as OTLPHttpOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk._configuration import (
    _get_exporter_names,
    _get_id_generator,
    _get_sampler,
    _import_exporters,
    _import_id_generator,
    _import_sampler,
    _init_logging,
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
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    MetricExporter,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.metrics.view import LastValueAggregation, View
from opentelemetry.sdk.resources import Resource, get_aggregated_resources
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import set_tracer_provider

DEPRECATED_APP_SIGNALS_ENABLED_CONFIG = "OTEL_AWS_APP_SIGNALS_ENABLED"
APPLICATION_SIGNALS_ENABLED_CONFIG = "OTEL_AWS_APPLICATION_SIGNALS_ENABLED"
APPLICATION_SIGNALS_RUNTIME_ENABLED_CONFIG = "OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED"
DEPRECATED_APP_SIGNALS_EXPORTER_ENDPOINT_CONFIG = "OTEL_AWS_APP_SIGNALS_EXPORTER_ENDPOINT"
APPLICATION_SIGNALS_EXPORTER_ENDPOINT_CONFIG = "OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT"
METRIC_EXPORT_INTERVAL_CONFIG = "OTEL_METRIC_EXPORT_INTERVAL"
DEFAULT_METRIC_EXPORT_INTERVAL = 60000.0
AWS_LAMBDA_FUNCTION_NAME_CONFIG = "AWS_LAMBDA_FUNCTION_NAME"
AWS_XRAY_DAEMON_ADDRESS_CONFIG = "AWS_XRAY_DAEMON_ADDRESS"
OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED_CONFIG = "OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED"
SYSTEM_METRICS_INSTRUMENTATION_SCOPE_NAME = "opentelemetry.instrumentation.system_metrics"
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
# UDP package size is not larger than 64KB
LAMBDA_SPAN_EXPORT_BATCH_SIZE = 10

_logger: Logger = getLogger(__name__)


class AwsOpenTelemetryConfigurator(_OTelSDKConfigurator):
    """
    This AwsOpenTelemetryConfigurator extend _OTelSDKConfigurator configuration with the following change:

    - Use AlwaysRecordSampler to record all spans.
    - Add SpanMetricsProcessor to create metrics.
    - Add AttributePropagatingSpanProcessor to propagate span attributes from parent to child spans.
    - Add AwsMetricAttributesSpanExporter to add more attributes to all spans.

    You can control when these customizations are applied using the environment variable
    OTEL_AWS_APPLICATION_SIGNALS_ENABLED. This flag is disabled by default.
    """

    # pylint: disable=no-self-use
    @override
    def _configure(self, **kwargs):
        if _is_defer_to_workers_enabled() and _is_wsgi_master_process():
            _logger.info(
                "Skipping ADOT initialization since deferral to worker is enabled, and this is a master process."
            )
            return
        _initialize_components()


# The OpenTelemetry Authors code
# Long term, we wish to contribute this to upstream to improve initialization customizability and reduce dependency on
# internal logic.
def _initialize_components():
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
    auto_resource = _customize_versions(auto_resource)

    resource_detectors = (
        [
            AwsEc2ResourceDetector(),
            AwsEksResourceDetector(),
            AwsEcsResourceDetector(),
        ]
        if not _is_lambda_environment()
        else []
    )

    resource = _customize_resource(get_aggregated_resources(resource_detectors).merge(Resource.create(auto_resource)))

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
        trace_provider.add_span_processor(
            BatchSpanProcessor(span_exporter=span_exporter, max_export_batch_size=_span_export_batch_size())
        )

    _customize_span_processors(trace_provider, resource)

    set_tracer_provider(trace_provider)


def _init_metrics(
    exporters_or_readers: Dict[str, Union[Type[MetricExporter], Type[MetricReader]]],
    resource: Resource = None,
):
    metric_readers = []
    views = []

    for _, exporter_or_reader_class in exporters_or_readers.items():
        exporter_args = {}

        if issubclass(exporter_or_reader_class, MetricReader):
            metric_readers.append(exporter_or_reader_class(**exporter_args))
        else:
            metric_readers.append(PeriodicExportingMetricReader(exporter_or_reader_class(**exporter_args)))

    _customize_metric_exporters(metric_readers, views)

    provider = MeterProvider(resource=resource, metric_readers=metric_readers, views=views)
    set_meter_provider(provider)


# END The OpenTelemetry Authors code


def _export_unsampled_span_for_lambda(trace_provider: TracerProvider, resource: Resource = None):
    if not _is_application_signals_enabled():
        return
    if not _is_lambda_environment():
        return

    traces_endpoint = os.environ.get(AWS_XRAY_DAEMON_ADDRESS_CONFIG, "127.0.0.1:2000")

    span_exporter = AwsMetricAttributesSpanExporterBuilder(
        OTLPUdpSpanExporter(endpoint=traces_endpoint, sampled=False), resource
    ).build()

    trace_provider.add_span_processor(
        BatchUnsampledSpanProcessor(span_exporter=span_exporter, max_export_batch_size=LAMBDA_SPAN_EXPORT_BATCH_SIZE)
    )


def _is_defer_to_workers_enabled():
    return os.environ.get(OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED_CONFIG, "false").strip().lower() == "true"


def _is_wsgi_master_process():
    # Since the auto-instrumentation loads whenever a process is created and due to known issues with instrumenting
    # WSGI apps using OTel, we want to skip the instrumentation of master process.
    # This function is used to identify if the current process is a WSGI server's master process or not.
    # Typically, a WSGI fork process model server spawns a single master process and multiple worker processes.
    # When the master process starts, we use an environment variable as a marker. Since child worker processes inherit
    # the master process environment, checking this marker in worker will tell that master process has been seen.
    # Note: calling this function more than once in the same master process will return incorrect result.
    # So use carefully.
    if os.environ.get("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", "false").lower() == "true":
        _logger.info("pid %s identified as a worker process", str(os.getpid()))
        return False
    os.environ["IS_WSGI_MASTER_PROCESS_ALREADY_SEEN"] = "true"
    _logger.info("pid %s identified as a master process", str(os.getpid()))
    return True


def _exclude_urls_for_instrumentations():
    urls_to_exclude_instr = "SamplingTargets,GetSamplingRules"
    requests_excluded_urls = os.environ.pop("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", "")
    urllib3_excluded_urls = os.environ.pop("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", "")
    if len(requests_excluded_urls) > 0:
        requests_excluded_urls = ",".join([requests_excluded_urls, urls_to_exclude_instr])
    else:
        requests_excluded_urls = urls_to_exclude_instr
    if len(urllib3_excluded_urls) > 0:
        urllib3_excluded_urls = ",".join([urllib3_excluded_urls, urls_to_exclude_instr])
    else:
        urllib3_excluded_urls = urls_to_exclude_instr
    os.environ.setdefault("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", requests_excluded_urls)
    os.environ.setdefault("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", urllib3_excluded_urls)


def _custom_import_sampler(sampler_name: str, resource: Resource) -> Sampler:
    # sampler_name from _get_sampler() can be None if `OTEL_TRACES_SAMPLER` is unset. Upstream TracerProvider is able to
    # accept None as sampler, however we require the sampler to be not None to create `AlwaysRecordSampler` beforehand.
    # Default value of `OTEL_TRACES_SAMPLER` should be `parentbased_always_on`.
    # https://opentelemetry.io/docs/languages/sdk-configuration/general/#otel_traces_sampler
    # Ideally, _get_sampler() should default to `parentbased_always_on` in upstream.
    if sampler_name is None:
        sampler_name = "parentbased_always_on"

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
        # Until `suppress_instrumentation` is available in next OTEL Python version (>=1.23.0/0.44b0),
        # suppress recording of X-Ray sampler's Request POST calls via setting `exclude urls` Env Vars. This
        # should be done in this class's `_configure()` method which is run before any instrumentation is loaded
        # TODO: Replace usage of `exclude urls` by wrapping X-Ray sampler POST calls with `suppress_instrumentation`
        _exclude_urls_for_instrumentations()

        _logger.debug("XRay Sampler Endpoint: %s", str(endpoint))
        _logger.debug("XRay Sampler Polling Interval: %s", str(polling_interval))
        return AwsXRayRemoteSampler(resource=resource, endpoint=endpoint, polling_interval=polling_interval)
    return _import_sampler(sampler_name)


def _customize_sampler(sampler: Sampler) -> Sampler:
    if not _is_application_signals_enabled():
        return sampler
    return AlwaysRecordSampler(sampler)


def _customize_exporter(span_exporter: SpanExporter, resource: Resource) -> SpanExporter:
    if _is_lambda_environment():
        # Override OTLP http default endpoint to UDP
        if isinstance(span_exporter, OTLPSpanExporter) and os.getenv(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is None:
            traces_endpoint = os.environ.get(AWS_XRAY_DAEMON_ADDRESS_CONFIG, "127.0.0.1:2000")
            span_exporter = OTLPUdpSpanExporter(endpoint=traces_endpoint)

    if not _is_application_signals_enabled():
        return span_exporter

    return AwsMetricAttributesSpanExporterBuilder(span_exporter, resource).build()


def _customize_span_processors(provider: TracerProvider, resource: Resource) -> None:
    if not _is_application_signals_enabled():
        return

    # Construct and set local and remote attributes span processor
    provider.add_span_processor(AttributePropagatingSpanProcessorBuilder().build())

    # Export 100% spans and not export Application-Signals metrics if on Lambda.
    if _is_lambda_environment():
        _export_unsampled_span_for_lambda(provider, resource)
        return

    # Construct meterProvider
    _logger.info("AWS Application Signals enabled")
    otel_metric_exporter = ApplicationSignalsExporterProvider().create_exporter()

    periodic_exporting_metric_reader = PeriodicExportingMetricReader(
        exporter=otel_metric_exporter, export_interval_millis=_get_metric_export_interval()
    )
    meter_provider: MeterProvider = MeterProvider(resource=resource, metric_readers=[periodic_exporting_metric_reader])
    # Construct and set application signals metrics processor
    provider.add_span_processor(AwsSpanMetricsProcessorBuilder(meter_provider, resource).build())

    return


def _customize_metric_exporters(metric_readers: List[MetricReader], views: List[View]) -> None:
    if _is_application_signals_runtime_enabled():
        _get_runtime_metric_views(views, 0 == len(metric_readers))

        application_signals_metric_exporter = ApplicationSignalsExporterProvider().create_exporter()
        scope_based_periodic_exporting_metric_reader = ScopeBasedPeriodicExportingMetricReader(
            exporter=application_signals_metric_exporter,
            export_interval_millis=_get_metric_export_interval(),
            registered_scope_names={SYSTEM_METRICS_INSTRUMENTATION_SCOPE_NAME},
        )
        metric_readers.append(scope_based_periodic_exporting_metric_reader)


def _get_runtime_metric_views(views: List[View], retain_runtime_only: bool) -> None:
    runtime_metrics_scope_name = SYSTEM_METRICS_INSTRUMENTATION_SCOPE_NAME
    _logger.info("Registered scope %s", runtime_metrics_scope_name)
    views.append(
        View(
            instrument_name="system.network.connections",
            meter_name=runtime_metrics_scope_name,
            aggregation=LastValueAggregation(),
        )
    )
    views.append(
        View(
            instrument_name="process.open_file_descriptor.count",
            meter_name=runtime_metrics_scope_name,
            aggregation=LastValueAggregation(),
        )
    )
    views.append(
        View(
            instrument_name="process.runtime.*.memory",
            meter_name=runtime_metrics_scope_name,
            aggregation=LastValueAggregation(),
        )
    )
    views.append(
        View(
            instrument_name="process.runtime.*.gc_count",
            meter_name=runtime_metrics_scope_name,
            aggregation=LastValueAggregation(),
        )
    )
    views.append(
        View(
            instrument_name="process.runtime.*.thread_count",
            meter_name=runtime_metrics_scope_name,
            aggregation=LastValueAggregation(),
        )
    )
    if retain_runtime_only:
        views.append(ScopeBasedRetainingView(meter_name=runtime_metrics_scope_name))


def _customize_versions(auto_resource: Dict[str, any]) -> Dict[str, any]:
    distro_version = version("aws-opentelemetry-distro")
    auto_resource[ResourceAttributes.TELEMETRY_AUTO_VERSION] = distro_version + "-aws"
    _logger.debug("aws-opentelementry-distro - version: %s", auto_resource[ResourceAttributes.TELEMETRY_AUTO_VERSION])
    return auto_resource


def _customize_resource(resource: Resource) -> Resource:
    service_name, is_unknown = get_service_attribute(resource)
    if is_unknown:
        _logger.debug("No valid service name found")

    return resource.merge(Resource.create({AWS_LOCAL_SERVICE: service_name}))


def _is_application_signals_enabled():
    return (
        os.environ.get(
            APPLICATION_SIGNALS_ENABLED_CONFIG, os.environ.get(DEPRECATED_APP_SIGNALS_ENABLED_CONFIG, "false")
        ).lower()
        == "true"
    )


def _is_application_signals_runtime_enabled():
    return _is_application_signals_enabled() and (
        os.environ.get(APPLICATION_SIGNALS_RUNTIME_ENABLED_CONFIG, "true").lower() == "true"
    )


def _is_lambda_environment():
    # detect if running in AWS Lambda environment
    return AWS_LAMBDA_FUNCTION_NAME_CONFIG in os.environ


def _get_metric_export_interval():
    export_interval_millis = float(os.environ.get(METRIC_EXPORT_INTERVAL_CONFIG, DEFAULT_METRIC_EXPORT_INTERVAL))
    _logger.debug("Span Metrics export interval: %s", export_interval_millis)
    # Cap export interval to 60 seconds. This is currently required for metrics-trace correlation to work correctly.
    if export_interval_millis > DEFAULT_METRIC_EXPORT_INTERVAL:
        export_interval_millis = DEFAULT_METRIC_EXPORT_INTERVAL
        _logger.info("AWS Application Signals metrics export interval capped to %s", export_interval_millis)
    return export_interval_millis


def _span_export_batch_size():
    return LAMBDA_SPAN_EXPORT_BATCH_SIZE if _is_lambda_environment() else None


class ApplicationSignalsExporterProvider:
    _instance: ClassVar["ApplicationSignalsExporterProvider"] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # pylint: disable=no-self-use
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
            ObservableCounter,
            ObservableUpDownCounter,
            ObservableGauge,
            Histogram,
        ]:
            temporality_dict[typ] = AggregationTemporality.DELTA

        if protocol == "http/protobuf":
            application_signals_endpoint = os.environ.get(
                APPLICATION_SIGNALS_EXPORTER_ENDPOINT_CONFIG,
                os.environ.get(DEPRECATED_APP_SIGNALS_EXPORTER_ENDPOINT_CONFIG, "http://localhost:4316/v1/metrics"),
            )
            _logger.debug("AWS Application Signals export endpoint: %s", application_signals_endpoint)
            return OTLPHttpOTLPMetricExporter(
                endpoint=application_signals_endpoint, preferred_temporality=temporality_dict
            )
        if protocol == "grpc":
            # pylint: disable=import-outside-toplevel
            # Delay import to only occur if gRPC specifically requested. Vended Docker image will not have gRPC bundled,
            # so importing it at the class level can cause runtime failures.
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter as OTLPGrpcOTLPMetricExporter,
            )

            application_signals_endpoint = os.environ.get(
                APPLICATION_SIGNALS_EXPORTER_ENDPOINT_CONFIG,
                os.environ.get(DEPRECATED_APP_SIGNALS_EXPORTER_ENDPOINT_CONFIG, "localhost:4315"),
            )
            _logger.debug("AWS Application Signals export endpoint: %s", application_signals_endpoint)
            return OTLPGrpcOTLPMetricExporter(
                endpoint=application_signals_endpoint, preferred_temporality=temporality_dict
            )

        raise RuntimeError(f"Unsupported AWS Application Signals export protocol: {protocol} ")
