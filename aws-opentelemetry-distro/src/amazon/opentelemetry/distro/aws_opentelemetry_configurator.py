# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import logging
import os
import re
from logging import Logger, getLogger
from typing import ClassVar, Dict, List, NamedTuple, Optional, Type, Union

from importlib_metadata import version
from typing_extensions import override

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_LOCAL_SERVICE, AWS_SERVICE_TYPE
from amazon.opentelemetry.distro._aws_resource_attribute_configurator import get_service_attribute
from amazon.opentelemetry.distro._utils import get_aws_session, is_agent_observability_enabled
from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from amazon.opentelemetry.distro.aws_lambda_span_processor import AwsLambdaSpanProcessor
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter_builder import (
    AwsMetricAttributesSpanExporterBuilder,
)
from amazon.opentelemetry.distro.aws_span_metrics_processor_builder import AwsSpanMetricsProcessorBuilder
from amazon.opentelemetry.distro.otlp_udp_exporter import OTLPUdpSpanExporter
from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from amazon.opentelemetry.distro.scope_based_exporter import ScopeBasedPeriodicExportingMetricReader
from amazon.opentelemetry.distro.scope_based_filtering_view import ScopeBasedRetainingView
from opentelemetry._events import set_event_logger_provider
from opentelemetry._logs import get_logger_provider, set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as OTLPHttpOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import set_meter_provider
from opentelemetry.processor.baggage import BaggageSpanProcessor
from opentelemetry.sdk._configuration import (
    _get_exporter_names,
    _get_id_generator,
    _get_sampler,
    _import_exporters,
    _import_id_generator,
    _import_sampler,
    _OTelSDKConfigurator,
    _patch_basic_config,
)
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogExporter
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
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
OTEL_EXPORTER_OTLP_LOGS_HEADERS = "OTEL_EXPORTER_OTLP_LOGS_HEADERS"

XRAY_SERVICE = "xray"
LOGS_SERIVCE = "logs"
AWS_TRACES_OTLP_ENDPOINT_PATTERN = r"https://xray\.([a-z0-9-]+)\.amazonaws\.com/v1/traces$"
AWS_LOGS_OTLP_ENDPOINT_PATTERN = r"https://logs\.([a-z0-9-]+)\.amazonaws\.com/v1/logs$"

AWS_OTLP_LOGS_GROUP_HEADER = "x-aws-log-group"
AWS_OTLP_LOGS_STREAM_HEADER = "x-aws-log-stream"
AWS_EMF_METRICS_NAMESPACE = "x-aws-metric-namespace"

# UDP package size is not larger than 64KB
LAMBDA_SPAN_EXPORT_BATCH_SIZE = 10

OTEL_TRACES_EXPORTER = "OTEL_TRACES_EXPORTER"
OTEL_LOGS_EXPORTER = "OTEL_LOGS_EXPORTER"
OTEL_METRICS_EXPORTER = "OTEL_METRICS_EXPORTER"
OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"
OTEL_TRACES_SAMPLER = "OTEL_TRACES_SAMPLER"
OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"
OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED = "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"

_logger: Logger = getLogger(__name__)


class OtlpLogHeaderSetting(NamedTuple):
    log_group: Optional[str]
    log_stream: Optional[str]
    namespace: Optional[str]
    is_valid: bool


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
    # Remove 'awsemf' from OTEL_METRICS_EXPORTER if present to prevent validation errors
    # from _import_exporters in OTel dependencies which would try to load exporters
    # We will contribute emf exporter to upstream for supporting OTel metrics in SDK
    is_emf_enabled = _check_emf_exporter_enabled()

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
        if not (_is_lambda_environment() or is_agent_observability_enabled())
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

    _init_metrics(metric_exporters, resource, is_emf_enabled)
    logging_enabled = os.getenv(_OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED, "false")
    if logging_enabled.strip().lower() == "true":
        _init_logging(log_exporters, resource)


def _init_logging(
    exporters: dict[str, Type[LogExporter]],
    resource: Optional[Resource] = None,
    setup_logging_handler: bool = True,
):
    provider = LoggerProvider(resource=resource)
    set_logger_provider(provider)

    for _, exporter_class in exporters.items():
        exporter_args = {}
        _customize_log_record_processor(
            logger_provider=provider, log_exporter=_customize_logs_exporter(exporter_class(**exporter_args))
        )

    event_logger_provider = EventLoggerProvider(logger_provider=provider)
    set_event_logger_provider(event_logger_provider)

    if setup_logging_handler:
        _patch_basic_config()

        # Add OTel handler
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)
        logging.getLogger().addHandler(handler)


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
        span_exporter = _customize_span_exporter(span_exporter, resource)
        trace_provider.add_span_processor(
            BatchSpanProcessor(span_exporter=span_exporter, max_export_batch_size=_span_export_batch_size())
        )

    _customize_span_processors(trace_provider, resource)

    set_tracer_provider(trace_provider)


def _init_metrics(
    exporters_or_readers: Dict[str, Union[Type[MetricExporter], Type[MetricReader]]],
    resource: Resource = None,
    is_emf_enabled: bool = False,
):
    metric_readers = []
    views = []

    for _, exporter_or_reader_class in exporters_or_readers.items():
        exporter_args = {}

        if issubclass(exporter_or_reader_class, MetricReader):
            metric_readers.append(exporter_or_reader_class(**exporter_args))
        else:
            metric_readers.append(PeriodicExportingMetricReader(exporter_or_reader_class(**exporter_args)))

    _customize_metric_exporters(metric_readers, views, is_emf_enabled)

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


def _export_unsampled_span_for_agent_observability(trace_provider: TracerProvider, resource: Resource = None):
    if not is_agent_observability_enabled():
        return

    traces_endpoint = os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)
    if traces_endpoint and _is_aws_otlp_endpoint(traces_endpoint, XRAY_SERVICE):
        endpoint, region = _extract_endpoint_and_region_from_otlp_endpoint(traces_endpoint)
        span_exporter = _create_aws_otlp_exporter(endpoint=endpoint, service=XRAY_SERVICE, region=region)

        trace_provider.add_span_processor(BatchUnsampledSpanProcessor(span_exporter=span_exporter))


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

    if sampler_name == XRAY_SERVICE:
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


def _customize_span_exporter(span_exporter: SpanExporter, resource: Resource) -> SpanExporter:
    traces_endpoint = os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)
    if _is_lambda_environment():
        # Override OTLP http default endpoint to UDP
        if isinstance(span_exporter, OTLPSpanExporter) and traces_endpoint is None:
            traces_endpoint = os.environ.get(AWS_XRAY_DAEMON_ADDRESS_CONFIG, "127.0.0.1:2000")
            span_exporter = OTLPUdpSpanExporter(endpoint=traces_endpoint)

    if traces_endpoint and _is_aws_otlp_endpoint(traces_endpoint, XRAY_SERVICE):
        _logger.info("Detected using AWS OTLP Traces Endpoint.")

        if isinstance(span_exporter, OTLPSpanExporter):
            endpoint, region = _extract_endpoint_and_region_from_otlp_endpoint(traces_endpoint)
            return _create_aws_otlp_exporter(endpoint=endpoint, service=XRAY_SERVICE, region=region)

        _logger.warning(
            "Improper configuration: please export/set "
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf and OTEL_TRACES_EXPORTER=otlp"
        )

    if not _is_application_signals_enabled():
        return span_exporter

    return AwsMetricAttributesSpanExporterBuilder(span_exporter, resource).build()


def _customize_log_record_processor(logger_provider: LoggerProvider, log_exporter: Optional[LogExporter]) -> None:
    if not log_exporter:
        return

    if is_agent_observability_enabled():
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor import (
            AwsCloudWatchOtlpBatchLogRecordProcessor,
        )

        logger_provider.add_log_record_processor(AwsCloudWatchOtlpBatchLogRecordProcessor(exporter=log_exporter))
    else:
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter=log_exporter))


def _customize_logs_exporter(log_exporter: LogExporter) -> LogExporter:
    logs_endpoint = os.environ.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT)

    if logs_endpoint and _is_aws_otlp_endpoint(logs_endpoint, LOGS_SERIVCE):

        _logger.info("Detected using AWS OTLP Logs Endpoint.")

        if isinstance(log_exporter, OTLPLogExporter):

            if _validate_and_fetch_logs_header().is_valid:
                endpoint, region = _extract_endpoint_and_region_from_otlp_endpoint(logs_endpoint)
                # Setting default compression mode to Gzip as this is the behavior in upstream's
                # collector otlp http exporter:
                # https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter
                return _create_aws_otlp_exporter(endpoint=endpoint, service=LOGS_SERIVCE, region=region)

            _logger.warning(
                "Improper configuration: Please configure the environment variable OTEL_EXPORTER_OTLP_LOGS_HEADERS "
                "to have values for x-aws-log-group and x-aws-log-stream"
            )

        _logger.warning(
            "Improper configuration: please export/set "
            "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL=http/protobuf and OTEL_LOGS_EXPORTER=otlp"
        )

    return log_exporter


def _customize_span_processors(provider: TracerProvider, resource: Resource) -> None:
    # Add LambdaSpanProcessor to list of processors regardless of application signals.
    if _is_lambda_environment():
        provider.add_span_processor(AwsLambdaSpanProcessor())

    # We always send 100% spans to Genesis platform for agent observability because
    # AI applications typically have low throughput traffic patterns and require
    # comprehensive monitoring to catch subtle failure modes like hallucinations
    # and quality degradation that sampling could miss.
    # Add session.id baggage attribute to span attributes to support AI Agent use cases
    # enabling session ID tracking in spans.
    if is_agent_observability_enabled():
        _export_unsampled_span_for_agent_observability(provider, resource)

        def session_id_predicate(baggage_key: str) -> bool:
            return baggage_key == "session.id"

        provider.add_span_processor(BaggageSpanProcessor(session_id_predicate))

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


def _customize_metric_exporters(
    metric_readers: List[MetricReader], views: List[View], is_emf_enabled: bool = False
) -> None:
    if _is_application_signals_runtime_enabled():
        _get_runtime_metric_views(views, 0 == len(metric_readers))

        application_signals_metric_exporter = ApplicationSignalsExporterProvider().create_exporter()
        scope_based_periodic_exporting_metric_reader = ScopeBasedPeriodicExportingMetricReader(
            exporter=application_signals_metric_exporter,
            export_interval_millis=_get_metric_export_interval(),
            registered_scope_names={SYSTEM_METRICS_INSTRUMENTATION_SCOPE_NAME},
        )
        metric_readers.append(scope_based_periodic_exporting_metric_reader)

    if is_emf_enabled:
        emf_exporter = _create_emf_exporter()
        if emf_exporter:
            metric_readers.append(PeriodicExportingMetricReader(emf_exporter))


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

    custom_attributes = {AWS_LOCAL_SERVICE: service_name}

    if is_agent_observability_enabled():
        # Add aws.service.type if it doesn't exist in the resource
        if resource and resource.attributes.get(AWS_SERVICE_TYPE) is None:
            # Set a default agent type for AI agent observability
            custom_attributes[AWS_SERVICE_TYPE] = "gen_ai_agent"

    return resource.merge(Resource.create(custom_attributes))


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


def _is_aws_otlp_endpoint(otlp_endpoint: Optional[str], service: str) -> bool:
    """Is the given endpoint an AWS OTLP endpoint?"""

    if not otlp_endpoint:
        return False

    pattern = AWS_TRACES_OTLP_ENDPOINT_PATTERN if service == XRAY_SERVICE else AWS_LOGS_OTLP_ENDPOINT_PATTERN

    return bool(re.match(pattern, otlp_endpoint.lower()))


def _extract_endpoint_and_region_from_otlp_endpoint(endpoint: str):
    endpoint = endpoint.lower()
    region = endpoint.split(".")[1]

    return endpoint, region


def _validate_and_fetch_logs_header() -> OtlpLogHeaderSetting:
    """Checks if x-aws-log-group and x-aws-log-stream are present in the headers in order to send logs to
    AWS OTLP Logs endpoint."""

    logs_headers = os.environ.get(OTEL_EXPORTER_OTLP_LOGS_HEADERS)

    if not logs_headers:
        _logger.warning(
            "Improper configuration: Please configure the environment variable OTEL_EXPORTER_OTLP_LOGS_HEADERS "
            "to include x-aws-log-group and x-aws-log-stream"
        )
        return OtlpLogHeaderSetting(None, None, None, False)

    log_group = None
    log_stream = None
    namespace = None

    for pair in logs_headers.split(","):
        if "=" in pair:
            split = pair.split("=", 1)
            key = split[0]
            value = split[1]
            if key == AWS_OTLP_LOGS_GROUP_HEADER and value:
                log_group = value
            elif key == AWS_OTLP_LOGS_STREAM_HEADER and value:
                log_stream = value
            elif key == AWS_EMF_METRICS_NAMESPACE and value:
                namespace = value

    is_valid = log_group is not None and log_stream is not None

    return OtlpLogHeaderSetting(log_group, log_stream, namespace, is_valid)


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


def _check_emf_exporter_enabled() -> bool:
    """
    Checks if OTEL_METRICS_EXPORTER contains "awsemf", removes it if present,
    and updates the environment variable.

    Remove 'awsemf' from OTEL_METRICS_EXPORTER if present to prevent validation errors
    from _import_exporters in OTel dependencies which would try to load exporters
    We will contribute emf exporter to upstream for supporting OTel metrics in SDK

    Returns:
    bool: True if "awsemf" was found and removed, False otherwise.
    """
    # Get the current exporter value
    exporter_value = os.environ.get("OTEL_METRICS_EXPORTER", "")

    # Check if it's empty
    if not exporter_value:
        return False

    # Split by comma and convert to list
    exporters = [exp.strip() for exp in exporter_value.split(",")]

    # Check if awsemf is in the list
    if "awsemf" not in exporters:
        return False

    # Remove awsemf from the list
    exporters.remove("awsemf")

    # Join the remaining exporters and update the environment variable
    new_value = ",".join(exporters) if exporters else ""

    # Set the new value (or unset if empty)
    if new_value:
        os.environ["OTEL_METRICS_EXPORTER"] = new_value
    elif "OTEL_METRICS_EXPORTER" in os.environ:
        del os.environ["OTEL_METRICS_EXPORTER"]

    return True


def _create_emf_exporter():
    """Create and configure the CloudWatch EMF exporter."""
    try:
        session = get_aws_session()
        # Check if botocore is available before importing the EMF exporter
        if not session:
            _logger.warning("botocore is not installed. EMF exporter requires botocore")
            return None

        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter import (
            AwsCloudWatchEmfExporter,
        )

        log_header_setting = _validate_and_fetch_logs_header()

        if not log_header_setting.is_valid:
            return None

        return AwsCloudWatchEmfExporter(
            session=session,
            namespace=log_header_setting.namespace,
            log_group_name=log_header_setting.log_group,
            log_stream_name=log_header_setting.log_stream,
        )
    # pylint: disable=broad-exception-caught
    except Exception as errors:
        _logger.error("Failed to create EMF exporter: %s", errors)
        return None


def _create_aws_otlp_exporter(endpoint: str, service: str, region: str):
    """Create and configure the AWS OTLP exporters."""
    try:
        session = get_aws_session()
        # Check if botocore is available before importing the AWS exporter
        if not session:
            _logger.warning("Sigv4 Auth requires botocore to be enabled")
            return None

        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter
        from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter

        if service == XRAY_SERVICE:
            if is_agent_observability_enabled():
                # Span exporter needs an instance of logger provider in ai agent
                # observability case because we need to split input/output prompts
                # from span attributes and send them to the logs pipeline per
                # the new Gen AI semantic convention from OTel
                # ref: https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/
                return OTLPAwsSpanExporter(
                    session=session, endpoint=endpoint, aws_region=region, logger_provider=get_logger_provider()
                )

            return OTLPAwsSpanExporter(session=session, endpoint=endpoint, aws_region=region)

        if service == LOGS_SERIVCE:
            return OTLPAwsLogExporter(session=session, aws_region=region)

        return None
    # pylint: disable=broad-exception-caught
    except Exception as errors:
        _logger.error("Failed to create AWS OTLP exporter: %s", errors)
        return None
