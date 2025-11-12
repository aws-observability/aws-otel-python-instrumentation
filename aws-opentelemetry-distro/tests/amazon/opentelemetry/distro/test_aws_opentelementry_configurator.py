# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=too-many-lines

import os
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from requests import Session

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_LOCAL_SERVICE, AWS_SERVICE_TYPE
from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from amazon.opentelemetry.distro.aws_lambda_span_processor import AwsLambdaSpanProcessor
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from amazon.opentelemetry.distro.aws_opentelemetry_configurator import (
    LAMBDA_SPAN_EXPORT_BATCH_SIZE,
    OTEL_AWS_ENHANCED_CODE_ATTRIBUTES,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_LOGS_HEADERS,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    ApplicationSignalsExporterProvider,
    AwsOpenTelemetryConfigurator,
    OtlpLogHeaderSetting,
    _check_emf_exporter_enabled,
    _clear_logs_header_cache,
    _create_aws_otlp_exporter,
    _create_emf_exporter,
    _custom_import_sampler,
    _customize_log_record_processor,
    _customize_logs_exporter,
    _customize_metric_exporters,
    _customize_resource,
    _customize_sampler,
    _customize_span_exporter,
    _customize_span_processors,
    _export_unsampled_span_for_agent_observability,
    _export_unsampled_span_for_lambda,
    _fetch_logs_header,
    _init_logging,
    _is_application_signals_enabled,
    _is_application_signals_runtime_enabled,
    _is_defer_to_workers_enabled,
    _is_wsgi_master_process,
    is_enhanced_code_attributes,
)
from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro
from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from amazon.opentelemetry.distro.exporter.console.logs.compact_console_log_exporter import CompactConsoleLogExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession

# pylint: disable=line-too-long
from amazon.opentelemetry.distro.exporter.otlp.aws.logs._aws_cw_otlp_batch_log_record_processor import (
    AwsCloudWatchOtlpBatchLogRecordProcessor,
)
from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from amazon.opentelemetry.distro.otlp_udp_exporter import OTLPUdpSpanExporter
from amazon.opentelemetry.distro.sampler._aws_xray_sampling_client import _AwsXRaySamplingClient
from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import _AwsXRayRemoteSampler
from amazon.opentelemetry.distro.scope_based_exporter import ScopeBasedPeriodicExportingMetricReader
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.exporter.otlp.proto.common._internal.metrics_encoder import OTLPMetricExporterMixin
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as OTLPGrpcLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as OTLPGrpcOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as OTLPGrpcSpanExporter
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as OTLPHttpOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import get_meter_provider
from opentelemetry.processor.baggage import BaggageSpanProcessor
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter
from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG
from opentelemetry.sdk.metrics._internal.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, Tracer, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.sampling import DEFAULT_ON, Sampler
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import get_tracer_provider


# pylint: disable=too-many-public-methods
class TestAwsOpenTelemetryConfigurator(TestCase):
    """Tests AwsOpenTelemetryConfigurator and AwsOpenTelemetryDistro

    NOTE: This class setup Tracer Provider Globally, which can only be set once. If there is another setup for tracer
    provider, it may cause issues for those tests.
    """

    @classmethod
    def setUpClass(cls):
        # Store original environment variables to restore later
        cls._original_env = {}
        for key in list(os.environ.keys()):
            if key.startswith("OTEL_"):
                cls._original_env[key] = os.environ[key]
                del os.environ[key]

        # Run AwsOpenTelemetryDistro to set up environment, then validate expected env values.
        aws_open_telemetry_distro: AwsOpenTelemetryDistro = AwsOpenTelemetryDistro()
        aws_open_telemetry_distro.configure(apply_patches=False)
        validate_distro_environ()

        # Overwrite exporter configs to keep tests clean, set sampler configs for tests
        os.environ[OTEL_TRACES_EXPORTER] = "none"
        os.environ[OTEL_METRICS_EXPORTER] = "console"
        os.environ[OTEL_LOGS_EXPORTER] = "none"
        os.environ[OTEL_TRACES_SAMPLER] = "traceidratio"
        os.environ[OTEL_TRACES_SAMPLER_ARG] = "0.01"

        # Run configurator and get trace provider
        aws_otel_configurator: AwsOpenTelemetryConfigurator = AwsOpenTelemetryConfigurator()
        aws_otel_configurator.configure()
        cls.tracer_provider: TracerProvider = get_tracer_provider()

    @classmethod
    def tearDownClass(cls):
        # Explicitly shut down meter provider to avoid I/O errors on Python 3.9 with gevent
        # This ensures ConsoleMetricExporter is properly closed before Python cleanup
        try:
            meter_provider = get_meter_provider()
            if hasattr(meter_provider, "force_flush"):
                meter_provider.force_flush()
            if hasattr(meter_provider, "shutdown"):
                meter_provider.shutdown()
        except (ValueError, RuntimeError):
            # Ignore errors during cleanup:
            # - ValueError: I/O operation on closed file (the exact error we're trying to prevent)
            # - RuntimeError: Provider already shut down or threading issues
            pass

    def tearDown(self):
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", None)

    # The probability of this passing once without correct IDs is low, 20 times is inconceivable.
    def test_provide_generate_xray_ids(self):
        for _ in range(20):
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            start_time_sec: int = int(time.time())
            span: Span = tracer.start_span("test")
            trace_id: int = span.get_span_context().trace_id
            trace_id_4_byte_hex: str = hex(trace_id)[2:10]
            trace_id_4_byte_int: int = int(trace_id_4_byte_hex, 16)
            self.assertGreaterEqual(trace_id_4_byte_int, start_time_sec)

    # Sanity check that the trace ID ratio sampler works fine with the x-ray generator.
    def test_trace_id_ratio_sampler(self):
        for _ in range(20):
            num_spans: int = 100000
            num_sampled: int = 0
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            for _ in range(num_spans):
                span: Span = tracer.start_span("test")
                if span.get_span_context().trace_flags.sampled:
                    num_sampled += 1
                span.end()
            # Configured for 1%, confirm there are at most 5% to account for randomness and reduce test flakiness.
            self.assertGreater(0.05, num_sampled / num_spans)

    # Test method for importing xray sampler
    # Cannot test this logic via `aws_otel_configurator.configure()` because that will
    # attempt to setup tracer provider again, which can be only be done once (already done)
    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_import_xray_sampler_without_environment_arguments(self):
        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "http://127.0.0.1:2000/GetSamplingRules",
        )

    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_import_xray_sampler_with_valid_environment_arguments(self):
        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint=http://localhost:2000,polling_interval=600")

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 600)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://localhost:2000/GetSamplingRules"
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "polling_interval=123")

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 123)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://127.0.0.1:2000/GetSamplingRules"
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint=http://cloudwatch-agent.amazon-cloudwatch:2000")

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "http://cloudwatch-agent.amazon-cloudwatch:2000/GetSamplingRules",
        )

    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_import_xray_sampler_with_invalid_environment_arguments(self):
        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint=h=tt=p://=loca=lho=st:2000,polling_interval=FOOBAR")

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "h=tt=p://=loca=lho=st:2000/GetSamplingRules",
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, ",,=,==,,===,")

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "http://127.0.0.1:2000/GetSamplingRules",
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint,polling_interval")

        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._root._root._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._root._root._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://127.0.0.1:2000/GetSamplingRules"
        )

    def test_import_default_sampler_when_env_var_is_not_set(self):
        os.environ.pop(OTEL_TRACES_SAMPLER, None)
        default_sampler: Sampler = _custom_import_sampler(None, resource=None)

        self.assertIsNotNone(default_sampler)
        self.assertEqual(default_sampler.get_description(), DEFAULT_ON.get_description())
        # DEFAULT_ON is a ParentBased(ALWAYS_ON) sampler

    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_using_xray_sampler_sets_url_exclusion_env_vars(self):
        targets_to_exclude = "SamplingTargets,GetSamplingRules"
        os.environ.pop("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None)
        os.environ.pop("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None), None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None), None)

        _: Sampler = _custom_import_sampler("xray", resource=None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None), targets_to_exclude)
        self.assertEqual(os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None), targets_to_exclude)

    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(_AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_using_xray_sampler_appends_url_exclusion_env_vars(self):
        targets_to_exclude = "SamplingTargets,GetSamplingRules"
        os.environ.pop("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None)
        os.environ.pop("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None), None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None), None)
        os.environ.setdefault("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", ",,,target_A,target_B,,,")
        os.environ.setdefault("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", "target_C,target_D")

        _: Sampler = _custom_import_sampler("xray", resource=None)
        self.assertTrue(targets_to_exclude in os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None))
        self.assertTrue(targets_to_exclude in os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None))

    def test_not_using_xray_sampler_does_not_modify_url_exclusion_env_vars(self):
        os.environ.pop("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None)
        os.environ.pop("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None), None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None), None)

        _: Sampler = _custom_import_sampler("traceidratio", resource=None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None), None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None), None)

        os.environ.setdefault("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", ",,,target_A,target_B,,,")
        os.environ.setdefault("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", "target_C,target_D")

        _: Sampler = _custom_import_sampler("traceidratio", resource=None)
        self.assertEqual(os.environ.get("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None), ",,,target_A,target_B,,,")
        self.assertEqual(os.environ.get("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None), "target_C,target_D")

    def test_is_application_signals_enabled(self):
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        self.assertTrue(_is_application_signals_enabled())
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "False")
        self.assertFalse(_is_application_signals_enabled())
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        self.assertFalse(_is_application_signals_enabled())

    def test_is_application_signals_runtime_enabled(self):
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        self.assertTrue(_is_application_signals_runtime_enabled())
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        self.assertFalse(_is_application_signals_runtime_enabled())

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "False")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "True")
        self.assertFalse(_is_application_signals_runtime_enabled())

        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", None)
        self.assertFalse(_is_application_signals_enabled())

    def test_customize_sampler(self):
        mock_sampler: Sampler = MagicMock()
        customized_sampler: Sampler = _customize_sampler(mock_sampler)
        self.assertEqual(mock_sampler, customized_sampler)

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        customized_sampler = _customize_sampler(mock_sampler)
        self.assertNotEqual(mock_sampler, customized_sampler)
        self.assertIsInstance(customized_sampler, AlwaysRecordSampler)
        self.assertEqual(mock_sampler, customized_sampler._root_sampler)

    def test_customize_span_exporter(self):
        mock_exporter: SpanExporter = MagicMock(spec=OTLPSpanExporter)
        customized_exporter: SpanExporter = _customize_span_exporter(mock_exporter, Resource.get_empty())
        self.assertEqual(mock_exporter, customized_exporter)

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        customized_exporter = _customize_span_exporter(mock_exporter, Resource.get_empty())
        self.assertNotEqual(mock_exporter, customized_exporter)
        self.assertIsInstance(customized_exporter, AwsMetricAttributesSpanExporter)
        self.assertEqual(mock_exporter, customized_exporter._delegate)

        # when Application Signals is enabled and running in lambda
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        os.environ.setdefault("AWS_LAMBDA_FUNCTION_NAME", "myLambdaFunc")
        customized_exporter = _customize_span_exporter(mock_exporter, Resource.get_empty())
        self.assertNotEqual(mock_exporter, customized_exporter)
        self.assertIsInstance(customized_exporter, AwsMetricAttributesSpanExporter)
        self.assertIsInstance(customized_exporter._delegate, OTLPUdpSpanExporter)
        os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)

    def test_customize_span_exporter_with_agent_observability(self):
        # Test that logger_provider is passed when agent observability is enabled
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"
        os.environ[OTEL_EXPORTER_OTLP_TRACES_ENDPOINT] = "https://xray.us-east-1.amazonaws.com/v1/traces"

        mock_logger_provider = MagicMock()
        with patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_logger_provider",
            return_value=mock_logger_provider,
        ):
            mock_exporter = MagicMock(spec=OTLPSpanExporter)
            result = _customize_span_exporter(mock_exporter, Resource.get_empty())

            self.assertIsInstance(result, OTLPAwsSpanExporter)
            self.assertEqual(result._logger_provider, mock_logger_provider)

        # Test that logger_provider is not passed when agent observability is disabled
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "false"

        mock_exporter = MagicMock(spec=OTLPSpanExporter)
        result = _customize_span_exporter(mock_exporter, Resource.get_empty())

        self.assertIsInstance(result, OTLPAwsSpanExporter)
        self.assertIsNone(result._logger_provider)

        # Clean up
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, None)

    def test_customize_span_processors_with_agent_observability(self):
        mock_tracer_provider: TracerProvider = MagicMock()

        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        mock_tracer_provider.reset_mock()

        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "https://xray.us-east-1.amazonaws.com/v1/traces"
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 2)

        first_processor = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
        self.assertIsInstance(first_processor, BatchUnsampledSpanProcessor)
        second_processor = mock_tracer_provider.add_span_processor.call_args_list[1].args[0]
        self.assertIsInstance(second_processor, BaggageSpanProcessor)

        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)

    def test_baggage_span_processor_session_id_filtering(self):
        """Test that BaggageSpanProcessor only set session.id filter by default"""

        # Set up agent observability
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"

        # Create a new tracer provider for this test
        tracer_provider = TracerProvider()

        # Add our span processors
        _customize_span_processors(tracer_provider, Resource.get_empty())

        # Verify that the BaggageSpanProcessor was added
        # The _active_span_processor is a composite processor containing all processors
        active_processor = tracer_provider._active_span_processor

        # Check if it's a composite processor with multiple processors
        if hasattr(active_processor, "_span_processors"):
            processors = active_processor._span_processors
        else:
            # If it's a single processor, wrap it in a list
            processors = [active_processor]

        baggage_processors = [
            processor for processor in processors if processor.__class__.__name__ == "BaggageSpanProcessor"
        ]
        self.assertEqual(len(baggage_processors), 1)

        # Verify the predicate function only accepts session.id
        baggage_processor = baggage_processors[0]
        predicate = baggage_processor._baggage_key_predicate

        # Test the predicate function directly
        self.assertTrue(predicate("session.id"))
        self.assertFalse(predicate("user.id"))
        self.assertFalse(predicate("request.id"))
        self.assertFalse(predicate("other.key"))
        self.assertFalse(predicate(""))
        self.assertFalse(predicate("session"))
        self.assertFalse(predicate("id"))

        # Clean up
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)

    def test_customize_span_exporter_sigv4(self):

        traces_good_endpoints = [
            "https://xray.us-east-1.amazonaws.com/v1/traces",
            "https://XRAY.US-EAST-1.AMAZONAWS.COM/V1/TRACES",
            "https://xray.us-east-1.amazonaws.com/v1/traces",
            "https://XRAY.US-EAST-1.amazonaws.com/v1/traces",
            "https://xray.US-EAST-1.AMAZONAWS.com/v1/traces",
            "https://Xray.Us-East-1.amazonaws.com/v1/traces",
            "https://xRAY.us-EAST-1.amazonaws.com/v1/traces",
            "https://XRAY.us-EAST-1.AMAZONAWS.com/v1/TRACES",
            "https://xray.US-EAST-1.amazonaws.com/V1/Traces",
            "https://xray.us-east-1.AMAZONAWS.COM/v1/traces",
            "https://XrAy.Us-EaSt-1.AmAzOnAwS.cOm/V1/TrAcEs",
            "https://xray.US-EAST-1.amazonaws.com/v1/traces",
            "https://xray.us-east-1.amazonaws.com/V1/TRACES",
            "https://XRAY.US-EAST-1.AMAZONAWS.COM/v1/traces",
            "https://xray.us-east-1.AMAZONAWS.COM/V1/traces",
        ]

        traces_bad_endpoints = [
            "http://localhost:4318/v1/traces",
            "http://xray.us-east-1.amazonaws.com/v1/traces",
            "ftp://xray.us-east-1.amazonaws.com/v1/traces",
            "https://ray.us-east-1.amazonaws.com/v1/traces",
            "https://xra.us-east-1.amazonaws.com/v1/traces",
            "https://x-ray.us-east-1.amazonaws.com/v1/traces",
            "https://xray.amazonaws.com/v1/traces",
            "https://xray.us-east-1.amazon.com/v1/traces",
            "https://xray.us-east-1.aws.com/v1/traces",
            "https://xray.us_east_1.amazonaws.com/v1/traces",
            "https://xray.us.east.1.amazonaws.com/v1/traces",
            "https://xray..amazonaws.com/v1/traces",
            "https://xray.us-east-1.amazonaws.com/traces",
            "https://xray.us-east-1.amazonaws.com/v2/traces",
            "https://xray.us-east-1.amazonaws.com/v1/trace",
            "https://xray.us-east-1.amazonaws.com/v1/traces/",
            "https://xray.us-east-1.amazonaws.com//v1/traces",
            "https://xray.us-east-1.amazonaws.com/v1//traces",
            "https://xray.us-east-1.amazonaws.com/v1/traces?param=value",
            "https://xray.us-east-1.amazonaws.com/v1/traces#fragment",
            "https://xray.us-east-1.amazonaws.com:443/v1/traces",
            "https:/xray.us-east-1.amazonaws.com/v1/traces",
            "https:://xray.us-east-1.amazonaws.com/v1/traces",
        ]

        good_configs = []
        bad_configs = []

        for endpoint in traces_good_endpoints:
            config = {
                OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: endpoint,
            }

            good_configs.append(config)

        for endpoint in traces_bad_endpoints:
            config = {
                OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: endpoint,
            }

            bad_configs.append(config)

        for config in good_configs:
            _clear_logs_header_cache()
            self.customize_exporter_test(
                config,
                _customize_span_exporter,
                OTLPSpanExporter(),
                OTLPAwsSpanExporter,
                AwsAuthSession,
                Compression.NoCompression,
                Resource.get_empty(),
            )

        for config in bad_configs:
            _clear_logs_header_cache()
            self.customize_exporter_test(
                config,
                _customize_span_exporter,
                OTLPSpanExporter(),
                OTLPSpanExporter,
                Session,
                Compression.NoCompression,
                Resource.get_empty(),
            )

        self.assertIsInstance(
            _customize_span_exporter(OTLPGrpcSpanExporter(), Resource.get_empty()), OTLPGrpcSpanExporter
        )

    def test_customize_logs_exporter_sigv4(self):
        logs_good_endpoints = [
            "https://logs.us-east-1.amazonaws.com/v1/logs",
            "https://LOGS.US-EAST-1.AMAZONAWS.COM/V1/LOGS",
            "https://logs.us-east-1.amazonaws.com/v1/logs",
            "https://LOGS.US-EAST-1.amazonaws.com/v1/logs",
            "https://logs.US-EAST-1.AMAZONAWS.com/v1/logs",
            "https://Logs.Us-East-1.amazonaws.com/v1/logs",
            "https://lOGS.us-EAST-1.amazonaws.com/v1/logs",
            "https://LOGS.us-EAST-1.AMAZONAWS.com/v1/LOGS",
            "https://logs.US-EAST-1.amazonaws.com/V1/Logs",
            "https://logs.us-east-1.AMAZONAWS.COM/v1/logs",
            "https://LoGs.Us-EaSt-1.AmAzOnAwS.cOm/V1/LoGs",
            "https://logs.US-EAST-1.amazonaws.com/v1/logs",
            "https://logs.us-east-1.amazonaws.com/V1/LOGS",
            "https://LOGS.US-EAST-1.AMAZONAWS.COM/v1/logs",
            "https://logs.us-east-1.AMAZONAWS.COM/V1/logs",
        ]

        logs_bad_endpoints = [
            "http://localhost:4318/v1/logs",
            "http://logs.us-east-1.amazonaws.com/v1/logs",
            "ftp://logs.us-east-1.amazonaws.com/v1/logs",
            "https://log.us-east-1.amazonaws.com/v1/logs",
            "https://logging.us-east-1.amazonaws.com/v1/logs",
            "https://cloud-logs.us-east-1.amazonaws.com/v1/logs",
            "https://logs.amazonaws.com/v1/logs",
            "https://logs.us-east-1.amazon.com/v1/logs",
            "https://logs.us-east-1.aws.com/v1/logs",
            "https://logs.us_east_1.amazonaws.com/v1/logs",
            "https://logs.us.east.1.amazonaws.com/v1/logs",
            "https://logs..amazonaws.com/v1/logs",
            "https://logs.us-east-1.amazonaws.com/logs",
            "https://logs.us-east-1.amazonaws.com/v2/logs",
            "https://logs.us-east-1.amazonaws.com/v1/log",
            "https://logs.us-east-1.amazonaws.com/v1/logs/",
            "https://logs.us-east-1.amazonaws.com//v1/logs",
            "https://logs.us-east-1.amazonaws.com/v1//logs",
            "https://logs.us-east-1.amazonaws.com/v1/logs?param=value",
            "https://logs.us-east-1.amazonaws.com/v1/logs#fragment",
            "https://logs.us-east-1.amazonaws.com:443/v1/logs",
            "https:/logs.us-east-1.amazonaws.com/v1/logs",
            "https:://logs.us-east-1.amazonaws.com/v1/logs",
            "https://logs.us-east-1.amazonaws.com/v1/logging",
            "https://logs.us-east-1.amazonaws.com/v1/cloudwatchlogs",
            "https://logs.us-east-1.amazonaws.com/v1/cwlogs",
        ]

        logs_bad_headers = [
            "x-aws-log-group=,x-aws-log-stream=test",
            "x-aws-log-stream=test",
            "x-aws-log-group=test",
            "",
        ]

        good_configs = []
        bad_configs = []

        for endpoint in logs_good_endpoints:
            config = {
                OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: endpoint,
                OTEL_EXPORTER_OTLP_LOGS_HEADERS: "x-aws-log-group=test,x-aws-log-stream=test",
            }

            good_configs.append(config)

        for endpoint in logs_bad_endpoints:
            config = {
                OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: endpoint,
                OTEL_EXPORTER_OTLP_LOGS_HEADERS: "x-aws-log-group=test,x-aws-log-stream=test",
            }

            bad_configs.append(config)

        for headers in logs_bad_headers:
            config = {
                OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: "https://logs.us-east-1.amazonaws.com/v1/logs",
                OTEL_EXPORTER_OTLP_LOGS_HEADERS: headers,
            }

            bad_configs.append(config)

        for config in good_configs:
            _clear_logs_header_cache()
            self.customize_exporter_test(
                config,
                _customize_logs_exporter,
                OTLPLogExporter(),
                OTLPAwsLogExporter,
                AwsAuthSession,
                Compression.Gzip,
            )

        for config in bad_configs:
            _clear_logs_header_cache()
            self.customize_exporter_test(
                config, _customize_logs_exporter, OTLPLogExporter(), OTLPLogExporter, Session, Compression.NoCompression
            )

        self.assertIsInstance(_customize_logs_exporter(OTLPGrpcLogExporter()), OTLPGrpcLogExporter)

    # Need to patch all of these to prevent some weird multi-threading error with the LogProvider
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.LoggingHandler", return_value=MagicMock())
    @patch("logging.getLogger", return_value=MagicMock())
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._customize_logs_exporter")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.LoggerProvider", return_value=MagicMock())
    @patch(
        "amazon.opentelemetry.distro.aws_opentelemetry_configurator.BatchLogRecordProcessor", return_value=MagicMock()
    )
    def test_init_logging(
        self,
        mock_batch_processor,
        mock_logger_provider,
        mock_customize_logs_exporter,
        mock_get_logger,
        mock_logging_handler,
    ):

        captured_exporter = None

        def capture_exporter(*args, **kwargs):
            nonlocal captured_exporter
            result = _customize_logs_exporter(*args, **kwargs)
            captured_exporter = result
            return result

        mock_customize_logs_exporter.side_effect = capture_exporter

        test_cases = [
            [{"otlp": OTLPLogExporter}, OTLPLogExporter],
            [{}, OTLPLogExporter],
            [{"grpc": OTLPGrpcLogExporter}, OTLPGrpcLogExporter],
        ]

        os.environ[OTEL_EXPORTER_OTLP_LOGS_ENDPOINT] = "https://logs.us-east-1.amazonaws.com/v1/logs"

        for tc in test_cases:
            exporter_dict = tc[0]
            expected_exporter = tc[1]
            _init_logging(exporter_dict, Resource.get_empty())

            self.assertIsInstance(captured_exporter, expected_exporter)

        os.environ.pop(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.LoggingHandler", return_value=MagicMock())
    @patch("logging.getLogger", return_value=MagicMock())
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._customize_logs_exporter")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.LoggerProvider", return_value=MagicMock())
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._customize_log_record_processor")
    def test_init_logging_console_exporter_replacement(
        self,
        mock_customize_processor,
        mock_logger_provider,
        mock_customize_logs_exporter,
        mock_get_logger,
        mock_logging_handler,
    ):
        """Test that ConsoleLogExporter is replaced with CompactConsoleLogExporter when in Lambda"""

        # Mock _is_lambda_environment to return True
        with patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment", return_value=True
        ):
            # Test with ConsoleLogExporter
            exporters = {"console": ConsoleLogExporter}
            _init_logging(exporters, Resource.get_empty())

            # Verify that _customize_log_record_processor was called
            mock_customize_processor.assert_called_once()

            # Get the exporter that was passed to _customize_logs_exporter
            call_args = mock_customize_logs_exporter.call_args
            exporter_instance = call_args[0][0]

            # Verify it's a CompactConsoleLogExporter instance
            self.assertIsInstance(exporter_instance, CompactConsoleLogExporter)

        # Reset mocks
        mock_customize_processor.reset_mock()
        mock_customize_logs_exporter.reset_mock()

        # Test when not in Lambda environment - should not replace
        with patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment", return_value=False
        ):
            exporters = {"console": ConsoleLogExporter}
            _init_logging(exporters, Resource.get_empty())

            # Get the exporter that was passed to _customize_logs_exporter
            call_args = mock_customize_logs_exporter.call_args
            exporter_instance = call_args[0][0]

            # Verify it's still a regular ConsoleLogExporter
            self.assertIsInstance(exporter_instance, ConsoleLogExporter)
            self.assertNotIsInstance(exporter_instance, CompactConsoleLogExporter)

    def test_customize_span_processors(self):
        mock_tracer_provider: TracerProvider = MagicMock()
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", None)

        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        mock_tracer_provider.reset_mock()

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 2)
        first_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
        self.assertIsInstance(first_processor, AttributePropagatingSpanProcessor)
        second_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[1].args[0]
        self.assertIsInstance(second_processor, AwsSpanMetricsProcessor)

        mock_tracer_provider.reset_mock()

        os.environ.setdefault("AGENT_OBSERVABILITY_ENABLED", "true")
        os.environ.setdefault("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://xray.us-east-1.amazonaws.com/v1/traces")
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 4)

        processors = [call.args[0] for call in mock_tracer_provider.add_span_processor.call_args_list]
        self.assertIsInstance(processors[0], BatchUnsampledSpanProcessor)
        self.assertIsInstance(processors[1], BaggageSpanProcessor)
        self.assertIsInstance(processors[2], AttributePropagatingSpanProcessor)
        self.assertIsInstance(processors[3], AwsSpanMetricsProcessor)

        os.environ.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")

    def test_customize_span_processors_with_code_correlation_enabled(self):
        """Test that CodeAttributesSpanProcessor is added when code correlation is enabled"""
        mock_tracer_provider: TracerProvider = MagicMock()

        # Clean up environment to ensure consistent test state
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop(OTEL_AWS_ENHANCED_CODE_ATTRIBUTES, None)

        # Test without code correlation enabled - should not add CodeAttributesSpanProcessor
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        mock_tracer_provider.reset_mock()

        # Test with code correlation enabled - should add CodeAttributesSpanProcessor
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "true"

        with patch(
            "amazon.opentelemetry.distro.code_correlation.CodeAttributesSpanProcessor"
        ) as mock_code_processor_class:
            mock_code_processor_instance = MagicMock()
            mock_code_processor_class.return_value = mock_code_processor_instance

            _customize_span_processors(mock_tracer_provider, Resource.get_empty())

            # Verify CodeAttributesSpanProcessor was created and added
            mock_code_processor_class.assert_called_once()
            mock_tracer_provider.add_span_processor.assert_called_once_with(mock_code_processor_instance)

        mock_tracer_provider.reset_mock()

        # Test with code correlation enabled along with application signals
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "true"
        os.environ["OTEL_AWS_APPLICATION_SIGNALS_ENABLED"] = "True"
        os.environ["OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED"] = "False"

        with patch(
            "amazon.opentelemetry.distro.code_correlation.CodeAttributesSpanProcessor"
        ) as mock_code_processor_class:
            mock_code_processor_instance = MagicMock()
            mock_code_processor_class.return_value = mock_code_processor_instance

            _customize_span_processors(mock_tracer_provider, Resource.get_empty())

            # Should have 3 processors: CodeAttributesSpanProcessor, AttributePropagatingSpanProcessor,
            # and AwsSpanMetricsProcessor
            self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 3)

            # First should be CodeAttributesSpanProcessor
            first_call_args = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
            self.assertEqual(first_call_args, mock_code_processor_instance)

            # Second should be AttributePropagatingSpanProcessor
            second_call_args = mock_tracer_provider.add_span_processor.call_args_list[1].args[0]
            self.assertIsInstance(second_call_args, AttributePropagatingSpanProcessor)

            # Third should be AwsSpanMetricsProcessor
            third_call_args = mock_tracer_provider.add_span_processor.call_args_list[2].args[0]
            self.assertIsInstance(third_call_args, AwsSpanMetricsProcessor)

        # Clean up
        os.environ.pop(OTEL_AWS_ENHANCED_CODE_ATTRIBUTES, None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", None)

    def test_customize_span_processors_lambda(self):
        mock_tracer_provider: TracerProvider = MagicMock()
        # Clean up environment to ensure consistent test state
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)

        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("AWS_LAMBDA_FUNCTION_NAME", "myLambdaFunc")
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 3)
        first_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
        self.assertIsInstance(first_processor, AwsLambdaSpanProcessor)
        second_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[1].args[0]
        self.assertIsInstance(second_processor, AttributePropagatingSpanProcessor)
        third_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[2].args[0]
        self.assertIsInstance(third_processor, BatchUnsampledSpanProcessor)
        self.assertEqual(third_processor._batch_processor._max_export_batch_size, LAMBDA_SPAN_EXPORT_BATCH_SIZE)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)

    def test_application_signals_exporter_provider(self):
        # Check default protocol - HTTP, as specified by AwsOpenTelemetryDistro.
        exporter: OTLPMetricExporterMixin = ApplicationSignalsExporterProvider().create_exporter()
        self.assertIsInstance(exporter, OTLPHttpOTLPMetricExporter)
        self.assertEqual("http://localhost:4316/v1/metrics", exporter._endpoint)

        # Overwrite protocol to gRPC.
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "grpc"
        exporter: SpanExporter = ApplicationSignalsExporterProvider().create_exporter()
        self.assertIsInstance(exporter, OTLPGrpcOTLPMetricExporter)
        self.assertEqual("localhost:4315", exporter._endpoint)

        # Overwrite protocol back to HTTP.
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        exporter: SpanExporter = ApplicationSignalsExporterProvider().create_exporter()
        self.assertIsInstance(exporter, OTLPHttpOTLPMetricExporter)
        self.assertEqual("http://localhost:4316/v1/metrics", exporter._endpoint)

    def test_is_defer_to_workers_enabled(self):
        os.environ.setdefault("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        self.assertTrue(_is_defer_to_workers_enabled())
        os.environ.pop("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", None)

        os.environ.setdefault("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", "False")
        self.assertFalse(_is_defer_to_workers_enabled())
        os.environ.pop("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", None)
        self.assertFalse(_is_defer_to_workers_enabled())

    def test_is_wsgi_master_process_first_time(self):
        self.assertTrue(_is_wsgi_master_process())
        self.assertEqual(os.environ["IS_WSGI_MASTER_PROCESS_ALREADY_SEEN"], "true")
        os.environ.pop("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", None)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._initialize_components")
    def test_initialize_components_skipped_in_master_when_deferred_enabled(self, mock_initialize_components):
        os.environ.setdefault("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", "True")
        os.environ.pop("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", None)
        self.assertTrue(_is_defer_to_workers_enabled())
        AwsOpenTelemetryConfigurator()._configure()
        mock_initialize_components.assert_not_called()
        os.environ.pop("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", None)
        os.environ.pop("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", None)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._initialize_components")
    def test_initialize_components_called_in_worker_when_deferred_enabled(self, mock_initialize_components):
        os.environ.setdefault("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", "True")
        os.environ.setdefault("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", "true")
        self.assertTrue(_is_defer_to_workers_enabled())
        self.assertFalse(_is_wsgi_master_process())
        AwsOpenTelemetryConfigurator()._configure()
        mock_initialize_components.assert_called_once()
        os.environ.pop("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", None)
        os.environ.pop("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", None)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._initialize_components")
    def test_initialize_components_called_when_deferred_disabled(self, mock_initialize_components):
        os.environ.pop("OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED", None)
        self.assertFalse(_is_defer_to_workers_enabled())
        AwsOpenTelemetryConfigurator()._configure()
        mock_initialize_components.assert_called_once()
        os.environ.pop("IS_WSGI_MASTER_PROCESS_ALREADY_SEEN", None)

    def test_export_unsampled_span_for_lambda(self):
        mock_tracer_provider: TracerProvider = MagicMock()
        _export_unsampled_span_for_lambda(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("AWS_LAMBDA_FUNCTION_NAME", "myfunction")
        _export_unsampled_span_for_lambda(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 1)
        first_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
        self.assertIsInstance(first_processor, BatchUnsampledSpanProcessor)
        os.environ.pop("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", None)
        os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)

    # pylint: disable=no-self-use
    def test_export_unsampled_span_for_agent_observability(self):
        mock_tracer_provider: TracerProvider = MagicMock()

        _export_unsampled_span_for_agent_observability(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        mock_tracer_provider.reset_mock()

        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "https://xray.us-east-1.amazonaws.com/v1/traces"
        _export_unsampled_span_for_agent_observability(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 1)
        processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
        self.assertIsInstance(processor, BatchUnsampledSpanProcessor)

        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)

    # pylint: disable=no-self-use
    def test_export_unsampled_span_for_agent_observability_uses_aws_exporter(self):
        """Test that OTLPAwsSpanExporter is used for AWS endpoints"""
        mock_tracer_provider: TracerProvider = MagicMock()

        with patch(
            "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.OTLPAwsSpanExporter"
        ) as mock_aws_exporter:
            with patch(
                "amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_logger_provider"
            ) as mock_logger_provider:
                with patch(
                    "amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session"
                ) as mock_session:
                    mock_session.return_value = MagicMock()
                    os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"
                    os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "https://xray.us-east-1.amazonaws.com/v1/traces"

                    _export_unsampled_span_for_agent_observability(mock_tracer_provider, Resource.get_empty())

                    # Verify OTLPAwsSpanExporter is created with correct parameters
                    mock_aws_exporter.assert_called_once_with(
                        session=mock_session.return_value,
                        endpoint="https://xray.us-east-1.amazonaws.com/v1/traces",
                        aws_region="us-east-1",
                        logger_provider=mock_logger_provider.return_value,
                    )
                    # Verify processor is added to tracer provider
                    mock_tracer_provider.add_span_processor.assert_called_once()

        # Clean up
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        os.environ.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)

    # pylint: disable=no-self-use
    def test_customize_span_processors_calls_export_unsampled_span(self):
        """Test that _customize_span_processors calls _export_unsampled_span_for_agent_observability"""
        mock_tracer_provider: TracerProvider = MagicMock()

        with patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_configurator._export_unsampled_span_for_agent_observability"
        ) as mock_agent_observability:
            # Test that agent observability function is NOT called when disabled
            os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
            _customize_span_processors(mock_tracer_provider, Resource.get_empty())
            mock_agent_observability.assert_not_called()

            # Test that agent observability function is called when enabled
            mock_agent_observability.reset_mock()
            os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"
            _customize_span_processors(mock_tracer_provider, Resource.get_empty())
            mock_agent_observability.assert_called_once_with(mock_tracer_provider, Resource.get_empty())

            # Clean up
            os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)

    def test_customize_metric_exporter(self):
        metric_readers = []
        views = []

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "True")
        os.environ.setdefault("OTEL_METRIC_EXPORT_INTERVAL", "1000")

        _customize_metric_exporters(metric_readers, views)
        self.assertEqual(1, len(metric_readers))
        self.assertEqual(6, len(views))
        self.assertIsInstance(metric_readers[0], ScopeBasedPeriodicExportingMetricReader)
        pmr: ScopeBasedPeriodicExportingMetricReader = metric_readers[0]
        self.assertEqual(1000, pmr._export_interval_millis)
        pmr.shutdown()

        periodic_exporting_metric_reader: PeriodicExportingMetricReader = MagicMock()
        metric_readers = [periodic_exporting_metric_reader]
        views = []
        _customize_metric_exporters(metric_readers, views)
        self.assertEqual(2, len(metric_readers))
        self.assertIsInstance(metric_readers[1], ScopeBasedPeriodicExportingMetricReader)
        pmr: ScopeBasedPeriodicExportingMetricReader = metric_readers[1]
        self.assertEqual(1000, pmr._export_interval_millis)
        pmr.shutdown()
        self.assertEqual(5, len(views))

        os.environ.pop("OTEL_METRIC_EXPORT_INTERVAL", None)

    def customize_exporter_test(
        self, config, executor, default_exporter, expected_exporter_type, expected_session, expected_compression, *args
    ):
        for key, value in config.items():
            os.environ[key] = value

        try:
            result = executor(default_exporter, *args)
            self.assertIsInstance(result, expected_exporter_type)
            self.assertIsInstance(result._session, expected_session)
            self.assertEqual(result._compression, expected_compression)
        finally:
            for key in config.keys():
                os.environ.pop(key, None)

    def test_check_emf_exporter_enabled(self):
        # Test when OTEL_METRICS_EXPORTER is not set
        os.environ.pop("OTEL_METRICS_EXPORTER", None)
        self.assertFalse(_check_emf_exporter_enabled())

        # Test when OTEL_METRICS_EXPORTER is empty
        os.environ["OTEL_METRICS_EXPORTER"] = ""
        self.assertFalse(_check_emf_exporter_enabled())

        # Test when awsemf is not in the list
        os.environ["OTEL_METRICS_EXPORTER"] = "console,otlp"
        self.assertFalse(_check_emf_exporter_enabled())

        # Test when awsemf is in the list
        os.environ["OTEL_METRICS_EXPORTER"] = "console,awsemf,otlp"
        self.assertTrue(_check_emf_exporter_enabled())
        # Should remove awsemf from the list
        self.assertEqual(os.environ["OTEL_METRICS_EXPORTER"], "console,otlp")

        # Test when awsemf is the only exporter
        os.environ["OTEL_METRICS_EXPORTER"] = "awsemf"
        self.assertTrue(_check_emf_exporter_enabled())
        # Should remove the environment variable entirely
        self.assertNotIn("OTEL_METRICS_EXPORTER", os.environ)

        # Test with spaces in the list
        os.environ["OTEL_METRICS_EXPORTER"] = " console , awsemf , otlp "
        self.assertTrue(_check_emf_exporter_enabled())
        self.assertEqual(os.environ["OTEL_METRICS_EXPORTER"], "console,otlp")

        # Clean up
        os.environ.pop("OTEL_METRICS_EXPORTER", None)

    def test_fetch_logs_header(self):
        _clear_logs_header_cache()

        # Test when headers are not set
        os.environ.pop(OTEL_EXPORTER_OTLP_LOGS_HEADERS, None)
        result = _fetch_logs_header()
        self.assertIsInstance(result, OtlpLogHeaderSetting)
        self.assertIsNone(result.log_group)
        self.assertIsNone(result.log_stream)
        self.assertIsNone(result.namespace)
        self.assertFalse(result.is_valid())

        # Test singleton behavior - should return the same cached instance
        result2 = _fetch_logs_header()
        self.assertIs(result, result2)  # Same object reference

        _clear_logs_header_cache()
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-group=test-group,x-aws-log-stream=test-stream"
        result = _fetch_logs_header()
        self.assertEqual(result.log_group, "test-group")
        self.assertEqual(result.log_stream, "test-stream")
        self.assertIsNone(result.namespace)
        self.assertTrue(result.is_valid())

        # Test singleton behavior again
        result2 = _fetch_logs_header()
        self.assertIs(result, result2)

        _clear_logs_header_cache()
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = (
            "x-aws-log-group=test-group,x-aws-log-stream=test-stream,x-aws-metric-namespace=test-namespace"
        )
        result = _fetch_logs_header()
        self.assertEqual(result.namespace, "test-namespace")
        self.assertTrue(result.is_valid())

        _clear_logs_header_cache()
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-stream=test-stream"
        result = _fetch_logs_header()
        self.assertEqual(result.log_stream, "test-stream")
        self.assertFalse(result.is_valid())

        _clear_logs_header_cache()
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-group=test-group"
        result = _fetch_logs_header()
        self.assertEqual(result.log_group, "test-group")
        self.assertIsNone(result.log_stream)
        self.assertFalse(result.is_valid())

        _clear_logs_header_cache()
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-group=,x-aws-log-stream=test-stream"
        result = _fetch_logs_header()
        self.assertIsNone(result.log_group)
        self.assertEqual(result.log_stream, "test-stream")
        self.assertFalse(result.is_valid())

        _clear_logs_header_cache()
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-group=test-group,x-aws-log-stream="
        result = _fetch_logs_header()
        self.assertEqual(result.log_group, "test-group")
        self.assertIsNone(result.log_stream)
        self.assertFalse(result.is_valid())

        # Clean up
        os.environ.pop(OTEL_EXPORTER_OTLP_LOGS_HEADERS, None)
        _clear_logs_header_cache()

    @patch(
        "amazon.opentelemetry.distro.aws_opentelemetry_configurator.is_agent_observability_enabled", return_value=False
    )
    def test_customize_log_record_processor_without_agent_observability(self, _):
        """Test that BatchLogRecordProcessor is used when agent observability is not enabled"""
        mock_logger_provider = MagicMock()
        mock_exporter = MagicMock(spec=OTLPAwsLogExporter)

        _customize_log_record_processor(mock_logger_provider, mock_exporter)

        mock_logger_provider.add_log_record_processor.assert_called_once()
        added_processor = mock_logger_provider.add_log_record_processor.call_args[0][0]
        self.assertIsInstance(added_processor, BatchLogRecordProcessor)

    @patch(
        "amazon.opentelemetry.distro.aws_opentelemetry_configurator.is_agent_observability_enabled", return_value=True
    )
    def test_customize_log_record_processor_with_agent_observability(self, _):
        """Test that AwsCloudWatchOtlpBatchLogRecordProcessor is used when agent observability is enabled"""
        mock_logger_provider = MagicMock()
        mock_exporter = MagicMock(spec=OTLPAwsLogExporter)

        _customize_log_record_processor(mock_logger_provider, mock_exporter)

        mock_logger_provider.add_log_record_processor.assert_called_once()
        added_processor = mock_logger_provider.add_log_record_processor.call_args[0][0]
        self.assertIsInstance(added_processor, AwsCloudWatchOtlpBatchLogRecordProcessor)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_logger_provider")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_aws_otlp_exporter(self, mock_get_session, mock_is_agent_enabled, mock_get_logger_provider):
        # Test when botocore is not installed
        mock_get_session.return_value = None
        result = _create_aws_otlp_exporter("https://xray.us-east-1.amazonaws.com/v1/traces", "xray", "us-east-1")
        self.assertIsNone(result)

        # Reset mock for subsequent tests
        mock_get_session.reset_mock()
        mock_get_session.return_value = MagicMock()
        mock_get_logger_provider.return_value = MagicMock()

        # Test xray service without agent observability
        mock_is_agent_enabled.return_value = False
        with patch(
            "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.OTLPAwsSpanExporter"
        ) as mock_span_exporter_class:
            mock_exporter_instance = MagicMock()
            mock_span_exporter_class.return_value = mock_exporter_instance

            result = _create_aws_otlp_exporter("https://xray.us-east-1.amazonaws.com/v1/traces", "xray", "us-east-1")
            self.assertIsNotNone(result)
            self.assertEqual(result, mock_exporter_instance)
            mock_span_exporter_class.assert_called_with(
                session=mock_get_session.return_value,
                endpoint="https://xray.us-east-1.amazonaws.com/v1/traces",
                aws_region="us-east-1",
            )

        # Test xray service with agent observability
        mock_is_agent_enabled.return_value = True
        with patch(
            "amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter.OTLPAwsSpanExporter"
        ) as mock_span_exporter_class:
            mock_exporter_instance = MagicMock()
            mock_span_exporter_class.return_value = mock_exporter_instance

            result = _create_aws_otlp_exporter("https://xray.us-east-1.amazonaws.com/v1/traces", "xray", "us-east-1")
            self.assertIsNotNone(result)
            self.assertEqual(result, mock_exporter_instance)
            mock_span_exporter_class.assert_called_with(
                session=mock_get_session.return_value,
                endpoint="https://xray.us-east-1.amazonaws.com/v1/traces",
                aws_region="us-east-1",
                logger_provider=mock_get_logger_provider.return_value,
            )

        # Test logs service
        with patch(
            "amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter.OTLPAwsLogExporter"
        ) as mock_log_exporter_class:
            mock_exporter_instance = MagicMock()
            mock_log_exporter_class.return_value = mock_exporter_instance

            result = _create_aws_otlp_exporter("https://logs.us-east-1.amazonaws.com/v1/logs", "logs", "us-east-1")
            self.assertIsNotNone(result)
            self.assertEqual(result, mock_exporter_instance)
            mock_log_exporter_class.assert_called_with(session=mock_get_session.return_value, aws_region="us-east-1")

        # Test exception handling
        mock_get_session.side_effect = Exception("Test exception")
        result = _create_aws_otlp_exporter("https://xray.us-east-1.amazonaws.com/v1/traces", "xray", "us-east-1")
        self.assertIsNone(result)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_service_attribute")
    def test_customize_resource_without_agent_observability(self, mock_get_service_attribute, mock_is_agent_enabled):
        """Test _customize_resource when agent observability is disabled"""
        mock_is_agent_enabled.return_value = False
        mock_get_service_attribute.return_value = ("test-service", False)

        resource = Resource.create({ResourceAttributes.SERVICE_NAME: "test-service"})
        result = _customize_resource(resource)

        # Should only have AWS_LOCAL_SERVICE added
        self.assertEqual(result.attributes[AWS_LOCAL_SERVICE], "test-service")
        self.assertNotIn(AWS_SERVICE_TYPE, result.attributes)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_service_attribute")
    def test_customize_resource_with_agent_observability_default(
        self, mock_get_service_attribute, mock_is_agent_enabled
    ):
        """Test _customize_resource when agent observability is enabled with default agent type"""
        mock_is_agent_enabled.return_value = True
        mock_get_service_attribute.return_value = ("test-service", False)

        resource = Resource.create({ResourceAttributes.SERVICE_NAME: "test-service"})
        result = _customize_resource(resource)

        # Should have both AWS_LOCAL_SERVICE and AWS_SERVICE_TYPE with default value
        self.assertEqual(result.attributes[AWS_LOCAL_SERVICE], "test-service")
        self.assertEqual(result.attributes[AWS_SERVICE_TYPE], "gen_ai_agent")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_service_attribute")
    def test_customize_resource_with_existing_agent_type(self, mock_get_service_attribute, mock_is_agent_enabled):
        """Test _customize_resource when agent type already exists in resource"""
        mock_is_agent_enabled.return_value = True
        mock_get_service_attribute.return_value = ("test-service", False)

        # Create resource with existing agent type
        resource = Resource.create(
            {ResourceAttributes.SERVICE_NAME: "test-service", AWS_SERVICE_TYPE: "existing-agent"}
        )
        result = _customize_resource(resource)

        # Should preserve existing agent type and not override it
        self.assertEqual(result.attributes[AWS_LOCAL_SERVICE], "test-service")
        self.assertEqual(result.attributes[AWS_SERVICE_TYPE], "existing-agent")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_emf_exporter_lambda_without_valid_headers(
        self, mock_get_session, mock_is_lambda, mock_fetch_headers
    ):
        """Test _create_emf_exporter returns ConsoleEmfExporter for Lambda without valid log headers"""
        # Setup mocks
        mock_is_lambda.return_value = True
        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = False
        mock_header_setting.namespace = "test-namespace"
        mock_fetch_headers.return_value = mock_header_setting

        with patch(
            "amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.ConsoleEmfExporter"
        ) as mock_console_exporter:
            mock_exporter_instance = MagicMock()
            mock_console_exporter.return_value = mock_exporter_instance

            result = _create_emf_exporter()

            self.assertEqual(result, mock_exporter_instance)
            mock_console_exporter.assert_called_once_with(namespace="test-namespace")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_emf_exporter_lambda_with_valid_headers(self, mock_get_session, mock_is_lambda, mock_fetch_headers):
        """Test _create_emf_exporter returns AwsCloudWatchEmfExporter for Lambda with valid headers"""
        # Setup mocks
        mock_is_lambda.return_value = True
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = True
        mock_header_setting.namespace = "test-namespace"
        mock_header_setting.log_group = "test-group"
        mock_header_setting.log_stream = "test-stream"
        mock_fetch_headers.return_value = mock_header_setting

        with patch(
            "amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter.AwsCloudWatchEmfExporter"
        ) as mock_cloudwatch_exporter:
            mock_exporter_instance = MagicMock()
            mock_cloudwatch_exporter.return_value = mock_exporter_instance

            result = _create_emf_exporter()

            self.assertEqual(result, mock_exporter_instance)
            mock_cloudwatch_exporter.assert_called_once_with(
                session=mock_session,
                namespace="test-namespace",
                log_group_name="test-group",
                log_stream_name="test-stream",
            )

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_emf_exporter_non_lambda_with_valid_headers(
        self, mock_get_session, mock_is_lambda, mock_fetch_headers
    ):
        """Test _create_emf_exporter returns AwsCloudWatchEmfExporter for non-Lambda with valid headers"""
        # Setup mocks
        mock_is_lambda.return_value = False
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = True
        mock_header_setting.namespace = "test-namespace"
        mock_header_setting.log_group = "test-group"
        mock_header_setting.log_stream = "test-stream"
        mock_fetch_headers.return_value = mock_header_setting

        with patch(
            "amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter.AwsCloudWatchEmfExporter"
        ) as mock_cloudwatch_exporter:
            mock_exporter_instance = MagicMock()
            mock_cloudwatch_exporter.return_value = mock_exporter_instance

            result = _create_emf_exporter()

            self.assertEqual(result, mock_exporter_instance)
            mock_cloudwatch_exporter.assert_called_once_with(
                session=mock_session,
                namespace="test-namespace",
                log_group_name="test-group",
                log_stream_name="test-stream",
            )

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_emf_exporter_non_lambda_without_valid_headers(
        self, mock_get_session, mock_is_lambda, mock_fetch_headers
    ):
        """Test _create_emf_exporter returns None for non-Lambda without valid headers"""
        # Setup mocks
        mock_is_lambda.return_value = False
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = False
        mock_fetch_headers.return_value = mock_header_setting

        result = _create_emf_exporter()

        self.assertIsNone(result)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._logger")
    def test_create_emf_exporter_no_botocore_session(
        self, mock_logger, mock_get_session, mock_is_lambda, mock_fetch_headers
    ):
        """Test _create_emf_exporter returns None when botocore session is not available"""
        # Setup mocks
        mock_is_lambda.return_value = False
        mock_get_session.return_value = None  # Simulate missing botocore

        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = True
        mock_fetch_headers.return_value = mock_header_setting

        result = _create_emf_exporter()

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once_with("botocore is not installed. EMF exporter requires botocore")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._logger")
    def test_create_emf_exporter_exception_handling(self, mock_logger, mock_fetch_headers):
        """Test _create_emf_exporter handles exceptions gracefully"""
        # Setup mocks to raise exception
        test_exception = Exception("Test exception")
        mock_fetch_headers.side_effect = test_exception

        result = _create_emf_exporter()

        self.assertIsNone(result)
        mock_logger.error.assert_called_once_with("Failed to create EMF exporter: %s", test_exception)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_emf_exporter_lambda_without_valid_headers_none_namespace(
        self, mock_get_session, mock_is_lambda, mock_fetch_headers
    ):
        """Test _create_emf_exporter with Lambda environment and None namespace"""
        # Setup mocks
        mock_is_lambda.return_value = True
        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = False
        mock_header_setting.namespace = None
        mock_fetch_headers.return_value = mock_header_setting

        with patch(
            "amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter.ConsoleEmfExporter"
        ) as mock_console_exporter:
            mock_exporter_instance = MagicMock()
            mock_console_exporter.return_value = mock_exporter_instance

            result = _create_emf_exporter()

            self.assertEqual(result, mock_exporter_instance)
            mock_console_exporter.assert_called_once_with(namespace=None)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._fetch_logs_header")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._is_lambda_environment")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.get_aws_session")
    def test_create_emf_exporter_cloudwatch_exporter_import_error(
        self, mock_get_session, mock_is_lambda, mock_fetch_headers
    ):
        """Test _create_emf_exporter handles import errors for CloudWatch exporter"""
        # Setup mocks
        mock_is_lambda.return_value = False
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_header_setting = MagicMock()
        mock_header_setting.is_valid.return_value = True
        mock_fetch_headers.return_value = mock_header_setting

        # Mock import to raise ImportError
        with patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator._logger") as mock_logger:
            with patch("builtins.__import__", side_effect=ImportError("Cannot import CloudWatch exporter")):
                result = _create_emf_exporter()

                self.assertIsNone(result)
                mock_logger.error.assert_called_once()

    def test_is_enhanced_code_attributes(self):
        """Test is_enhanced_code_attributes function with various environment variable values"""
        # Test when environment variable is not set (default state)
        os.environ.pop(OTEL_AWS_ENHANCED_CODE_ATTRIBUTES, None)
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        # Test when environment variable is set to 'true' (case insensitive)
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "true"
        result = is_enhanced_code_attributes()
        self.assertTrue(result)

        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "TRUE"
        result = is_enhanced_code_attributes()
        self.assertTrue(result)

        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "True"
        result = is_enhanced_code_attributes()
        self.assertTrue(result)

        # Test when environment variable is set to 'false' (case insensitive)
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "false"
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "FALSE"
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "False"
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        # Test with leading/trailing whitespace
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "  true  "
        result = is_enhanced_code_attributes()
        self.assertTrue(result)

        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "  false  "
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        # Test invalid values (should return False)
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "invalid"
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        # Test another invalid value
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = "yes"
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        # Test empty string (invalid)
        os.environ[OTEL_AWS_ENHANCED_CODE_ATTRIBUTES] = ""
        result = is_enhanced_code_attributes()
        self.assertFalse(result)

        # Clean up
        os.environ.pop(OTEL_AWS_ENHANCED_CODE_ATTRIBUTES, None)


def validate_distro_environ():
    tc: TestCase = TestCase()
    # Set by OpenTelemetryDistro
    tc.assertEqual("otlp", os.environ.get("OTEL_TRACES_EXPORTER"))
    tc.assertEqual("otlp", os.environ.get("OTEL_METRICS_EXPORTER"))

    # Set by AwsOpenTelemetryDistro
    tc.assertEqual("http/protobuf", os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL"))
    tc.assertEqual(
        "base2_exponential_bucket_histogram", os.environ.get("OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION")
    )
    tc.assertEqual("baggage,xray,tracecontext", os.environ.get("OTEL_PROPAGATORS"))
    tc.assertEqual("xray", os.environ.get("OTEL_PYTHON_ID_GENERATOR"))

    # Not set
    tc.assertEqual(None, os.environ.get("OTEL_TRACES_SAMPLER"))
    tc.assertEqual(None, os.environ.get("OTEL_TRACES_SAMPLER_ARG"))
