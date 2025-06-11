# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from requests import Session

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from amazon.opentelemetry.distro.aws_lambda_span_processor import AwsLambdaSpanProcessor
from amazon.opentelemetry.distro.aws_metric_attributes_span_exporter import AwsMetricAttributesSpanExporter
from amazon.opentelemetry.distro.aws_opentelemetry_configurator import (
    LAMBDA_SPAN_EXPORT_BATCH_SIZE,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_LOGS_HEADERS,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    ApplicationSignalsExporterProvider,
    AwsOpenTelemetryConfigurator,
    _custom_import_sampler,
    _customize_logs_exporter,
    _customize_metric_exporters,
    _customize_sampler,
    _customize_span_exporter,
    _customize_span_processors,
    _export_unsampled_span_for_lambda,
    _init_logging,
    _is_application_signals_enabled,
    _is_application_signals_runtime_enabled,
    _is_defer_to_workers_enabled,
    _is_wsgi_master_process,
)
from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro
from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
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
from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG
from opentelemetry.sdk.metrics._internal.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, Tracer, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.sampling import DEFAULT_ON, Sampler
from opentelemetry.trace import get_tracer_provider


# pylint: disable=too-many-public-methods
class TestAwsOpenTelemetryConfigurator(TestCase):
    """Tests AwsOpenTelemetryConfigurator and AwsOpenTelemetryDistro

    NOTE: This class setup Tracer Provider Globally, which can only be set once. If there is another setup for tracer
    provider, it may cause issues for those tests.
    """

    @classmethod
    def setUpClass(cls):
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
            self.customize_exporter_test(
                config,
                _customize_span_exporter,
                OTLPSpanExporter(),
                OTLPAwsSpanExporter,
                AwsAuthSession,
                Compression.NoCompression,
            )

        for config in bad_configs:
            self.customize_exporter_test(
                config,
                _customize_span_exporter,
                OTLPSpanExporter(),
                OTLPSpanExporter,
                Session,
                Compression.NoCompression,
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
            self.customize_exporter_test(
                config,
                _customize_logs_exporter,
                OTLPLogExporter(),
                OTLPAwsLogExporter,
                AwsAuthSession,
                Compression.Gzip,
            )

        for config in bad_configs:
            self.customize_exporter_test(
                config, _customize_logs_exporter, OTLPLogExporter(), OTLPLogExporter, Session, Compression.NoCompression
            )

        self.assertIsInstance(
            _customize_logs_exporter(OTLPGrpcLogExporter(), Resource.get_empty()), OTLPGrpcLogExporter
        )

    # Need to patch all of these to prevent some weird multi-threading error with the LogProvider
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.LoggingHandler", return_value=MagicMock())
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_configurator.getLogger", return_value=MagicMock())
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

    def test_customize_span_processors(self):
        mock_tracer_provider: TracerProvider = MagicMock()
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 0)

        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "True")
        os.environ.setdefault("OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED", "False")
        _customize_span_processors(mock_tracer_provider, Resource.get_empty())
        self.assertEqual(mock_tracer_provider.add_span_processor.call_count, 2)
        first_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[0].args[0]
        self.assertIsInstance(first_processor, AttributePropagatingSpanProcessor)
        second_processor: SpanProcessor = mock_tracer_provider.add_span_processor.call_args_list[1].args[0]
        self.assertIsInstance(second_processor, AwsSpanMetricsProcessor)

    def test_customize_span_processors_lambda(self):
        mock_tracer_provider: TracerProvider = MagicMock()
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
        self.assertEqual(third_processor.max_export_batch_size, LAMBDA_SPAN_EXPORT_BATCH_SIZE)
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
        self,
        config,
        executor,
        default_exporter,
        expected_exporter_type,
        expected_session,
        expected_compression,
    ):
        for key, value in config.items():
            os.environ[key] = value

        try:
            result = executor(default_exporter, Resource.get_empty())
            self.assertIsInstance(result, expected_exporter_type)
            self.assertIsInstance(result._session, expected_session)
            self.assertEqual(result._compression, expected_compression)
        finally:
            for key in config.keys():
                os.environ.pop(key, None)


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
    tc.assertEqual("xray,tracecontext,b3,b3multi", os.environ.get("OTEL_PROPAGATORS"))
    tc.assertEqual("xray", os.environ.get("OTEL_PYTHON_ID_GENERATOR"))

    # Not set
    tc.assertEqual(None, os.environ.get("OTEL_TRACES_SAMPLER"))
    tc.assertEqual(None, os.environ.get("OTEL_TRACES_SAMPLER_ARG"))
