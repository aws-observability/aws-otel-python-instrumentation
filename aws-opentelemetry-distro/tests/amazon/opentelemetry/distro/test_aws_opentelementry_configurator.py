# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import (
    AwsOpenTelemetryConfigurator,
    _custom_import_sampler,
)
from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro
from amazon.opentelemetry.distro.sampler._aws_xray_sampling_client import _AwsXRaySamplingClient
from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG
from opentelemetry.sdk.trace import Span, Tracer, TracerProvider
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.trace import get_tracer_provider


# This class setup Tracer Provider Globally, which can only set once
# if there is another setup for tracer provider, may cause issue
class TestAwsOpenTelemetryConfigurator(TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "none")
        os.environ.setdefault(OTEL_METRICS_EXPORTER, "none")
        os.environ.setdefault(OTEL_LOGS_EXPORTER, "none")
        os.environ.setdefault(OTEL_TRACES_SAMPLER, "traceidratio")
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "0.01")
        aws_open_telemetry_distro: AwsOpenTelemetryDistro = AwsOpenTelemetryDistro()
        aws_open_telemetry_distro.configure(apply_patches=False)
        aws_otel_configurator: AwsOpenTelemetryConfigurator = AwsOpenTelemetryConfigurator()
        aws_otel_configurator.configure()
        cls.tracer_provider: TracerProvider = get_tracer_provider()

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
    @patch.object(AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_import_xray_sampler_without_environment_arguments(self):
        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://127.0.0.1:2000/GetSamplingRules"
        )

    @patch.object(AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_import_xray_sampler_with_valid_environment_arguments(self):
        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint=http://localhost:2000,polling_interval=600")

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 600)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://localhost:2000/GetSamplingRules"
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "polling_interval=123")

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 123)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://127.0.0.1:2000/GetSamplingRules"
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint=http://cloudwatch-agent.amazon-cloudwatch:2000")

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "http://cloudwatch-agent.amazon-cloudwatch:2000/GetSamplingRules",
        )

    @patch.object(AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_rule_poller", lambda x: None)
    @patch.object(AwsXRayRemoteSampler, "_AwsXRayRemoteSampler__start_sampling_target_poller", lambda x: None)
    def test_import_xray_sampler_with_invalid_environment_arguments(self):
        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint=h=tt=p://=loca=lho=st:2000,polling_interval=FOOBAR")

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint,
            "h=tt=p://=loca=lho=st:2000/GetSamplingRules",
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, ",,=,==,,===,")

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://127.0.0.1:2000/GetSamplingRules"
        )

        os.environ.pop(OTEL_TRACES_SAMPLER_ARG, None)
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "endpoint,polling_interval")

        # May log http request error as xray sampler will attempt to fetch rules
        xray_sampler: Sampler = _custom_import_sampler("xray", resource=None)
        xray_client: _AwsXRaySamplingClient = xray_sampler._AwsXRayRemoteSampler__xray_client
        self.assertEqual(xray_sampler._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertEqual(
            xray_client._AwsXRaySamplingClient__get_sampling_rules_endpoint, "http://127.0.0.1:2000/GetSamplingRules"
        )
