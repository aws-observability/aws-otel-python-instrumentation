# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import os
import time
from importlib import reload
from logging import getLogger
from unittest import TestCase
from unittest.mock import patch

import requests

import opentelemetry.instrumentation.requests as requests_instrumentation
import opentelemetry.instrumentation.urllib3 as urllib3_instrumentation
from amazon.opentelemetry.distro.sampler._aws_xray_sampling_client import _AwsXRaySamplingClient
from opentelemetry import trace as trace_api
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor
from opentelemetry.sdk.trace import TracerProvider, export
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.util._once import Once

SAMPLING_CLIENT_LOGGER_NAME = "amazon.opentelemetry.distro.sampler._aws_xray_sampling_client"
_sampling_client_logger = getLogger(SAMPLING_CLIENT_LOGGER_NAME)
_logger = getLogger(__name__)

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(TEST_DIR, "data")


class TestAwsXRaySamplingClient(TestCase):
    @patch("requests.Session.post")
    def test_get_no_sampling_rules(self, mock_post=None):
        mock_post.return_value.configure_mock(**{"json.return_value": {"SamplingRuleRecords": []}})
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_rules = client.get_sampling_rules()
        self.assertTrue(len(sampling_rules) == 0)

    @patch("requests.Session.post")
    def test_get_invalid_responses(self, mock_post=None):
        mock_post.return_value.configure_mock(**{"json.return_value": {}})
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        with self.assertLogs(_sampling_client_logger, level="ERROR"):
            sampling_rules = client.get_sampling_rules()
            self.assertTrue(len(sampling_rules) == 0)

    @patch("requests.Session.post")
    def test_get_sampling_rule_missing_in_records(self, mock_post=None):
        mock_post.return_value.configure_mock(**{"json.return_value": {"SamplingRuleRecords": [{}]}})
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        with self.assertLogs(_sampling_client_logger, level="ERROR"):
            sampling_rules = client.get_sampling_rules()
            self.assertTrue(len(sampling_rules) == 0)

    @patch("requests.Session.post")
    def test_default_values_used_when_missing_properties_in_sampling_rule(self, mock_post=None):
        mock_post.return_value.configure_mock(**{"json.return_value": {"SamplingRuleRecords": [{"SamplingRule": {}}]}})
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_rules = client.get_sampling_rules()
        self.assertTrue(len(sampling_rules) == 1)

        sampling_rule = sampling_rules[0]
        self.assertEqual(sampling_rule.Attributes, {})
        self.assertEqual(sampling_rule.FixedRate, 0.0)
        self.assertEqual(sampling_rule.HTTPMethod, "")
        self.assertEqual(sampling_rule.Host, "")
        self.assertEqual(sampling_rule.Priority, 10001)
        self.assertEqual(sampling_rule.ReservoirSize, 0)
        self.assertEqual(sampling_rule.ResourceARN, "")
        self.assertEqual(sampling_rule.RuleARN, "")
        self.assertEqual(sampling_rule.RuleName, "")
        self.assertEqual(sampling_rule.ServiceName, "")
        self.assertEqual(sampling_rule.ServiceType, "")
        self.assertEqual(sampling_rule.URLPath, "")
        self.assertEqual(sampling_rule.Version, 0)

    @patch("requests.Session.post")
    def test_get_correct_number_of_sampling_rules(self, mock_post=None):
        sampling_records = []
        with open(f"{DATA_DIR}/get-sampling-rules-response-sample.json", encoding="UTF-8") as file:
            sample_response = json.load(file)
            sampling_records = sample_response["SamplingRuleRecords"]
            mock_post.return_value.configure_mock(**{"json.return_value": sample_response})
            file.close()
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_rules = client.get_sampling_rules()
        self.assertEqual(len(sampling_rules), 3)
        self.assertEqual(len(sampling_rules), len(sampling_records))
        self.validate_match_sampling_rules_properties_with_records(sampling_rules, sampling_records)

    def validate_match_sampling_rules_properties_with_records(self, sampling_rules, sampling_records):
        for _, (sampling_rule, sampling_record) in enumerate(zip(sampling_rules, sampling_records)):
            self.assertIsNotNone(sampling_rule.Attributes)
            self.assertEqual(sampling_rule.Attributes, sampling_record["SamplingRule"]["Attributes"])
            self.assertIsNotNone(sampling_rule.FixedRate)
            self.assertEqual(sampling_rule.FixedRate, sampling_record["SamplingRule"]["FixedRate"])
            self.assertIsNotNone(sampling_rule.HTTPMethod)
            self.assertEqual(sampling_rule.HTTPMethod, sampling_record["SamplingRule"]["HTTPMethod"])
            self.assertIsNotNone(sampling_rule.Host)
            self.assertEqual(sampling_rule.Host, sampling_record["SamplingRule"]["Host"])
            self.assertIsNotNone(sampling_rule.Priority)
            self.assertEqual(sampling_rule.Priority, sampling_record["SamplingRule"]["Priority"])
            self.assertIsNotNone(sampling_rule.ReservoirSize)
            self.assertEqual(sampling_rule.ReservoirSize, sampling_record["SamplingRule"]["ReservoirSize"])
            self.assertIsNotNone(sampling_rule.ResourceARN)
            self.assertEqual(sampling_rule.ResourceARN, sampling_record["SamplingRule"]["ResourceARN"])
            self.assertIsNotNone(sampling_rule.RuleARN)
            self.assertEqual(sampling_rule.RuleARN, sampling_record["SamplingRule"]["RuleARN"])
            self.assertIsNotNone(sampling_rule.RuleName)
            self.assertEqual(sampling_rule.RuleName, sampling_record["SamplingRule"]["RuleName"])
            self.assertIsNotNone(sampling_rule.ServiceName)
            self.assertEqual(sampling_rule.ServiceName, sampling_record["SamplingRule"]["ServiceName"])
            self.assertIsNotNone(sampling_rule.ServiceType)
            self.assertEqual(sampling_rule.ServiceType, sampling_record["SamplingRule"]["ServiceType"])
            self.assertIsNotNone(sampling_rule.URLPath)
            self.assertEqual(sampling_rule.URLPath, sampling_record["SamplingRule"]["URLPath"])
            self.assertIsNotNone(sampling_rule.Version)
            self.assertEqual(sampling_rule.Version, sampling_record["SamplingRule"]["Version"])

    @patch("requests.Session.post")
    def test_get_sampling_targets(self, mock_post=None):
        with open(f"{DATA_DIR}/get-sampling-targets-response-sample.json", encoding="UTF-8") as file:
            sample_response = json.load(file)
            mock_post.return_value.configure_mock(**{"json.return_value": sample_response})
            file.close()
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_targets_response = client.get_sampling_targets(statistics=[])
        self.assertEqual(len(sampling_targets_response.SamplingTargetDocuments), 2)
        self.assertEqual(len(sampling_targets_response.UnprocessedStatistics), 0)
        self.assertEqual(sampling_targets_response.LastRuleModification, 1707551387.0)

    @patch("requests.Session.post")
    def test_get_invalid_sampling_targets(self, mock_post=None):
        mock_post.return_value.configure_mock(
            **{
                "json.return_value": {
                    "LastRuleModification": None,
                    "SamplingTargetDocuments": None,
                    "UnprocessedStatistics": None,
                }
            }
        )
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_targets_response = client.get_sampling_targets(statistics=[])
        self.assertEqual(sampling_targets_response.SamplingTargetDocuments, [])
        self.assertEqual(sampling_targets_response.UnprocessedStatistics, [])
        self.assertEqual(sampling_targets_response.LastRuleModification, 0.0)

    # pylint: disable=too-many-statements
    def test_urls_excluded_from_sampling(self):
        """
        This test case needs the following trace_api configurations since
        TestAwsOpenTelemetryConfigurator has already set tracer_provider.

        See `reset_trace_globals()`:
        https://github.com/open-telemetry/opentelemetry-python/blob/main/tests/opentelemetry-test-utils/src/opentelemetry/test/globals_test.py
        """
        trace_api._TRACER_PROVIDER_SET_ONCE = Once()
        trace_api._TRACER_PROVIDER = None
        trace_api._PROXY_TRACER_PROVIDER = trace_api.ProxyTracerProvider()

        tracer_provider = TracerProvider(sampler=ALWAYS_ON)
        memory_exporter = InMemorySpanExporter()
        span_processor = export.SimpleSpanProcessor(memory_exporter)
        tracer_provider.add_span_processor(span_processor)
        trace_api.set_tracer_provider(tracer_provider)

        # Reload instrumentors, where Sampling calls are instrumented for requests/urllib3
        RequestsInstrumentor().uninstrument()
        URLLib3Instrumentor().uninstrument()
        os.environ.pop("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None)
        os.environ.pop("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None)
        reload(requests_instrumentation)
        reload(urllib3_instrumentation)
        RequestsInstrumentor().instrument()
        URLLib3Instrumentor().instrument()

        client = _AwsXRaySamplingClient("http://this_is_a_fake_url:3849", log_level=logging.CRITICAL)

        span_list = memory_exporter.get_finished_spans()
        self.assertEqual(0, len(span_list))

        try:
            client.get_sampling_rules()
        except requests.exceptions.RequestException:
            pass

        timeout = time.time() + 1
        span_list = memory_exporter.get_finished_spans()
        while len(span_list) != 1 and timeout > time.time():
            span_list = memory_exporter.get_finished_spans()
            time.sleep(0.1)
        self.assertEqual(1, len(span_list))
        span_http_url = span_list[0].attributes.get("http.url")
        self.assertEqual(span_http_url, "http://this_is_a_fake_url:3849/GetSamplingRules")

        try:
            client.get_sampling_targets([])
        except requests.exceptions.RequestException:
            pass

        timeout = time.time() + 1
        span_list = memory_exporter.get_finished_spans()
        while len(span_list) != 2 and timeout > time.time():
            span_list = memory_exporter.get_finished_spans()
            time.sleep(0.1)
        self.assertEqual(2, len(span_list))
        span_http_url = span_list[1].attributes.get("http.url")
        self.assertEqual(span_http_url, "http://this_is_a_fake_url:3849/SamplingTargets")

        # Reload instrumentors, this time with Env Vars to exclude Sampling URLs for requests/urllib3
        urls_to_exclude_instr = (
            ",,,SamplingTargets,,endpoint1,endpoint2,,,GetSamplingRules,,SamplingTargets,GetSamplingRules"
        )

        memory_exporter.clear()
        URLLib3Instrumentor().uninstrument()
        RequestsInstrumentor().uninstrument()
        os.environ.pop("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", None)
        os.environ.pop("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", None)
        os.environ.setdefault("OTEL_PYTHON_REQUESTS_EXCLUDED_URLS", urls_to_exclude_instr)
        os.environ.setdefault("OTEL_PYTHON_URLLIB3_EXCLUDED_URLS", urls_to_exclude_instr)
        reload(requests_instrumentation)
        reload(urllib3_instrumentation)
        RequestsInstrumentor().instrument()
        URLLib3Instrumentor().instrument()

        client = _AwsXRaySamplingClient("http://this_is_a_fake_url:3849", log_level=logging.CRITICAL)

        try:
            client.get_sampling_rules()
        except requests.exceptions.RequestException:
            pass

        span_list = memory_exporter.get_finished_spans()
        self.assertEqual(0, len(span_list))

        try:
            client.get_sampling_targets([])
        except requests.exceptions.RequestException:
            pass

        span_list = memory_exporter.get_finished_spans()
        self.assertEqual(0, len(span_list))

        URLLib3Instrumentor().uninstrument()
        RequestsInstrumentor().uninstrument()
