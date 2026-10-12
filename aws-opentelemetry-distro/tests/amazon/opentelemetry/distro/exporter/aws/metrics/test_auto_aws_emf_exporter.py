# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import sys
from contextlib import redirect_stdout
from io import StringIO
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro import _utils
from amazon.opentelemetry.distro.exporter.aws.metrics._auto_aws_emf_exporter import _AutoAwsEmfExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.logs._log_header_config import fetch_otlp_logs_header
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_LOGS_HEADERS
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricExportResult, MetricsData, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

CLOUDWATCH_HEADERS = "x-aws-log-group=test-group,x-aws-log-stream=test-stream,x-aws-metric-namespace=test-namespace"


class TestAutoAwsEmfExporter(TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        fetch_otlp_logs_header.cache_clear()
        self.addCleanup(fetch_otlp_logs_header.cache_clear)

        self.logs_client = MagicMock()
        self.logs_client.put_log_events.return_value = {}
        client = patch("botocore.session.Session.create_client", return_value=self.logs_client)
        self.create_client = client.start()
        self.addCleanup(client.stop)
        self.exporter = None
        self.provider = None
        self.meter = None
        self.output = None
        self.addCleanup(self.shutdown_provider)

    def init_provider(self):
        self.output = StringIO()
        self.exporter = _AutoAwsEmfExporter()
        self.assertTrue(self.exporter.enabled)
        self.provider = MeterProvider(
            resource=Resource.get_empty(),
            metric_readers=[PeriodicExportingMetricReader(self.exporter, export_interval_millis=600000)],
        )
        self.meter = self.provider.get_meter("test")

    def shutdown_provider(self):
        if self.provider is not None:
            provider = self.provider
            self.provider = None
            provider.shutdown()

    def export_counter(self, flush=True):
        self.init_provider()
        with redirect_stdout(self.output):
            self.meter.create_counter("test_counter").add(7)
            if flush:
                self.assertTrue(self.provider.force_flush())
            self.shutdown_provider()
        return [json.loads(line) for line in self.output.getvalue().splitlines()]

    def assert_counter(self, records, namespace):
        matching = [record for record in records if record.get("test_counter") == 7]
        self.assertTrue(matching)
        self.assertEqual(matching[0]["_aws"]["CloudWatchMetrics"][0]["Namespace"], namespace)

    def assert_cloudwatch_counter(self):
        requests = [call.kwargs for call in self.logs_client.put_log_events.call_args_list]
        self.assertTrue(requests)
        for request in requests:
            self.assertEqual(request["logGroupName"], "test-group")
            self.assertEqual(request["logStreamName"], "test-stream")
        records = [json.loads(event["message"]) for request in requests for event in request["logEvents"]]
        self.assert_counter(records, "test-namespace")

    def assert_disabled(self, exporter):
        self.assertFalse(exporter.enabled)
        self.assertEqual(exporter.export(MetricsData(resource_metrics=[])), MetricExportResult.FAILURE)
        self.assertTrue(exporter.force_flush())
        self.assertTrue(exporter.shutdown())
        self.logs_client.put_log_events.assert_not_called()

    def test_when_otlp_logs_headers_are_missing_in_lambda_uses_console_exporter(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        for headers in (None, ""):
            with self.subTest(headers=headers):
                fetch_otlp_logs_header.cache_clear()
                if headers is None:
                    os.environ.pop(OTEL_EXPORTER_OTLP_LOGS_HEADERS, None)
                else:
                    os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                self.assert_counter(self.export_counter(), "default")
        self.create_client.assert_not_called()

    def test_when_otlp_logs_headers_are_incomplete_in_lambda_uses_console_exporter(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        for headers in ("x-aws-log-group=test-group", "x-aws-log-stream=test-stream"):
            with self.subTest(headers=headers):
                fetch_otlp_logs_header.cache_clear()
                os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                self.assert_counter(self.export_counter(), "default")
        self.create_client.assert_not_called()

    def test_when_otlp_logs_headers_are_valid_in_lambda_uses_cloudwatchlogs_exporter(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = CLOUDWATCH_HEADERS
        self.assertEqual(self.export_counter(), [])
        self.assert_cloudwatch_counter()
        self.create_client.assert_called_once_with("logs", region_name=None)

    def test_when_otlp_logs_headers_are_missing_in_non_lambda_exporter_is_disabled(self):
        for headers in (None, ""):
            with self.subTest(headers=headers):
                fetch_otlp_logs_header.cache_clear()
                if headers is None:
                    os.environ.pop(OTEL_EXPORTER_OTLP_LOGS_HEADERS, None)
                else:
                    os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                with self.assertLogs(level="WARNING") as logs:
                    exporter = _AutoAwsEmfExporter()
                self.assert_disabled(exporter)
                self.create_client.assert_not_called()
                self.assertEqual(len(logs.output), 1)
                self.assertIn(
                    "OTEL_EXPORTER_OTLP_LOGS_HEADERS to include x-aws-log-group and x-aws-log-stream", logs.output[0]
                )

    def test_when_otlp_logs_headers_are_incomplete_in_non_lambda_exporter_is_disabled(self):
        for headers in ("x-aws-log-group=test-group", "x-aws-log-stream=test-stream"):
            with self.subTest(headers=headers):
                fetch_otlp_logs_header.cache_clear()
                os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                with self.assertLogs(level="WARNING") as logs:
                    exporter = _AutoAwsEmfExporter()
                self.assert_disabled(exporter)
                self.create_client.assert_not_called()
                self.assertEqual(len(logs.output), 1)
                self.assertIn(
                    "OTEL_EXPORTER_OTLP_LOGS_HEADERS to include x-aws-log-group and x-aws-log-stream", logs.output[0]
                )

    def test_when_otlp_logs_headers_are_valid_in_non_lambda_uses_cloudwatchlogs_exporter(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = CLOUDWATCH_HEADERS
        self.assertEqual(self.export_counter(), [])
        self.assert_cloudwatch_counter()
        self.create_client.assert_called_once_with("logs", region_name=None)

    def test_when_metric_namespace_is_set_in_lambda_console_exporter_uses_configured_namespace(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-metric-namespace=test-namespace"
        self.assert_counter(self.export_counter(), "test-namespace")
        self.create_client.assert_not_called()

    def test_when_botocore_is_not_installed_in_lambda_uses_console_exporter(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        for headers, namespace in (("", "default"), ("x-aws-metric-namespace=test", "test")):
            with self.subTest(headers=headers):
                fetch_otlp_logs_header.cache_clear()
                os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                with patch.object(_utils, "IS_BOTOCORE_INSTALLED", False), self.assertNoLogs(level="WARNING"):
                    records = self.export_counter()
                self.assert_counter(records, namespace)
        self.create_client.assert_not_called()

    def test_when_botocore_is_not_installed_in_non_lambda_exporter_is_disabled(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = CLOUDWATCH_HEADERS
        with patch.object(_utils, "IS_BOTOCORE_INSTALLED", False), self.assertLogs(level="WARNING") as logs:
            exporter = _AutoAwsEmfExporter()
        self.assert_disabled(exporter)
        self.create_client.assert_not_called()
        self.assertEqual(len(logs.output), 1)
        self.assertIn("botocore is not installed. EMF exporter requires botocore", logs.output[0])

    def test_when_cloudwatchlogs_exporter_initialization_fails_in_non_lambda_exporter_is_disabled(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = CLOUDWATCH_HEADERS
        self.create_client.side_effect = RuntimeError("Test exception")
        with self.assertLogs(level="ERROR") as logs:
            exporter = _AutoAwsEmfExporter()
        self.assert_disabled(exporter)
        self.assertEqual(len(logs.output), 1)
        self.assertIn("Failed to create EMF exporter: Test exception", logs.output[0])

    def test_when_cloudwatchlogs_exporter_import_fails_in_non_lambda_exporter_is_disabled(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = CLOUDWATCH_HEADERS
        cloudwatch_module = "amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter"
        with self.assertLogs(level="ERROR") as logs, patch.dict(sys.modules, {cloudwatch_module: None}):
            exporter = _AutoAwsEmfExporter()
        self.assert_disabled(exporter)
        self.create_client.assert_not_called()
        self.assertEqual(len(logs.output), 1)
        self.assertIn("Failed to create EMF exporter:", logs.output[0])
        self.assertIn(cloudwatch_module, logs.output[0])

    def test_when_metrics_are_flushed_in_lambda_console_exporter_emits_deltas_and_exponential_histograms(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        self.init_provider()
        with redirect_stdout(self.output):
            counter = self.meter.create_counter("test_counter")
            histogram = self.meter.create_histogram("test_histogram")
            for value in (7, 3):
                counter.add(value)
                histogram.record(1)
                histogram.record(2)
                self.assertTrue(self.provider.force_flush())
            self.shutdown_provider()
        records = [json.loads(line) for line in self.output.getvalue().splitlines()]
        self.assertEqual([record["test_counter"] for record in records if record.get("test_counter", 0) > 0], [7, 3])
        histograms = [record["test_histogram"] for record in records if "test_histogram" in record]
        self.assertTrue(histograms)
        self.assertTrue(all("Values" in histogram and "Counts" in histogram for histogram in histograms))

    def test_when_provider_is_shutdown_in_non_lambda_cloudwatchlogs_exporter_flushes_metrics(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = CLOUDWATCH_HEADERS
        self.assertEqual(self.export_counter(flush=False), [])
        self.assert_cloudwatch_counter()
