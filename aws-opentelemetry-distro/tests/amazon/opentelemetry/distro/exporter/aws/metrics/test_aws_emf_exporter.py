# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import _init_metrics
from amazon.opentelemetry.distro.exporter.aws.metrics.aws_emf_exporter import AwsEmfExporter, _maybe_create_emf_exporter
from amazon.opentelemetry.distro.exporter.aws.metrics.console_emf_exporter import ConsoleEmfExporter
from amazon.opentelemetry.distro.exporter.otlp.aws.logs._log_header_config import _fetch_logs_header
from amazon.opentelemetry.distro.scope_based_exporter import ScopeBasedPeriodicExportingMetricReader
from amazon.opentelemetry.distro.scope_based_filtering_view import ScopeBasedRetainingView
from opentelemetry.environment_variables import OTEL_METRICS_EXPORTER
from opentelemetry.sdk._configuration import _get_exporter_names, _import_exporters
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_LOGS_HEADERS
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, InMemoryMetricReader, MetricExportResult
from opentelemetry.sdk.resources import Resource

EMF_MODULE = "amazon.opentelemetry.distro.exporter.aws.metrics.aws_emf_exporter"
CONFIGURATOR_MODULE = "amazon.opentelemetry.distro.aws_opentelemetry_configurator"
CLOUDWATCH_EXPORTER = (
    "amazon.opentelemetry.distro.exporter.aws.metrics.aws_cloudwatch_emf_exporter.AwsCloudWatchEmfExporter"
)


class TestAwsEmfExporter(TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        _fetch_logs_header.cache_clear()
        self.addCleanup(_fetch_logs_header.cache_clear)

    def test_lambda_stdout_does_not_require_botocore(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        for headers, namespace in (("", "default"), ("x-aws-metric-namespace=test", "test")):
            with self.subTest(headers=headers):
                _fetch_logs_header.cache_clear()
                os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                with patch(f"{EMF_MODULE}.get_aws_session") as get_session:
                    exporter = _maybe_create_emf_exporter()
                self.assertIsInstance(exporter, ConsoleEmfExporter)
                self.assertEqual(exporter.namespace, namespace)
                get_session.assert_not_called()

    def test_valid_headers_select_cloudwatch_on_lambda_and_elsewhere(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = (
            "x-aws-log-group=test-group,x-aws-log-stream=test-stream,x-aws-metric-namespace=test"
        )
        for is_lambda in (False, True):
            with self.subTest(is_lambda=is_lambda):
                if is_lambda:
                    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
                with patch(f"{EMF_MODULE}.get_aws_session") as get_session, patch(CLOUDWATCH_EXPORTER) as cloudwatch:
                    self.assertIs(_maybe_create_emf_exporter(), cloudwatch.return_value)
                    cloudwatch.assert_called_once_with(
                        session=get_session.return_value,
                        namespace="test",
                        log_group_name="test-group",
                        log_stream_name="test-stream",
                    )

    def test_incomplete_destination_disables_cloudwatch(self):
        for headers in ("", "x-aws-log-group=test-group", "x-aws-log-stream=test-stream"):
            with self.subTest(headers=headers):
                _fetch_logs_header.cache_clear()
                os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = headers
                with patch(f"{EMF_MODULE}.get_aws_session"), patch(CLOUDWATCH_EXPORTER) as cloudwatch:
                    self.assertIsNone(_maybe_create_emf_exporter())
                    cloudwatch.assert_not_called()

    def test_missing_botocore_disables_cloudwatch(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-group=test,x-aws-log-stream=test"
        with patch(f"{EMF_MODULE}.get_aws_session", return_value=None), self.assertLogs(
            EMF_MODULE, level="WARNING"
        ) as logs:
            self.assertIsNone(_maybe_create_emf_exporter())
        self.assertIn("botocore is not installed. EMF exporter requires botocore", logs.output[0])

    def test_destination_initialization_failure_is_nonfatal(self):
        for error in (RuntimeError("invalid headers"), ImportError("cannot import CloudWatch exporter")):
            with self.subTest(error=error):
                with patch(f"{EMF_MODULE}._fetch_logs_header", side_effect=error), self.assertLogs(
                    EMF_MODULE, level="ERROR"
                ) as logs:
                    exporter = AwsEmfExporter()
                self.assertFalse(exporter.enabled)
                self.assertEqual(exporter.export(MagicMock()), MetricExportResult.FAILURE)
                self.assertTrue(exporter.force_flush())
                self.assertTrue(exporter.shutdown())
                self.assertIn("Failed to create EMF exporter:", logs.output[0])

    def test_destination_import_failure_is_nonfatal(self):
        os.environ[OTEL_EXPORTER_OTLP_LOGS_HEADERS] = "x-aws-log-group=test,x-aws-log-stream=test"
        with self.assertLogs(EMF_MODULE, level="ERROR"), patch(f"{EMF_MODULE}.get_aws_session"), patch(
            "builtins.__import__", side_effect=ImportError("cannot import CloudWatch exporter")
        ):
            self.assertIsNone(_maybe_create_emf_exporter())

    def test_exporter_preserves_destination_preferences_and_lifecycle(self):
        destination = ConsoleEmfExporter()
        destination.export = MagicMock(return_value=MetricExportResult.SUCCESS)
        destination.force_flush = MagicMock(return_value=True)
        destination.shutdown = MagicMock(return_value=True)
        with patch(f"{EMF_MODULE}._maybe_create_emf_exporter", return_value=destination):
            exporter = AwsEmfExporter()
        # pylint: disable=protected-access
        self.assertEqual(exporter._preferred_temporality, destination._preferred_temporality)
        self.assertEqual(exporter._preferred_aggregation, destination._preferred_aggregation)
        metrics = MagicMock()
        self.assertEqual(exporter.export(metrics, timeout_millis=123), MetricExportResult.SUCCESS)
        self.assertTrue(exporter.force_flush(timeout_millis=456))
        self.assertTrue(exporter.shutdown(timeout_millis=789))
        destination.export.assert_called_once_with(metrics, timeout_millis=123)
        destination.force_flush.assert_called_once_with(timeout_millis=456)
        destination.shutdown.assert_called_once_with(timeout_millis=789)

    def test_entry_point_discovers_single_and_mixed_exporters_without_mutating_environment(self):
        for value in ("awsemf", "console,awsemf"):
            with self.subTest(exporters=value):
                os.environ[OTEL_METRICS_EXPORTER] = value
                _, exporters, _ = _import_exporters([], _get_exporter_names("metrics"), [])
                self.assertIs(exporters["awsemf"], AwsEmfExporter)
                self.assertEqual(set(exporters), set(value.split(",")))
                self.assertEqual(os.environ[OTEL_METRICS_EXPORTER], value)

    def test_registered_exporter_sends_lambda_metrics_to_stdout(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        os.environ[OTEL_METRICS_EXPORTER] = "awsemf"
        _, exporters, _ = _import_exporters([], _get_exporter_names("metrics"), [])
        with patch(f"{CONFIGURATOR_MODULE}.set_meter_provider") as set_provider, patch("builtins.print") as output:
            _init_metrics(exporters, Resource.get_empty())
            provider = set_provider.call_args.args[0]
            try:
                provider.get_meter("test").create_counter("test_counter").add(7)
                provider.force_flush()
                records = [json.loads(call.args[0]) for call in output.call_args_list]
                self.assertTrue(any(record.get("test_counter") == 7 for record in records))
            finally:
                provider.shutdown()
        self.assertEqual(os.environ[OTEL_METRICS_EXPORTER], "awsemf")

    def test_unconfigured_exporter_does_not_create_a_metric_reader(self):
        with patch(f"{EMF_MODULE}.get_aws_session", return_value=None), patch(
            f"{CONFIGURATOR_MODULE}.set_meter_provider"
        ) as set_provider:
            _init_metrics({"awsemf": AwsEmfExporter}, Resource.get_empty())
        provider = set_provider.call_args.args[0]
        self.addCleanup(provider.shutdown)
        # pylint: disable=protected-access
        self.assertEqual(provider._metric_readers, [])

    def test_runtime_view_selection_and_reader_order_are_preserved(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "test-function"
        os.environ["OTEL_AWS_APPLICATION_SIGNALS_ENABLED"] = "true"
        os.environ["OTEL_AWS_APPLICATION_SIGNALS_RUNTIME_ENABLED"] = "true"
        for extra_exporter in (False, True):
            with self.subTest(extra_exporter=extra_exporter):
                exporters = {"awsemf": AwsEmfExporter}
                if extra_exporter:
                    exporters["console"] = ConsoleMetricExporter
                with patch(f"{CONFIGURATOR_MODULE}.set_meter_provider") as set_provider, patch(
                    f"{CONFIGURATOR_MODULE}.ApplicationSignalsExporterProvider.create_exporter",
                    return_value=ConsoleMetricExporter(),
                ):
                    _init_metrics(exporters, Resource.get_empty())
                provider = set_provider.call_args.args[0]
                try:
                    # pylint: disable=protected-access
                    readers = provider._metric_readers
                    self.assertIsInstance(readers[-1]._exporter, AwsEmfExporter)
                    self.assertIsInstance(readers[-2], ScopeBasedPeriodicExportingMetricReader)
                    has_runtime_filter = any(
                        isinstance(view, ScopeBasedRetainingView) for view in provider._sdk_config.views
                    )
                    self.assertEqual(has_runtime_filter, not extra_exporter)
                finally:
                    provider.shutdown()

    def test_registered_metric_reader_keeps_its_existing_initialization_path(self):
        with patch(f"{CONFIGURATOR_MODULE}.set_meter_provider") as set_provider:
            _init_metrics({"test_reader": InMemoryMetricReader}, Resource.get_empty())
        provider = set_provider.call_args.args[0]
        self.addCleanup(provider.shutdown)
        # pylint: disable=protected-access
        self.assertIsInstance(provider._metric_readers[0], InMemoryMetricReader)
