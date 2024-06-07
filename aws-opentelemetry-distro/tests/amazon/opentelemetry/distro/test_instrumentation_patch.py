# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from os import environ
from typing import Dict, Tuple
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pkg_resources

from amazon.opentelemetry.distro.patches._instrumentation_patch import apply_instrumentation_patches
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as GrpcOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as HttpOTLPMetricExporter
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import AggregationTemporality
from opentelemetry.sdk.metrics.view import Aggregation, ExponentialBucketHistogramAggregation
from opentelemetry.semconv.trace import SpanAttributes

_STREAM_NAME: str = "streamName"
_BUCKET_NAME: str = "bucketName"
_QUEUE_NAME: str = "queueName"
_QUEUE_URL: str = "queueUrl"


class TestInstrumentationPatch(TestCase):
    """
    This test class has exactly one test, test_instrumentation_patch. This is an anti-pattern, but the scenario is
    fairly unusual and we feel justifies the code smell. Essentially the _instrumentation_patch module monkey-patches
    upstream components, so once it's run, it's challenging to "undo" between tests. To work around this, we have a
    monolith test framework that tests two major categories of test scenarios:
    1. Patch behaviour
    2. Patch mechanism

    Patch behaviour tests validate upstream behaviour without patches, apply patches, and validate patched behaviour.
    Patch mechanism tests validate the logic that is used to actually apply patches, and can be run regardless of the
    pre- or post-patch behaviour.
    """

    mock_get_distribution: patch
    mock_metric_exporter_init: patch

    def test_instrumentation_patch(self):
        # Set up mocks used by all tests
        self.mock_get_distribution = patch(
            "amazon.opentelemetry.distro.patches._instrumentation_patch.pkg_resources.get_distribution"
        ).start()
        self.mock_metric_exporter_init = patch(
            "opentelemetry.sdk.metrics._internal.export.MetricExporter.__init__"
        ).start()

        # Run tests that validate patch behaviour before and after patching
        self._run_patch_behaviour_tests()
        # Run tests not specifically related to patch behaviour
        self._run_patch_mechanism_tests()

        # Clean up method patches
        self.mock_get_distribution.stop()
        self.mock_metric_exporter_init.stop()

    def _run_patch_behaviour_tests(self):
        # Test setup
        environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "grpc"
        self.mock_get_distribution.return_value = "CorrectDistributionObject"

        # Validate unpatched upstream behaviour - important to detect upstream changes that may break instrumentation
        self._test_unpatched_botocore_instrumentation()
        self._test_unpatched_otlp_metric_exporters()

        # Apply patches
        apply_instrumentation_patches()

        # Validate patched upstream behaviour - important to detect downstream changes that may break instrumentation
        self._test_patched_botocore_instrumentation()
        self._test_patched_otlp_metric_exporters()

        # Test teardown
        environ.pop("OTEL_EXPORTER_OTLP_PROTOCOL")
        self._reset_mocks()

    def _run_patch_mechanism_tests(self):
        self._test_botocore_installed_flag()
        self._reset_mocks()
        self._test_otlp_protocol_flag()
        self._reset_mocks()

    def _test_unpatched_botocore_instrumentation(self):
        # Kinesis
        self.assertFalse("kinesis" in _KNOWN_EXTENSIONS, "Upstream has added a Kinesis extension")

        # S3
        self.assertFalse("s3" in _KNOWN_EXTENSIONS, "Upstream has added a S3 extension")

        # SQS
        self.assertTrue("sqs" in _KNOWN_EXTENSIONS, "Upstream has removed the SQS extension")
        attributes: Dict[str, str] = _do_extract_sqs_attributes()
        self.assertTrue("aws.queue_url" in attributes)
        self.assertFalse("aws.sqs.queue_url" in attributes)
        self.assertFalse("aws.sqs.queue_name" in attributes)

    def _test_unpatched_otlp_metric_exporters(self):
        (temporality_dict, aggregation_dict) = _get_metric_exporter_dicts()

        HttpOTLPMetricExporter(preferred_temporality=temporality_dict, preferred_aggregation=aggregation_dict)
        self.mock_metric_exporter_init.assert_called_once()
        self.assertEqual(temporality_dict, self.mock_metric_exporter_init.call_args[1]["preferred_temporality"])
        self.assertNotEqual(aggregation_dict, self.mock_metric_exporter_init.call_args[1]["preferred_aggregation"])
        self.mock_metric_exporter_init.reset_mock()

        GrpcOTLPMetricExporter(preferred_temporality=temporality_dict, preferred_aggregation=aggregation_dict)
        self.mock_metric_exporter_init.assert_called_once()
        self.assertEqual(temporality_dict, self.mock_metric_exporter_init.call_args[1]["preferred_temporality"])
        self.assertNotEqual(aggregation_dict, self.mock_metric_exporter_init.call_args[1]["preferred_aggregation"])
        self.mock_metric_exporter_init.reset_mock()

    def _test_patched_botocore_instrumentation(self):
        # Kinesis
        self.assertTrue("kinesis" in _KNOWN_EXTENSIONS)
        kinesis_attributes: Dict[str, str] = _do_extract_kinesis_attributes()
        self.assertTrue("aws.kinesis.stream_name" in kinesis_attributes)
        self.assertEqual(kinesis_attributes["aws.kinesis.stream_name"], _STREAM_NAME)

        # S3
        self.assertTrue("s3" in _KNOWN_EXTENSIONS)
        s3_attributes: Dict[str, str] = _do_extract_s3_attributes()
        self.assertTrue(SpanAttributes.AWS_S3_BUCKET in s3_attributes)
        self.assertEqual(s3_attributes[SpanAttributes.AWS_S3_BUCKET], _BUCKET_NAME)

        # SQS
        self.assertTrue("sqs" in _KNOWN_EXTENSIONS)
        sqs_attributes: Dict[str, str] = _do_extract_sqs_attributes()
        self.assertTrue("aws.queue_url" in sqs_attributes)
        self.assertTrue("aws.sqs.queue_url" in sqs_attributes)
        self.assertEqual(sqs_attributes["aws.sqs.queue_url"], _QUEUE_URL)
        self.assertTrue("aws.sqs.queue_name" in sqs_attributes)
        self.assertEqual(sqs_attributes["aws.sqs.queue_name"], _QUEUE_NAME)

    def _test_patched_otlp_metric_exporters(self):
        (temporality_dict, aggregation_dict) = _get_metric_exporter_dicts()

        HttpOTLPMetricExporter(preferred_temporality=temporality_dict, preferred_aggregation=aggregation_dict)
        self.mock_metric_exporter_init.assert_called_once()
        self.assertEqual(temporality_dict, self.mock_metric_exporter_init.call_args[1]["preferred_temporality"])
        self.assertEqual(aggregation_dict, self.mock_metric_exporter_init.call_args[1]["preferred_aggregation"])
        self.mock_metric_exporter_init.reset_mock()

        GrpcOTLPMetricExporter(preferred_temporality=temporality_dict, preferred_aggregation=aggregation_dict)
        self.mock_metric_exporter_init.assert_called_once()
        self.assertEqual(temporality_dict, self.mock_metric_exporter_init.call_args[1]["preferred_temporality"])
        self.assertEqual(aggregation_dict, self.mock_metric_exporter_init.call_args[1]["preferred_aggregation"])
        self.mock_metric_exporter_init.reset_mock()

    def _test_botocore_installed_flag(self):
        with patch(
            "amazon.opentelemetry.distro.patches._botocore_patches._apply_botocore_instrumentation_patches"
        ) as mock_apply_patches:
            self.mock_get_distribution.side_effect = pkg_resources.DistributionNotFound
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            self.mock_get_distribution.side_effect = pkg_resources.VersionConflict("botocore==1.0.0", "botocore==0.0.1")
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            self.mock_get_distribution.side_effect = None
            self.mock_get_distribution.return_value = "CorrectDistributionObject"
            apply_instrumentation_patches()
            mock_apply_patches.assert_called()

    # pylint: disable=no-self-use
    def _test_otlp_protocol_flag(self):
        with patch(
            "amazon.opentelemetry.distro.patches._otlp_metric_exporter_patches._apply_grpc_otlp_metric_exporter_patches"
        ) as mock_apply_patch:
            environ["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] = "http/protobuf"
            environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
            apply_instrumentation_patches()
            mock_apply_patch.assert_not_called()

            environ.pop("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
            environ.pop("OTEL_EXPORTER_OTLP_PROTOCOL")
            apply_instrumentation_patches()
            mock_apply_patch.assert_not_called()

            environ["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] = "http/protobuf"
            environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "grpc"
            apply_instrumentation_patches()
            mock_apply_patch.assert_not_called()

            environ["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] = "grpc"
            environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
            apply_instrumentation_patches()
            mock_apply_patch.assert_called_once()
            mock_apply_patch.reset_mock()

            environ["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] = "grpc"
            environ.pop("OTEL_EXPORTER_OTLP_PROTOCOL")
            apply_instrumentation_patches()
            mock_apply_patch.assert_called_once()
            mock_apply_patch.reset_mock()

            environ.pop("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
            environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "grpc"
            apply_instrumentation_patches()
            mock_apply_patch.assert_called_once()
            mock_apply_patch.reset_mock()

    def _reset_mocks(self):
        self.mock_get_distribution.reset_mock()
        self.mock_metric_exporter_init.reset_mock()


def _do_extract_kinesis_attributes() -> Dict[str, str]:
    service_name: str = "kinesis"
    params: Dict[str, str] = {"StreamName": _STREAM_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_s3_attributes() -> Dict[str, str]:
    service_name: str = "s3"
    params: Dict[str, str] = {"Bucket": _BUCKET_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_sqs_attributes() -> Dict[str, str]:
    service_name: str = "sqs"
    params: Dict[str, str] = {"QueueUrl": _QUEUE_URL, "QueueName": _QUEUE_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_attributes(service_name: str, params: Dict[str, str]) -> Dict[str, str]:
    mock_call_context: MagicMock = MagicMock()
    mock_call_context.params = params
    attributes: Dict[str, str] = {}
    sqs_extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    sqs_extension.extract_attributes(attributes)
    return attributes


def _get_metric_exporter_dicts() -> Tuple:
    temporality_dict: Dict[type, AggregationTemporality] = {}
    for typ in [
        Counter,
        UpDownCounter,
        Histogram,
        ObservableCounter,
        ObservableUpDownCounter,
        ObservableGauge,
    ]:
        temporality_dict[typ] = AggregationTemporality.DELTA

    aggregation_dict: Dict[type, Aggregation] = {Histogram: ExponentialBucketHistogramAggregation(99, 20)}
    return temporality_dict, aggregation_dict
