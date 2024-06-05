# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pkg_resources

from amazon.opentelemetry.distro.patches._instrumentation_patch import apply_instrumentation_patches
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.semconv.trace import SpanAttributes

_STREAM_NAME: str = "streamName"
_CONSUMER_ARN: str = "consumerArn"
_BUCKET_NAME: str = "bucketName"
_QUEUE_NAME: str = "queueName"
_QUEUE_URL: str = "queueUrl"


class TestInstrumentationPatch(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mock_get_distribution = patch(
            "amazon.opentelemetry.distro.patches._instrumentation_patch.pkg_resources.get_distribution"
        ).start()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.mock_get_distribution.stop()

    def test_botocore_not_installed(self):
        # Test scenario 1: Botocore package not installed
        self.mock_get_distribution.side_effect = pkg_resources.DistributionNotFound
        apply_instrumentation_patches()
        with patch(
            "amazon.opentelemetry.distro.patches._botocore_patches._apply_botocore_instrumentation_patches"
        ) as mock_apply_patches:
            mock_apply_patches.assert_not_called()

    def test_botocore_installed_wrong_version(self):
        # Test scenario 2: Botocore package installed with wrong version
        self.mock_get_distribution.side_effect = pkg_resources.VersionConflict("botocore==1.0.0", "botocore==0.0.1")
        apply_instrumentation_patches()
        with patch(
            "amazon.opentelemetry.distro.patches._botocore_patches._apply_botocore_instrumentation_patches"
        ) as mock_apply_patches:
            mock_apply_patches.assert_not_called()

    def test_botocore_installed_correct_version(self):
        # Test scenario 3: Botocore package installed with correct version
        # Validate unpatched upstream behaviour - important to detect upstream changes that may break instrumentation
        self._validate_unpatched_botocore_instrumentation()

        self.mock_get_distribution.return_value = "CorrectDistributionObject"

        # Apply patches
        apply_instrumentation_patches()

        # Validate patched upstream behaviour - important to detect downstream changes that may break instrumentation
        self._validate_patched_botocore_instrumentation()

    def _validate_unpatched_botocore_instrumentation(self):
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

    def _validate_patched_botocore_instrumentation(self):
        # Kinesis
        self.assertTrue("kinesis" in _KNOWN_EXTENSIONS)
        kinesis_attributes: Dict[str, str] = _do_extract_kinesis_attributes()
        self.assertTrue("aws.kinesis.stream_name" in kinesis_attributes)
        self.assertEqual(kinesis_attributes["aws.kinesis.stream_name"], _STREAM_NAME)
        self.assertTrue("aws.kinesis.stream_consumer_arn" in kinesis_attributes)
        self.assertEqual(kinesis_attributes["aws.kinesis.stream_consumer_arn"], _CONSUMER_ARN)

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


def _do_extract_kinesis_attributes() -> Dict[str, str]:
    service_name: str = "kinesis"
    params: Dict[str, str] = {"StreamName": _STREAM_NAME, "ConsumerARN": _CONSUMER_ARN}
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
