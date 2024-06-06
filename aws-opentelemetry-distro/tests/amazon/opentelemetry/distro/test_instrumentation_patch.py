# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pkg_resources

from amazon.opentelemetry.distro.patches._instrumentation_patch import apply_instrumentation_patches
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.span import Span

_STREAM_NAME: str = "streamName"
_BUCKET_NAME: str = "bucketName"
_QUEUE_NAME: str = "queueName"
_QUEUE_URL: str = "queueUrl"
_SECRET_ARN: str = "arn:aws:secretsmanager:us-west-2:000000000000:secret:testSecret-ABCDEF"
_STATE_MACHINE_ARN: str = "arn:aws:states:us-west-2:000000000000:stateMachine:testStateMachine"


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

        # SecretsManager
        self.assertFalse("secretsmanager" in _KNOWN_EXTENSIONS, "Upstream has added a SecretsManager extension")

        # StepFunctions
        self.assertFalse("stepfunctions" in _KNOWN_EXTENSIONS, "Upstream has added a StepFunctions extension")

    def _validate_patched_botocore_instrumentation(self):
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

        # SecretsManager
        self.assertTrue("secretsmanager" in _KNOWN_EXTENSIONS)
        secretsmanager_attributes: Dict[str, str] = _do_extract_secretsmanager_attributes()
        self.assertTrue("aws.secretsmanager.secret_arn" in secretsmanager_attributes)
        self.assertEqual(secretsmanager_attributes["aws.secretsmanager.secret_arn"], _SECRET_ARN)
        secretsmanager_sucess_attributes: Dict[str, str] = _do_secretsmanager_on_success()
        self.assertTrue("aws.secretsmanager.secret_arn" in secretsmanager_sucess_attributes)
        self.assertEqual(secretsmanager_sucess_attributes["aws.secretsmanager.secret_arn"], _SECRET_ARN)

        # StepFunctions
        self.assertTrue("stepfunctions" in _KNOWN_EXTENSIONS)
        stepfunctions_attributes: Dict[str, str] = _do_extract_stepfunctions_attributes()
        self.assertTrue("aws.stepfunctions.state_machine_arn" in stepfunctions_attributes)
        self.assertEqual(stepfunctions_attributes["aws.stepfunctions.state_machine_arn"], _STATE_MACHINE_ARN)
        stepfunctions_sucess_attributes: Dict[str, str] = _do_stepfunctions_on_success()
        self.assertTrue("aws.stepfunctions.state_machine_arn" in stepfunctions_sucess_attributes)
        self.assertEqual(stepfunctions_sucess_attributes["aws.stepfunctions.state_machine_arn"], _STATE_MACHINE_ARN)


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


def _do_extract_secretsmanager_attributes() -> Dict[str, str]:
    service_name: str = "secretsmanager"
    params: Dict[str, str] = {"SecretId": _SECRET_ARN}
    return _do_extract_attributes(service_name, params)


def _do_secretsmanager_on_success() -> Dict[str, str]:
    service_name: str = "secretsmanager"
    result: Dict[str, Any] = {"ARN": _SECRET_ARN}
    return _do_on_success(service_name, result)


def _do_extract_stepfunctions_attributes() -> Dict[str, str]:
    service_name: str = "stepfunctions"
    params: Dict[str, str] = {"stateMachineArn": _STATE_MACHINE_ARN}
    return _do_extract_attributes(service_name, params)


def _do_stepfunctions_on_success() -> Dict[str, str]:
    service_name: str = "stepfunctions"
    result: Dict[str, Any] = {"stateMachineArn": _STATE_MACHINE_ARN}
    return _do_on_success(service_name, result)


def _do_extract_attributes(service_name: str, params: Dict[str, str]) -> Dict[str, str]:
    mock_call_context: MagicMock = MagicMock()
    mock_call_context.params = params
    attributes: Dict[str, str] = {}
    sqs_extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    sqs_extension.extract_attributes(attributes)
    return attributes


def _do_on_success(service_name: str, result: Dict[str, Any]) -> Dict[str, str]:
    span_mock: Span = MagicMock()
    span_attributes: Dict[str, str] = {}

    def set_side_effect(set_key, set_value):
        span_attributes[set_key] = set_value

    span_mock.set_attribute.side_effect = set_side_effect
    extension = _KNOWN_EXTENSIONS[service_name]()(span_mock)
    extension.on_success(span_mock, result)

    return span_attributes
