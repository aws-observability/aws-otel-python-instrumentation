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

# Patch names
GET_DISTRIBUTION_PATCH: str = (
    "amazon.opentelemetry.distro.patches._instrumentation_patch.pkg_resources.get_distribution"
)


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

    method_patches: Dict[str, patch] = {}
    mock_metric_exporter_init: patch

    def test_instrumentation_patch(self):
        # Set up method patches used by all tests
        self.method_patches[GET_DISTRIBUTION_PATCH] = patch(GET_DISTRIBUTION_PATCH).start()

        # Run tests that validate patch behaviour before and after patching
        self._run_patch_behaviour_tests()
        # Run tests not specifically related to patch behaviour
        self._run_patch_mechanism_tests()

        # Clean up method patches
        for method_patch in self.method_patches.values():
            method_patch.stop()

    def _run_patch_behaviour_tests(self):
        # Test setup
        self.method_patches[GET_DISTRIBUTION_PATCH].return_value = "CorrectDistributionObject"

        # Validate unpatched upstream behaviour - important to detect upstream changes that may break instrumentation
        self._test_unpatched_botocore_instrumentation()

        # Apply patches
        apply_instrumentation_patches()

        # Validate patched upstream behaviour - important to detect downstream changes that may break instrumentation
        self._test_patched_botocore_instrumentation()

        # Test teardown
        self._reset_mocks()

    def _run_patch_mechanism_tests(self):
        """
        Each test should be invoked, resetting mocks in between each test. E.g.:
            self.test_x()
            self.reset_mocks()
            self.test_y()
            self.reset_mocks()
            etc.
        """
        self._test_botocore_installed_flag()
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

        # SecretsManager
        self.assertFalse("secretsmanager" in _KNOWN_EXTENSIONS, "Upstream has added a SecretsManager extension")

        # StepFunctions
        self.assertFalse("stepfunctions" in _KNOWN_EXTENSIONS, "Upstream has added a StepFunctions extension")

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


    def _test_botocore_installed_flag(self):
        with patch(
            "amazon.opentelemetry.distro.patches._botocore_patches._apply_botocore_instrumentation_patches"
        ) as mock_apply_patches:
            get_distribution_patch: patch = self.method_patches[GET_DISTRIBUTION_PATCH]
            get_distribution_patch.side_effect = pkg_resources.DistributionNotFound
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            get_distribution_patch.side_effect = pkg_resources.VersionConflict("botocore==1.0.0", "botocore==0.0.1")
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            get_distribution_patch.side_effect = None
            get_distribution_patch.return_value = "CorrectDistributionObject"
            apply_instrumentation_patches()
            mock_apply_patches.assert_called()

    def _reset_mocks(self):
        for method_patch in self.method_patches.values():
            method_patch.reset_mock()


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
