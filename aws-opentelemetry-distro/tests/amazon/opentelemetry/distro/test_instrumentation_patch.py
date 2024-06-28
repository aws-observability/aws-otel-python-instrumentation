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
_BEDROCK_AGENT_ID: str = "agentId"
_BEDROCK_DATASOURCE_ID: str = "DataSourceId"
_BEDROCK_GUARDRAIL_ID: str = "GuardrailId"
_BEDROCK_KNOWLEDGEBASE_ID: str = "KnowledgeBaseId"

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

        # Bedrock
        self.assertFalse("bedrock" in _KNOWN_EXTENSIONS, "Upstream has added a Bedrock extension")

        # Bedrock Agent
        self.assertFalse("bedrock-agent" in _KNOWN_EXTENSIONS, "Upstream has added a Bedrock Agent extension")

        # Bedrock Agent Runtime
        self.assertFalse(
            "bedrock-agent-runtime" in _KNOWN_EXTENSIONS, "Upstream has added a Bedrock Agent Runtime extension"
        )

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

        # Bedrock
        bedrock_sucess_attributes: Dict[str, str] = _do_bedrock_on_success()
        self.assertTrue("aws.bedrock.guardrail_id" in bedrock_sucess_attributes)
        self.assertEqual(bedrock_sucess_attributes["aws.bedrock.guardrail_id"], _BEDROCK_GUARDRAIL_ID)

        # Bedrock Agent Operation
        self.assertTrue("bedrock-agent" in _KNOWN_EXTENSIONS)
        bedrock_agent_op_attributes: Dict[str, str] = _do_extract_bedrock_agent_op_attributes()
        self.assertTrue("aws.bedrock.agent_id" in bedrock_agent_op_attributes)
        self.assertEqual(bedrock_agent_op_attributes["aws.bedrock.agent_id"], _BEDROCK_AGENT_ID)
        bedrock_agent_op_sucess_attributes: Dict[str, str] = _do_bedrock_agent_op_on_success()
        self.assertTrue("aws.bedrock.agent_id" in bedrock_agent_op_sucess_attributes)
        self.assertEqual(bedrock_agent_op_sucess_attributes["aws.bedrock.agent_id"], _BEDROCK_AGENT_ID)

        # Bedrock DataSource Operation
        self.assertTrue("bedrock-agent" in _KNOWN_EXTENSIONS)
        bedrock_datasource_op_attributes: Dict[str, str] = _do_extract_bedrock_datasource_op_attributes()
        self.assertTrue("aws.bedrock.datasource_id" in bedrock_datasource_op_attributes)
        self.assertEqual(bedrock_datasource_op_attributes["aws.bedrock.datasource_id"], _BEDROCK_DATASOURCE_ID)
        bedrock_datasource_op_sucess_attributes: Dict[str, str] = _do_bedrock_datasource_op_on_success()
        self.assertTrue("aws.bedrock.datasource_id" in bedrock_datasource_op_sucess_attributes)
        self.assertEqual(bedrock_datasource_op_sucess_attributes["aws.bedrock.datasource_id"], _BEDROCK_DATASOURCE_ID)

        # Bedrock KnowledgeBase Operation
        self.assertTrue("bedrock-agent" in _KNOWN_EXTENSIONS)
        bedrock_knowledgebase_op_attributes: Dict[str, str] = _do_extract_bedrock_knowledgebase_op_attributes()
        self.assertTrue("aws.bedrock.knowledgebase_id" in bedrock_knowledgebase_op_attributes)
        self.assertEqual(bedrock_knowledgebase_op_attributes["aws.bedrock.knowledgebase_id"], _BEDROCK_KNOWLEDGEBASE_ID)

        # Bedrock Agent Runtime
        self.assertTrue("bedrock-agent-runtime" in _KNOWN_EXTENSIONS)
        bedrock_agent_runtime_attributes: Dict[str, str] = _do_extract_bedrock_agent_runtime_attributes()
        self.assertTrue("aws.bedrock.agent_id" in bedrock_agent_runtime_attributes)
        self.assertEqual(bedrock_agent_runtime_attributes["aws.bedrock.agent_id"], _BEDROCK_AGENT_ID)
        self.assertTrue("aws.bedrock.knowledgebase_id" in bedrock_agent_runtime_attributes)
        self.assertEqual(bedrock_agent_runtime_attributes["aws.bedrock.knowledgebase_id"], _BEDROCK_KNOWLEDGEBASE_ID)

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


def _do_bedrock_on_success() -> Dict[str, str]:
    service_name: str = "bedrock"
    result: Dict[str, Any] = {"guardrailId": _BEDROCK_GUARDRAIL_ID}
    return _do_on_success(service_name, result)


def _do_extract_bedrock_agent_op_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-agent"
    params: Dict[str, str] = {"agentId": _BEDROCK_AGENT_ID}
    operation: str = "CreateAgentAlias"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_agent_op_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-agent"
    result: Dict[str, Any] = {"agentId": _BEDROCK_AGENT_ID}
    operation: str = "CreateAgentAlias"
    return _do_on_success(service_name, result, operation)


def _do_extract_bedrock_datasource_op_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-agent"
    params: Dict[str, str] = {"dataSourceId": _BEDROCK_DATASOURCE_ID}
    operation: str = "UpdateDataSource"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_datasource_op_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-agent"
    result: Dict[str, Any] = {"dataSourceId": _BEDROCK_DATASOURCE_ID}
    operation: str = "UpdateDataSource"
    return _do_on_success(service_name, result, operation)


def _do_extract_bedrock_knowledgebase_op_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-agent"
    params: Dict[str, str] = {"knowledgeBaseId": _BEDROCK_KNOWLEDGEBASE_ID}
    operation: str = "GetKnowledgeBase"
    return _do_extract_attributes(service_name, params, operation)


def _do_extract_bedrock_agent_runtime_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-agent-runtime"
    params: Dict[str, str] = {"agentId": _BEDROCK_AGENT_ID, "knowledgeBaseId": _BEDROCK_KNOWLEDGEBASE_ID}
    return _do_extract_attributes(service_name, params)


def _do_extract_attributes(service_name: str, params: Dict[str, Any], operation: str = None) -> Dict[str, str]:
    mock_call_context: MagicMock = MagicMock()
    mock_call_context.params = params
    if operation:
        mock_call_context.operation = operation
    attributes: Dict[str, str] = {}
    sqs_extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    sqs_extension.extract_attributes(attributes)
    return attributes


def _do_on_success(
    service_name: str, result: Dict[str, Any], operation: str = None, params: Dict[str, Any] = None
) -> Dict[str, str]:
    span_mock: Span = MagicMock()
    mock_call_context = MagicMock()
    span_attributes: Dict[str, str] = {}

    def set_side_effect(set_key, set_value):
        span_attributes[set_key] = set_value

    span_mock.set_attribute.side_effect = set_side_effect

    if operation:
        mock_call_context.operation = operation

    if params:
        mock_call_context.params = params

    extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    extension.on_success(span_mock, result)

    return span_attributes
