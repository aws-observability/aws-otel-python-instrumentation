# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
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
_GEN_AI_SYSTEM: str = "AWS Bedrock"
_GEN_AI_REQUEST_TITAN_MODEL: str = "amazon.titan-test-id"
_GEN_AI_REQUEST_CLAUDE_MODEL: str = "anthropic.claude-test-id"
_GEN_AI_REQUEST_LLAMA2_MODEL: str = "meta.llama2-test-id"
_GEN_AI_REQUEST_TOP_P: float = 0.9
_GEN_AI_REQUEST_TEMPERATURE: float = 0.7
_GEN_AI_REQUEST_MAX_TOKENS: int = 1234
_GEN_AI_USAGE_PROMOT_TOKENS: int = 55
_GEN_AI_USAGE_COMPLETION_TOKENS: int = 24


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

        # BedrockRuntime
        self.assertFalse("bedrock-runtime" in _KNOWN_EXTENSIONS, "Upstream has added a bedrock-runtime extension")

    # pylint: disable=too-many-statements
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

        # BedrockRuntime
        # A. amazon.titan
        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_bedrock_runtime_titan_attributes()
        self.assertTrue("gen_ai.system" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertTrue("gen_ai.request.model" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], _GEN_AI_REQUEST_TITAN_MODEL)
        self.assertTrue("gen_ai.request.top_p" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], _GEN_AI_REQUEST_TOP_P)
        self.assertTrue("gen_ai.request.temperature" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], _GEN_AI_REQUEST_TEMPERATURE)
        self.assertTrue("gen_ai.request.max_tokens" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], _GEN_AI_REQUEST_MAX_TOKENS)

        bedrock_runtime_sucess_attributes: Dict[str, str] = _do_bedrock_runtime_titan_on_success()
        self.assertTrue("gen_ai.usage.prompt_tokens" in bedrock_runtime_sucess_attributes)
        self.assertEqual(bedrock_runtime_sucess_attributes["gen_ai.usage.prompt_tokens"], _GEN_AI_USAGE_PROMOT_TOKENS)
        self.assertTrue("gen_ai.usage.completion_tokens" in bedrock_runtime_sucess_attributes)
        self.assertEqual(
            bedrock_runtime_sucess_attributes["gen_ai.usage.completion_tokens"], _GEN_AI_USAGE_COMPLETION_TOKENS
        )

        bedrock_runtime_no_valid_attributes: Dict[str, str] = _do_extract_bedrock_runtime_titan_no_valid_attributes()
        self.assertTrue("gen_ai.system" in bedrock_runtime_no_valid_attributes)
        self.assertEqual(bedrock_runtime_no_valid_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertTrue("gen_ai.request.model" in bedrock_runtime_no_valid_attributes)
        self.assertEqual(bedrock_runtime_no_valid_attributes["gen_ai.request.model"], _GEN_AI_REQUEST_TITAN_MODEL)
        self.assertFalse("gen_ai.request.top_p" in bedrock_runtime_no_valid_attributes)
        self.assertFalse("gen_ai.request.temperature" in bedrock_runtime_no_valid_attributes)
        self.assertFalse("gen_ai.request.max_tokens" in bedrock_runtime_no_valid_attributes)

        bedrock_runtime_sucess_no_valid_attributes: Dict[str, str] = _do_bedrock_runtime_titan_no_valid_on_success()
        self.assertFalse("gen_ai.usage.prompt_tokens" in bedrock_runtime_sucess_no_valid_attributes)
        self.assertFalse("gen_ai.usage.completion_tokens" in bedrock_runtime_sucess_no_valid_attributes)

        # B. anthropic.claude
        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_bedrock_runtime_claude_attributes()
        self.assertTrue("gen_ai.system" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertTrue("gen_ai.request.model" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], _GEN_AI_REQUEST_CLAUDE_MODEL)
        self.assertTrue("gen_ai.request.top_p" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], _GEN_AI_REQUEST_TOP_P)
        self.assertTrue("gen_ai.request.temperature" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], _GEN_AI_REQUEST_TEMPERATURE)
        self.assertTrue("gen_ai.request.max_tokens" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], _GEN_AI_REQUEST_MAX_TOKENS)

        bedrock_runtime_sucess_attributes: Dict[str, str] = _do_bedrock_runtime_claude_on_success()
        self.assertTrue("gen_ai.usage.prompt_tokens" in bedrock_runtime_sucess_attributes)
        self.assertEqual(bedrock_runtime_sucess_attributes["gen_ai.usage.prompt_tokens"], _GEN_AI_USAGE_PROMOT_TOKENS)
        self.assertTrue("gen_ai.usage.completion_tokens" in bedrock_runtime_sucess_attributes)
        self.assertEqual(
            bedrock_runtime_sucess_attributes["gen_ai.usage.completion_tokens"], _GEN_AI_USAGE_COMPLETION_TOKENS
        )

        bedrock_runtime_no_valid_attributes: Dict[str, str] = _do_extract_bedrock_runtime_claude_no_valid_attributes()
        self.assertTrue("gen_ai.system" in bedrock_runtime_no_valid_attributes)
        self.assertEqual(bedrock_runtime_no_valid_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertTrue("gen_ai.request.model" in bedrock_runtime_no_valid_attributes)
        self.assertEqual(bedrock_runtime_no_valid_attributes["gen_ai.request.model"], _GEN_AI_REQUEST_CLAUDE_MODEL)
        self.assertFalse("gen_ai.request.top_p" in bedrock_runtime_no_valid_attributes)
        self.assertFalse("gen_ai.request.temperature" in bedrock_runtime_no_valid_attributes)
        self.assertFalse("gen_ai.request.max_tokens" in bedrock_runtime_no_valid_attributes)

        bedrock_runtime_sucess_no_valid_attributes: Dict[str, str] = _do_bedrock_runtime_claude_no_valid_on_success()
        self.assertFalse("gen_ai.usage.prompt_tokens" in bedrock_runtime_sucess_no_valid_attributes)
        self.assertFalse("gen_ai.usage.completion_tokens" in bedrock_runtime_sucess_no_valid_attributes)

        # C. meta.llama2
        self.assertTrue("bedrock-runtime" in _KNOWN_EXTENSIONS)
        bedrock_runtime_attributes: Dict[str, str] = _do_extract_bedrock_runtime_llama2_attributes()
        self.assertTrue("gen_ai.system" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertTrue("gen_ai.request.model" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.model"], _GEN_AI_REQUEST_LLAMA2_MODEL)
        self.assertTrue("gen_ai.request.top_p" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.top_p"], _GEN_AI_REQUEST_TOP_P)
        self.assertTrue("gen_ai.request.temperature" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.temperature"], _GEN_AI_REQUEST_TEMPERATURE)
        self.assertTrue("gen_ai.request.max_tokens" in bedrock_runtime_attributes)
        self.assertEqual(bedrock_runtime_attributes["gen_ai.request.max_tokens"], _GEN_AI_REQUEST_MAX_TOKENS)

        bedrock_runtime_sucess_attributes: Dict[str, str] = _do_bedrock_runtime_llama2_on_success()
        self.assertTrue("gen_ai.usage.prompt_tokens" in bedrock_runtime_sucess_attributes)
        self.assertEqual(bedrock_runtime_sucess_attributes["gen_ai.usage.prompt_tokens"], _GEN_AI_USAGE_PROMOT_TOKENS)
        self.assertTrue("gen_ai.usage.completion_tokens" in bedrock_runtime_sucess_attributes)
        self.assertEqual(
            bedrock_runtime_sucess_attributes["gen_ai.usage.completion_tokens"], _GEN_AI_USAGE_COMPLETION_TOKENS
        )

        bedrock_runtime_no_valid_attributes: Dict[str, str] = _do_extract_bedrock_runtime_llama2_no_valid_attributes()
        self.assertTrue("gen_ai.system" in bedrock_runtime_no_valid_attributes)
        self.assertEqual(bedrock_runtime_no_valid_attributes["gen_ai.system"], _GEN_AI_SYSTEM)
        self.assertTrue("gen_ai.request.model" in bedrock_runtime_no_valid_attributes)
        self.assertEqual(bedrock_runtime_no_valid_attributes["gen_ai.request.model"], _GEN_AI_REQUEST_LLAMA2_MODEL)
        self.assertFalse("gen_ai.request.top_p" in bedrock_runtime_no_valid_attributes)
        self.assertFalse("gen_ai.request.temperature" in bedrock_runtime_no_valid_attributes)
        self.assertFalse("gen_ai.request.max_tokens" in bedrock_runtime_no_valid_attributes)

        bedrock_runtime_sucess_no_valid_attributes: Dict[str, str] = _do_bedrock_runtime_llama2_no_valid_on_success()
        self.assertFalse("gen_ai.usage.prompt_tokens" in bedrock_runtime_sucess_no_valid_attributes)
        self.assertFalse("gen_ai.usage.completion_tokens" in bedrock_runtime_sucess_no_valid_attributes)


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


def _do_extract_bedrock_runtime_titan_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    body: Dict[str, Any] = {
        "inputText": "Test input texts.",
        "textGenerationConfig": {
            "maxTokenCount": _GEN_AI_REQUEST_MAX_TOKENS,
            "temperature": _GEN_AI_REQUEST_TEMPERATURE,
            "topP": _GEN_AI_REQUEST_TOP_P,
        },
    }
    body = json.dumps(body)
    params: Dict[str, Any] = {
        "body": body,
        "modelId": _GEN_AI_REQUEST_TITAN_MODEL,
    }
    operation = "InvokeModel"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_runtime_titan_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    result: Dict[str, Any] = _get_bedrock_runtime_sample_result()
    operation = "InvokeModel"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_TITAN_MODEL,
    }
    return _do_on_success(service_name, result, operation, params)


def _do_extract_bedrock_runtime_titan_no_valid_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    invalid_body: Dict[str, Any] = {
        "inputText": "Test input texts.",
    }
    body = json.dumps(invalid_body)
    params: Dict[str, Any] = {
        "body": body,
        "modelId": _GEN_AI_REQUEST_TITAN_MODEL,
    }
    operation = "InvokeModel"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_runtime_titan_no_valid_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    result: Dict[str, Any] = _get_bedrock_runtime_invalid_sample_result()
    operation = "InvokeModel"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_TITAN_MODEL,
    }
    return _do_on_success(service_name, result, operation, params)


def _do_extract_bedrock_runtime_claude_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    body: Dict[str, Any] = {
        "max_tokens": _GEN_AI_REQUEST_MAX_TOKENS,
        "system": "Test input texts.",
        "messages": [{"role": "user", "content": "Test use content"}],
        "temperature": _GEN_AI_REQUEST_TEMPERATURE,
        "top_p": _GEN_AI_REQUEST_TOP_P,
    }
    body = json.dumps(body)
    params: Dict[str, Any] = {
        "body": body,
        "modelId": _GEN_AI_REQUEST_CLAUDE_MODEL,
    }
    operation = "InvokeModel"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_runtime_claude_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    result: Dict[str, Any] = _get_bedrock_runtime_sample_result()
    operation = "InvokeModel"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_CLAUDE_MODEL,
    }
    return _do_on_success(service_name, result, operation, params)


def _do_extract_bedrock_runtime_claude_no_valid_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_CLAUDE_MODEL,
    }
    operation = "InvokeModel"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_runtime_claude_no_valid_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    result: Dict[str, Any] = _get_bedrock_runtime_invalid_sample_result()
    operation = "InvokeModel"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_CLAUDE_MODEL,
    }
    return _do_on_success(service_name, result, operation, params)


def _do_extract_bedrock_runtime_llama2_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    body: Dict[str, Any] = {
        "prompt": "Test input texts.",
        "max_gen_len": _GEN_AI_REQUEST_MAX_TOKENS,
        "temperature": _GEN_AI_REQUEST_TEMPERATURE,
        "top_p": _GEN_AI_REQUEST_TOP_P,
    }
    body = json.dumps(body)
    params: Dict[str, Any] = {
        "body": body,
        "modelId": _GEN_AI_REQUEST_LLAMA2_MODEL,
    }
    operation = "InvokeModel"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_runtime_llama2_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    result: Dict[str, Any] = _get_bedrock_runtime_sample_result()
    operation = "InvokeModel"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_LLAMA2_MODEL,
    }
    return _do_on_success(service_name, result, operation, params)


def _do_extract_bedrock_runtime_llama2_no_valid_attributes() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_LLAMA2_MODEL,
    }
    operation = "InvokeModel"
    return _do_extract_attributes(service_name, params, operation)


def _do_bedrock_runtime_llama2_no_valid_on_success() -> Dict[str, str]:
    service_name: str = "bedrock-runtime"
    result: Dict[str, Any] = _get_bedrock_runtime_invalid_sample_result()
    operation = "InvokeModel"
    params: Dict[str, Any] = {
        "modelId": _GEN_AI_REQUEST_LLAMA2_MODEL,
    }
    return _do_on_success(service_name, result, operation, params)


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


def _get_bedrock_runtime_sample_result():
    result: Dict[str, Any] = {
        "ResponseMetadata": {
            "HTTPHeaders": {
                "x-amzn-bedrock-output-token-count": str(_GEN_AI_USAGE_COMPLETION_TOKENS),
                "x-amzn-bedrock-input-token-count": str(_GEN_AI_USAGE_PROMOT_TOKENS),
            },
        },
        "body": None,
    }
    return result


def _get_bedrock_runtime_invalid_sample_result():
    result: Dict[str, Any] = {
        "ResponseMetadata": {
            "InvalidMetafata": "test_metadata",
        },
        "body": None,
    }
    return result
