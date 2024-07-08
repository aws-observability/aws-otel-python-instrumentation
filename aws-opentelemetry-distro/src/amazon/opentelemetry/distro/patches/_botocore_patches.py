# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import importlib

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_KINESIS_STREAM_NAME,
    AWS_SQS_QUEUE_NAME,
    AWS_SQS_QUEUE_URL,
)
from amazon.opentelemetry.distro.patches._bedrock_patches import (  # noqa # pylint: disable=unused-import
    _BedrockAgentExtension,
    _BedrockAgentRuntimeExtension,
    _BedrockExtension,
    _BedrockRuntimeExtension,
)
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.instrumentation.botocore.extensions.sqs import _SqsExtension
from opentelemetry.instrumentation.botocore.extensions.types import _AttributeMapT, _AwsSdkExtension
from opentelemetry.semconv.trace import SpanAttributes


def _apply_botocore_instrumentation_patches() -> None:
    """Botocore instrumentation patches

    Adds patches to provide additional support and Java parity for Kinesis, S3, and SQS.
    """
    _apply_botocore_kinesis_patch()
    _apply_botocore_s3_patch()
    _apply_botocore_sqs_patch()
    _apply_botocore_bedrock_patch()


def _apply_botocore_kinesis_patch() -> None:
    """Botocore instrumentation patch for Kinesis

    This patch adds an extension to the upstream's list of known extension for Kinesis. Extensions allow for custom
    logic for adding service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    the `aws.kinesis.stream.name` attribute, to be used to generate RemoteTarget and achieve parity with the Java
    instrumentation.
    """
    _KNOWN_EXTENSIONS["kinesis"] = _lazy_load(".", "_KinesisExtension")


def _apply_botocore_s3_patch() -> None:
    """Botocore instrumentation patch for S3

    This patch adds an extension to the upstream's list of known extension for S3. Extensions allow for custom
    logic for adding service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    the AWS_S3_BUCKET attribute, to be used to generate RemoteTarget and achieve parity with the Java instrumentation.
    Callout that AWS_S3_BUCKET is in the AWS Semantic Conventions, and is simply not implemented in Python
    instrumentation.
    """
    _KNOWN_EXTENSIONS["s3"] = _lazy_load(".", "_S3Extension")


def _apply_botocore_sqs_patch() -> None:
    """Botocore instrumentation patch for SQS

    This patch extends the existing upstream extension for SQS. Extensions allow for custom logic for adding
    service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    `aws.sqs.queue.url` and `aws.sqs.queue.name` attributes, to be used to generate RemoteTarget and achieve parity
    with the Java instrumentation. Callout that today, the upstream logic adds `aws.queue_url` but we feel that
    `aws.sqs` is more in line with existing AWS Semantic Convention attributes like `AWS_S3_BUCKET`, etc.
    """
    old_extract_attributes = _SqsExtension.extract_attributes

    def patch_extract_attributes(self, attributes: _AttributeMapT):
        old_extract_attributes(self, attributes)
        queue_name = self._call_context.params.get("QueueName")
        queue_url = self._call_context.params.get("QueueUrl")
        if queue_name:
            attributes[AWS_SQS_QUEUE_NAME] = queue_name
        if queue_url:
            attributes[AWS_SQS_QUEUE_URL] = queue_url

    _SqsExtension.extract_attributes = patch_extract_attributes


def _apply_botocore_bedrock_patch() -> None:
    """Botocore instrumentation patch for Bedrock, Bedrock Agent, Bedrock Runtime and Bedrock Agent Runtime

    This patch adds an extension to the upstream's list of known extension for Bedrock.
    Extensions allow for custom logic for adding service-specific information to spans, such as attributes.
    Specifically, we are adding logic to add the AWS_BEDROCK attributes referenced in _aws_attribute_keys,
    GEN_AI_REQUEST_MODEL and GEN_AI_SYSTEM attributes referenced in _aws_span_processing_util.
    """
    _KNOWN_EXTENSIONS["bedrock"] = _lazy_load(".", "_BedrockExtension")
    _KNOWN_EXTENSIONS["bedrock-agent"] = _lazy_load(".", "_BedrockAgentExtension")
    _KNOWN_EXTENSIONS["bedrock-agent-runtime"] = _lazy_load(".", "_BedrockAgentRuntimeExtension")
    _KNOWN_EXTENSIONS["bedrock-runtime"] = _lazy_load(".", "_BedrockRuntimeExtension")


# The OpenTelemetry Authors code
def _lazy_load(module, cls):
    """Clone of upstream opentelemetry.instrumentation.botocore.extensions.lazy_load

    The logic in this method is dependent on the file path of where it is implemented, so must be cloned here.
    """

    def loader():
        imported_mod = importlib.import_module(module, __name__)
        return getattr(imported_mod, cls, None)

    return loader


# END The OpenTelemetry Authors code


class _S3Extension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        bucket_name = self._call_context.params.get("Bucket")
        if bucket_name:
            attributes[SpanAttributes.AWS_S3_BUCKET] = bucket_name


class _KinesisExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        stream_name = self._call_context.params.get("StreamName")
        if stream_name:
            attributes[AWS_KINESIS_STREAM_NAME] = stream_name
