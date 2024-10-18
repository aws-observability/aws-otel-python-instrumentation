# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import importlib

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_KINESIS_STREAM_NAME,
    AWS_LAMBDA_FUNCTION_ARN,
    AWS_LAMBDA_FUNCTION_NAME,
    AWS_LAMBDA_RESOURCEMAPPING_ID,
    AWS_SECRETSMANAGER_SECRET_ARN,
    AWS_SNS_TOPIC_ARN,
    AWS_SQS_QUEUE_NAME,
    AWS_SQS_QUEUE_URL,
    AWS_STEPFUNCTIONS_ACTIVITY_ARN,
    AWS_STEPFUNCTIONS_STATEMACHINE_ARN,
)
from amazon.opentelemetry.distro.patches._bedrock_patches import (  # noqa # pylint: disable=unused-import
    _BedrockAgentExtension,
    _BedrockAgentRuntimeExtension,
    _BedrockExtension,
    _BedrockRuntimeExtension,
)
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.instrumentation.botocore.extensions.lmbd import _LambdaExtension
from opentelemetry.instrumentation.botocore.extensions.sns import _SnsExtension
from opentelemetry.instrumentation.botocore.extensions.sqs import _SqsExtension
from opentelemetry.instrumentation.botocore.extensions.types import _AttributeMapT, _AwsSdkExtension, _BotoResultT
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.span import Span


def _apply_botocore_instrumentation_patches() -> None:
    """Botocore instrumentation patches

    Adds patches to provide additional support and Java parity for Kinesis, S3, and SQS.
    """
    _apply_botocore_kinesis_patch()
    _apply_botocore_s3_patch()
    _apply_botocore_sqs_patch()
    _apply_botocore_bedrock_patch()
    _apply_botocore_secretsmanager_patch()
    _apply_botocore_sns_patch()
    _apply_botocore_stepfunctions_patch()
    _apply_botocore_lambda_patch()


def _apply_botocore_lambda_patch() -> None:
    """Botocore instrumentation patch for Lambda

    This patch adds an extension to the upstream's list of known extensions for Lambda.
    Extensions allow for custom logic for adding service-specific information to spans,
    such as attributes. Specifically, we are adding logic to add the
    `aws.lambda.function.name` and  `aws.lambda.resource_mapping.id` attributes

    Sidenote: There exists SpanAttributes.FAAS_INVOKED_NAME for invoke operations
    in upstream. However, we want to cover more operations to extract 'FunctionName',
    so we define `aws.lambda.function.name` separately. Additionally, this helps
    us maintain naming consistency with the other AWS resources.
    """
    old_extract_attributes = _LambdaExtension.extract_attributes

    def patch_extract_attributes(self, attributes: _AttributeMapT):
        old_extract_attributes(self, attributes)
        # This param can be passed as an arn or a name. We standardize it to be the name.
        function_name_param = self._call_context.params.get("FunctionName")
        if function_name_param:
            function_name = function_name_param
            if function_name_param.startswith("arn:aws:lambda:"):
                function_name = function_name_param.split(":")[-1]
            attributes[AWS_LAMBDA_FUNCTION_NAME] = function_name
        resource_mapping_id = self._call_context.params.get("UUID")
        if resource_mapping_id:
            attributes[AWS_LAMBDA_RESOURCEMAPPING_ID] = resource_mapping_id

    old_on_success = _LambdaExtension.on_success

    def patch_on_success(self, span: Span, result: _BotoResultT):
        old_on_success(self, span, result)
        lambda_configuration = result.get("Configuration", {})
        function_arn = lambda_configuration.get("FunctionArn")
        if function_arn:
            span.set_attribute(AWS_LAMBDA_FUNCTION_ARN, function_arn)

    _LambdaExtension.extract_attributes = patch_extract_attributes
    _LambdaExtension.on_success = patch_on_success


def _apply_botocore_stepfunctions_patch() -> None:
    """Botocore instrumentation patch for StepFunctions

    This patch adds an extension to the upstream's list of known extensions for
    StepFunctions. Extensions allow for custom logic for adding service-specific
    information to spans, such as attributes. Specifically, we are adding logic
    to add the `aws.stepfunctions.state_machine.arn` and `aws.stepfunctions.activity.arn`
    attributes, to be used to generate RemoteTarget and achieve partity with the
    Java instrumentation.
    """
    _KNOWN_EXTENSIONS["stepfunctions"] = _lazy_load(".", "_StepFunctionsExtension")


def _apply_botocore_sns_patch() -> None:
    """Botocore instrumentation patch for SNS

    This patch adds an extension to the upstream's list of known extensions for SNS.
    Extensions allow for custom logic for adding service-specific information to
    spans, such as attributes. Specifically, we are adding logic to add the
    `aws.sns.topic.arn` attribute, to be used to generate RemoteTarget and achieve
    parity with the Java instrumentation.

    Sidenote: There exists SpanAttributes.MESSAGING_DESTINATION_NAME in the upstream
    logic that we could re-purpose here. We do not use it here to maintain consistent
    naming patterns with other AWS resources.
    """
    old_extract_attributes = _SnsExtension.extract_attributes

    def patch_extract_attributes(self, attributes: _AttributeMapT):
        old_extract_attributes(self, attributes)
        topic_arn = self._call_context.params.get("TopicArn")
        if topic_arn:
            attributes[AWS_SNS_TOPIC_ARN] = topic_arn

    _SnsExtension.extract_attributes = patch_extract_attributes


def _apply_botocore_secretsmanager_patch() -> None:
    """Botocore instrumentation patch for SecretsManager

    This patch adds an extension to the upstream's list of known extension for SecretsManager.
    Extensions allow for custom logic for adding service-specific information to spans, such as
    attributes. Specifically, we are adding logic to add the `aws.secretsmanager.secret.arn`
    attribute, to be used to generate RemoteTarget and achieve parity with the Java
    instrumentation.
    """
    _KNOWN_EXTENSIONS["secretsmanager"] = _lazy_load(".", "_SecretsManagerExtension")


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

    old_on_success = _SqsExtension.on_success

    def patch_on_success(self, span: Span, result: _BotoResultT):
        old_on_success(self, span, result)
        queue_url = result.get("QueueUrl")
        if queue_url:
            span.set_attribute(AWS_SQS_QUEUE_URL, queue_url)

    _SqsExtension.extract_attributes = patch_extract_attributes
    _SqsExtension.on_success = patch_on_success


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


class _StepFunctionsExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        state_machine_arn = self._call_context.params.get("stateMachineArn")
        if state_machine_arn:
            attributes[AWS_STEPFUNCTIONS_STATEMACHINE_ARN] = state_machine_arn
        activity_arn = self._call_context.params.get("activityArn")
        if activity_arn:
            attributes[AWS_STEPFUNCTIONS_ACTIVITY_ARN] = activity_arn


class _SecretsManagerExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        """
        SecretId can be secret name or secret arn, the function extracts attributes
        only if the SecretId parameter is provided as an arn which starts with
        `arn:aws:secretsmanager:`
        """
        secret_id = self._call_context.params.get("SecretId")
        if secret_id and secret_id.startswith("arn:aws:secretsmanager:"):
            attributes[AWS_SECRETSMANAGER_SECRET_ARN] = secret_id

    # pylint: disable=no-self-use
    def on_success(self, span: Span, result: _BotoResultT):
        secret_arn = result.get("ARN")
        if secret_arn:
            span.set_attribute(AWS_SECRETSMANAGER_SECRET_ARN, secret_arn)


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
