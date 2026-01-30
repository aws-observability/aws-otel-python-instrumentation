# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import importlib

from botocore.exceptions import ClientError

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_AUTH_ACCESS_KEY,
    AWS_AUTH_REGION,
    AWS_DYNAMODB_TABLE_ARN,
    AWS_KINESIS_STREAM_ARN,
    AWS_KINESIS_STREAM_NAME,
    AWS_LAMBDA_FUNCTION_ARN,
    AWS_LAMBDA_FUNCTION_NAME,
    AWS_LAMBDA_RESOURCEMAPPING_ID,
    AWS_SQS_QUEUE_NAME,
    AWS_SQS_QUEUE_URL,
)
from amazon.opentelemetry.distro.patches._bedrock_agentcore_patches import (  # noqa # pylint: disable=unused-import
    _BedrockAgentCoreExtension,
)
from amazon.opentelemetry.distro.patches._bedrock_patches import (  # noqa # pylint: disable=unused-import
    _BedrockAgentExtension,
    _BedrockAgentRuntimeExtension,
    _BedrockExtension,
)
from opentelemetry.instrumentation.botocore import (
    BotocoreInstrumentor,
    _apply_response_attributes,
    _determine_call_context,
    _safe_invoke,
)
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS, _find_extension
from opentelemetry.instrumentation.botocore.extensions.dynamodb import _DynamoDbExtension
from opentelemetry.instrumentation.botocore.extensions.lmbd import _LambdaExtension
from opentelemetry.instrumentation.botocore.extensions.sqs import _SqsExtension
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkExtension,
    _BotocoreInstrumentorContext,
    _BotoResultT,
)
from opentelemetry.instrumentation.botocore.utils import get_server_attributes
from opentelemetry.instrumentation.utils import is_instrumentation_enabled, suppress_http_instrumentation
from opentelemetry.propagate import get_global_textmap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.span import Span


def _apply_botocore_instrumentation_patches() -> None:
    """Botocore instrumentation patches

    Adds patches to provide additional support and Java parity for Kinesis, S3, and SQS.
    """
    _apply_botocore_propagator_patch()
    _apply_botocore_api_call_patch()
    _apply_botocore_kinesis_patch()
    _apply_botocore_s3_patch()
    _apply_botocore_sqs_patch()
    _apply_botocore_bedrock_patch()
    _apply_botocore_lambda_patch()
    _apply_botocore_dynamodb_patch()


# Known issue in OpenTelemetry upstream botocore auto-instrumentation
# TODO: Contribute fix upstream and remove from ADOT patch after the contribution
def _apply_botocore_propagator_patch() -> None:
    """Botocore instrumentation patch for propagator

    Changes the default propagator from AwsXRayPropagator to the global propagator.
    This allows the propagator to be configured via OTEL_PROPAGATORS environment variable.
    """
    # Store the original __init__ method
    original_init = BotocoreInstrumentor.__init__

    def patched_init(self):
        # Call the original __init__
        original_init(self)
        # Replace the propagator with the global one
        self.propagator = get_global_textmap()

    # Apply the patch
    BotocoreInstrumentor.__init__ = patched_init


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

    def patch_on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
        old_on_success(self, span, result, instrumentor_context)
        lambda_configuration = result.get("Configuration", {})
        function_arn = lambda_configuration.get("FunctionArn")
        if function_arn:
            span.set_attribute(AWS_LAMBDA_FUNCTION_ARN, function_arn)

    _LambdaExtension.extract_attributes = patch_extract_attributes
    _LambdaExtension.on_success = patch_on_success


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

    def patch_on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
        old_on_success(self, span, result, instrumentor_context)
        queue_url = result.get("QueueUrl")
        if queue_url:
            span.set_attribute(AWS_SQS_QUEUE_URL, queue_url)

    _SqsExtension.extract_attributes = patch_extract_attributes
    _SqsExtension.on_success = patch_on_success


def _apply_botocore_bedrock_patch() -> None:
    """Botocore instrumentation patch for Bedrock, Bedrock Agent, and Bedrock Agent Runtime

    This patch adds an extension to the upstream's list of known extension for Bedrock.
    Extensions allow for custom logic for adding service-specific information to spans, such as attributes.
    Specifically, we are adding logic to add the AWS_BEDROCK attributes referenced in _aws_attribute_keys.
    Note: Bedrock Runtime uses the upstream extension directly.
    """
    _KNOWN_EXTENSIONS["bedrock"] = _lazy_load(".", "_BedrockExtension")
    _KNOWN_EXTENSIONS["bedrock-agent"] = _lazy_load(".", "_BedrockAgentExtension")
    _KNOWN_EXTENSIONS["bedrock-agent-runtime"] = _lazy_load(".", "_BedrockAgentRuntimeExtension")
    _KNOWN_EXTENSIONS["bedrock-agentcore"] = _lazy_load(".._bedrock_agentcore_patches", "_BedrockAgentCoreExtension")
    _KNOWN_EXTENSIONS["bedrock-agentcore-control"] = _lazy_load(
        ".._bedrock_agentcore_patches", "_BedrockAgentCoreExtension"
    )


def _apply_botocore_dynamodb_patch() -> None:
    """Botocore instrumentation patch for DynamoDB

    This patch adds an extension to the upstream's list of known extensions for DynamoDB.
    Extensions allow for custom logic for adding service-specific information to
    spans, such as attributes. Specifically, we are adding logic to add the
    `aws.table.arn` attribute, to be used to generate RemoteTarget and achieve
    parity with the Java instrumentation.
    """
    old_on_success = _DynamoDbExtension.on_success

    def patch_on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
        old_on_success(self, span, result, instrumentor_context)
        table = result.get("Table", {})
        table_arn = table.get("TableArn")
        if table_arn:
            span.set_attribute(AWS_DYNAMODB_TABLE_ARN, table_arn)

    _DynamoDbExtension.on_success = patch_on_success


def _apply_botocore_api_call_patch() -> None:
    # pylint: disable=too-many-locals,too-many-statements
    def patched_api_call(self, original_func, instance, args, kwargs):
        """Botocore instrumentation patch to capture AWS authentication details

        This patch extends the upstream implementation to include additional AWS authentication
        attributes:
            - aws.auth.account.access_key
            - aws.auth.region

        Note: Current implementation duplicates upstream code in v1.33.x-0.54bx. Future improvements should:
        1. Propose refactoring upstream _patched_api_call into smaller components
        2. Apply targeted patches to these components to reduce code duplication

        Reference: https://github.com/open-telemetry/opentelemetry-python-contrib/blob/
        release/v1.33.x-0.54bx/instrumentation/opentelemetry-instrumentation-botocore/src/
        opentelemetry/instrumentation/botocore/__init__.py#L263
        """
        if not is_instrumentation_enabled():
            return original_func(*args, **kwargs)

        call_context = _determine_call_context(instance, args)
        if call_context is None:
            return original_func(*args, **kwargs)

        extension = _find_extension(call_context)
        if not extension.should_trace_service_call():
            return original_func(*args, **kwargs)

        attributes = {
            SpanAttributes.RPC_SYSTEM: "aws-api",
            SpanAttributes.RPC_SERVICE: call_context.service_id,
            SpanAttributes.RPC_METHOD: call_context.operation,
            # TODO: update when semantic conventions exist
            "aws.region": call_context.region,
            **get_server_attributes(call_context.endpoint_url),
            AWS_AUTH_REGION: call_context.region,
        }

        credentials = instance._get_credentials()
        if credentials is not None:
            access_key = credentials.access_key
            if access_key is not None:
                attributes[AWS_AUTH_ACCESS_KEY] = access_key

        _safe_invoke(extension.extract_attributes, attributes)
        end_span_on_exit = extension.should_end_span_on_exit()

        tracer = self._get_tracer(extension)
        meter = self._get_meter(extension)
        metrics = self._get_metrics(extension, meter)
        instrumentor_ctx = _BotocoreInstrumentorContext(
            logger=self._get_logger(extension),
            metrics=metrics,
        )
        with tracer.start_as_current_span(
            call_context.span_name,
            kind=call_context.span_kind,
            attributes=attributes,
            # tracing streaming services require to close the span manually
            # at a later time after the stream has been consumed
            end_on_exit=end_span_on_exit,
        ) as span:
            _safe_invoke(extension.before_service_call, span, instrumentor_ctx)
            self._call_request_hook(span, call_context)

            try:
                with suppress_http_instrumentation():
                    result = None
                    try:
                        result = original_func(*args, **kwargs)
                    except ClientError as error:
                        result = getattr(error, "response", None)
                        _apply_response_attributes(span, result)
                        _safe_invoke(extension.on_error, span, error, instrumentor_ctx)
                        raise
                    _apply_response_attributes(span, result)
                    _safe_invoke(extension.on_success, span, result, instrumentor_ctx)
            finally:
                _safe_invoke(extension.after_service_call, instrumentor_ctx)
                self._call_response_hook(span, call_context, result)

            return result

    BotocoreInstrumentor._patched_api_call = patched_api_call


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
        stream_arn = self._call_context.params.get("StreamARN")
        if stream_arn:
            attributes[AWS_KINESIS_STREAM_ARN] = stream_arn
