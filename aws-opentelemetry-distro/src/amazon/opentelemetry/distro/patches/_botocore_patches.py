# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import importlib
import json
from typing import Any, Dict, Optional, Sequence

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
    AWS_SECRETSMANAGER_SECRET_ARN,
    AWS_SNS_TOPIC_ARN,
    AWS_SQS_QUEUE_NAME,
    AWS_SQS_QUEUE_URL,
    AWS_STEPFUNCTIONS_ACTIVITY_ARN,
    AWS_STEPFUNCTIONS_STATEMACHINE_ARN,
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
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS, _find_extension, bedrock_utils
from opentelemetry.instrumentation.botocore.extensions.dynamodb import _DynamoDbExtension
from opentelemetry.instrumentation.botocore.extensions.lmbd import _LambdaExtension
from opentelemetry.instrumentation.botocore.extensions.sns import _SnsExtension
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
    _apply_botocore_secretsmanager_patch()
    _apply_botocore_sns_patch()
    _apply_botocore_stepfunctions_patch()
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

    def patch_on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
        old_on_success(self, span, result, instrumentor_context)
        queue_url = result.get("QueueUrl")
        if queue_url:
            span.set_attribute(AWS_SQS_QUEUE_URL, queue_url)

    _SqsExtension.extract_attributes = patch_extract_attributes
    _SqsExtension.on_success = patch_on_success


def _apply_botocore_bedrock_patch() -> None:  # pylint: disable=too-many-statements
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

    # TODO: The following code is to patch bedrock-runtime bugs that are fixed in
    # opentelemetry-instrumentation-botocore==0.56b0 in these PRs:
    # https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3548
    # https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3544
    # Remove this code once we've bumped opentelemetry-instrumentation-botocore dependency to 0.56b0

    old_init = bedrock_utils.ConverseStreamWrapper.__init__
    old_process_event = bedrock_utils.ConverseStreamWrapper._process_event

    # The OpenTelemetry Authors code
    def patched_init(self, *args, **kwargs):
        old_init(self, *args, **kwargs)
        self._tool_json_input_buf = ""

    def patched_process_event(self, event):
        if "contentBlockStart" in event:
            start = event["contentBlockStart"].get("start", {})
            if "toolUse" in start:
                self._content_block = {"toolUse": start["toolUse"]}
            return

        if "contentBlockDelta" in event:
            if self._record_message:
                delta = event["contentBlockDelta"].get("delta", {})
                if "text" in delta:
                    self._content_block.setdefault("text", "")
                    self._content_block["text"] += delta["text"]
                elif "toolUse" in delta:
                    if (input_buf := delta["toolUse"].get("input")) is not None:
                        self._tool_json_input_buf += input_buf
            return

        if "contentBlockStop" in event:
            if self._record_message:
                if self._tool_json_input_buf:
                    try:
                        self._content_block["toolUse"]["input"] = json.loads(self._tool_json_input_buf)
                    except json.JSONDecodeError:
                        self._content_block["toolUse"]["input"] = self._tool_json_input_buf
                self._message["content"].append(self._content_block)
                self._content_block = {}
                self._tool_json_input_buf = ""
            return

        old_process_event(self, event)

    def patched_extract_tool_calls(
        message: dict[str, Any], capture_content: bool
    ) -> Optional[Sequence[Dict[str, Any]]]:
        content = message.get("content")
        if not content:
            return None

        tool_uses = [item["toolUse"] for item in content if "toolUse" in item]
        if not tool_uses:
            tool_uses = [item for item in content if isinstance(item, dict) and item.get("type") == "tool_use"]
            tool_id_key = "id"
        else:
            tool_id_key = "toolUseId"

        if not tool_uses:
            return None

        tool_calls = []
        for tool_use in tool_uses:
            tool_call = {"type": "function"}
            if call_id := tool_use.get(tool_id_key):
                tool_call["id"] = call_id

            if function_name := tool_use.get("name"):
                tool_call["function"] = {"name": function_name}

            if (function_input := tool_use.get("input")) and capture_content:
                tool_call.setdefault("function", {})
                tool_call["function"]["arguments"] = function_input

            tool_calls.append(tool_call)
        return tool_calls

    # TODO: The following code is to patch a bedrock bug that was fixed in
    # opentelemetry-instrumentation-botocore==0.60b0 in:
    # https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3875
    # Remove this code once we've bumped opentelemetry-instrumentation-botocore dependency to 0.60b0
    def patched_process_anthropic_claude_chunk(self, chunk):
        # pylint: disable=too-many-return-statements,too-many-branches
        if not (message_type := chunk.get("type")):
            return

        if message_type == "message_start":
            # {'type': 'message_start', 'message': {'id': 'id', 'type': 'message', 'role': 'assistant',
            # 'model': 'claude-2.0', 'content': [], 'stop_reason': None, 'stop_sequence': None,
            # 'usage': {'input_tokens': 18, 'output_tokens': 1}}}
            if chunk.get("message", {}).get("role") == "assistant":
                self._record_message = True
                message = chunk["message"]
                self._message = {
                    "role": message["role"],
                    "content": message.get("content", []),
                }
            return

        if message_type == "content_block_start":
            # {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}
            # {'type': 'content_block_start', 'index': 1, 'content_block':
            # {'type': 'tool_use', 'id': 'id', 'name': 'func_name', 'input': {}}}
            if self._record_message:
                block = chunk.get("content_block", {})
                if block.get("type") == "text":
                    self._content_block = block
                elif block.get("type") == "tool_use":
                    self._content_block = block
            return

        if message_type == "content_block_delta":
            # {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'Here'}}
            # {'type': 'content_block_delta', 'index': 1, 'delta': {'type': 'input_json_delta', 'partial_json': ''}}
            if self._record_message:
                delta = chunk.get("delta", {})
                if delta.get("type") == "text_delta":
                    self._content_block["text"] += delta.get("text", "")
                elif delta.get("type") == "input_json_delta":
                    self._tool_json_input_buf += delta.get("partial_json", "")
            return

        if message_type == "content_block_stop":
            # {'type': 'content_block_stop', 'index': 0}
            if self._tool_json_input_buf:
                try:
                    self._content_block["input"] = json.loads(self._tool_json_input_buf)
                except json.JSONDecodeError:
                    self._content_block["input"] = self._tool_json_input_buf
            self._message["content"].append(self._content_block)
            self._content_block = {}
            self._tool_json_input_buf = ""
            return

        if message_type == "message_delta":
            # {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn', 'stop_sequence': None},
            # 'usage': {'output_tokens': 123}}
            if (stop_reason := chunk.get("delta", {}).get("stop_reason")) is not None:
                self._response["stopReason"] = stop_reason
            return

        if message_type == "message_stop":
            # {'type': 'message_stop', 'amazon-bedrock-invocationMetrics':
            # {'inputTokenCount': 18, 'outputTokenCount': 123, 'invocationLatency': 5250, 'firstByteLatency': 290}}
            if invocation_metrics := chunk.get("amazon-bedrock-invocationMetrics"):
                self._process_invocation_metrics(invocation_metrics)

            if self._record_message:
                self._response["output"] = {"message": self._message}
                self._record_message = False
                self._message = None

            self._stream_done_callback(self._response)
            return

    bedrock_utils.ConverseStreamWrapper.__init__ = patched_init
    bedrock_utils.ConverseStreamWrapper._process_event = patched_process_event
    bedrock_utils.InvokeModelWithResponseStreamWrapper._process_anthropic_claude_chunk = (
        patched_process_anthropic_claude_chunk
    )
    bedrock_utils.extract_tool_calls = patched_extract_tool_calls

    # END The OpenTelemetry Authors code


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
        event_logger = self._get_event_logger(extension)
        meter = self._get_meter(extension)
        metrics = self._get_metrics(extension, meter)
        instrumentor_ctx = _BotocoreInstrumentorContext(
            event_logger=event_logger,
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
    def on_success(self, span: Span, result: _BotoResultT, instrumentor_context: _BotocoreInstrumentorContext):
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
        stream_arn = self._call_context.params.get("StreamARN")
        if stream_arn:
            attributes[AWS_KINESIS_STREAM_ARN] = stream_arn
