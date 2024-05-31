# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import importlib
import json
import copy

from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.instrumentation.botocore.extensions.sqs import _SqsExtension
from opentelemetry.instrumentation.botocore.extensions.types import _AttributeMapT, _AwsSdkExtension, _BotoResultT
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.span import Span
from botocore.response import StreamingBody


def _apply_botocore_instrumentation_patches() -> None:
    """Botocore instrumentation patches

    Adds patches to provide additional support and Java parity for Kinesis, S3, and SQS.
    """
    _apply_botocore_kinesis_patch()
    _apply_botocore_s3_patch()
    _apply_botocore_sqs_patch()
    _apply_botocore_bedrock_runtime_patch()


def _apply_botocore_kinesis_patch() -> None:
    """Botocore instrumentation patch for Kinesis

    This patch adds an extension to the upstream's list of known extension for Kinesis. Extensions allow for custom
    logic for adding service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    the `aws.kinesis.stream_name` attribute, to be used to generate RemoteTarget and achieve parity with the Java
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
    `aws.sqs.queue_url` and `aws.sqs.queue_name` attributes, to be used to generate RemoteTarget and achieve parity
    with the Java instrumentation. Callout that today, the upstream logic adds `aws.queue_url` but we feel that
    `aws.sqs` is more in line with existing AWS Semantic Convention attributes like `AWS_S3_BUCKET`, etc.
    """
    old_extract_attributes = _SqsExtension.extract_attributes

    def patch_extract_attributes(self, attributes: _AttributeMapT):
        old_extract_attributes(self, attributes)
        queue_name = self._call_context.params.get("QueueName")
        queue_url = self._call_context.params.get("QueueUrl")
        if queue_name:
            attributes["aws.sqs.queue_name"] = queue_name
        if queue_url:
            attributes["aws.sqs.queue_url"] = queue_url

    _SqsExtension.extract_attributes = patch_extract_attributes

def _apply_botocore_bedrock_runtime_patch() -> None:
    """Botocore instrumentation patch for S3

    This patch adds an extension to the upstream's list of known extension for S3. Extensions allow for custom
    logic for adding service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    the AWS_S3_BUCKET attribute, to be used to generate RemoteTarget and achieve parity with the Java instrumentation.
    Callout that AWS_S3_BUCKET is in the AWS Semantic Conventions, and is simply not implemented in Python
    instrumentation.
    """
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
            attributes["aws.kinesis.stream_name"] = stream_name

class _BedrockRuntimeExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        context_param = self._call_context.params
        # with open('/Users/zzhlogin/workplace/sample_app_python/aws-otel-python-instrumentation/log.txt', 'a') as file:
        #     # Append text to the file
        #     file.write("context_param: \n")
        #     json.dump(context_param, file, indent=4)
        # gen_ai.request.model: modelId = context_param.modelId !!!!
        # gen_ai.system: "AWS Bedrock" !!!!!!!
        # gen_ai.prompt: prompt_token = context_param.body.prompt !!!!!!

        # model_provider?
        # gen_ai.request.top_p = context_param.body.textGenerationConfig.topP ??????????
        # gen_ai.request.temperature = context_param.body.textGenerationConfig.temperature ??????????
        # gen_ai.request.max_tokens = context_param.body.textGenerationConfig.maxTokenCount ??????????

    def on_success(self, span: Span, result: _BotoResultT):
        with open('/Users/zzhlogin/workplace/sample_app_python/aws-otel-python-instrumentation/log.txt', 'a') as file:
            # Append text to the file
            # file.write("on_success result: \n")
            # file.write(str(result) + "\n")

            # file.write("result body readable: \n")
            # file.write(str(result["body"].readable()))


            file.write("result body read: \n")
            # copy_result_body = copy.deepcopy(result["body"])
            body = result["body"].readlines()
            file.write("result body readlines: \n")
            file.write(body.__class__.__name__)
            file.write(str(body))

            file.write("result body read _amount_read: \n")
            file.write(str(result["body"]._amount_read))
            file.write("result body read _content_length: \n")
            file.write(str(result["body"]._content_length))
            # file.write("result body read second: \n")
            # file.write(str(result["body"].read()))
            #
            #
            # # file.write(result["body"]._raw_stream.__class__.__name__)
            # # body = StreamingBody(copy.deepcopy(result["body"]._raw_stream), result["body"]._content_length)
            # file.write("result body: \n")
            # content_chunks = []
            # file.write("result content_chunks: \n")
            # for chunk in body.iter_chunks():
            #     file.write("result chunk: \n")
            #     content_chunks.append(chunk)
            #     file.write("result chunk append: \n")
            # content = b"".join(content_chunks)
            # file.write("result content: \n")
            # file.write(str(content))
            # body = result["body"].read()




            # file.write("result body class name: \n")
            # file.write(body.__class__.__name__)
            # file.write("result body: \n")
            # file.write(str(body))
            # file.write("result body decode: \n")
            # file.write(str(body.decode('utf-8')))
            # file.write("result body decode type: \n")
            # file.write(body.decode('utf-8').__class__.__name__)
            # file.write("result body json: \n")
            # json.dump(json.loads(content.decode('utf-8')), file, indent=4)
            # # file.write("result body results: \n")
            # # json.dump(json.loads(body.decode('utf-8')).get("results"), file, indent=4)
            # # file.write("result body results[0]: \n")
            # # json.dump(json.loads(body.decode('utf-8')).get("results")[0], file, indent=4)
            # file.write("result body completionReason: \n")
            # json.dump(json.loads(content.decode('utf-8')).get("results")[0].get("completionReason"), file, indent=4)
            #
            # file.write("original body read: \n")
            # file.write(str(result["body"]._content_length))
            # file.write("original body _amount_read: \n")
            # file.write(str(result["body"]._amount_read))
            # file.write(str(result["body"].read()))

        # gen_ai.completion_text: output_token = result.body.? !!!!!!!!!
        # gen_ai.response.finish_reason: completion_reason = result.body.results.completionReason

        # gen_ai.usage.completion_tokens: (generation_token_count)result.ResponseMetadata.HTTPHeaders.x-amzn-bedrock-output-token-count
        # gen_ai.usage.prompt_tokens: (prompt_token_count)result.ResponseMetadata.HTTPHeaders.x-amzn-bedrock-input-token-count

