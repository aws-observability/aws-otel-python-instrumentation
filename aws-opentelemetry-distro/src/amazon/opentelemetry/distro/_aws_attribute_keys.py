# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


class _AwsAttributeKeys:
    """Utility class holding attribute keys with special meaning to AWS components"""

    AWS_SPAN_KIND: str = "aws.span.kind"
    AWS_LOCAL_SERVICE: str = "aws.local.service"
    AWS_LOCAL_OPERATION: str = "aws.local.operation"
    AWS_REMOTE_SERVICE: str = "aws.remote.service"
    AWS_REMOTE_OPERATION: str = "aws.remote.operation"
    AWS_REMOTE_TARGET: str = "aws.remote.target"
    AWS_SDK_DESCENDANT: str = "aws.sdk.descendant"
    AWS_CONSUMER_PARENT_SPAN_KIND: str = "aws.consumer.parent.span.kind"

    # Use the same AWS Resource attribute name defined by OTel java auto-instr for aws_sdk_v_1_1
    # TODO: all AWS specific attributes should be defined in semconv package and reused cross all
    #   otel packages. Related sim - https://github.com/open-telemetry/opentelemetry-java-instrumentation/issues/8710

    AWS_BUCKET_NAME: str = "aws.bucket.name"
    AWS_QUEUE_NAME: str = "aws.queue.name"
    AWS_STREAM_NAME: str = "aws.stream.name"
    AWS_TABLE_NAME: str = "aws.table.name"
