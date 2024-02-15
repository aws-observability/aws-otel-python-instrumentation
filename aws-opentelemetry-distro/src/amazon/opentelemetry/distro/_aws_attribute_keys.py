# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Utility module holding attribute keys with special meaning to AWS components"""
from opentelemetry.semconv.trace import SpanAttributes

AWS_SPAN_KIND: str = "aws.span.kind"
AWS_LOCAL_SERVICE: str = "aws.local.service"
AWS_LOCAL_OPERATION: str = "aws.local.operation"
AWS_REMOTE_SERVICE: str = "aws.remote.service"
AWS_REMOTE_OPERATION: str = "aws.remote.operation"
AWS_REMOTE_TARGET: str = "aws.remote.target"
AWS_SDK_DESCENDANT: str = "aws.sdk.descendant"
AWS_CONSUMER_PARENT_SPAN_KIND: str = "aws.consumer.parent.span.kind"

# AWS_#_NAME attributes are not supported in python as they are not part of the Semantic Conventions.
# Use SpanAttributes for AWS_TABLE_NAME and AWS_BUCKET_NAME.
# TODO：Move to Semantic Conventions when these attributes are added.
AWS_TABLE_NAME: str = SpanAttributes.AWS_DYNAMODB_TABLE_NAMES
AWS_BUCKET_NAME: str = SpanAttributes.AWS_S3_BUCKET
AWS_QUEUE_URL: str = "aws.queue_url"
AWS_QUEUE_NAME: str = "aws.queue.name"
AWS_STREAM_NAME: str = "aws.stream.name"
