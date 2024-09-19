# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Utility module holding attribute keys with special meaning to AWS components"""
AWS_SPAN_KIND: str = "aws.span.kind"
AWS_LOCAL_SERVICE: str = "aws.local.service"
AWS_LOCAL_OPERATION: str = "aws.local.operation"
AWS_REMOTE_DB_USER: str = "aws.remote.db.user"
AWS_REMOTE_SERVICE: str = "aws.remote.service"
AWS_REMOTE_OPERATION: str = "aws.remote.operation"
AWS_REMOTE_RESOURCE_TYPE: str = "aws.remote.resource.type"
AWS_REMOTE_RESOURCE_IDENTIFIER: str = "aws.remote.resource.identifier"
AWS_SDK_DESCENDANT: str = "aws.sdk.descendant"
AWS_CONSUMER_PARENT_SPAN_KIND: str = "aws.consumer.parent.span.kind"
AWS_TRACE_FLAG_SAMPLED: str = "aws.trace.flag.sampled"

# AWS_#_NAME attributes are not supported in python as they are not part of the Semantic Conventions.
# TODO：Move to Semantic Conventions when these attributes are added.
AWS_SQS_QUEUE_URL: str = "aws.sqs.queue.url"
AWS_SQS_QUEUE_NAME: str = "aws.sqs.queue.name"
AWS_KINESIS_STREAM_NAME: str = "aws.kinesis.stream.name"
AWS_BEDROCK_DATA_SOURCE_ID: str = "aws.bedrock.data_source.id"
AWS_BEDROCK_KNOWLEDGE_BASE_ID: str = "aws.bedrock.knowledge_base.id"
AWS_BEDROCK_AGENT_ID: str = "aws.bedrock.agent.id"
AWS_BEDROCK_GUARDRAIL_ID: str = "aws.bedrock.guardrail.id"
