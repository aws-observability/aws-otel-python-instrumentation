# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Utility module holding attribute keys with special meaning to AWS components"""
AWS_SPAN_KIND: str = "aws.span.kind"
AWS_LOCAL_SERVICE: str = "aws.local.service"
AWS_LOCAL_OPERATION: str = "aws.local.operation"
AWS_REMOTE_DB_USER: str = "aws.remote.db.user"
AWS_REMOTE_SERVICE: str = "aws.remote.service"
AWS_REMOTE_OPERATION: str = "aws.remote.operation"
AWS_REMOTE_ENVIRONMENT: str = "aws.remote.environment"
AWS_REMOTE_RESOURCE_TYPE: str = "aws.remote.resource.type"
AWS_REMOTE_RESOURCE_IDENTIFIER: str = "aws.remote.resource.identifier"
AWS_SDK_DESCENDANT: str = "aws.sdk.descendant"
AWS_CONSUMER_PARENT_SPAN_KIND: str = "aws.consumer.parent.span.kind"
AWS_TRACE_FLAG_SAMPLED: str = "aws.trace.flag.sampled"
AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER: str = "aws.trace.lambda.multiple-server"
AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER: str = "aws.remote.resource.cfn.primary.identifier"

# AWS_#_NAME attributes are not supported in python as they are not part of the Semantic Conventions.
# TODO：Move to Semantic Conventions when these attributes are added.
AWS_AUTH_ACCESS_KEY: str = "aws.auth.account.access_key"
AWS_AUTH_REGION: str = "aws.auth.region"
AWS_SQS_QUEUE_URL: str = "aws.sqs.queue.url"
AWS_SQS_QUEUE_NAME: str = "aws.sqs.queue.name"
AWS_KINESIS_STREAM_ARN: str = "aws.kinesis.stream.arn"
AWS_KINESIS_STREAM_NAME: str = "aws.kinesis.stream.name"
AWS_BEDROCK_DATA_SOURCE_ID: str = "aws.bedrock.data_source.id"
AWS_BEDROCK_KNOWLEDGE_BASE_ID: str = "aws.bedrock.knowledge_base.id"
AWS_BEDROCK_AGENT_ID: str = "aws.bedrock.agent.id"
AWS_BEDROCK_GUARDRAIL_ID: str = "aws.bedrock.guardrail.id"
AWS_BEDROCK_GUARDRAIL_ARN: str = "aws.bedrock.guardrail.arn"
AWS_SECRETSMANAGER_SECRET_ARN: str = "aws.secretsmanager.secret.arn"
AWS_SNS_TOPIC_ARN: str = "aws.sns.topic.arn"
AWS_STEPFUNCTIONS_STATEMACHINE_ARN: str = "aws.stepfunctions.state_machine.arn"
AWS_STEPFUNCTIONS_ACTIVITY_ARN: str = "aws.stepfunctions.activity.arn"
AWS_LAMBDA_FUNCTION_NAME: str = "aws.lambda.function.name"
AWS_LAMBDA_RESOURCEMAPPING_ID: str = "aws.lambda.resource_mapping.id"
AWS_LAMBDA_FUNCTION_ARN: str = "aws.lambda.function.arn"
AWS_DYNAMODB_TABLE_ARN: str = "aws.dynamodb.table.arn"
AWS_REMOTE_RESOURCE_ACCESS_KEY: str = "aws.remote.resource.account.access_key"
AWS_REMOTE_RESOURCE_ACCOUNT_ID: str = "aws.remote.resource.account.id"
AWS_REMOTE_RESOURCE_REGION: str = "aws.remote.resource.region"
AWS_SERVICE_TYPE: str = "aws.service.type"
