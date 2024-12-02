# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import math
from logging import INFO, Logger, getLogger
from typing import Dict, List

from docker.types import EndpointConfig
from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from testcontainers.localstack import LocalStackContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME, ContractTestBase
from amazon.utils.application_signals_constants import (
    AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER,
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_RESOURCE_IDENTIFIER,
    AWS_REMOTE_RESOURCE_TYPE,
    AWS_REMOTE_SERVICE,
    AWS_SPAN_KIND,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import ExponentialHistogramDataPoint, Metric
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from opentelemetry.semconv.trace import SpanAttributes

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)

_AWS_SQS_QUEUE_URL: str = "aws.sqs.queue.url"
_AWS_SQS_QUEUE_NAME: str = "aws.sqs.queue.name"
_AWS_KINESIS_STREAM_NAME: str = "aws.kinesis.stream.name"
_AWS_BEDROCK_AGENT_ID: str = "aws.bedrock.agent.id"
_AWS_BEDROCK_GUARDRAIL_ID: str = "aws.bedrock.guardrail.id"
_AWS_BEDROCK_KNOWLEDGE_BASE_ID: str = "aws.bedrock.knowledge_base.id"
_AWS_BEDROCK_DATA_SOURCE_ID: str = "aws.bedrock.data_source.id"

_GEN_AI_REQUEST_MODEL: str = "gen_ai.request.model"
_GEN_AI_REQUEST_TEMPERATURE: str = "gen_ai.request.temperature"
_GEN_AI_REQUEST_TOP_P: str = "gen_ai.request.top_p"
_GEN_AI_REQUEST_MAX_TOKENS: str = "gen_ai.request.max_tokens"
_GEN_AI_RESPONSE_FINISH_REASONS: str = "gen_ai.response.finish_reasons"
_GEN_AI_USAGE_INPUT_TOKENS: str = "gen_ai.usage.input_tokens"
_GEN_AI_USAGE_OUTPUT_TOKENS: str = "gen_ai.usage.output_tokens"

_AWS_SECRET_ARN: str = "aws.secretsmanager.secret.arn"
_AWS_STATE_MACHINE_ARN: str = "aws.stepfunctions.state_machine.arn"
_AWS_ACTIVITY_ARN: str = "aws.stepfunctions.activity.arn"
_AWS_SNS_TOPIC_ARN: str = "aws.sns.topic.arn"


# pylint: disable=too-many-public-methods,too-many-lines
class BotocoreTest(ContractTestBase):
    _local_stack: LocalStackContainer

    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {
            "AWS_SDK_S3_ENDPOINT": "http://s3.localstack:4566",
            "AWS_SDK_ENDPOINT": "http://localstack:4566",
            "AWS_REGION": "us-west-2",
            # To avoid boto3 instrumentation influence SQS test
            "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS": "boto3",
        }

    @override
    def get_application_network_aliases(self) -> List[str]:
        return ["error.test", "fault.test"]

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-botocore-app"

    @classmethod
    @override
    def set_up_dependency_container(cls):
        local_stack_networking_config: Dict[str, EndpointConfig] = {
            NETWORK_NAME: EndpointConfig(
                version="1.22",
                aliases=[
                    "localstack",
                    "s3.localstack",
                ],
            )
        }
        cls._local_stack: LocalStackContainer = (
            LocalStackContainer(image="localstack/localstack:3.5.0")
            .with_name("localstack")
            .with_services("s3", "sqs", "dynamodb", "kinesis", "secretsmanager", "iam", "stepfunctions", "sns")
            .with_env("DEFAULT_REGION", "us-west-2")
            .with_kwargs(network=NETWORK_NAME, networking_config=local_stack_networking_config)
        )
        cls._local_stack.start()

    @classmethod
    @override
    def tear_down_dependency_container(cls):
        _logger.info("LocalStack stdout")
        _logger.info(cls._local_stack.get_logs()[0].decode())
        _logger.info("LocalStack stderr")
        _logger.info(cls._local_stack.get_logs()[1].decode())
        cls._local_stack.stop()

    def test_s3_create_bucket(self):
        self.do_test_requests(
            "s3/createbucket/create-bucket",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::S3",
            remote_operation="CreateBucket",
            remote_resource_type="AWS::S3::Bucket",
            remote_resource_identifier="test-bucket-name",
            cloudformation_primary_identifier="test-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-bucket-name",
            },
            span_name="S3.CreateBucket",
        )

    def test_s3_create_object(self):
        self.do_test_requests(
            "s3/createobject/put-object/some-object",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::S3",
            remote_operation="PutObject",
            remote_resource_type="AWS::S3::Bucket",
            remote_resource_identifier="test-put-object-bucket-name",
            cloudformation_primary_identifier="test-put-object-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-put-object-bucket-name",
            },
            span_name="S3.PutObject",
        )

    def test_s3_get_object(self):
        self.do_test_requests(
            "s3/getobject/get-object/some-object",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::S3",
            remote_operation="GetObject",
            remote_resource_type="AWS::S3::Bucket",
            remote_resource_identifier="test-get-object-bucket-name",
            cloudformation_primary_identifier="test-get-object-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-get-object-bucket-name",
            },
            span_name="S3.GetObject",
        )

    def test_s3_error(self):
        self.do_test_requests(
            "s3/error",
            "GET",
            400,
            1,
            0,
            remote_service="AWS::S3",
            remote_operation="CreateBucket",
            remote_resource_type="AWS::S3::Bucket",
            remote_resource_identifier="-",
            cloudformation_primary_identifier="-",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "-",
            },
            span_name="S3.CreateBucket",
        )

    def test_s3_fault(self):
        self.do_test_requests(
            "s3/fault",
            "GET",
            500,
            0,
            1,
            remote_service="AWS::S3",
            remote_operation="CreateBucket",
            remote_resource_type="AWS::S3::Bucket",
            remote_resource_identifier="valid-bucket-name",
            cloudformation_primary_identifier="valid-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "valid-bucket-name",
            },
            span_name="S3.CreateBucket",
        )

    def test_dynamodb_create_table(self):
        self.do_test_requests(
            "ddb/createtable/some-table",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::DynamoDB",
            remote_operation="CreateTable",
            remote_resource_type="AWS::DynamoDB::Table",
            remote_resource_identifier="test_table",
            cloudformation_primary_identifier="test_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["test_table"],
            },
            span_name="DynamoDB.CreateTable",
        )

    def test_dynamodb_put_item(self):
        self.do_test_requests(
            "ddb/putitem/putitem-table/key",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::DynamoDB",
            remote_operation="PutItem",
            remote_resource_type="AWS::DynamoDB::Table",
            remote_resource_identifier="put_test_table",
            cloudformation_primary_identifier="put_test_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["put_test_table"],
            },
            span_name="DynamoDB.PutItem",
        )

    def test_dynamodb_error(self):
        self.do_test_requests(
            "ddb/error",
            "GET",
            400,
            1,
            0,
            remote_service="AWS::DynamoDB",
            remote_operation="PutItem",
            remote_resource_type="AWS::DynamoDB::Table",
            remote_resource_identifier="invalid_table",
            cloudformation_primary_identifier="invalid_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["invalid_table"],
            },
            span_name="DynamoDB.PutItem",
        )

    def test_dynamodb_fault(self):
        self.do_test_requests(
            "ddb/fault",
            "GET",
            500,
            0,
            1,
            remote_service="AWS::DynamoDB",
            remote_operation="PutItem",
            remote_resource_type="AWS::DynamoDB::Table",
            remote_resource_identifier="invalid_table",
            cloudformation_primary_identifier="invalid_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["invalid_table"],
            },
            span_name="DynamoDB.PutItem",
        )

    def test_sqs_create_queue(self):
        self.do_test_requests(
            "sqs/createqueue/some-queue",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::SQS",
            remote_operation="CreateQueue",
            remote_resource_type="AWS::SQS::Queue",
            remote_resource_identifier="test_queue",
            cloudformation_primary_identifier=(
                "http://sqs.us-west-2.localhost.localstack.cloud:4566/000000000000/test_queue"
            ),
            request_specific_attributes={
                _AWS_SQS_QUEUE_NAME: "test_queue",
            },
            response_specific_attributes={
                _AWS_SQS_QUEUE_URL: "http://sqs.us-west-2.localhost.localstack.cloud:4566/000000000000/test_queue",
            },
            span_name="SQS.CreateQueue",
        )

    def test_sqs_send_message(self):
        self.do_test_requests(
            "sqs/publishqueue/some-queue",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::SQS",
            remote_operation="SendMessage",
            remote_resource_type="AWS::SQS::Queue",
            remote_resource_identifier="test_put_get_queue",
            cloudformation_primary_identifier="http://localstack:4566/000000000000/test_put_get_queue",
            request_specific_attributes={
                _AWS_SQS_QUEUE_URL: "http://localstack:4566/000000000000/test_put_get_queue",
            },
            span_name="SQS.SendMessage",
        )

    def test_sqs_receive_message(self):
        self.do_test_requests(
            "sqs/consumequeue/some-queue",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::SQS",
            remote_operation="ReceiveMessage",
            remote_resource_type="AWS::SQS::Queue",
            remote_resource_identifier="test_put_get_queue",
            cloudformation_primary_identifier="http://localstack:4566/000000000000/test_put_get_queue",
            request_specific_attributes={
                _AWS_SQS_QUEUE_URL: "http://localstack:4566/000000000000/test_put_get_queue",
            },
            span_name="SQS.ReceiveMessage",
        )

    def test_sqs_error(self):
        self.do_test_requests(
            "sqs/error",
            "GET",
            400,
            1,
            0,
            remote_service="AWS::SQS",
            remote_operation="SendMessage",
            remote_resource_type="AWS::SQS::Queue",
            remote_resource_identifier="sqserror",
            cloudformation_primary_identifier="http://error.test:8080/000000000000/sqserror",
            request_specific_attributes={
                _AWS_SQS_QUEUE_URL: "http://error.test:8080/000000000000/sqserror",
            },
            span_name="SQS.SendMessage",
        )

    def test_sqs_fault(self):
        self.do_test_requests(
            "sqs/fault",
            "GET",
            500,
            0,
            1,
            remote_service="AWS::SQS",
            remote_operation="CreateQueue",
            remote_resource_type="AWS::SQS::Queue",
            remote_resource_identifier="invalid_test",
            cloudformation_primary_identifier="invalid_test",
            request_specific_attributes={
                _AWS_SQS_QUEUE_NAME: "invalid_test",
            },
            span_name="SQS.CreateQueue",
        )

    def test_kinesis_put_record(self):
        self.do_test_requests(
            "kinesis/putrecord/my-stream",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::Kinesis",
            remote_operation="PutRecord",
            remote_resource_type="AWS::Kinesis::Stream",
            remote_resource_identifier="test_stream",
            cloudformation_primary_identifier="test_stream",
            request_specific_attributes={
                _AWS_KINESIS_STREAM_NAME: "test_stream",
            },
            span_name="Kinesis.PutRecord",
        )

    def test_kinesis_error(self):
        self.do_test_requests(
            "kinesis/error",
            "GET",
            400,
            1,
            0,
            remote_service="AWS::Kinesis",
            remote_operation="PutRecord",
            remote_resource_type="AWS::Kinesis::Stream",
            remote_resource_identifier="invalid_stream",
            cloudformation_primary_identifier="invalid_stream",
            request_specific_attributes={
                _AWS_KINESIS_STREAM_NAME: "invalid_stream",
            },
            span_name="Kinesis.PutRecord",
        )

    def test_kinesis_fault(self):
        self.do_test_requests(
            "kinesis/fault",
            "GET",
            500,
            0,
            1,
            remote_service="AWS::Kinesis",
            remote_operation="PutRecord",
            remote_resource_type="AWS::Kinesis::Stream",
            remote_resource_identifier="test_stream",
            cloudformation_primary_identifier="test_stream",
            request_specific_attributes={
                _AWS_KINESIS_STREAM_NAME: "test_stream",
            },
            span_name="Kinesis.PutRecord",
        )

    def test_bedrock_runtime_invoke_model_amazon_titan(self):
        self.do_test_requests(
            "bedrock/invokemodel/invoke-model/amazon.titan-text-premier-v1:0",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Runtime",
            remote_service="AWS::BedrockRuntime",
            remote_operation="InvokeModel",
            remote_resource_type="AWS::Bedrock::Model",
            remote_resource_identifier="amazon.titan-text-premier-v1:0",
            cloudformation_primary_identifier="amazon.titan-text-premier-v1:0",
            request_specific_attributes={
                _GEN_AI_REQUEST_MODEL: "amazon.titan-text-premier-v1:0",
                _GEN_AI_REQUEST_MAX_TOKENS: 3072,
                _GEN_AI_REQUEST_TEMPERATURE: 0.7,
                _GEN_AI_REQUEST_TOP_P: 0.9,
            },
            response_specific_attributes={
                _GEN_AI_RESPONSE_FINISH_REASONS: ["CONTENT_FILTERED"],
                _GEN_AI_USAGE_INPUT_TOKENS: 15,
                _GEN_AI_USAGE_OUTPUT_TOKENS: 13,
            },
            span_name="Bedrock Runtime.InvokeModel",
        )

    def test_bedrock_runtime_invoke_model_anthropic_claude(self):
        self.do_test_requests(
            "bedrock/invokemodel/invoke-model/anthropic.claude-v2:1",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Runtime",
            remote_service="AWS::BedrockRuntime",
            remote_operation="InvokeModel",
            remote_resource_type="AWS::Bedrock::Model",
            remote_resource_identifier="anthropic.claude-v2:1",
            cloudformation_primary_identifier="anthropic.claude-v2:1",
            request_specific_attributes={
                _GEN_AI_REQUEST_MODEL: "anthropic.claude-v2:1",
                _GEN_AI_REQUEST_MAX_TOKENS: 1000,
                _GEN_AI_REQUEST_TEMPERATURE: 0.99,
                _GEN_AI_REQUEST_TOP_P: 1,
            },
            response_specific_attributes={
                _GEN_AI_RESPONSE_FINISH_REASONS: ["end_turn"],
                _GEN_AI_USAGE_INPUT_TOKENS: 15,
                _GEN_AI_USAGE_OUTPUT_TOKENS: 13,
            },
            span_name="Bedrock Runtime.InvokeModel",
        )

    def test_bedrock_runtime_invoke_model_meta_llama(self):
        self.do_test_requests(
            "bedrock/invokemodel/invoke-model/meta.llama2-13b-chat-v1",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Runtime",
            remote_service="AWS::BedrockRuntime",
            remote_operation="InvokeModel",
            remote_resource_type="AWS::Bedrock::Model",
            remote_resource_identifier="meta.llama2-13b-chat-v1",
            cloudformation_primary_identifier="meta.llama2-13b-chat-v1",
            request_specific_attributes={
                _GEN_AI_REQUEST_MODEL: "meta.llama2-13b-chat-v1",
                _GEN_AI_REQUEST_MAX_TOKENS: 512,
                _GEN_AI_REQUEST_TEMPERATURE: 0.5,
                _GEN_AI_REQUEST_TOP_P: 0.9,
            },
            response_specific_attributes={
                _GEN_AI_RESPONSE_FINISH_REASONS: ["stop"],
                _GEN_AI_USAGE_INPUT_TOKENS: 31,
                _GEN_AI_USAGE_OUTPUT_TOKENS: 49,
            },
            span_name="Bedrock Runtime.InvokeModel",
        )

    def test_bedrock_runtime_invoke_model_cohere_command(self):
        self.do_test_requests(
            "bedrock/invokemodel/invoke-model/cohere.command-r-v1:0",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Runtime",
            remote_service="AWS::BedrockRuntime",
            remote_operation="InvokeModel",
            remote_resource_type="AWS::Bedrock::Model",
            remote_resource_identifier="cohere.command-r-v1:0",
            cloudformation_primary_identifier="cohere.command-r-v1:0",
            request_specific_attributes={
                _GEN_AI_REQUEST_MODEL: "cohere.command-r-v1:0",
                _GEN_AI_REQUEST_MAX_TOKENS: 512,
                _GEN_AI_REQUEST_TEMPERATURE: 0.5,
                _GEN_AI_REQUEST_TOP_P: 0.65,
            },
            response_specific_attributes={
                _GEN_AI_RESPONSE_FINISH_REASONS: ["COMPLETE"],
                _GEN_AI_USAGE_INPUT_TOKENS: math.ceil(
                    len("Describe the purpose of a 'hello world' program in one line.") / 6
                ),
                _GEN_AI_USAGE_OUTPUT_TOKENS: math.ceil(len("test-generation-text") / 6),
            },
            span_name="Bedrock Runtime.InvokeModel",
        )

    def test_bedrock_runtime_invoke_model_ai21_jamba(self):
        self.do_test_requests(
            "bedrock/invokemodel/invoke-model/ai21.jamba-1-5-large-v1:0",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Runtime",
            remote_service="AWS::BedrockRuntime",
            remote_operation="InvokeModel",
            remote_resource_type="AWS::Bedrock::Model",
            remote_resource_identifier="ai21.jamba-1-5-large-v1:0",
            cloudformation_primary_identifier="ai21.jamba-1-5-large-v1:0",
            request_specific_attributes={
                _GEN_AI_REQUEST_MODEL: "ai21.jamba-1-5-large-v1:0",
                _GEN_AI_REQUEST_MAX_TOKENS: 512,
                _GEN_AI_REQUEST_TEMPERATURE: 0.6,
                _GEN_AI_REQUEST_TOP_P: 0.8,
            },
            response_specific_attributes={
                _GEN_AI_RESPONSE_FINISH_REASONS: ["stop"],
                _GEN_AI_USAGE_INPUT_TOKENS: 21,
                _GEN_AI_USAGE_OUTPUT_TOKENS: 24,
            },
            span_name="Bedrock Runtime.InvokeModel",
        )

    def test_bedrock_runtime_invoke_model_mistral(self):
        self.do_test_requests(
            "bedrock/invokemodel/invoke-model/mistral.mistral-7b-instruct-v0:2",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Runtime",
            remote_service="AWS::BedrockRuntime",
            remote_operation="InvokeModel",
            remote_resource_type="AWS::Bedrock::Model",
            remote_resource_identifier="mistral.mistral-7b-instruct-v0:2",
            cloudformation_primary_identifier="mistral.mistral-7b-instruct-v0:2",
            request_specific_attributes={
                _GEN_AI_REQUEST_MODEL: "mistral.mistral-7b-instruct-v0:2",
                _GEN_AI_REQUEST_MAX_TOKENS: 4096,
                _GEN_AI_REQUEST_TEMPERATURE: 0.75,
                _GEN_AI_REQUEST_TOP_P: 0.99,
            },
            response_specific_attributes={
                _GEN_AI_RESPONSE_FINISH_REASONS: ["stop"],
                _GEN_AI_USAGE_INPUT_TOKENS: math.ceil(
                    len("Describe the purpose of a 'hello world' program in one line.") / 6
                ),
                _GEN_AI_USAGE_OUTPUT_TOKENS: math.ceil(len("test-output-text") / 6),
            },
            span_name="Bedrock Runtime.InvokeModel",
        )

    def test_bedrock_get_guardrail(self):
        self.do_test_requests(
            "bedrock/getguardrail/get-guardrail",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock",
            remote_service="AWS::Bedrock",
            remote_operation="GetGuardrail",
            remote_resource_type="AWS::Bedrock::Guardrail",
            remote_resource_identifier="bt4o77i015cu",
            cloudformation_primary_identifier="arn:aws:bedrock:us-east-1:000000000000:guardrail/bt4o77i015cu",
            request_specific_attributes={
                _AWS_BEDROCK_GUARDRAIL_ID: "bt4o77i015cu",
            },
            span_name="Bedrock.GetGuardrail",
        )

    def test_bedrock_agent_runtime_invoke_agent(self):
        self.do_test_requests(
            "bedrock/invokeagent/invoke_agent",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Agent Runtime",
            remote_service="AWS::Bedrock",
            remote_operation="InvokeAgent",
            remote_resource_type="AWS::Bedrock::Agent",
            remote_resource_identifier="Q08WFRPHVL",
            cloudformation_primary_identifier="Q08WFRPHVL",
            request_specific_attributes={
                _AWS_BEDROCK_AGENT_ID: "Q08WFRPHVL",
            },
            span_name="Bedrock Agent Runtime.InvokeAgent",
        )

    def test_bedrock_agent_runtime_retrieve(self):
        self.do_test_requests(
            "bedrock/retrieve/retrieve",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Agent Runtime",
            remote_service="AWS::Bedrock",
            remote_operation="Retrieve",
            remote_resource_type="AWS::Bedrock::KnowledgeBase",
            remote_resource_identifier="test-knowledge-base-id",
            cloudformation_primary_identifier="test-knowledge-base-id",
            request_specific_attributes={
                _AWS_BEDROCK_KNOWLEDGE_BASE_ID: "test-knowledge-base-id",
            },
            span_name="Bedrock Agent Runtime.Retrieve",
        )

    def test_bedrock_agent_get_agent(self):
        self.do_test_requests(
            "bedrock/getagent/get-agent",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Agent",
            remote_service="AWS::Bedrock",
            remote_operation="GetAgent",
            remote_resource_type="AWS::Bedrock::Agent",
            remote_resource_identifier="TESTAGENTID",
            cloudformation_primary_identifier="TESTAGENTID",
            request_specific_attributes={
                _AWS_BEDROCK_AGENT_ID: "TESTAGENTID",
            },
            span_name="Bedrock Agent.GetAgent",
        )

    def test_bedrock_agent_get_knowledge_base(self):
        self.do_test_requests(
            "bedrock/getknowledgebase/get_knowledge_base",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Agent",
            remote_service="AWS::Bedrock",
            remote_operation="GetKnowledgeBase",
            remote_resource_type="AWS::Bedrock::KnowledgeBase",
            remote_resource_identifier="invalid-knowledge-base-id",
            cloudformation_primary_identifier="invalid-knowledge-base-id",
            request_specific_attributes={
                _AWS_BEDROCK_KNOWLEDGE_BASE_ID: "invalid-knowledge-base-id",
            },
            span_name="Bedrock Agent.GetKnowledgeBase",
        )

    def test_bedrock_agent_get_data_source(self):
        self.do_test_requests(
            "bedrock/getdatasource/get_data_source",
            "GET",
            200,
            0,
            0,
            rpc_service="Bedrock Agent",
            remote_service="AWS::Bedrock",
            remote_operation="GetDataSource",
            remote_resource_type="AWS::Bedrock::DataSource",
            remote_resource_identifier="DATASURCID",
            cloudformation_primary_identifier="TESTKBSEID|DATASURCID",
            request_specific_attributes={
                _AWS_BEDROCK_DATA_SOURCE_ID: "DATASURCID",
                _AWS_BEDROCK_KNOWLEDGE_BASE_ID: "TESTKBSEID",
            },
            span_name="Bedrock Agent.GetDataSource",
        )

    def test_secretsmanager_describe_secret(self):
        self.do_test_requests(
            "secretsmanager/describesecret/my-secret",
            "GET",
            200,
            0,
            0,
            rpc_service="Secrets Manager",
            remote_service="AWS::SecretsManager",
            remote_operation="DescribeSecret",
            remote_resource_type="AWS::SecretsManager::Secret",
            remote_resource_identifier=r"testSecret-[a-zA-Z0-9]{6}$",
            cloudformation_primary_identifier=(
                r"arn:aws:secretsmanager:us-west-2:000000000000:secret:testSecret-[a-zA-Z0-9]{6}$"
            ),
            response_specific_attributes={
                _AWS_SECRET_ARN: r"arn:aws:secretsmanager:us-west-2:000000000000:secret:testSecret-[a-zA-Z0-9]{6}$",
            },
            span_name="Secrets Manager.DescribeSecret",
        )

    def test_secretsmanager_error(self):
        self.do_test_requests(
            "secretsmanager/error",
            "GET",
            400,
            1,
            0,
            rpc_service="Secrets Manager",
            remote_service="AWS::SecretsManager",
            remote_operation="DescribeSecret",
            remote_resource_type="AWS::SecretsManager::Secret",
            remote_resource_identifier="unExistSecret",
            cloudformation_primary_identifier="arn:aws:secretsmanager:us-west-2:000000000000:secret:unExistSecret",
            request_specific_attributes={
                _AWS_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:000000000000:secret:unExistSecret",
            },
            span_name="Secrets Manager.DescribeSecret",
        )

    def test_secretsmanager_fault(self):
        self.do_test_requests(
            "secretsmanager/fault",
            "GET",
            500,
            0,
            1,
            rpc_service="Secrets Manager",
            remote_service="AWS::SecretsManager",
            remote_operation="GetSecretValue",
            remote_resource_type="AWS::SecretsManager::Secret",
            remote_resource_identifier="nonexistent-secret",
            cloudformation_primary_identifier="arn:aws:secretsmanager:us-west-2:000000000000:secret:nonexistent-secret",
            request_specific_attributes={
                _AWS_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:000000000000:secret:nonexistent-secret",
            },
            span_name="Secrets Manager.GetSecretValue",
        )

    def test_sns_get_topic_attributes(self):
        self.do_test_requests(
            "sns/gettopicattributes/test-topic",
            "GET",
            200,
            0,
            0,
            rpc_service="SNS",
            remote_service="AWS::SNS",
            remote_operation="GetTopicAttributes",
            remote_resource_type="AWS::SNS::Topic",
            remote_resource_identifier="test-topic",
            cloudformation_primary_identifier="arn:aws:sns:us-west-2:000000000000:test-topic",
            request_specific_attributes={_AWS_SNS_TOPIC_ARN: "arn:aws:sns:us-west-2:000000000000:test-topic"},
            span_name="SNS.GetTopicAttributes",
        )

    # TODO: Add error case for sns - our test setup is not setting the http status code properly
    # for this resource

    def test_sns_fault(self):
        self.do_test_requests(
            "sns/fault",
            "GET",
            500,
            0,
            1,
            rpc_service="SNS",
            remote_service="AWS::SNS",
            remote_operation="GetTopicAttributes",
            remote_resource_type="AWS::SNS::Topic",
            remote_resource_identifier="invalid-topic",
            cloudformation_primary_identifier="arn:aws:sns:us-west-2:000000000000:invalid-topic",
            request_specific_attributes={
                _AWS_SNS_TOPIC_ARN: "arn:aws:sns:us-west-2:000000000000:invalid-topic",
            },
            span_name="SNS.GetTopicAttributes",
        )

    def test_stepfunctions_describe_state_machine(self):
        self.do_test_requests(
            "stepfunctions/describestatemachine/my-state-machine",
            "GET",
            200,
            0,
            0,
            rpc_service="SFN",
            remote_service="AWS::StepFunctions",
            remote_operation="DescribeStateMachine",
            remote_resource_type="AWS::StepFunctions::StateMachine",
            remote_resource_identifier="testStateMachine",
            cloudformation_primary_identifier="arn:aws:states:us-west-2:000000000000:stateMachine:testStateMachine",
            request_specific_attributes={
                _AWS_STATE_MACHINE_ARN: "arn:aws:states:us-west-2:000000000000:stateMachine:testStateMachine",
            },
            span_name="SFN.DescribeStateMachine",
        )

    def test_stepfunctions_describe_activity(self):
        self.do_test_requests(
            "stepfunctions/describeactivity/my-activity",
            "GET",
            200,
            0,
            0,
            rpc_service="SFN",
            remote_service="AWS::StepFunctions",
            remote_operation="DescribeActivity",
            remote_resource_type="AWS::StepFunctions::Activity",
            remote_resource_identifier="testActivity",
            cloudformation_primary_identifier="arn:aws:states:us-west-2:000000000000:activity:testActivity",
            request_specific_attributes={
                _AWS_ACTIVITY_ARN: "arn:aws:states:us-west-2:000000000000:activity:testActivity"
            },
            span_name="SFN.DescribeActivity",
        )

    def test_stepfunctions_error(self):
        self.do_test_requests(
            "stepfunctions/error",
            "GET",
            400,
            1,
            0,
            rpc_service="SFN",
            remote_service="AWS::StepFunctions",
            remote_operation="DescribeStateMachine",
            remote_resource_type="AWS::StepFunctions::StateMachine",
            remote_resource_identifier="unExistStateMachine",
            cloudformation_primary_identifier="arn:aws:states:us-west-2:000000000000:stateMachine:unExistStateMachine",
            request_specific_attributes={
                _AWS_STATE_MACHINE_ARN: "arn:aws:states:us-west-2:000000000000:stateMachine:unExistStateMachine",
            },
            span_name="SFN.DescribeStateMachine",
        )

    def test_stepfunctions_fault(self):
        self.do_test_requests(
            "stepfunctions/fault",
            "GET",
            500,
            0,
            1,
            rpc_service="SFN",
            remote_service="AWS::StepFunctions",
            remote_operation="ListStateMachineVersions",
            remote_resource_type="AWS::StepFunctions::StateMachine",
            remote_resource_identifier="invalid-state-machine",
            cloudformation_primary_identifier=(
                "arn:aws:states:us-west-2:000000000000:stateMachine:invalid-state-machine"
            ),
            request_specific_attributes={
                _AWS_STATE_MACHINE_ARN: "arn:aws:states:us-west-2:000000000000:stateMachine:invalid-state-machine",
            },
            span_name="SFN.ListStateMachineVersions",
        )

    # TODO: Add contract test for lambda event source mapping resource

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self._assert_aws_attributes(
            target_spans[0].attributes,
            kwargs.get("remote_service"),
            kwargs.get("remote_operation"),
            "LOCAL_ROOT",
            kwargs.get("remote_resource_type", "None"),
            kwargs.get("remote_resource_identifier", "None"),
            kwargs.get("cloudformation_primary_identifier", "None"),
        )

    def _assert_aws_attributes(
        self,
        attributes_list: List[KeyValue],
        service: str,
        operation: str,
        span_kind: str,
        remote_resource_type: str,
        remote_resource_identifier: str,
        cloudformation_primary_identifier: str,
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # InternalOperation as OTEL does not instrument the basic server we are using, so the client span is a local
        # root.
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_SERVICE, service)
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_OPERATION, operation)
        if remote_resource_type != "None":
            self._assert_str_attribute(attributes_dict, AWS_REMOTE_RESOURCE_TYPE, remote_resource_type)
        if remote_resource_identifier != "None":
            if self._is_valid_regex(remote_resource_identifier):
                self._assert_match_attribute(
                    attributes_dict, AWS_REMOTE_RESOURCE_IDENTIFIER, remote_resource_identifier
                )
            else:
                self._assert_str_attribute(attributes_dict, AWS_REMOTE_RESOURCE_IDENTIFIER, remote_resource_identifier)
        if cloudformation_primary_identifier != "None":
            if self._is_valid_regex(remote_resource_identifier):
                self._assert_match_attribute(
                    attributes_dict, AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER, cloudformation_primary_identifier
                )
            else:
                self._assert_str_attribute(
                    attributes_dict, AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER, cloudformation_primary_identifier
                )
        # See comment above AWS_LOCAL_OPERATION
        self._assert_str_attribute(attributes_dict, AWS_SPAN_KIND, span_kind)

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind == Span.SPAN_KIND_CLIENT:
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self.assertEqual(target_spans[0].name, kwargs.get("span_name"))
        self._assert_semantic_conventions_attributes(
            target_spans[0].attributes,
            kwargs.get("rpc_service") if "rpc_service" in kwargs else kwargs.get("remote_service").split("::")[-1],
            kwargs.get("remote_operation"),
            status_code,
            kwargs.get("request_specific_attributes", {}),
            kwargs.get("response_specific_attributes", {}),
        )

    # pylint: disable=unidiomatic-typecheck
    def _assert_semantic_conventions_attributes(
        self,
        attributes_list: List[KeyValue],
        service: str,
        operation: str,
        status_code: int,
        request_specific_attributes: dict,
        response_specific_attributes: dict,
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_METHOD, operation)
        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_SYSTEM, "aws-api")
        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_SERVICE, service.split("::")[-1])
        self._assert_int_attribute(attributes_dict, SpanAttributes.HTTP_STATUS_CODE, status_code)
        # TODO: botocore instrumentation is not respecting PEER_SERVICE
        # self._assert_str_attribute(attributes_dict, SpanAttributes.PEER_SERVICE, "backend:8080")
        for key, value in request_specific_attributes.items():
            self._assert_attribute(attributes_dict, key, value)

        for key, value in response_specific_attributes.items():
            self._assert_attribute(attributes_dict, key, value)

    def _assert_attribute(self, attributes_dict: Dict[str, AnyValue], key, value) -> None:
        if isinstance(value, str):
            if self._is_valid_regex(value):
                self._assert_match_attribute(attributes_dict, key, value)
            else:
                self._assert_str_attribute(attributes_dict, key, value)
        elif isinstance(value, int):
            self._assert_int_attribute(attributes_dict, key, value)
        elif isinstance(value, float):
            self._assert_float_attribute(attributes_dict, key, value)
        else:
            self._assert_array_value_ddb_table_name(attributes_dict, key, value)

    @override
    def _assert_metric_attributes(
        self,
        resource_scope_metrics: List[ResourceScopeMetric],
        metric_name: str,
        expected_sum: int,
        **kwargs,
    ) -> None:
        target_metrics: List[Metric] = []
        for resource_scope_metric in resource_scope_metrics:
            if resource_scope_metric.metric.name.lower() == metric_name.lower():
                target_metrics.append(resource_scope_metric.metric)

        self.assertEqual(len(target_metrics), 1)
        target_metric: Metric = target_metrics[0]
        dp_list: List[ExponentialHistogramDataPoint] = target_metric.exponential_histogram.data_points
        dp_list_count: int = kwargs.get("dp_count", 2)
        self.assertEqual(len(dp_list), dp_list_count)
        dependency_dp: ExponentialHistogramDataPoint = dp_list[0]
        service_dp: ExponentialHistogramDataPoint = dp_list[1]
        if len(dp_list[1].attributes) > len(dp_list[0].attributes):
            dependency_dp = dp_list[1]
            service_dp = dp_list[0]
        attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(dependency_dp.attributes)
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # See comment on AWS_LOCAL_OPERATION in _assert_aws_attributes
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_SERVICE, kwargs.get("remote_service"))
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_OPERATION, kwargs.get("remote_operation"))
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "CLIENT")
        remote_resource_type = kwargs.get("remote_resource_type", "None")
        remote_resource_identifier = kwargs.get("remote_resource_identifier", "None")
        if remote_resource_type != "None":
            self._assert_str_attribute(attribute_dict, AWS_REMOTE_RESOURCE_TYPE, remote_resource_type)
        if remote_resource_identifier != "None":
            if self._is_valid_regex(remote_resource_identifier):
                self._assert_match_attribute(attribute_dict, AWS_REMOTE_RESOURCE_IDENTIFIER, remote_resource_identifier)
            else:
                self._assert_str_attribute(attribute_dict, AWS_REMOTE_RESOURCE_IDENTIFIER, remote_resource_identifier)
        self.check_sum(metric_name, dependency_dp.sum, expected_sum)

        attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(service_dp.attributes)
        # See comment on AWS_LOCAL_OPERATION in _assert_aws_attributes
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "LOCAL_ROOT")
        self.check_sum(metric_name, service_dp.sum, expected_sum)

    # pylint: disable=consider-using-enumerate
    def _assert_array_value_ddb_table_name(self, attributes_dict: Dict[str, AnyValue], key: str, expect_values: list):
        self.assertIn(key, attributes_dict)
        actual_values: [AnyValue] = attributes_dict[key].array_value
        self.assertEqual(len(actual_values.values), len(expect_values))
        for index in range(len(actual_values.values)):
            self.assertEqual(actual_values.values[index].string_value, expect_values[index])
