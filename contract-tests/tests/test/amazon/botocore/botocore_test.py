# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import Dict, List

from docker.types import EndpointConfig
from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from testcontainers.localstack import LocalStackContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME, ContractTestBase
from amazon.utils.application_signals_constants import (
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

_AWS_QUEUE_URL: str = "aws.sqs.queue_url"
_AWS_QUEUE_NAME: str = "aws.sqs.queue_name"
_AWS_STREAM_NAME: str = "aws.kinesis.stream_name"
_AWS_TOPIC_ARN: str = "aws.sns.topic_arn"


# pylint: disable=too-many-public-methods
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
    def get_application_image_name(self) -> str:
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
            LocalStackContainer(image="localstack/localstack:2.0.1")
            .with_name("localstack")
            .with_services("s3", "sqs", "dynamodb", "kinesis")
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
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-bucket-name",
            },
            span_name="S3.CreateBucket",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-put-object-bucket-name",
            },
            span_name="S3.PutObject",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-get-object-bucket-name",
            },
            span_name="S3.GetObject",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "-",
            },
            span_name="S3.CreateBucket",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "valid-bucket-name",
            },
            span_name="S3.CreateBucket",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["test_table"],
            },
            span_name="DynamoDB.CreateTable",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["put_test_table"],
            },
            span_name="DynamoDB.PutItem",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["invalid_table"],
            },
            span_name="DynamoDB.PutItem",
            span_kind="CLIENT",
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
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["invalid_table"],
            },
            span_name="DynamoDB.PutItem",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_QUEUE_NAME: "test_queue",
            },
            span_name="SQS.CreateQueue",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_QUEUE_URL: "http://localstack:4566/000000000000/test_put_get_queue",
            },
            span_name="SQS.SendMessage",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_QUEUE_URL: "http://localstack:4566/000000000000/test_put_get_queue",
            },
            span_name="SQS.ReceiveMessage",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_QUEUE_URL: "http://error.test:8080/000000000000/sqserror",
            },
            span_name="SQS.SendMessage",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_QUEUE_NAME: "invalid_test",
            },
            span_name="SQS.CreateQueue",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_STREAM_NAME: "test_stream",
            },
            span_name="Kinesis.PutRecord",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_STREAM_NAME: "invalid_stream",
            },
            span_name="Kinesis.PutRecord",
            span_kind="CLIENT",
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
            request_specific_attributes={
                _AWS_STREAM_NAME: "test_stream",
            },
            span_name="Kinesis.PutRecord",
            span_kind="CLIENT",
        )

    def test_sns_get_topic_attributes(self):
        self.do_test_requests(
            "sns/gettopattributes/get-topic-attributes",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::SNS",
            remote_operation="GetTopicAttributes",
            remote_resource_type="AWS::SNS::Topic",
            remote_resource_identifier="arn:aws:sns:us-west-2:000000000000:test_topic",
            request_specific_attributes={
                _AWS_TOPIC_ARN: "arn:aws:sns:us-west-2:000000000000:test_topic",
            },
            span_name="SNS.GetTopicAttributes",
            span_kind="CLIENT",
        )

    def test_sns_publish_message(self):
        self.do_test_requests(
            "sns/publishmessage/publish-message/some-message",
            "GET",
            200,
            0,
            0,
            remote_service="AWS::SNS",
            remote_operation="Publish",
            remote_resource_type="AWS::SNS::Topic",
            remote_resource_identifier="arn:aws:sns:us-west-2:000000000000:test_topic",
            request_specific_attributes={
                _AWS_TOPIC_ARN: "arn:aws:sns:us-west-2:000000000000:test_topic",
            },
            span_name="test_topic send",
            span_kind="PRODUCER",
        )

    def test_sns_error(self):
        self.do_test_requests(
            "sns/error",
            "GET",
            400,
            1,
            0,
            remote_service="AWS::SNS",
            remote_operation="Publish",
            remote_resource_type="AWS::SNS::Topic",
            remote_resource_identifier="arn:aws:sns:us-west-2:000000000000:test_topic/snserror",
            request_specific_attributes={
                _AWS_TOPIC_ARN: "arn:aws:sns:us-west-2:000000000000:test_topic/snserror",
            },
            span_name="test_topic/snserror send",
            span_kind="PRODUCER",
        )

    def test_sns_fault(self):
        self.do_test_requests(
            "sns/fault",
            "GET",
            500,
            0,
            1,
            remote_service="AWS::SNS",
            remote_operation="GetTopicAttributes",
            remote_resource_type="AWS::SNS::Topic",
            remote_resource_identifier="invalid_topic_arn",
            request_specific_attributes={
                _AWS_TOPIC_ARN: "invalid_topic_arn",
            },
            span_name="SNS.GetTopicAttributes",
            span_kind="CLIENT",
        )

    @override
    def _assert_aws_span_attributes(self, resource_scope_spans: List[ResourceScopeSpan], path: str, **kwargs) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind in (Span.SPAN_KIND_CLIENT, Span.SPAN_KIND_PRODUCER):
                self.assertEqual(
                    resource_scope_span.span.kind, Span.SpanKind.Value("SPAN_KIND_" + kwargs.get("span_kind"))
                )
                target_spans.append(resource_scope_span.span)
        self.assertEqual(len(target_spans), 1)
        self._assert_aws_attributes(
            target_spans[0].attributes,
            kwargs.get("remote_service"),
            kwargs.get("remote_operation"),
            "LOCAL_ROOT",
            kwargs.get("remote_resource_type", "None"),
            kwargs.get("remote_resource_identifier", "None"),
        )

    def _assert_aws_attributes(
        self,
        attributes_list: List[KeyValue],
        service: str,
        operation: str,
        span_kind: str,
        remote_resource_type: str,
        remote_resource_identifier: str,
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
            self._assert_str_attribute(attributes_dict, AWS_REMOTE_RESOURCE_IDENTIFIER, remote_resource_identifier)
        # See comment above AWS_LOCAL_OPERATION
        self._assert_str_attribute(attributes_dict, AWS_SPAN_KIND, span_kind)

    @override
    def _assert_semantic_conventions_span_attributes(
        self, resource_scope_spans: List[ResourceScopeSpan], method: str, path: str, status_code: int, **kwargs
    ) -> None:
        target_spans: List[Span] = []
        for resource_scope_span in resource_scope_spans:
            # pylint: disable=no-member
            if resource_scope_span.span.kind in (Span.SPAN_KIND_CLIENT, Span.SPAN_KIND_PRODUCER):
                self.assertEqual(
                    resource_scope_span.span.kind, Span.SpanKind.Value("SPAN_KIND_" + kwargs.get("span_kind"))
                )
                target_spans.append(resource_scope_span.span)

        self.assertEqual(len(target_spans), 1)
        self.assertEqual(target_spans[0].name, kwargs.get("span_name"))
        self._assert_semantic_conventions_attributes(
            target_spans[0].attributes,
            kwargs.get("remote_service"),
            kwargs.get("remote_operation"),
            status_code,
            kwargs.get("request_specific_attributes", {}),
        )

    # pylint: disable=unidiomatic-typecheck
    def _assert_semantic_conventions_attributes(
        self,
        attributes_list: List[KeyValue],
        service: str,
        operation: str,
        status_code: int,
        request_specific_attributes: dict,
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)

        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_METHOD, operation)
        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_SYSTEM, "aws-api")
        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_SERVICE, service.split("::")[-1])
        self._assert_int_attribute(attributes_dict, SpanAttributes.HTTP_STATUS_CODE, status_code)
        # TODO: botocore instrumentation is not respecting PEER_SERVICE
        # self._assert_str_attribute(attributes_dict, SpanAttributes.PEER_SERVICE, "backend:8080")
        for key, value in request_specific_attributes.items():
            if isinstance(value, str):
                self._assert_str_attribute(attributes_dict, key, value)
            elif isinstance(value, int):
                self._assert_int_attribute(attributes_dict, key, value)
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
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, kwargs.get("span_kind"))
        remote_resource_type = kwargs.get("remote_resource_type", "None")
        remote_resource_identifier = kwargs.get("remote_resource_identifier", "None")
        if remote_resource_type != "None":
            self._assert_str_attribute(attribute_dict, AWS_REMOTE_RESOURCE_TYPE, remote_resource_type)
        if remote_resource_identifier != "None":
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
