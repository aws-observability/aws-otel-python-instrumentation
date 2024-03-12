# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import Dict, List

from docker.types import EndpointConfig
from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from testcontainers.localstack import LocalStackContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME, ContractTestBase
from amazon.utils.app_signals_constants import (
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_REMOTE_TARGET,
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
        return "aws-appsignals-tests-botocore-app"

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
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "s3/createbucket/create-bucket",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.S3",
            operation="CreateBucket",
            remote_target="::s3:::test-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-bucket-name",
            },
        )

    def test_s3_create_object(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "s3/createobject/put-object/some-object",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.S3",
            operation="PutObject",
            remote_target="::s3:::test-put-object-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-put-object-bucket-name",
            },
        )

    def test_s3_get_object(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "s3/getobject/get-object/some-object",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.S3",
            operation="GetObject",
            remote_target="::s3:::test-get-object-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "test-get-object-bucket-name",
            },
        )

    def test_s3_error(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "s3/error",
            "GET",
            400,
            1,
            0,
            service="AWS.SDK.S3",
            operation="CreateBucket",
            remote_target="::s3:::-",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "-",
            },
        )

    def test_s3_fault(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "s3/fault",
            "GET",
            500,
            0,
            1,
            service="AWS.SDK.S3",
            operation="CreateBucket",
            remote_target="::s3:::valid-bucket-name",
            request_specific_attributes={
                SpanAttributes.AWS_S3_BUCKET: "valid-bucket-name",
            },
        )

    def test_dynamodb_create_table(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "ddb/createtable/some-table",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.DynamoDB",
            operation="CreateTable",
            remote_target="::dynamodb:::table/test_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["test_table"],
            },
        )

    def test_dynamodb_put_item(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "ddb/putitem/putitem-table/key",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.DynamoDB",
            operation="PutItem",
            remote_target="::dynamodb:::table/put_test_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["put_test_table"],
            },
        )

    def test_dynamodb_error(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "ddb/error",
            "GET",
            400,
            1,
            0,
            service="AWS.SDK.DynamoDB",
            operation="PutItem",
            remote_target="::dynamodb:::table/invalid_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["invalid_table"],
            },
        )

    def test_dynamodb_fault(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "ddb/fault",
            "GET",
            500,
            0,
            1,
            service="AWS.SDK.DynamoDB",
            operation="PutItem",
            remote_target="::dynamodb:::table/invalid_table",
            request_specific_attributes={
                SpanAttributes.AWS_DYNAMODB_TABLE_NAMES: ["invalid_table"],
            },
        )

    def test_sqs_create_queue(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "sqs/createqueue/some-queue",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.SQS",
            operation="CreateQueue",
            remote_target="::sqs:::test_queue",
            request_specific_attributes={
                _AWS_QUEUE_NAME: "test_queue",
            },
        )

    def test_sqs_send_message(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "sqs/publishqueue/some-queue",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.SQS",
            operation="SendMessage",
            remote_target="::sqs::000000000000:test_put_get_queue",
            request_specific_attributes={
                _AWS_QUEUE_URL: "http://localstack:4566/000000000000/test_put_get_queue",
            },
        )

    def test_sqs_receive_message(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "sqs/consumequeue/some-queue",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.SQS",
            operation="ReceiveMessage",
            remote_target="::sqs::000000000000:test_put_get_queue",
            request_specific_attributes={
                _AWS_QUEUE_URL: "http://localstack:4566/000000000000/test_put_get_queue",
            },
        )

    def test_sqs_error(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "sqs/error",
            "GET",
            400,
            1,
            0,
            service="AWS.SDK.SQS",
            operation="SendMessage",
            request_specific_attributes={
                _AWS_QUEUE_URL: "http://error.test:8080/sqserror",
            },
        )

    def test_sqs_fault(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "sqs/fault",
            "GET",
            500,
            0,
            1,
            service="AWS.SDK.SQS",
            operation="CreateQueue",
            remote_target="::sqs:::invalid_test",
            request_specific_attributes={
                _AWS_QUEUE_NAME: "invalid_test",
            },
        )

    def test_kinesis_put_record(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "kinesis/putrecord/my-stream",
            "GET",
            200,
            0,
            0,
            service="AWS.SDK.Kinesis",
            operation="PutRecord",
            remote_target="::kinesis:::stream/test_stream",
            request_specific_attributes={
                _AWS_STREAM_NAME: "test_stream",
            },
        )

    def test_kinesis_error(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "kinesis/error",
            "GET",
            400,
            1,
            0,
            service="AWS.SDK.Kinesis",
            operation="PutRecord",
            remote_target="::kinesis:::stream/invalid_stream",
            request_specific_attributes={
                _AWS_STREAM_NAME: "invalid_stream",
            },
        )

    def test_kinesis_fault(self):
        self.mock_collector_client.clear_signals()
        self.do_test_requests(
            "kinesis/fault",
            "GET",
            500,
            0,
            1,
            service="AWS.SDK.Kinesis",
            operation="PutRecord",
            remote_target="::kinesis:::stream/test_stream",
            request_specific_attributes={
                _AWS_STREAM_NAME: "test_stream",
            },
        )

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
            kwargs.get("service"),
            kwargs.get("operation"),
            "LOCAL_ROOT",
            kwargs.get("remote_target", "None"),
        )

    def _assert_aws_attributes(
        self, attributes_list: List[KeyValue], service: str, operation: str, span_kind: str, remote_target: str
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # InternalOperation as OTEL does not instrument the basic server we are using, so the client span is a local
        # root.
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_SERVICE, service)
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_OPERATION, operation)
        if remote_target != "None":
            self._assert_str_attribute(attributes_dict, AWS_REMOTE_TARGET, remote_target)
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
        self.assertEqual(target_spans[0].name, kwargs.get("service").split(".")[-1] + "." + kwargs.get("operation"))
        self._assert_semantic_conventions_attributes(
            target_spans[0].attributes,
            kwargs.get("service"),
            kwargs.get("operation"),
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
        self._assert_str_attribute(attributes_dict, SpanAttributes.RPC_SERVICE, service.split(".")[-1])
        self._assert_int_attribute(attributes_dict, SpanAttributes.HTTP_STATUS_CODE, status_code)
        # TODO: botocore instrumentation is not respecting PEER_SERVICE
        # self._assert_str_attribute(attributes_dict, SpanAttributes.PEER_SERVICE, "backend:8080")
        print(attributes_dict)
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
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_SERVICE, kwargs.get("service"))
        self._assert_str_attribute(attribute_dict, AWS_REMOTE_OPERATION, kwargs.get("operation"))
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "CLIENT")
        self.check_sum(metric_name, dependency_dp.sum, expected_sum)

        attribute_dict: Dict[str, AnyValue] = self._get_attributes_dict(service_dp.attributes)
        # See comment on AWS_LOCAL_OPERATION in _assert_aws_attributes
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        self._assert_str_attribute(attribute_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attribute_dict, AWS_SPAN_KIND, "LOCAL_ROOT")
        self.check_sum(metric_name, service_dp.sum, expected_sum)

    # pylint: disable=consider-using-enumerate
    def _assert_array_value_ddb_table_name(self, attributes_dict: Dict[str, AnyValue], key: str, values: list):
        self.assertIn(key, attributes_dict)
        actual_values: [AnyValue] = attributes_dict[key].array_value
        for index in range(len(actual_values.values)):
            self.assertEqual(actual_values.values[index].string_value, values[index])
