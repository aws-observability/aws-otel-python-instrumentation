# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=too-many-lines

import os
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro._aws_attribute_keys import (
    AWS_AUTH_ACCESS_KEY,
    AWS_AUTH_CREDENTIAL_PROVIDER,
    AWS_AUTH_REGION,
    AWS_BEDROCK_AGENT_ID,
    AWS_BEDROCK_AGENTCORE_BROWSER_ARN,
    AWS_BEDROCK_AGENTCORE_CODE_INTERPRETER_ARN,
    AWS_BEDROCK_AGENTCORE_GATEWAY_ARN,
    AWS_BEDROCK_AGENTCORE_MEMORY_ARN,
    AWS_BEDROCK_AGENTCORE_RUNTIME_ARN,
    AWS_BEDROCK_AGENTCORE_RUNTIME_ENDPOINT_ARN,
    AWS_BEDROCK_DATA_SOURCE_ID,
    AWS_BEDROCK_GUARDRAIL_ARN,
    AWS_BEDROCK_GUARDRAIL_ID,
    AWS_BEDROCK_KNOWLEDGE_BASE_ID,
    AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER,
    AWS_CONSUMER_PARENT_SPAN_KIND,
    AWS_DYNAMODB_TABLE_ARN,
    AWS_GATEWAY_TARGET_ID,
    AWS_KINESIS_STREAM_ARN,
    AWS_KINESIS_STREAM_NAME,
    AWS_LAMBDA_FUNCTION_ARN,
    AWS_LAMBDA_FUNCTION_NAME,
    AWS_LAMBDA_RESOURCEMAPPING_ID,
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_DB_USER,
    AWS_REMOTE_ENVIRONMENT,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_RESOURCE_ACCESS_KEY,
    AWS_REMOTE_RESOURCE_ACCOUNT_ID,
    AWS_REMOTE_RESOURCE_IDENTIFIER,
    AWS_REMOTE_RESOURCE_REGION,
    AWS_REMOTE_RESOURCE_TYPE,
    AWS_REMOTE_SERVICE,
    AWS_SECRETSMANAGER_SECRET_ARN,
    AWS_SNS_TOPIC_ARN,
    AWS_SPAN_KIND,
    AWS_SQS_QUEUE_NAME,
    AWS_SQS_QUEUE_URL,
    AWS_STEPFUNCTIONS_ACTIVITY_ARN,
    AWS_STEPFUNCTIONS_STATEMACHINE_ARN,
)
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro.metric_attribute_generator import DEPENDENCY_METRIC, SERVICE_METRIC
from amazon.opentelemetry.distro.patches.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_BROWSER_ID,
    GEN_AI_CODE_INTERPRETER_ID,
    GEN_AI_GATEWAY_ID,
    GEN_AI_MEMORY_ID,
    GEN_AI_RUNTIME_ID,
)
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import _DEFAULT_RESOURCE, SERVICE_NAME
from opentelemetry.sdk.trace import ReadableSpan, Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import GEN_AI_REQUEST_MODEL
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.util.types import Attributes

# Protected constants with uppercase naming and type annotations

_AWS_LOCAL_OPERATION_VALUE: str = "AWS local operation"
_AWS_REMOTE_SERVICE_VALUE: str = "AWS remote service"
_AWS_REMOTE_OPERATION_VALUE: str = "AWS remote operation"
_SERVICE_NAME_VALUE: str = "Service name"
_SPAN_NAME_VALUE: str = "Span name"
_AWS_REMOTE_RESOURCE_REGION: str = "us-east-1"
_AWS_REMOTE_RESOURCE_ACCESS_KEY: str = "AWS access key"

_UNKNOWN_SERVICE: str = "UnknownService"
_UNKNOWN_OPERATION: str = "UnknownOperation"
_UNKNOWN_REMOTE_SERVICE: str = "UnknownRemoteService"
_UNKNOWN_REMOTE_OPERATION: str = "UnknownRemoteOperation"

_INTERNAL_OPERATION: str = "InternalOperation"
_LOCAL_ROOT: str = "LOCAL_ROOT"

_GENERATOR: _AwsMetricAttributeGenerator = _AwsMetricAttributeGenerator()


class TestUtil(TestCase):
    def setUp(self):
        self.attributes_mock: Attributes = MagicMock()
        self.instrumentation_scope_info_mock: InstrumentationScope = MagicMock()
        self.instrumentation_scope_info_mock.name = "Scope name"
        self.span_mock: ReadableSpan = MagicMock()
        self.span_mock.name = None
        self.span_mock.attributes = self.attributes_mock
        self.attributes_mock.get.return_value = None
        self.span_mock.instrumentation_scope = self.instrumentation_scope_info_mock
        self.span_mock.get_span_context.return_value = MagicMock()
        self.parent_span_context: SpanContext = MagicMock()
        self.parent_span_context.is_valid = True
        self.parent_span_context.is_remote = False
        self.span_mock.parent = self.parent_span_context

        # OTel strongly recommends to start out with the default instead of Resource.empty()
        self.resource: Resource = _DEFAULT_RESOURCE

    def _mock_attribute(
        self,
        keys: List[str],
        values: Optional[List[str]],
        exist_keys: Optional[List[str]] = None,
        exist_values: Optional[List[str]] = None,
    ) -> (Optional[List[str]], Optional[List[str]]):
        if exist_keys is not None and exist_values is not None:
            for key in exist_keys:
                if key not in keys:
                    keys = keys + [key]
                    values = values + [exist_values[exist_keys.index(key)]]

        def get_side_effect(get_key):
            if get_key in keys:
                return values[keys.index(get_key)]
            return None

        self.attributes_mock.get.side_effect = get_side_effect

        return keys, values

    def _validate_expected_remote_attributes(
        self, expected_remote_service: str, expected_remote_operation: str
    ) -> None:
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes[AWS_REMOTE_SERVICE], expected_remote_service)
        self.assertEqual(actual_attributes[AWS_REMOTE_OPERATION], expected_remote_operation)

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes[AWS_REMOTE_SERVICE], expected_remote_service)
        self.assertEqual(actual_attributes[AWS_REMOTE_OPERATION], expected_remote_operation)

    def _validate_and_remove_remote_attributes(
        self,
        remote_service_key: str,
        remote_service_value: str,
        remote_operation_key: str,
        remote_operation_value: str,
        keys: Optional[List[str]],
        values: Optional[List[str]],
    ):
        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [remote_service_value, remote_operation_value], keys, values
        )
        self._validate_expected_remote_attributes(remote_service_value, remote_operation_value)

        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [None, remote_operation_value], keys, values
        )
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, remote_operation_value)

        keys, values = self._mock_attribute(
            [remote_service_key, remote_operation_key], [remote_service_value, None], keys, values
        )
        self._validate_expected_remote_attributes(remote_service_value, _UNKNOWN_REMOTE_OPERATION)

        keys, values = self._mock_attribute([remote_service_key, remote_operation_key], [None, None], keys, values)
        return keys, values

    def _validate_remote_resource_attributes(
        self,
        expected_type: str,
        expected_identifier: str,
        expected_cfn_primary_id: str = None,
        expected_region: str = None,
        expected_account_id: str = None,
        expected_access_key: str = None,
    ) -> None:
        # If expected_cfn_primary_id is not provided, it defaults to expected_identifier
        if expected_cfn_primary_id is None:
            expected_cfn_primary_id = expected_identifier

        # Client, Producer, and Consumer spans should generate the expected remote resource attribute
        for kind in [SpanKind.CLIENT, SpanKind.PRODUCER, SpanKind.CONSUMER]:
            self.span_mock.kind = kind
            actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
                DEPENDENCY_METRIC
            )
            self.assertEqual(expected_type, actual_attributes.get(AWS_REMOTE_RESOURCE_TYPE))
            self.assertEqual(expected_identifier, actual_attributes.get(AWS_REMOTE_RESOURCE_IDENTIFIER))
            self.assertEqual(expected_cfn_primary_id, actual_attributes.get(AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER))

            # Cross account support
            if expected_region is not None:
                self.assertEqual(expected_region, actual_attributes.get(AWS_REMOTE_RESOURCE_REGION))
            else:
                self.assertNotIn(AWS_REMOTE_RESOURCE_REGION, actual_attributes)

            if expected_access_key is not None:
                self.assertEqual(expected_access_key, actual_attributes.get(AWS_REMOTE_RESOURCE_ACCESS_KEY))
                self.assertNotIn(AWS_REMOTE_RESOURCE_ACCOUNT_ID, actual_attributes)
            else:
                self.assertNotIn(AWS_REMOTE_RESOURCE_ACCESS_KEY, actual_attributes)

            if expected_account_id is not None:
                self.assertEqual(expected_account_id, actual_attributes.get(AWS_REMOTE_RESOURCE_ACCOUNT_ID))
                self.assertNotIn(AWS_REMOTE_RESOURCE_ACCESS_KEY, actual_attributes)
            else:
                self.assertNotIn(AWS_REMOTE_RESOURCE_ACCOUNT_ID, actual_attributes)

        # Server span should not generate remote resource attribute
        self.span_mock.kind = SpanKind.SERVER
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            SERVICE_METRIC
        )
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, actual_attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, actual_attributes)
        self.assertNotIn(AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER, actual_attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_REGION, actual_attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_ACCOUNT_ID, actual_attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_ACCESS_KEY, actual_attributes)

        self._mock_attribute([SpanAttributes.DB_SYSTEM], [None])

    def _validate_attributes_produced_for_non_local_root_span_of_kind(
        self, expected_attributes: Attributes, kind: SpanKind
    ) -> None:
        self.span_mock.kind = kind

        attribute_map: {str, BoundedAttributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        service_attributes: BoundedAttributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: BoundedAttributes = attribute_map.get(DEPENDENCY_METRIC)
        if attribute_map is not None and len(attribute_map) > 0:
            if kind in [SpanKind.PRODUCER, SpanKind.CLIENT, SpanKind.CONSUMER]:
                self.assertIsNone(service_attributes)
                self.assertEqual(len(dependency_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(dependency_attributes, BoundedAttributes(attributes=expected_attributes))
            else:
                self.assertIsNone(dependency_attributes)
                self.assertEqual(len(service_attributes), len(BoundedAttributes(attributes=expected_attributes)))
                self.assertEqual(service_attributes, BoundedAttributes(attributes=expected_attributes))

    def validate_bedrock_agentcore_resource(
        self, attribute_keys, attribute_values, expected_type, expected_identifier, expected_cfn_primary_identifier
    ):
        keys = [SpanAttributes.RPC_SYSTEM, SpanAttributes.RPC_SERVICE]
        values = ["aws-api", "Bedrock AgentCore"]

        self._mock_attribute(keys, values)
        self.span_mock.kind = SpanKind.CLIENT

        self._mock_attribute(attribute_keys, attribute_values, keys, values)
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes.get(AWS_REMOTE_RESOURCE_TYPE), expected_type)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_RESOURCE_IDENTIFIER), expected_identifier)
        self.assertEqual(actual_attributes.get(AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER), expected_cfn_primary_identifier)
        self._mock_attribute(attribute_keys, [None] * len(attribute_keys))


# pylint: disable=too-many-public-methods
class TestAwsMetricAttributeGenerator(TestUtil):
    def test_span_attributes_for_empty_resource(self):
        self.resource = Resource.get_empty()
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_consumer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_producer_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_without_attributes(self):
        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: _UNKNOWN_SERVICE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)

    def test_internal_span(self):
        # Spans with internal span kind should not produce any attributes.
        self._validate_attributes_produced_for_non_local_root_span_of_kind({}, SpanKind.INTERNAL)

    def test_local_root_server_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE,
            }
        }

        self.span_mock.kind = SpanKind.SERVER
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_internal_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            }
        }

        self.span_mock.kind = SpanKind.INTERNAL
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_client_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.CLIENT.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_consumer_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.CONSUMER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.CONSUMER
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_local_root_producer_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.name = _SPAN_NAME_VALUE
        self._mock_attribute(
            [AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION], [_AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE]
        )

        expected_attributes_map: Dict[str, BoundedAttributes] = {
            SERVICE_METRIC: {
                AWS_SPAN_KIND: _LOCAL_ROOT,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
            },
            DEPENDENCY_METRIC: {
                AWS_SPAN_KIND: SpanKind.PRODUCER.name,
                AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
                AWS_LOCAL_OPERATION: _INTERNAL_OPERATION,
                AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
                AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
            },
        }

        self.span_mock.kind = SpanKind.PRODUCER
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        self.assertEqual(actual_attributes_map, expected_attributes_map)

    def test_consumer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CONSUMER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
            AWS_REMOTE_SERVICE: _UNKNOWN_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION: _UNKNOWN_REMOTE_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CONSUMER)

    def test_server_span_with_attributes(self):
        self._update_resource_with_service_name()
        self.span_mock.name = _SPAN_NAME_VALUE

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _SPAN_NAME_VALUE,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_null_span_name(self):
        self._update_resource_with_service_name()
        self.span_mock.name = None

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)

    def test_server_span_with_span_name_as_http_method(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "GET"
        self._mock_attribute([SpanAttributes.HTTP_METHOD], ["GET"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)

    def test_server_span_with_span_name_with_http_target(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_TARGET], ["POST", "/payment/123"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /payment",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_TARGET, None)

    def test_server_span_with_span_name_with_target_and_url(self):
        # when http.target & http.url are present, the local operation should be derived from the http.target
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute(
            [SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_TARGET, SpanAttributes.HTTP_URL],
            ["POST", "/my-target/09876", "http://127.0.0.1:8000/payment/123"],
        )

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /my-target",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_TARGET, None)
        self._mock_attribute(SpanAttributes.HTTP_URL, None)

    def test_server_span_with_span_name_with_http_url(self):
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute(
            [SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_URL], ["POST", "http://127.0.0.1:8000/payment/123"]
        )

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /payment",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_URL, None)

    def test_server_span_with_http_url_with_no_path(self):
        # http.url with no path should result in local operation to be "POST /"
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_URL], ["POST", "http://www.example.com"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_URL, None)

    def test_server_span_with_http_url_as_none(self):
        # if http.url is none, local operation should default to UnknownOperation
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_URL], ["POST", None])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _UNKNOWN_OPERATION,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_URL, None)

    def test_server_span_with_http_url_as_empty(self):
        # if http.url is empty, local operation should default to "POST /"
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_URL], ["POST", ""])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_URL, None)

    def test_server_span_with_http_url_as_invalid(self):
        # if http.url is invalid, local operation should default to "POST /"
        self._update_resource_with_service_name()
        self.span_mock.name = "POST"
        self._mock_attribute([SpanAttributes.HTTP_METHOD, SpanAttributes.HTTP_URL], ["POST", "invalid_url"])

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.SERVER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: "POST /",
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.SERVER)
        self._mock_attribute(SpanAttributes.HTTP_METHOD, None)
        self._mock_attribute(SpanAttributes.HTTP_URL, None)

    def test_producer_span_with_attributes(self):
        self._update_resource_with_service_name()
        self._mock_attribute(
            [AWS_LOCAL_OPERATION, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION],
            [_AWS_LOCAL_OPERATION_VALUE, _AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE],
        )

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.PRODUCER.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _AWS_LOCAL_OPERATION_VALUE,
            AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.PRODUCER)

    def test_client_span_with_attributes(self):
        self._update_resource_with_service_name()
        self._mock_attribute(
            [AWS_LOCAL_OPERATION, AWS_REMOTE_SERVICE, AWS_REMOTE_OPERATION],
            [_AWS_LOCAL_OPERATION_VALUE, _AWS_REMOTE_SERVICE_VALUE, _AWS_REMOTE_OPERATION_VALUE],
        )

        expected_attributes: Attributes = {
            AWS_SPAN_KIND: SpanKind.CLIENT.name,
            AWS_LOCAL_SERVICE: _SERVICE_NAME_VALUE,
            AWS_LOCAL_OPERATION: _AWS_LOCAL_OPERATION_VALUE,
            AWS_REMOTE_SERVICE: _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION: _AWS_REMOTE_OPERATION_VALUE,
        }
        self._validate_attributes_produced_for_non_local_root_span_of_kind(expected_attributes, SpanKind.CLIENT)

    # pylint: disable=too-many-statements
    def test_remote_attributes_combinations(self):
        # Set all expected fields to a test string, we will overwrite them in descending order to test
        # the priority-order logic in AwsMetricAttributeGenerator remote attribute methods.
        keys: List[str] = [
            AWS_REMOTE_SERVICE,
            AWS_REMOTE_OPERATION,
            SpanAttributes.RPC_SERVICE,
            SpanAttributes.RPC_METHOD,
            SpanAttributes.DB_SYSTEM,
            SpanAttributes.DB_OPERATION,
            SpanAttributes.DB_STATEMENT,
            SpanAttributes.FAAS_INVOKED_PROVIDER,
            SpanAttributes.FAAS_INVOKED_NAME,
            SpanAttributes.MESSAGING_SYSTEM,
            SpanAttributes.MESSAGING_OPERATION,
            SpanAttributes.GRAPHQL_OPERATION_TYPE,
            # Do not set dummy value for PEER_SERVICE, since it has special behaviour.
            # Two unused attributes to show that we will not make use of unrecognized attributes
            "unknown.service.key",
            "unknown.operation.key",
        ]
        values: List[str] = [
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
            "TestString",
        ]
        self._mock_attribute(keys, values)

        # Validate behaviour of various combinations of AWS remote attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            AWS_REMOTE_SERVICE,
            _AWS_REMOTE_SERVICE_VALUE,
            AWS_REMOTE_OPERATION,
            _AWS_REMOTE_OPERATION_VALUE,
            keys,
            values,
        )

        # Validate behaviour of various combinations of RPC attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.RPC_SERVICE, "RPC service", SpanAttributes.RPC_METHOD, "RPC method", keys, values
        )

        # Validate db.operation not exist, but db.statement exist, where SpanAttributes.DB_STATEMENT is invalid
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "invalid DB statement", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", _UNKNOWN_REMOTE_OPERATION)

        # Validate both db.operation and db.statement not exist.
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", None, None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", _UNKNOWN_REMOTE_OPERATION)

        # Validate db.operation exist, then remove it.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.DB_SYSTEM, "DB system", SpanAttributes.DB_OPERATION, "DB operation", keys, values
        )

        # Validate behaviour of various combinations of FAAS attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.FAAS_INVOKED_NAME,
            "FAAS invoked name",
            SpanAttributes.FAAS_TRIGGER,
            "FAAS trigger name",
            keys,
            values,
        )

        # Validate behaviour of various combinations of Messaging attributes, then remove them.
        keys, values = self._validate_and_remove_remote_attributes(
            SpanAttributes.MESSAGING_SYSTEM,
            "Messaging system",
            SpanAttributes.MESSAGING_OPERATION,
            "Messaging operation",
            keys,
            values,
        )

        # Validate behaviour of GraphQL operation type attribute, then remove it.
        keys, values = self._mock_attribute(
            [SpanAttributes.GRAPHQL_OPERATION_TYPE], ["GraphQL operation type"], keys, values
        )
        self._validate_expected_remote_attributes("graphql", "GraphQL operation type")
        keys, values = self._mock_attribute([SpanAttributes.GRAPHQL_OPERATION_TYPE], [None], keys, values)

        # Validate behaviour of extracting Remote Service from net.peer.name
        keys, values = self._mock_attribute([SpanAttributes.NET_PEER_NAME], ["www.example.com"], keys, values)
        self._validate_expected_remote_attributes("www.example.com", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.NET_PEER_NAME], [None], keys, values)

        # Validate behaviour of extracting Remote Service from net.peer.name and net.peer.port
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_PEER_NAME, SpanAttributes.NET_PEER_PORT],
            ["192.168.0.0", "8081"],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("192.168.0.0:8081", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_PEER_NAME, SpanAttributes.NET_PEER_PORT], [None, None], keys, values
        )

        # Validate behaviour of extracting Remote Service from net.peer.socket.addr
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_SOCK_PEER_ADDR],
            ["www.example.com"],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("www.example.com", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.NET_SOCK_PEER_ADDR], [None], keys, values)

        # Validate behaviour of extracting Remote Service from net.peer.socket.addr and net.sock.peer.port
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_SOCK_PEER_ADDR, SpanAttributes.NET_SOCK_PEER_PORT],
            ["192.168.0.0", "8081"],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("192.168.0.0:8081", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute(
            [SpanAttributes.NET_SOCK_PEER_ADDR, SpanAttributes.NET_SOCK_PEER_PORT], [None, None], keys, values
        )

        # Validate behavior of Remote Operation from HttpTarget - with 1st api part. Also validates that
        # RemoteService is extracted from http.url.
        keys, values = self._mock_attribute(
            [SpanAttributes.HTTP_URL], ["http://www.example.com/payment/123"], keys, values
        )
        self._validate_expected_remote_attributes("www.example.com", "/payment")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behavior of Remote Operation from HttpTarget - without 1st api part, then remove it
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["http://www.example.com"], keys, values)
        self._validate_expected_remote_attributes("www.example.com", "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of extracting Remote Service from http.url. When url is None, it should default to
        # _UNKNOWN_REMOTE_SERVICE
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of extracting Remote Service from http.url. When url is empty, it should default to
        # _UNKNOWN_REMOTE_SERVICE
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [""], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of extracting Remote Service from http.url. When url is invalid, it should default to
        # _UNKNOWN_REMOTE_SERVICE
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["invalid_url"], keys, values)
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of extracting Remote Service from http.url. When url is a host name like
        # https://www.example.com, it should extract the netaddr name as www.example.com
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["https://www.example.com"], keys, values)
        self._validate_expected_remote_attributes("www.example.com", "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of extracting Remote Service from http.url. When url is an ip address with port like
        # http://192.168.1.1:1234, it should extract the netaddr name as 192.168.1.1:1234
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], ["http://192.168.1.1:1234"], keys, values)
        self._validate_expected_remote_attributes("192.168.1.1:1234", "/")
        keys, values = self._mock_attribute([SpanAttributes.HTTP_URL], [None], keys, values)

        # Validate behaviour of Peer service attribute, then remove it.
        keys, values = self._mock_attribute([SpanAttributes.PEER_SERVICE], ["Peer service"], keys, values)
        self._validate_expected_remote_attributes("Peer service", _UNKNOWN_REMOTE_OPERATION)
        keys, values = self._mock_attribute([SpanAttributes.PEER_SERVICE], [None], keys, values)

        # Once we have removed all usable metrics, we only have "unknown" attributes, which are unused.
        self._validate_expected_remote_attributes(_UNKNOWN_REMOTE_SERVICE, _UNKNOWN_REMOTE_OPERATION)

    # Validate behaviour of various combinations of DB attributes.
    def test_get_db_statement_remote_operation(self):
        # Set all expected fields to a test string, we will overwrite them in descending order to test
        keys: List[str] = [
            SpanAttributes.DB_SYSTEM,
            SpanAttributes.DB_OPERATION,
            SpanAttributes.DB_STATEMENT,
        ]
        values: List[str] = [
            "TestString",
            "TestString",
            "TestString",
        ]
        self._mock_attribute(keys, values)

        # Validate SpanAttributes.DB_OPERATION not exist, but SpanAttributes.DB_STATEMENT exist,
        # where SpanAttributes.DB_STATEMENT is valid
        # Case 1: Only 1 valid keywords match
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "SELECT DB statement", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "SELECT")

        # Case 2: More than 1 valid keywords match, we want to pick the longest match
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "DROP VIEW DB statement", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "DROP VIEW")

        # Case 3: More than 1 valid keywords match, but the other keywords is not
        # at the start of the SpanAttributes.DB_STATEMENT. We want to only pick start match
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "SELECT data FROM domains", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "SELECT")

        # Case 4: Have valid keywords， but it is not at the start of SpanAttributes.DB_STATEMENT
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "invalid SELECT DB statement", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", _UNKNOWN_REMOTE_OPERATION)

        # Case 5: Have valid keywords, match the longest word
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "UUID", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "UUID")

        # Case 6: Have valid keywords, match with first word
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "FROM SELECT * ", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "FROM")

        # Case 7: Have valid keyword, match with first word
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "SELECT FROM *", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "SELECT")

        # Case 8: Have valid keywords, match with upper case
        keys, values = self._mock_attribute(
            [SpanAttributes.DB_SYSTEM, SpanAttributes.DB_STATEMENT, SpanAttributes.DB_OPERATION],
            ["DB system", "seLeCt *", None],
            keys,
            values,
        )
        self._validate_expected_remote_attributes("DB system", "SELECT")

    def test_peer_service_does_override_other_remote_services(self):
        self._validate_peer_service_does_override(SpanAttributes.RPC_SERVICE)
        self._validate_peer_service_does_override(SpanAttributes.DB_SYSTEM)
        self._validate_peer_service_does_override(SpanAttributes.FAAS_INVOKED_PROVIDER)
        self._validate_peer_service_does_override(SpanAttributes.MESSAGING_SYSTEM)
        self._validate_peer_service_does_override(SpanAttributes.GRAPHQL_OPERATION_TYPE)
        self._validate_peer_service_does_override(SpanAttributes.NET_PEER_NAME)
        self._validate_peer_service_does_override(SpanAttributes.NET_SOCK_PEER_ADDR)
        # Actually testing that peer service overrides "UnknownRemoteService".
        self._validate_peer_service_does_override("unknown.service.key")

    def test_peer_service_does_not_override_aws_remote_service(self):
        self._mock_attribute([AWS_REMOTE_SERVICE, SpanAttributes.PEER_SERVICE], ["TestString", "PeerService"])
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "TestString")

    def test_no_metric_when_consumer_process_with_consumer_parent(self):
        self._mock_attribute(
            [AWS_CONSUMER_PARENT_SPAN_KIND, SpanAttributes.MESSAGING_OPERATION],
            [SpanKind.CONSUMER, MessagingOperationValues.PROCESS],
        )
        self.span_mock.kind = SpanKind.CONSUMER

        attribute_map: {str: Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )

        service_attributes: Attributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: Attributes = attribute_map.get(DEPENDENCY_METRIC)

        self.assertIsNone(service_attributes)
        self.assertIsNone(dependency_attributes)

    def test_both_metric_when_local_root_consumer_process(self):
        self._mock_attribute(
            [AWS_CONSUMER_PARENT_SPAN_KIND, SpanAttributes.MESSAGING_OPERATION],
            [SpanKind.CONSUMER, MessagingOperationValues.PROCESS],
        )
        self.span_mock.kind = SpanKind.CONSUMER
        self.parent_span_context.is_valid = False

        attribute_map: {str: Attributes} = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )

        service_attributes: Attributes = attribute_map.get(SERVICE_METRIC)
        dependency_attributes: Attributes = attribute_map.get(DEPENDENCY_METRIC)

        self.assertIsNotNone(service_attributes)
        self.assertIsNotNone(dependency_attributes)

    def test_db_user_attribute(self):
        self._mock_attribute([SpanAttributes.DB_OPERATION, SpanAttributes.DB_USER], ["db_operation", "db_user"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_OPERATION), "db_operation")
        self.assertEqual(actual_attributes.get(AWS_REMOTE_DB_USER), "db_user")

    def test_db_user_attribute_absent(self):
        self._mock_attribute([SpanAttributes.DB_SYSTEM], ["db_system"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_DB_USER))

    def test_db_user_attribute_not_present_in_service_metric_for_server_span(self):
        self._mock_attribute([SpanAttributes.DB_USER, SpanAttributes.DB_SYSTEM], ["db_user", "db_system"])
        self.span_mock.kind = SpanKind.SERVER

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(SERVICE_METRIC)
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_DB_USER))

    def test_db_user_attribute_with_different_values(self):
        self._mock_attribute([SpanAttributes.DB_OPERATION, SpanAttributes.DB_USER], ["db_operation", "non_db_user"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_DB_USER), "non_db_user")

    def test_db_user_present_and_is_db_span_false(self):
        self._mock_attribute([SpanAttributes.DB_USER], ["db_user"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_DB_USER))

    def test_local_root_boto3_span(self):
        self._update_resource_with_service_name()
        self.parent_span_context.is_valid = False
        self.span_mock.kind = SpanKind.PRODUCER
        self.span_mock.instrumentation_scope.name = "opentelemetry.instrumentation.boto3sqs"

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        service_attributes: Attributes = actual_attributes.get(SERVICE_METRIC)
        dependency_attributes: Attributes = actual_attributes.get(DEPENDENCY_METRIC)

        # boto3sqs spans shouldn't generate aws service attributes even local root
        self.assertIsNone(service_attributes)
        # boto3sqs spans shouldn't generate aws dependency attributes
        self.assertIsNone(dependency_attributes)

    def test_non_local_root_boto3_span(self):
        self._update_resource_with_service_name()
        self.span_mock.kind = SpanKind.CONSUMER
        self.span_mock.instrumentation_scope.name = "opentelemetry.instrumentation.boto3sqs"

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        )
        service_attributes: Attributes = actual_attributes.get(SERVICE_METRIC)
        dependency_attributes: Attributes = actual_attributes.get(DEPENDENCY_METRIC)

        # boto3sqs spans shouldn't generate aws service attributes
        self.assertIsNone(service_attributes)
        # boto3sqs spans shouldn't generate aws dependency attributes
        self.assertIsNone(dependency_attributes)

    def test_normalize_remote_service_name_no_normalization(self):
        service_name: str = "non aws service"
        self._mock_attribute([SpanAttributes.RPC_SERVICE], [service_name])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), service_name)

    def test_normalize_remote_service_name_aws_sdk(self):
        self.validate_aws_sdk_service_normalization("DynamoDB", "AWS::DynamoDB")
        self.validate_aws_sdk_service_normalization("Kinesis", "AWS::Kinesis")
        self.validate_aws_sdk_service_normalization("S3", "AWS::S3")
        self.validate_aws_sdk_service_normalization("SQS", "AWS::SQS")
        self.validate_aws_sdk_service_normalization("Bedrock", "AWS::Bedrock")
        self.validate_aws_sdk_service_normalization("Bedrock Agent", "AWS::Bedrock")
        self.validate_aws_sdk_service_normalization("Bedrock Agent Runtime", "AWS::Bedrock")
        self.validate_aws_sdk_service_normalization("Bedrock Runtime", "AWS::BedrockRuntime")
        self.validate_aws_sdk_service_normalization("Bedrock AgentCore", "AWS::BedrockAgentCore")
        self.validate_aws_sdk_service_normalization("Bedrock AgentCore Control", "AWS::BedrockAgentCore")
        self.validate_aws_sdk_service_normalization("Secrets Manager", "AWS::SecretsManager")
        self.validate_aws_sdk_service_normalization("SNS", "AWS::SNS")
        self.validate_aws_sdk_service_normalization("SFN", "AWS::StepFunctions")

        # AWS SDK Lambda tests - non-Invoke operations
        self.validate_aws_sdk_service_normalization("Lambda", "AWS::Lambda")

        # Lambda Invoke with function name
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
            ],
            ["aws-api", "Lambda", "Invoke", "testFunction"],
        )
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "testFunction")

        # Lambda Invoke without AWS_LAMBDA_NAME - should fall back to UnknownRemoteService
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
            ],
            ["aws-api", "Lambda", "Invoke", None],
        )
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "UnknownRemoteService")

    def validate_aws_sdk_service_normalization(self, service_name: str, expected_remote_service: str):
        self._mock_attribute([SpanAttributes.RPC_SYSTEM, SpanAttributes.RPC_SERVICE], ["aws-api", service_name])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), expected_remote_service)

    def _update_resource_with_service_name(self) -> None:
        self.resource: Resource = Resource(attributes={SERVICE_NAME: _SERVICE_NAME_VALUE})

    def _validate_peer_service_does_override(self, remote_service_key: str) -> None:
        self._mock_attribute([remote_service_key, SpanAttributes.PEER_SERVICE], ["TestString", "PeerService"])
        self.span_mock.kind = SpanKind.CLIENT

        actual_attributes: Attributes = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertEqual(actual_attributes.get(AWS_REMOTE_SERVICE), "PeerService")

        self._mock_attribute([remote_service_key, SpanAttributes.PEER_SERVICE], [None, None])

    def test_sdk_client_span_with_remote_resource_attributes(self):
        keys: List[str] = [SpanAttributes.RPC_SYSTEM, AWS_AUTH_ACCESS_KEY, AWS_AUTH_REGION]
        values: List[str] = ["aws-api", _AWS_REMOTE_RESOURCE_ACCESS_KEY, _AWS_REMOTE_RESOURCE_REGION]
        self._mock_attribute(keys, values)
        # Validate behaviour of aws bucket name attribute, then remove it.
        self._mock_attribute([SpanAttributes.AWS_S3_BUCKET], ["aws_s3_bucket_name"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::S3::Bucket",
            "aws_s3_bucket_name",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([SpanAttributes.AWS_S3_BUCKET], [None])

        # Validate behaviour of AWS_SQS_QUEUE_NAME attribute, then remove it
        self._mock_attribute([AWS_SQS_QUEUE_NAME], ["aws_queue_name"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::SQS::Queue",
            "aws_queue_name",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_SQS_QUEUE_NAME], [None])

        # Validate behaviour of having both AWS_SQS_QUEUE_NAME and AWS_SQS_QUEUE_URL attribute, then remove them.
        # Queue name is more reliable than queue URL, so we prefer to use name over URL.
        self._mock_attribute(
            [AWS_SQS_QUEUE_URL, AWS_SQS_QUEUE_NAME],
            ["https://sqs.us-east-2.amazonaws.com/123456789012/Queue", "aws_queue_name"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::SQS::Queue",
            "aws_queue_name",
            "https://sqs.us-east-2.amazonaws.com/123456789012/Queue",
            "us-east-2",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_SQS_QUEUE_URL, AWS_SQS_QUEUE_NAME], [None, None])

        # Valid queue name with invalid queue URL, we should default to using the queue name.
        self._mock_attribute([AWS_SQS_QUEUE_URL, AWS_SQS_QUEUE_NAME], ["invalidUrl", "aws_queue_name"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::SQS::Queue",
            "aws_queue_name",
            "invalidUrl",
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_SQS_QUEUE_URL, AWS_SQS_QUEUE_NAME], [None, None])

        # Validate behaviour of AWS_KINESIS_STREAM_ARN attribute, then remove it.
        self._mock_attribute(
            [AWS_KINESIS_STREAM_ARN], ["arn:aws:kinesis:us-west-2:123456789012:stream/aws_stream_name"], keys, values
        )
        self._validate_remote_resource_attributes(
            "AWS::Kinesis::Stream",
            "aws_stream_name",
            None,
            "us-west-2",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_KINESIS_STREAM_ARN], [None])

        # Validate behaviour of AWS_KINESIS_STREAM_NAME attribute, then remove it.
        self._mock_attribute([AWS_KINESIS_STREAM_NAME], ["aws_stream_name"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Kinesis::Stream",
            "aws_stream_name",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_KINESIS_STREAM_NAME], [None])

        # Validate behaviour of AWS_DYNAMODB_TABLE_ARN attribute, then remove it.
        self._mock_attribute(
            [AWS_DYNAMODB_TABLE_ARN], ["arn:aws:dynamodb:us-west-2:123456789012:table/aws_table_name"], keys, values
        )
        self._validate_remote_resource_attributes(
            "AWS::DynamoDB::Table", "aws_table_name", None, "us-west-2", "123456789012", None
        )
        self._mock_attribute([AWS_DYNAMODB_TABLE_ARN], [None])

        # Validate behaviour of SpanAttributes.AWS_DYNAMODB_TABLE_NAMES attribute with one table name, then remove it.
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [["aws_table_name"]], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::DynamoDB::Table",
            "aws_table_name",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [None])

        # Validate behaviour of SpanAttributes.AWS_DYNAMODB_TABLE_NAMES attribute with no table name, then remove it.
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [[]], keys, values)
        self._validate_remote_resource_attributes(None, None)
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [None])

        # Validate behaviour of SpanAttributes.AWS_DYNAMODB_TABLE_NAMES attribute with two table names, then remove it.
        self._mock_attribute(
            [SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [["aws_table_name1", "aws_table_name1"]], keys, values
        )
        self._validate_remote_resource_attributes(None, None)
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [None])

        # Validate behaviour of AWS_TABLE_NAME attribute with special chars(|), then remove it.
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [["aws_table|name"]], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::DynamoDB::Table",
            "aws_table^|name",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [None])

        # Validate behaviour of AWS_TABLE_NAME attribute with special chars(^), then remove it.
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [["aws_table^name"]], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::DynamoDB::Table",
            "aws_table^^name",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [None])

        # Validate behaviour of AWS_BEDROCK_AGENT_ID attribute, then remove it.
        self._mock_attribute([AWS_BEDROCK_AGENT_ID], ["test_agent_id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::Agent",
            "test_agent_id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_BEDROCK_AGENT_ID], [None])

        # Validate behaviour of AWS_BEDROCK_AGENT_ID attribute with special chars(^), then remove it.
        self._mock_attribute([AWS_BEDROCK_AGENT_ID], ["test_agent_^id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::Agent",
            "test_agent_^^id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_BEDROCK_AGENT_ID], [None])

        # Validate behaviour of AWS_BEDROCK_DATA_SOURCE_ID attribute, then remove it.
        self._mock_attribute(
            [AWS_BEDROCK_DATA_SOURCE_ID, AWS_BEDROCK_KNOWLEDGE_BASE_ID],
            ["test_datasource_id", "test_knowledge_base_id"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::DataSource",
            "test_datasource_id",
            "test_knowledge_base_id|test_datasource_id",
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_BEDROCK_DATA_SOURCE_ID, AWS_BEDROCK_KNOWLEDGE_BASE_ID], [None, None])

        # Validate behaviour of AWS_BEDROCK_DATA_SOURCE_ID attribute with special chars(^), then remove it.
        self._mock_attribute(
            [AWS_BEDROCK_DATA_SOURCE_ID, AWS_BEDROCK_KNOWLEDGE_BASE_ID],
            ["test_datasource_^id", "test_knowledge_base_^id"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::DataSource",
            "test_datasource_^^id",
            "test_knowledge_base_^^id|test_datasource_^^id",
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_BEDROCK_DATA_SOURCE_ID, AWS_BEDROCK_KNOWLEDGE_BASE_ID], [None, None])

        # Validate behaviour of AWS_BEDROCK_GUARDRAIL_ID attribute, then remove it.
        self._mock_attribute(
            [AWS_BEDROCK_GUARDRAIL_ID, AWS_BEDROCK_GUARDRAIL_ARN],
            ["test_guardrail_id", "arn:aws:bedrock:us-east-1:123456789012:guardrail/test_guardrail_id"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::Guardrail",
            "test_guardrail_id",
            "arn:aws:bedrock:us-east-1:123456789012:guardrail/test_guardrail_id",
            "us-east-1",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_BEDROCK_GUARDRAIL_ID, AWS_BEDROCK_GUARDRAIL_ARN], [None, None])

        # Validate behaviour of AWS_BEDROCK_GUARDRAIL_ID attribute with special chars(^), then remove it.
        self._mock_attribute(
            [AWS_BEDROCK_GUARDRAIL_ID, AWS_BEDROCK_GUARDRAIL_ARN],
            ["test_guardrail_^id", "arn:aws:bedrock:us-east-1:123456789012:guardrail/test_guardrail_^id"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::Guardrail",
            "test_guardrail_^^id",
            "arn:aws:bedrock:us-east-1:123456789012:guardrail/test_guardrail_^^id",
            "us-east-1",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_BEDROCK_GUARDRAIL_ID, AWS_BEDROCK_GUARDRAIL_ARN], [None, None])

        # Validate behaviour of AWS_BEDROCK_KNOWLEDGE_BASE_ID attribute, then remove it.
        self._mock_attribute([AWS_BEDROCK_KNOWLEDGE_BASE_ID], ["test_knowledgeBase_id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::KnowledgeBase",
            "test_knowledgeBase_id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_BEDROCK_KNOWLEDGE_BASE_ID], [None])

        # Validate behaviour of AWS_BEDROCK_KNOWLEDGE_BASE_ID attribute with special chars(^), then remove it.
        self._mock_attribute([AWS_BEDROCK_KNOWLEDGE_BASE_ID], ["test_knowledgeBase_^id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::KnowledgeBase",
            "test_knowledgeBase_^^id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_BEDROCK_KNOWLEDGE_BASE_ID], [None])

        # Validate behaviour of GEN_AI_REQUEST_MODEL attribute, then remove it.
        self._mock_attribute([GEN_AI_REQUEST_MODEL], ["test.service_id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::Model",
            "test.service_id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([GEN_AI_REQUEST_MODEL], [None])

        # Validate behaviour of GEN_AI_REQUEST_MODEL attribute with special chars(^), then remove it.
        self._mock_attribute([GEN_AI_REQUEST_MODEL], ["test.service_^id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Bedrock::Model",
            "test.service_^^id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([GEN_AI_REQUEST_MODEL], [None])

        # Validate behaviour of AWS_SECRETSMANAGER_SECRET_ARN attribute, then remove it.
        self._mock_attribute(
            [AWS_SECRETSMANAGER_SECRET_ARN],
            ["arn:aws:secretsmanager:us-east-1:123456789012:secret:secret_name-lERW9H"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::SecretsManager::Secret",
            "secret_name-lERW9H",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:secret_name-lERW9H",
            "us-east-1",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_SECRETSMANAGER_SECRET_ARN], [None])

        # Validate behaviour of AWS_SNS_TOPIC_ARN attribute, then remove it.
        self._mock_attribute([AWS_SNS_TOPIC_ARN], ["arn:aws:sns:us-west-2:012345678901:test_topic"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::SNS::Topic",
            "test_topic",
            "arn:aws:sns:us-west-2:012345678901:test_topic",
            "us-west-2",
            "012345678901",
            None,
        )
        self._mock_attribute([AWS_SNS_TOPIC_ARN], [None])

        # Validate behaviour of AWS_STEPFUNCTIONS_STATEMACHINE_ARN attribute, then remove it.
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_STATEMACHINE_ARN],
            ["arn:aws:states:us-east-1:123456789012:stateMachine:test_state_machine"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::StepFunctions::StateMachine",
            "test_state_machine",
            "arn:aws:states:us-east-1:123456789012:stateMachine:test_state_machine",
            "us-east-1",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_STEPFUNCTIONS_STATEMACHINE_ARN], [None])

        # Validate behaviour of AWS_STEPFUNCTIONS_ACTIVITY_ARN attribute, then remove it.
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_ACTIVITY_ARN],
            ["arn:aws:states:us-east-1:123456789012:activity:testActivity"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::StepFunctions::Activity",
            "testActivity",
            "arn:aws:states:us-east-1:123456789012:activity:testActivity",
            "us-east-1",
            "123456789012",
            None,
        )
        self._mock_attribute([AWS_STEPFUNCTIONS_ACTIVITY_ARN], [None])

        # Validate behaviour of AWS_LAMBDA_RESOURCEMAPPING_ID attribute, then remove it.
        self._mock_attribute([AWS_LAMBDA_RESOURCEMAPPING_ID], ["aws_event_source_mapping_id"], keys, values)
        self._validate_remote_resource_attributes(
            "AWS::Lambda::EventSourceMapping",
            "aws_event_source_mapping_id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_LAMBDA_RESOURCEMAPPING_ID], [None])

        # Validate behaviour of AWS_LAMBDA_RESOURCE_MAPPING_ID,
        # then remove it.
        self._mock_attribute(
            [AWS_LAMBDA_RESOURCEMAPPING_ID],
            ["aws_event_source_mapping_id"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Lambda::EventSourceMapping",
            "aws_event_source_mapping_id",
            None,
            _AWS_REMOTE_RESOURCE_REGION,
            None,
            _AWS_REMOTE_RESOURCE_ACCESS_KEY,
        )
        self._mock_attribute([AWS_LAMBDA_RESOURCEMAPPING_ID], [None])

        # Test AWS Lambda Invoke scenario with default lambda remote environment
        self.span_mock.kind = SpanKind.CLIENT
        self._mock_attribute(
            [AWS_LAMBDA_FUNCTION_NAME, SpanAttributes.RPC_METHOD, SpanAttributes.RPC_SERVICE],
            ["test_downstream_lambda1", "Invoke", "Lambda"],
            keys,
            values,
        )
        dependency_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(dependency_attributes.get(AWS_REMOTE_SERVICE), "test_downstream_lambda1")
        self.assertEqual(dependency_attributes.get(AWS_REMOTE_ENVIRONMENT), "lambda:default")
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, dependency_attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, dependency_attributes)
        self.assertNotIn(AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER, dependency_attributes)
        self._mock_attribute(
            [AWS_LAMBDA_FUNCTION_NAME, SpanAttributes.RPC_METHOD, SpanAttributes.RPC_SERVICE], [None, None, None]
        )

        # Test AWS Lambda Invoke scenario with user-configured lambda remote environment
        os.environ["LAMBDA_APPLICATION_SIGNALS_REMOTE_ENVIRONMENT"] = "test"
        self.span_mock.kind = SpanKind.CLIENT
        self._mock_attribute(
            [AWS_LAMBDA_FUNCTION_NAME, SpanAttributes.RPC_METHOD, SpanAttributes.RPC_SERVICE],
            ["testLambdaFunction", "Invoke", "Lambda"],
            keys,
            values,
        )
        dependency_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(dependency_attributes.get(AWS_REMOTE_SERVICE), "testLambdaFunction")
        self.assertEqual(dependency_attributes.get(AWS_REMOTE_ENVIRONMENT), "lambda:test")
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, dependency_attributes)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, dependency_attributes)
        self.assertNotIn(AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER, dependency_attributes)
        self._mock_attribute(
            [AWS_LAMBDA_FUNCTION_NAME, SpanAttributes.RPC_METHOD, SpanAttributes.RPC_SERVICE], [None, None, None]
        )
        os.environ.pop("LAMBDA_APPLICATION_SIGNALS_REMOTE_ENVIRONMENT", None)

        # Test AWS Lambda non-Invoke scenario
        self.span_mock.kind = SpanKind.CLIENT
        lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:testLambda"
        self._mock_attribute(
            [AWS_LAMBDA_FUNCTION_NAME, AWS_LAMBDA_FUNCTION_ARN, SpanAttributes.RPC_METHOD],
            ["testLambdaFunction", lambda_arn, "GetFunction"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Lambda::Function", "testLambdaFunction", lambda_arn, "us-east-1", "123456789012", None
        )
        self._mock_attribute(
            [AWS_LAMBDA_FUNCTION_NAME, AWS_LAMBDA_FUNCTION_ARN, SpanAttributes.RPC_METHOD], [None, None, None]
        )

        # Validate behaviour of AWS_LAMBDA_NAME for non-Invoke operations (treated as resource)
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
                AWS_LAMBDA_FUNCTION_ARN,
            ],
            [
                "aws-api",
                "Lambda",
                "GetFunction",
                "testLambdaName",
                "arn:aws:lambda:us-east-1:123456789012:function:testLambdaName",
            ],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "AWS::Lambda::Function",
            "testLambdaName",
            "arn:aws:lambda:us-east-1:123456789012:function:testLambdaName",
            "us-east-1",
            "123456789012",
            None,
        )
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
                AWS_LAMBDA_FUNCTION_ARN,
            ],
            [None, None, None, None, None],
        )

        # Validate that Lambda Invoke with function name treats Lambda as a service, not a resource
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
            ],
            ["aws-api", "Lambda", "Invoke", "testLambdaName"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(None, None)
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
            ],
            [None, None, None, None],
        )

        # Cross account support
        # Invalid arn but account access key is available
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_STATEMACHINE_ARN],
            ["invalid_arn"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(None, None, None)
        self._mock_attribute([AWS_STEPFUNCTIONS_STATEMACHINE_ARN], [None])

        # Invalid arn and no account access key
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_STATEMACHINE_ARN, SpanAttributes.RPC_SYSTEM], ["invalid_arn", "aws-api"]
        )
        self._validate_remote_resource_attributes(None, None, None)
        self._mock_attribute([AWS_STEPFUNCTIONS_STATEMACHINE_ARN], [None])

        # Both account access key and account id are not available
        self._mock_attribute(
            [SpanAttributes.AWS_S3_BUCKET, SpanAttributes.RPC_SYSTEM], ["aws_s3_bucket_name", "aws-api"]
        )
        self._validate_remote_resource_attributes("AWS::S3::Bucket", "aws_s3_bucket_name")
        self._mock_attribute([SpanAttributes.AWS_S3_BUCKET], [None])

        # Account access key is not available
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_STATEMACHINE_ARN, SpanAttributes.RPC_SYSTEM],
            ["arn:aws:states:us-east-1:123456789123:stateMachine:testStateMachine", "aws-api"],
        )
        self._validate_remote_resource_attributes(
            "AWS::StepFunctions::StateMachine",
            "testStateMachine",
            "arn:aws:states:us-east-1:123456789123:stateMachine:testStateMachine",
            "us-east-1",
            "123456789123",
            None,
        )
        self._mock_attribute([AWS_STEPFUNCTIONS_STATEMACHINE_ARN], [None])

        # Arn with invalid account id
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_STATEMACHINE_ARN, SpanAttributes.RPC_SYSTEM],
            ["arn:aws:states:us-east-1:invalid_account_id:stateMachine:testStateMachine", "aws-api"],
        )
        self._validate_remote_resource_attributes(None, None, None)
        self._mock_attribute([AWS_STEPFUNCTIONS_STATEMACHINE_ARN], [None])

        # Arn with invalid region
        self._mock_attribute(
            [AWS_STEPFUNCTIONS_STATEMACHINE_ARN, SpanAttributes.RPC_SYSTEM],
            ["arn:aws:states:invalid_region:123456789123:stateMachine:testStateMachine", "aws-api"],
        )
        self._validate_remote_resource_attributes(
            "AWS::StepFunctions::StateMachine",
            "testStateMachine",
            "arn:aws:states:invalid_region:123456789123:stateMachine:testStateMachine",
            "invalid_region",
            "123456789123",
            None,
        )
        self._mock_attribute([AWS_STEPFUNCTIONS_STATEMACHINE_ARN], [None])

        self._mock_attribute([SpanAttributes.RPC_SYSTEM], [None])

    def test_client_db_span_with_remote_resource_attributes(self):
        keys: List[str] = [
            SpanAttributes.DB_SYSTEM,
        ]
        values: List[str] = [
            "mysql",
        ]
        self._mock_attribute(keys, values)
        # Validate behaviour of DB_NAME, SERVER_ADDRESS and SERVER_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS, SpanAttributes.SERVER_PORT],
            ["db_name", "abc.com", 3306],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name|abc.com|3306")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS, SpanAttributes.SERVER_PORT],
            [None, None, None],
        )

        # Validate behaviour of DB_NAME with '|' char, SERVER_ADDRESS and SERVER_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS, SpanAttributes.SERVER_PORT],
            ["db_name|special", "abc.com", 3306],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name^|special|abc.com|3306")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS, SpanAttributes.SERVER_PORT],
            [None, None, None],
        )

        # Validate behaviour of DB_NAME with '^' char, SERVER_ADDRESS and SERVER_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS, SpanAttributes.SERVER_PORT],
            ["db_name^special", "abc.com", 3306],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name^^special|abc.com|3306")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS, SpanAttributes.SERVER_PORT],
            [None, None, None],
        )

        # Validate behaviour of DB_NAME, SERVER_ADDRESS exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS],
            ["db_name", "abc.com"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name|abc.com")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_ADDRESS],
            [None, None],
        )

        # Validate behaviour of SERVER_ADDRESS exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.SERVER_ADDRESS],
            ["abc.com"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "abc.com")
        self._mock_attribute(
            [SpanAttributes.SERVER_ADDRESS],
            [None],
        )

        # Validate behaviour of SERVER_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.SERVER_PORT],
            [3306],
            keys,
            values,
        )
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, actual_attributes_map)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, actual_attributes_map)
        self._mock_attribute(
            [SpanAttributes.SERVER_PORT],
            [None],
        )

        # Validate behaviour of DB_NAME, NET_PEER_NAME and NET_PEER_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.NET_PEER_NAME, SpanAttributes.NET_PEER_PORT],
            ["db_name", "abc.com", 3306],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name|abc.com|3306")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.NET_PEER_NAME, SpanAttributes.NET_PEER_PORT],
            [None, None, None],
        )

        # Validate behaviour of DB_NAME, NET_PEER_NAME exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.NET_PEER_NAME],
            ["db_name", "abc.com"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name|abc.com")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.NET_PEER_NAME],
            [None, None],
        )

        # Validate behaviour of NET_PEER_NAME exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.NET_PEER_NAME],
            ["abc.com"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "abc.com")
        self._mock_attribute(
            [SpanAttributes.NET_PEER_NAME],
            [None],
        )

        # Validate behaviour of NET_PEER_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.NET_PEER_PORT],
            [3306],
            keys,
            values,
        )
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, actual_attributes_map)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, actual_attributes_map)
        self._mock_attribute(
            [SpanAttributes.NET_PEER_PORT],
            [None],
        )

        # Validate behaviour of DB_NAME, SERVER_SOCKET_ADDRESS and SERVER_SOCKET_PORT exist, then
        # remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_SOCKET_ADDRESS, SpanAttributes.SERVER_SOCKET_PORT],
            ["db_name", "abc.com", 3306],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name|abc.com|3306")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_SOCKET_ADDRESS, SpanAttributes.SERVER_SOCKET_PORT],
            [None, None, None],
        )

        # Validate behaviour of DB_NAME, SERVER_SOCKET_ADDRESS exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_SOCKET_ADDRESS],
            ["db_name", "abc.com"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "db_name|abc.com")
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.SERVER_SOCKET_ADDRESS],
            [None, None],
        )

        # Validate behaviour of SERVER_SOCKET_PORT exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.SERVER_SOCKET_PORT],
            [3306],
            keys,
            values,
        )
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, actual_attributes_map)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, actual_attributes_map)
        self._mock_attribute(
            [SpanAttributes.SERVER_SOCKET_PORT],
            [None],
        )

        # Validate behaviour of only DB_NAME exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME],
            ["db_name"],
            keys,
            values,
        )
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, actual_attributes_map)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, actual_attributes_map)
        self._mock_attribute(
            [SpanAttributes.DB_NAME],
            [None],
        )

        # Validate behaviour of DB_NAME and DB_CONNECTION_STRING exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.DB_CONNECTION_STRING],
            ["db_name", "mysql://test-apm.cluster-cnrw3s3ddo7n.us-east-1.rds.amazonaws.com:3306/petclinic"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "DB::Connection", "db_name|test-apm.cluster-cnrw3s3ddo7n.us-east-1.rds.amazonaws.com|3306"
        )
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.DB_CONNECTION_STRING],
            [None, None],
        )

        # Validate behaviour of DB_CONNECTION_STRING exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_CONNECTION_STRING],
            ["mysql://test-apm.cluster-cnrw3s3ddo7n.us-east-1.rds.amazonaws.com:3306/petclinic"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes(
            "DB::Connection", "test-apm.cluster-cnrw3s3ddo7n.us-east-1.rds.amazonaws.com|3306"
        )
        self._mock_attribute(
            [SpanAttributes.DB_CONNECTION_STRING],
            [None],
        )

        # Validate behaviour of DB_CONNECTION_STRING exist without port, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_CONNECTION_STRING],
            ["http://dbserver"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("DB::Connection", "dbserver")
        self._mock_attribute(
            [SpanAttributes.DB_CONNECTION_STRING],
            [None],
        )

        # Validate behaviour of DB_NAME and invalid DB_CONNECTION_STRING exist, then remove it.
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.DB_CONNECTION_STRING],
            ["db_name", "hsqldb:mem:"],
            keys,
            values,
        )
        self.span_mock.kind = SpanKind.CLIENT
        actual_attributes_map: Dict[str, BoundedAttributes] = _GENERATOR.generate_metric_attributes_dict_from_span(
            self.span_mock, self.resource
        ).get(DEPENDENCY_METRIC)
        self.assertNotIn(AWS_REMOTE_RESOURCE_TYPE, actual_attributes_map)
        self.assertNotIn(AWS_REMOTE_RESOURCE_IDENTIFIER, actual_attributes_map)
        self._mock_attribute(
            [SpanAttributes.DB_NAME, SpanAttributes.DB_CONNECTION_STRING],
            [None, None],
        )

        self._mock_attribute(
            [SpanAttributes.DB_SYSTEM],
            [None],
        )

    def test_set_remote_environment(self):
        """Test remote environment setting for Lambda invoke operations."""
        keys = []
        values = []

        # Test 1: Setting remote environment when all relevant attributes are present
        self.span_mock.kind = SpanKind.CLIENT
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
            ],
            ["aws-api", "Lambda", "Invoke", "testFunction"],
            keys,
            values,
        )
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes.get(AWS_REMOTE_ENVIRONMENT), "lambda:default")

        # Test 2: NOT setting it when RPC_SYSTEM is missing
        self._mock_attribute([SpanAttributes.RPC_SYSTEM], [None])
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_ENVIRONMENT))
        self._mock_attribute([SpanAttributes.RPC_SYSTEM], ["aws-api"], keys, values)

        # Test 3: NOT setting it when RPC_METHOD is missing
        self._mock_attribute([SpanAttributes.RPC_METHOD], [None])
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_ENVIRONMENT))
        self._mock_attribute([SpanAttributes.RPC_METHOD], ["Invoke"], keys, values)

        # Test 4: Still setting it to lambda:default when AWS_LAMBDA_FUNCTION_NAME is missing
        # Keep the other attributes but remove AWS_LAMBDA_FUNCTION_NAME
        self._mock_attribute(
            [
                SpanAttributes.RPC_SYSTEM,
                SpanAttributes.RPC_SERVICE,
                SpanAttributes.RPC_METHOD,
                AWS_LAMBDA_FUNCTION_NAME,
            ],
            ["aws-api", "Lambda", "Invoke", None],
            keys,
            values,
        )

        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertEqual(actual_attributes.get(AWS_REMOTE_ENVIRONMENT), "lambda:default")
        self._mock_attribute([AWS_LAMBDA_FUNCTION_NAME], ["testFunction"], keys, values)

        # Test 5: NOT setting it for non-Lambda services
        self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.RPC_METHOD],
            ["S3", "GetObject"],
            keys,
            values,
        )
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_ENVIRONMENT))

        # Test 6: NOT setting it for Lambda non-Invoke operations
        self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.RPC_METHOD],
            ["Lambda", "GetFunction"],
            keys,
            values,
        )
        actual_attributes = _GENERATOR.generate_metric_attributes_dict_from_span(self.span_mock, self.resource).get(
            DEPENDENCY_METRIC
        )
        self.assertIsNone(actual_attributes.get(AWS_REMOTE_ENVIRONMENT))

    def test_cloudformation_primary_identifier_fallback_to_remote_resource_identifier(self):
        """Test that when cloudformationPrimaryIdentifier is not explicitly set,
        it falls back to use the same value as remoteResourceIdentifier."""
        keys = []
        values = []

        keys, values = self._mock_attribute([SpanAttributes.RPC_SYSTEM], ["aws-api"], keys, values)
        self.span_mock.kind = SpanKind.CLIENT

        # Test case 1: S3 Bucket (no ARN available, should use bucket name for both)
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.AWS_S3_BUCKET], ["S3", "my-test-bucket"], keys, values
        )
        self._validate_remote_resource_attributes("AWS::S3::Bucket", "my-test-bucket")

        # Test S3 bucket with special characters
        keys, values = self._mock_attribute([SpanAttributes.AWS_S3_BUCKET], ["my-test|bucket^name"], keys, values)
        self._validate_remote_resource_attributes("AWS::S3::Bucket", "my-test^|bucket^^name")
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.AWS_S3_BUCKET], [None, None], keys, values
        )

        # Test case 2: SQS Queue by name (no ARN, should use queue name for both)
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, AWS_SQS_QUEUE_NAME], ["SQS", "my-test-queue"], keys, values
        )
        self._validate_remote_resource_attributes("AWS::SQS::Queue", "my-test-queue")

        # Test SQS queue with special characters
        keys, values = self._mock_attribute([AWS_SQS_QUEUE_NAME], ["my^queue|name"], keys, values)
        self._validate_remote_resource_attributes("AWS::SQS::Queue", "my^^queue^|name")
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, AWS_SQS_QUEUE_NAME], [None, None], keys, values
        )

        # Test case 3: DynamoDB Table (no ARN, should use table name for both)
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.AWS_DYNAMODB_TABLE_NAMES],
            ["DynamoDB", ["my-test-table"]],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("AWS::DynamoDB::Table", "my-test-table")

        # Test DynamoDB table with special characters
        keys, values = self._mock_attribute(
            [SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [["my|test^table"]], keys, values
        )
        self._validate_remote_resource_attributes("AWS::DynamoDB::Table", "my^|test^^table")
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.AWS_DYNAMODB_TABLE_NAMES], [None, None], keys, values
        )

        # Test case 4: Kinesis Stream
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, AWS_KINESIS_STREAM_NAME], ["Kinesis", "my-test-stream"], keys, values
        )
        self._validate_remote_resource_attributes("AWS::Kinesis::Stream", "my-test-stream")

        # Test Kinesis stream with special characters
        keys, values = self._mock_attribute([AWS_KINESIS_STREAM_NAME], ["my-stream^with|chars"], keys, values)
        self._validate_remote_resource_attributes("AWS::Kinesis::Stream", "my-stream^^with^|chars")
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, AWS_KINESIS_STREAM_NAME], [None, None], keys, values
        )

        # Test case 5: Lambda Function (non-invoke operation, no ARN)
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.RPC_METHOD, AWS_LAMBDA_FUNCTION_NAME],
            ["Lambda", "GetFunction", "my-test-function"],
            keys,
            values,
        )
        self._validate_remote_resource_attributes("AWS::Lambda::Function", "my-test-function")

        # Test Lambda function with special characters
        keys, values = self._mock_attribute([AWS_LAMBDA_FUNCTION_NAME], ["my-function|with^chars"], keys, values)
        self._validate_remote_resource_attributes("AWS::Lambda::Function", "my-function^|with^^chars")
        keys, values = self._mock_attribute(
            [SpanAttributes.RPC_SERVICE, SpanAttributes.RPC_METHOD, AWS_LAMBDA_FUNCTION_NAME],
            [None, None, None],
            keys,
            values,
        )

        keys, values = self._mock_attribute([SpanAttributes.RPC_SYSTEM], [None], keys, values)

    def test_bedrock_agentcore_browser_resource_attributes(self):
        """Test Bedrock AgentCore browser resource attributes."""

        # Test managed browser resource type when browser ID is aws.browser.v1
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_BROWSER_ID],
            attribute_values=["aws.browser.v1"],
            expected_type="AWS::BedrockAgentCore::Browser",
            expected_identifier="aws.browser.v1",
            expected_cfn_primary_identifier="aws.browser.v1",
        )
        # Test custom browser resource type when browser ID is not the standard aws.browser.v1
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_BROWSER_ID],
            attribute_values=["testBrowser-1234567890"],
            expected_type="AWS::BedrockAgentCore::BrowserCustom",
            expected_identifier="testBrowser-1234567890",
            expected_cfn_primary_identifier="testBrowser-1234567890",
        )
        # Test custom browser resource type when browser ARN is provided
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_BROWSER_ARN],
            attribute_values=["arn:aws:bedrock-agentcore:us-east-1:123456789012:browser/testBrowser-1234567890"],
            expected_type="AWS::BedrockAgentCore::BrowserCustom",
            expected_identifier="testBrowser-1234567890",
            expected_cfn_primary_identifier="testBrowser-1234567890",
        )
        # Test managed browser resource type when browser ARN contains aws.browser.v1
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_BROWSER_ARN],
            attribute_values=["arn:aws:bedrock-agentcore:us-east-1:123456789012:browser/aws.browser.v1"],
            expected_type="AWS::BedrockAgentCore::Browser",
            expected_identifier="aws.browser.v1",
            expected_cfn_primary_identifier="aws.browser.v1",
        )

    def test_bedrock_agentcore_gateway_resource_attributes(self):
        """Test Bedrock AgentCore gateway resource attributes."""

        # Test managed gateway resource: when both gateway ID and target ID are present, both resource and
        # CFN identifiers should be set to the gateway ID
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_GATEWAY_ID, AWS_GATEWAY_TARGET_ID],
            attribute_values=["agentGateway-123456789", "target-123456789"],
            expected_type="AWS::BedrockAgentCore::GatewayTarget",
            expected_identifier="agentGateway-123456789",
            expected_cfn_primary_identifier="agentGateway-123456789",
        )
        # Test gateway resource with ARN and target ID: both resource and CFN identifiers should be set to
        # the extracted gateway ID from ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_GATEWAY_ARN, AWS_GATEWAY_TARGET_ID],
            attribute_values=[
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:gateway/gateway-from-arn",
                "target-123456789",
            ],
            expected_type="AWS::BedrockAgentCore::GatewayTarget",
            expected_identifier="gateway-from-arn",
            expected_cfn_primary_identifier="gateway-from-arn",
        )
        # Test gateway target resource: when only target ID is present, both identifiers should be set to
        # the target ID
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_GATEWAY_TARGET_ID],
            attribute_values=["target-only-123456789"],
            expected_type="AWS::BedrockAgentCore::GatewayTarget",
            expected_identifier="target-only-123456789",
            expected_cfn_primary_identifier="target-only-123456789",
        )
        # Test gateway resource with ID only: both identifiers should be set to the gateway ID
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_GATEWAY_ID],
            attribute_values=["agentGateway-123456789"],
            expected_type="AWS::BedrockAgentCore::Gateway",
            expected_identifier="agentGateway-123456789",
            expected_cfn_primary_identifier="agentGateway-123456789",
        )
        # Test gateway resource with ARN only: both identifiers should be set to the extracted gateway ID
        # from ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_GATEWAY_ARN],
            attribute_values=["arn:aws:bedrock-agentcore:us-east-1:123456789012:gateway/gateway-arn-only"],
            expected_type="AWS::BedrockAgentCore::Gateway",
            expected_identifier="gateway-arn-only",
            expected_cfn_primary_identifier="gateway-arn-only",
        )

    def test_bedrock_agentcore_memory_resource_attributes(self):
        """Test Bedrock AgentCore memory resource attributes."""

        # Test memory resource with both ID and ARN: resource identifier should be set to the memory ID
        # and CFN primary identifier should be set to the memory ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_MEMORY_ID, AWS_BEDROCK_AGENTCORE_MEMORY_ARN],
            attribute_values=[
                "agentMemory-123456789",
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:memory/agentMemory-123456789",
            ],
            expected_type="AWS::BedrockAgentCore::Memory",
            expected_identifier="agentMemory-123456789",
            expected_cfn_primary_identifier=(
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:memory/agentMemory-123456789"
            ),
        )
        # Test memory resource with ARN only: resource identifier should be set to the extracted memory ID
        # and CFN primary identifier should be set to the memory ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_MEMORY_ARN],
            attribute_values=["arn:aws:bedrock-agentcore:us-east-1:123456789012:memory/memory-arn-only"],
            expected_type="AWS::BedrockAgentCore::Memory",
            expected_identifier="memory-arn-only",
            expected_cfn_primary_identifier=("arn:aws:bedrock-agentcore:us-east-1:123456789012:memory/memory-arn-only"),
        )
        # Test memory resource with ID only: both identifiers should be set to the memory ID
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_MEMORY_ID],
            attribute_values=["memory-id-only"],
            expected_type="AWS::BedrockAgentCore::Memory",
            expected_identifier="memory-id-only",
            expected_cfn_primary_identifier="memory-id-only",
        )
        # Test memory resource with both ID and ARN where ARN contains different ID: memory ID should be prioritized
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_MEMORY_ID, AWS_BEDROCK_AGENTCORE_MEMORY_ARN],
            attribute_values=[
                "direct-memory-id",
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:memory/arn-memory-id",
            ],
            expected_type="AWS::BedrockAgentCore::Memory",
            expected_identifier="direct-memory-id",
            expected_cfn_primary_identifier=("arn:aws:bedrock-agentcore:us-east-1:123456789012:memory/arn-memory-id"),
        )

    def test_bedrock_agentcore_code_interpreter_resource_attributes(self):
        """Test Bedrock AgentCore code interpreter resource attributes."""

        # Test managed code interpreter resource type when code interpreter ID is aws.codeinterpreter.v1
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_CODE_INTERPRETER_ID],
            attribute_values=["aws.codeinterpreter.v1"],
            expected_type="AWS::BedrockAgentCore::CodeInterpreter",
            expected_identifier="aws.codeinterpreter.v1",
            expected_cfn_primary_identifier="aws.codeinterpreter.v1",
        )
        # Test custom code interpreter resource type when code interpreter ID is not the standard
        # aws.codeinterpreter.v1
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_CODE_INTERPRETER_ID],
            attribute_values=["testCodeInt-1234567890"],
            expected_type="AWS::BedrockAgentCore::CodeInterpreterCustom",
            expected_identifier="testCodeInt-1234567890",
            expected_cfn_primary_identifier="testCodeInt-1234567890",
        )
        # Test custom code interpreter resource type when code interpreter ARN is provided
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_CODE_INTERPRETER_ARN],
            attribute_values=[
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:code-interpreter/" "testCodeInt-1234567890"
            ],
            expected_type="AWS::BedrockAgentCore::CodeInterpreterCustom",
            expected_identifier="testCodeInt-1234567890",
            expected_cfn_primary_identifier="testCodeInt-1234567890",
        )
        # Test managed code interpreter resource type when code interpreter ARN contains
        # aws.codeinterpreter.v1
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_CODE_INTERPRETER_ARN],
            attribute_values=[
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:code-interpreter/" "aws.codeinterpreter.v1"
            ],
            expected_type="AWS::BedrockAgentCore::CodeInterpreter",
            expected_identifier="aws.codeinterpreter.v1",
            expected_cfn_primary_identifier="aws.codeinterpreter.v1",
        )

    def test_bedrock_agentcore_identity_resource_attributes(self):
        """Test Bedrock AgentCore identity resource attributes."""

        # Test all OAuth and API key methods
        oauth_methods = [
            "CreateOauth2CredentialProvider",
            "DeleteOauth2CredentialProvider",
            "GetOauth2CredentialProvider",
            "ListOauth2CredentialProviders",
            "UpdateOauth2CredentialProvider",
            "GetResourceOauth2Token",
        ]

        apikey_methods = [
            "CreateApiKeyCredentialProvider",
            "DeleteApiKeyCredentialProvider",
            "GetApiKeyCredentialProvider",
            "ListApiKeyCredentialProviders",
            "UpdateApiKeyCredentialProvider",
            "GetResourceApiKey",
        ]

        for method in oauth_methods:
            self.validate_bedrock_agentcore_resource(
                attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
                attribute_values=["test-oauth2-provider", method],
                expected_type="AWS::BedrockAgentCore::OAuth2CredentialProvider",
                expected_identifier="test-oauth2-provider",
                expected_cfn_primary_identifier="test-oauth2-provider",
            )

        for method in apikey_methods:
            self.validate_bedrock_agentcore_resource(
                attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
                attribute_values=["test-apikey-provider", method],
                expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
                expected_identifier="test-apikey-provider",
                expected_cfn_primary_identifier="test-apikey-provider",
            )

        # Test API Key credential provider with ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=[
                "arn:aws:acps:us-east-1:123456789012:token-vault/my-vault/apikeycredentialprovider/my-api-key-provider",
                "GetResourceApiKey",
            ],
            expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
            expected_identifier="my-api-key-provider",
            expected_cfn_primary_identifier="my-api-key-provider",
        )

        # Test OAuth2 credential provider with ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=[
                "arn:aws:acps:us-east-1:123456789012:token-vault/my-vault/oauth2credentialprovider/my-oauth2-provider",
                "GetResourceOauth2Token",
            ],
            expected_type="AWS::BedrockAgentCore::OAuth2CredentialProvider",
            expected_identifier="my-oauth2-provider",
            expected_cfn_primary_identifier="my-oauth2-provider",
        )

        # Test malformed ARN (insufficient parts) falls back to original string
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["arn:aws:incomplete", "GetResourceApiKey"],
            expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
            expected_identifier="arn:aws:incomplete",
            expected_cfn_primary_identifier="arn:aws:incomplete",
        )

        # Test ARN with invalid account ID falls back to original string
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=[
                "arn:aws:acps:us-east-1:invalid-account:token-vault/my-vault/apikeycredentialprovider/test-provider",
                "GetResourceApiKey",
            ],
            expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
            expected_identifier=(
                "arn:aws:acps:us-east-1:invalid-account:token-vault/my-vault/apikeycredentialprovider/test-provider"
            ),
            expected_cfn_primary_identifier=(
                "arn:aws:acps:us-east-1:invalid-account:token-vault/my-vault/apikeycredentialprovider/test-provider"
            ),
        )

        # Test API Key credential provider with name only (no ARN)
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["my-apikeycredentialprovider-name", "GetResourceApiKey"],
            expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
            expected_identifier="my-apikeycredentialprovider-name",
            expected_cfn_primary_identifier="my-apikeycredentialprovider-name",
        )

        # Test OAuth2 credential provider with name only (no ARN)
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["my-oauth2credentialprovider-name", "GetResourceOauth2Token"],
            expected_type="AWS::BedrockAgentCore::OAuth2CredentialProvider",
            expected_identifier="my-oauth2credentialprovider-name",
            expected_cfn_primary_identifier="my-oauth2credentialprovider-name",
        )

        # Test API Key detection from RPC method
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["test-provider-123", "GetResourceApiKey"],
            expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
            expected_identifier="test-provider-123",
            expected_cfn_primary_identifier="test-provider-123",
        )

        # Test OAuth2 detection from RPC method
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["test-oauth2-provider-123", "GetResourceOauth2Token"],
            expected_type="AWS::BedrockAgentCore::OAuth2CredentialProvider",
            expected_identifier="test-oauth2-provider-123",
            expected_cfn_primary_identifier="test-oauth2-provider-123",
        )

        # Test malformed ARN-like string (starts with "arn" but invalid format)
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["arn:abc:123", "GetResourceApiKey"],
            expected_type="AWS::BedrockAgentCore::APIKeyCredentialProvider",
            expected_identifier="arn:abc:123",
            expected_cfn_primary_identifier="arn:abc:123",
        )

        # Test empty ARN string
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["", "GetResourceOauth2Token"],
            expected_type=None,
            expected_identifier=None,
            expected_cfn_primary_identifier=None,
        )

        # Test missing RPC method should not set resource type
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=["test-provider", None],
            expected_type=None,
            expected_identifier=None,
            expected_cfn_primary_identifier=None,
        )

        # Test missing credentials provider should not set resource type
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_AUTH_CREDENTIAL_PROVIDER, SpanAttributes.RPC_METHOD],
            attribute_values=[None, "GetResourceApiKey"],
            expected_type=None,
            expected_identifier=None,
            expected_cfn_primary_identifier=None,
        )

    def test_bedrock_agentcore_runtime_resource_attributes(self):
        """Test Bedrock AgentCore runtime resource attributes."""
        # Test runtime endpoint resource with all attributes: resource identifier should be set to extracted
        # endpoint ID and CFN primary identifier should be set to endpoint ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[
                GEN_AI_RUNTIME_ID,
                AWS_BEDROCK_AGENTCORE_RUNTIME_ARN,
                AWS_BEDROCK_AGENTCORE_RUNTIME_ENDPOINT_ARN,
            ],
            attribute_values=[
                "test-runtime-123",
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/test-runtime-123",
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime-endpoint/test-endpoint",
            ],
            expected_type="AWS::BedrockAgentCore::RuntimeEndpoint",
            expected_identifier="test-endpoint",
            expected_cfn_primary_identifier=(
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime-endpoint/test-endpoint"
            ),
        )
        # Test runtime endpoint resource with runtime ID and endpoint ARN: resource identifier should be
        # set to extracted endpoint ID and CFN primary identifier should be set to endpoint ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_RUNTIME_ID, AWS_BEDROCK_AGENTCORE_RUNTIME_ENDPOINT_ARN],
            attribute_values=[
                "test-runtime-123",
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime-endpoint/test-endpoint",
            ],
            expected_type="AWS::BedrockAgentCore::RuntimeEndpoint",
            expected_identifier="test-endpoint",
            expected_cfn_primary_identifier=(
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime-endpoint/test-endpoint"
            ),
        )
        # Test runtime endpoint resource with runtime ARN and endpoint ARN: resource identifier should be
        # set to extracted endpoint ID and CFN primary identifier should be set to endpoint ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_RUNTIME_ARN, AWS_BEDROCK_AGENTCORE_RUNTIME_ENDPOINT_ARN],
            attribute_values=[
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/test-runtime-123",
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime-endpoint/test-endpoint",
            ],
            expected_type="AWS::BedrockAgentCore::RuntimeEndpoint",
            expected_identifier="test-endpoint",
            expected_cfn_primary_identifier=(
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime-endpoint/test-endpoint"
            ),
        )
        # Test runtime resource with ID only: both identifiers should be set to the runtime ID
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[GEN_AI_RUNTIME_ID],
            attribute_values=["test-runtime-123"],
            expected_type="AWS::BedrockAgentCore::Runtime",
            expected_identifier="test-runtime-123",
            expected_cfn_primary_identifier="test-runtime-123",
        )
        # Test runtime resource with ARN only: both identifiers should be set to the extracted
        # runtime ID from ARN
        self.validate_bedrock_agentcore_resource(
            attribute_keys=[AWS_BEDROCK_AGENTCORE_RUNTIME_ARN],
            attribute_values=["arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/test-runtime-123"],
            expected_type="AWS::BedrockAgentCore::Runtime",
            expected_identifier="test-runtime-123",
            expected_cfn_primary_identifier="test-runtime-123",
        )
