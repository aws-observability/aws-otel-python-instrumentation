# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import INFO, Logger, getLogger
from typing import Dict, List

from docker.types import EndpointConfig
from requests import request, Response
from testcontainers.localstack import LocalStackContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME, ContractTestBase

_logger: Logger = getLogger(__name__)
_logger.setLevel(INFO)


class BotocoreTest(ContractTestBase):
    _local_stack: LocalStackContainer

    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {
            "AWS_SDK_S3_ENDPOINT": "http://s3.localstack:4566",
            "AWS_SDK_ENDPOINT": "http://localstack:4566",
            "AWS_REGION": "us-west-2"
        }

    @override
    def get_application_network_aliases(self) -> List[str]:
        return ["error-bucket.s3.test", "fault-bucket.s3.test", "error.test", "fault.test"]

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
                    "create-bucket.s3.localstack",
                    "put-object.s3.localstack",
                    "get-object.s3.localstack",
                ],
            )
        }
        cls._local_stack: LocalStackContainer = (
            LocalStackContainer()
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
        self._make_request("s3/createbucket/create-bucket")

    def test_s3_create_object(self):
        self._make_request("s3/createobject/put-object/some-object")

    def test_s3_get_object(self):
        self._make_request("s3/getobject/get-object/some-object")

    def test_s3_error(self):
        self._make_request("s3/error")

    def test_s3_fault(self):
        self._make_request("s3/fault")

    def test_dynamodb_create_table(self):
        self._make_request("ddb/createtable/some-table")

    def test_dynamodb_put_item(self):
            self._make_request("ddb/putitem/putitem-table/key")

    def test_dynamodb_error(self):
        self._make_request("ddb/error")

    def test_dynamodb_fault(self):
        self._make_request("ddb/fault")

    def test_sqs_create_queue(self):
        self._make_request("sqs/createqueue/some-queue")

    def test_sqs_send_message(self):
        self._make_request("sqs/publishqueue/some-queue")

    def test_sqs_receive_message(self):
        self._make_request("sqs/consumequeue/some-queue")

    def test_sqs_error(self):
        self._make_request("sqs/error")

    def test_sqs_fault(self):
        self._make_request("sqs/fault")

    def test_kinesis_put_record(self):
        self._make_request("kinesis/putrecord/my-stream")

    def test_kinesis_error(self):
        self._make_request("kinesis/error")

    def test_kinesis_fault(self):
        self._make_request("kinesis/fault")

    def _make_request(self, path: str) -> Response:
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/{path}"
        return request("GET", url, timeout=20)
