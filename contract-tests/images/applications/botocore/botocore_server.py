# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import os
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import boto3
import requests
from botocore.client import BaseClient
from botocore.config import Config
from typing_extensions import override

_PORT: int = 8080
_ERROR: str = "error"
_FAULT: str = "fault"

_AWS_SDK_S3_ENDPOINT: str = os.environ.get("AWS_SDK_S3_ENDPOINT")
_AWS_SDK_ENDPOINT: str = os.environ.get("AWS_SDK_ENDPOINT")
_AWS_REGION: str = os.environ.get("AWS_REGION")
_ERROR_ENDPOINT: str = "http://error.test:8080"
_FAULT_ENDPOINT: str = "http://fault.test:8080"
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testcontainers-localstack")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testcontainers-localstack")
_NO_RETRY_CONFIG: Config = Config(retries={"max_attempts": 0}, connect_timeout=3, read_timeout=3)


# pylint: disable=broad-exception-caught
class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if self.in_path("s3"):
            self._handle_s3_request()
        if self.in_path("ddb"):
            self._handle_ddb_request()
        if self.in_path("sqs"):
            self._handle_sqs_request()
        if self.in_path("kinesis"):
            self._handle_kinesis_request()

        self._end_request(self.main_status)

    # pylint: disable=invalid-name
    def do_POST(self):
        if self.in_path("sqserror"):
            self.send_response(400)
            self.send_header("Content-type", "text/xml")
            self.end_headers()

            xml_response = """<?xml version="1.0"?>
                            <ErrorResponse>
                                <Error>
                                    <Type>Sender</Type>
                                    <Code>InvalidAction</Code>
                                    <Message>The action or operation requested is invalid.</Message>
                                    <Detail/>
                                </Error>
                            </ErrorResponse>"""

            self.wfile.write(xml_response.encode())
        else:
            self._end_request(self.main_status)

    # pylint: disable=invalid-name
    def do_PUT(self):
        self._end_request(self.main_status)

    def in_path(self, sub_path: str) -> bool:
        return sub_path in self.path

    def _handle_s3_request(self) -> None:
        s3_client: BaseClient = boto3.client("s3", endpoint_url=_AWS_SDK_S3_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_ERROR):
            error_client: BaseClient = boto3.client("s3", endpoint_url=_ERROR_ENDPOINT, region_name=_AWS_REGION)
            set_main_status(400)
            try:
                error_client.create_bucket(Bucket="-")
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client: BaseClient = boto3.client(
                    "s3", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION, config=_NO_RETRY_CONFIG
                )
                fault_client.create_bucket(Bucket="valid-bucket-name")
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("createbucket/create-bucket"):
            set_main_status(200)
            s3_client.create_bucket(
                Bucket="test-bucket-name", CreateBucketConfiguration={"LocationConstraint": _AWS_REGION}
            )
        elif self.in_path("createobject/put-object/some-object"):
            set_main_status(200)
            with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                temp_file_name: str = temp_file.name
                temp_file.write(b"This is temp file for S3 upload")
                temp_file.flush()
                s3_client.upload_file(temp_file_name, "test-put-object-bucket-name", "test_object")
        elif self.in_path("getobject/get-object/some-object"):
            set_main_status(200)
            s3_client.get_object(Bucket="test-get-object-bucket-name", Key="test_object")
        else:
            set_main_status(404)

    def _handle_ddb_request(self) -> None:
        ddb_client = boto3.client("dynamodb", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_ERROR):
            set_main_status(400)
            error_client = boto3.client("dynamodb", endpoint_url=_ERROR_ENDPOINT, region_name=_AWS_REGION)
            item: dict = {"id": {"S": "1"}}
            try:
                error_client.put_item(TableName="invalid_table", Item=item)
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            item: dict = {"id": {"S": "1"}}
            try:
                fault_client = boto3.client(
                    "dynamodb", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION, config=_NO_RETRY_CONFIG
                )
                fault_client.put_item(TableName="invalid_table", Item=item)
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("createtable/some-table"):
            set_main_status(200)
            ddb_client.create_table(
                TableName="test_table",
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        elif self.in_path("putitem/putitem-table/key"):
            set_main_status(200)
            item: dict = {"id": {"S": "1"}}
            ddb_client.put_item(TableName="put_test_table", Item=item)
        else:
            set_main_status(404)

    def _handle_sqs_request(self) -> None:
        sqs_client = boto3.client("sqs", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_ERROR):
            set_main_status(400)
            try:
                error_client = boto3.client("sqs", endpoint_url=_ERROR_ENDPOINT + "/sqserror", region_name=_AWS_REGION)
                error_client.send_message(QueueUrl="http://error.test:8080/sqserror", MessageBody=_ERROR)
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client = boto3.client(
                    "sqs", endpoint_url=_FAULT_ENDPOINT, region_name="us-west-2", config=_NO_RETRY_CONFIG
                )
                fault_client.create_queue(QueueName="invalid_test")
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("createqueue/some-queue"):
            set_main_status(200)
            sqs_client.create_queue(QueueName="test_queue")
        elif self.in_path("publishqueue/some-queue"):
            set_main_status(200)
            sqs_client.send_message(
                QueueUrl="http://localstack:4566/000000000000/test_put_get_queue", MessageBody="test_message"
            )
        elif self.in_path("consumequeue/some-queue"):
            set_main_status(200)
            sqs_client.receive_message(
                QueueUrl="http://localstack:4566/000000000000/test_put_get_queue", MaxNumberOfMessages=1
            )
        else:
            set_main_status(404)

    def _handle_kinesis_request(self) -> None:
        kinesis_client = boto3.client("kinesis", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_ERROR):
            set_main_status(400)
            try:
                error_client = boto3.client("kinesis", endpoint_url=_ERROR_ENDPOINT, region_name=_AWS_REGION)
                error_client.put_record(StreamName="invalid_stream", Data=b"test", PartitionKey="partition_key")
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client = boto3.client(
                    "kinesis", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION, config=_NO_RETRY_CONFIG
                )
                fault_client.put_record(StreamName="test_stream", Data=b"test", PartitionKey="partition_key")
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("putrecord/my-stream"):
            set_main_status(200)
            kinesis_client.put_record(StreamName="test_stream", Data=b"test", PartitionKey="partition_key")
        else:
            set_main_status(404)

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


def prepare_aws_server() -> None:
    requests.Request(method="POST", url="http://localhost:4566/_localstack/state/reset")
    try:
        # Set up S3 so tests can access buckets and retrieve a file.
        s3_client: BaseClient = boto3.client("s3", endpoint_url=_AWS_SDK_S3_ENDPOINT, region_name=_AWS_REGION)
        s3_client.create_bucket(
            Bucket="test-put-object-bucket-name", CreateBucketConfiguration={"LocationConstraint": _AWS_REGION}
        )
        s3_client.create_bucket(
            Bucket="test-get-object-bucket-name", CreateBucketConfiguration={"LocationConstraint": _AWS_REGION}
        )
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            temp_file_name: str = temp_file.name
            temp_file.write(b"This is temp file for S3 upload")
            temp_file.flush()
            s3_client.upload_file(temp_file_name, "test-get-object-bucket-name", "test_object")

        # Set up DDB so tests can access a table.
        ddb_client: BaseClient = boto3.client("dynamodb", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        ddb_client.create_table(
            TableName="put_test_table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Set up SQS so tests can access a queue.
        sqs_client: BaseClient = boto3.client("sqs", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        sqs_client.create_queue(QueueName="test_put_get_queue")

        # Set up Kinesis so tests can access a stream.
        kinesis_client: BaseClient = boto3.client("kinesis", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        kinesis_client.create_stream(StreamName="test_stream", ShardCount=1)
    except Exception as exception:
        print("Unexpected exception occurred", exception)


def main() -> None:
    prepare_aws_server()
    server_address: tuple[str, int] = ("0.0.0.0", _PORT)
    request_handler_class: type = RequestHandler
    requests_server: ThreadingHTTPServer = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(requests_server.shutdown)
    server_thread: Thread = Thread(target=requests_server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()


if __name__ == "__main__":
    main()
