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
_NETWORK_ALIAS: str = "backend"
_SUCCESS: str = "success"
_ERROR: str = "error"
_FAULT: str = "fault"

_AWS_SDK_S3_ENDPOINT: str = os.environ.get("AWS_SDK_S3_ENDPOINT")
_AWS_SDK_ENDPOINT: str = os.environ.get("AWS_SDK_ENDPOINT")
_AWS_REGION: str = os.environ.get("AWS_REGION")
_NO_RETRY_CONFIG: Config = Config(retries={'max_attempts': 0}, connect_timeout=3, read_timeout=3)


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
        else:
            self._end_request(404)

    def in_path(self, sub_path: str) -> bool:
        return sub_path in self.path

    def _handle_s3_request(self) -> None:
        s3_client: BaseClient = boto3.client('s3', endpoint_url=_AWS_SDK_S3_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("error"):
            try:
                s3_client.create_bucket(Bucket="-")
            except Exception as exception:
                print("Exception occured", exception)
            set_main_status(400)
        elif self.in_path("fault"):
            try:
                s3_client: BaseClient = boto3.client('s3', endpoint_url="http://s3.test:8080", region_name='us-west-2',
                                                     config=_NO_RETRY_CONFIG)
                s3_client.create_bucket(Bucket="valid-bucket-name")
            except Exception as exception:
                print("Exception occured", exception)
            set_main_status(500)
        elif self.in_path("createbucket/create-bucket"):
            s3_client.create_bucket(Bucket="test-bucket-name", CreateBucketConfiguration={
                'LocationConstraint': _AWS_REGION})
            set_main_status(200)
        elif self.in_path("createobject/put-object/some-object"):
            with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                temp_file_name: str = temp_file.name
                temp_file.write(b'This is temp file for S3 upload')
                temp_file.flush()
                s3_client.upload_file(temp_file_name, "test-put-object-bucket-name", "test_object")
            set_main_status(200)
        elif self.in_path("getobject/get-object/some-object"):
            s3_client.get_object(Bucket="test-get-object-bucket-name", Key="test_object")
            set_main_status(200)
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _handle_ddb_request(self) -> None:
        ddb_client = boto3.client('dynamodb', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("error"):
            item: dict = {
                'id': {
                    'S': '1'
                }
            }
            try:
                ddb_client.put_item(TableName='invalid_table', Item=item)
            except Exception as exception:
                print("Exception occured", exception)
            finally:
                set_main_status(400)
        elif self.in_path("fault"):
            item: dict = {
                'id': {
                    'S': '1'
                }
            }
            try:
                ddb_client = boto3.client('dynamodb', endpoint_url="http://ddb.test:8080", region_name="us-west-2",
                                          config=_NO_RETRY_CONFIG)
                ddb_client.put_item(TableName='invalid_table', Item=item)
            except Exception as exception:
                print("Exception occured", exception)
            finally:
                set_main_status(500)
        elif self.in_path("createtable/some-table"):
            ddb_client.create_table(
                TableName="test_table",
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'S'
                    },
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            set_main_status(200)
        elif self.in_path("putitem/putitem-table/key"):
            item: dict = {
                'id': {
                    'S': '1'
                }
            }
            ddb_client.put_item(TableName='put_test_table', Item=item)
            set_main_status(200)
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _handle_sqs_request(self) -> None:
        sqs_client = boto3.client('sqs', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("error"):
            try:
                sqs_client.receive_message(QueueUrl="invalid_url", MaxNumberOfMessages=1)
            except Exception as exception:
                print("Exception occured", exception)
            finally:
                set_main_status(400)
        elif self.in_path("fault"):
            try:
                sqs_client = boto3.client('sqs', endpoint_url="http://sqs.test:8080", region_name="us-west-2",
                                          config=_NO_RETRY_CONFIG)
                sqs_client.create_queue(QueueName="invalid_test")
            except Exception as exception:
                print("Exception occured", exception)
            finally:
                set_main_status(500)
        elif self.in_path("createqueue/some-queue"):
            sqs_client.create_queue(QueueName="test_queue")
            set_main_status(200)
        elif self.in_path("publishqueue/some-queue"):
            sqs_client.send_message(QueueUrl='http://localstack:4566/000000000000/test_put_get_queue',
                                    MessageBody="test_message")
            set_main_status(200)
        elif self.in_path("sqs/consumequeue/some-queue"):
            sqs_client.receive_message(QueueUrl='http://localstack:4566/000000000000/test_put_get_queue',
                                       MaxNumberOfMessages=1)
            set_main_status(200)
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _handle_kinesis_request(self) -> None:
        kinesis_client = boto3.client('kinesis', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("error"):
            try:
                kinesis_client.put_record(StreamName="invalid_stream", Data=b'test', PartitionKey="partition_key")
            except Exception as exception:
                print("Exception occured", exception)
            finally:
                set_main_status(400)
        elif self.in_path("fault"):
            try:
                kinesis_client = boto3.client('kinesis', endpoint_url="http://kinesis.test:8080",
                                              region_name="us-west-2", config=_NO_RETRY_CONFIG)
                kinesis_client.put_record(StreamName="test_stream", Data=b'test', PartitionKey="partition_key")
            except Exception as exception:
                print("Exception occured", exception)
            finally:
                set_main_status(500)
        elif self.in_path("putrecord/my-stream"):
            kinesis_client.put_record(StreamName="test_stream", Data=b'test', PartitionKey="partition_key")
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


def prepare_aws_server() -> None:
    requests.Request(method='POST', url="http://localhost:4566/_localstack/state/reset")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testcontainers-localstack")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testcontainers-localstack")
    try:
        s3_client: BaseClient = boto3.client('s3', endpoint_url=_AWS_SDK_S3_ENDPOINT, region_name=_AWS_REGION)
        s3_client.create_bucket(Bucket="test-put-object-bucket-name", CreateBucketConfiguration={
            'LocationConstraint': _AWS_REGION})
        s3_client.create_bucket(Bucket="test-get-object-bucket-name", CreateBucketConfiguration={
            'LocationConstraint': _AWS_REGION})
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            temp_file_name: str = temp_file.name
            temp_file.write(b'This is temp file for S3 upload')
            temp_file.flush()
            s3_client.upload_file(temp_file_name, "test-get-object-bucket-name", "test_object")
        ddb_client: BaseClient = boto3.client('dynamodb', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        ddb_client.create_table(
            TableName="put_test_table",
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                },
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        sqs_client: BaseClient = boto3.client('sqs', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        sqs_client.create_queue(QueueName="test_put_get_queue")
        kinesis_client: BaseClient = boto3.client('kinesis', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        kinesis_client.create_stream(StreamName="test_stream", ShardCount=1)
    except Exception as exception:
        print("Exception occur", exception)


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
