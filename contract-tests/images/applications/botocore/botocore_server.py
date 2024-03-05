# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import os
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import boto3
from botocore.client import BaseClient
from typing_extensions import override

_PORT: int = 8080
_NETWORK_ALIAS: str = "backend"
_SUCCESS: str = "success"
_ERROR: str = "error"
_FAULT: str = "fault"

_AWS_SDK_S3_ENDPOINT: str = os.environ.get("AWS_SDK_S3_ENDPOINT")
_AWS_SDK_ENDPOINT: str = os.environ.get("AWS_SDK_ENDPOINT")
_AWS_REGION: str = os.environ.get("AWS_REGION")


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
            s3_client.create_bucket(Bucket="-")
            set_main_status(400)
        elif self.in_path("fault"):
            s3_client: BaseClient = boto3.client('s3', endpoint_url="invalid:12345", region_name='ca-west-1')
            s3_client.create_bucket(Bucket="valid-bucket-name")
            set_main_status(500)
        elif self.in_path("createbucket/create-bucket"):
            print("hit")
            s3_client.create_bucket(Bucket="test-bucket-name", CreateBucketConfiguration={
                'LocationConstraint': _AWS_REGION})
            set_main_status(200)
        elif self.in_path("createobject/put-object/some-object"):
            with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                temp_file_name: str = temp_file.name
                temp_file.write(b'This is temp file for S3 upload')
                temp_file.flush()
                s3_client.upload_file(temp_file_name, "test_put_object_bucket_name", "test_object")
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
                'id': '1'
            }
            table = ddb_client.Table("invalid_table")
            table.put_item(Item=item)
            set_main_status(400)
        if self.in_path("fault"):
            ddb_client = boto3.client('dynamodb', endpoint_url="invalid:12345", region_name="ca-west-1")
            item: dict = {
                'id': '1'
            }
            table = ddb_client.Table("put_test_table")
            table.put_item(Item=item)
            set_main_status(500)
        if self.in_path("createtable/some-table"):
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
        if self.in_path("putitem/putitem-table/key"):
            item: dict = {
                'id': '1'
            }
            table = ddb_client.Table("put_test_table")
            table.put_item(Item=item)
            set_main_status(200)
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _handle_sqs_request(self) -> None:
        sqs_client = boto3.client('sqs', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("error"):
            sqs_client.receive_message(QueueUrl="invalid_url", MaxNumberOfMessages=1)
            set_main_status(400)
        if self.in_path("fault"):
            sqs_client = boto3.client('sqs', endpoint_url="invalid:12345", region_name="ca-west-1")
            sqs_client.create_queue(QueueName="invalid_test")
            set_main_status(500)
        if self.in_path("createqueue/some-queue"):
            sqs_client.create_queue(QueueName="test_queue")
            set_main_status(200)
        if self.in_path("publishqueue/some-queue"):
            queue_url: str = os.environ.get("TEST_SQS_QUEUE_URL", "invalid")
            sqs_client.send_message(QueueUrl=queue_url, MessageBody="test_message")
            set_main_status(200)
        if self.in_path("sqs/consumequeue/some-queue"):
            queue_url: str = os.environ.get("TEST_SQS_QUEUE_URL", "invalid")
            sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _handle_kinesis_request(self) -> None:
        kinesis_client = boto3.client('kinesis', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("error"):
            kinesis_client.put_record(StreamName="invalid_stream", Data=b'test', PartitionKey="partition_key")
        if self.in_path("fault"):
            kinesis_client = boto3.client('kinesis', endpoint_url="invalid_url:12345", region_name="ca-west-1")
            kinesis_client.put_record(StreamName="test_stream", Data=b'test', PartitionKey="partition_key")
        if self.in_path("putrecord/my-stream"):
            kinesis_client.put_record(StreamName="test_stream", Data=b'test', PartitionKey="partition_key")
        else:
            self._end_request(404)
        self._end_request(self.main_status)

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status

def prepare_aws_server()->None:
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
        ddb_client = boto3.client('dynamodb', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
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
        sqs_client = boto3.client('sqs', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        sqs_response = sqs_client.create_queue(QueueName="test_put_get_queue")
        os.environ.setdefault("TEST_SQS_QUEUE_URL", sqs_response['QueueUrl'])
        kinesis_client = boto3.client('kinesis', endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
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

    # if self.in_path("examplebucket"):
    #     print(self.address_string())
    #     print("object")
    #     self._end_request(self.main_status)
    # elif self.in_path("error"):
    #     print("error")
    #     set_main_status(400)
    #     cfg: Config = C
    #     set_main_status(500onfig(retries={"max_attempts": 1})
    #     s3_client = boto3.client("s3", region_name="us-west-2", endpoint_url="http://localhost:8080", config=cfg)
    #     try:
    #         s3_client.get_object(
    #             Bucket="examplebucket",
    #             Key="HappyFace.jpg",
    #         )
    #     except Exception:
    #         pass
    #     self._end_request(400)
    # elif self.in_path("fault"):
    #     print("fault")
    #     set_main_status(500)
    #     cfg: Config = Config(retries={"max_attempts": 1})
    #     s3_client = boto3.client("s3", region_name="us-west-2", endpoint_url="http://fault.test:8080", config=cfg)
    #     try:
    #         s3_client.get_object(
    #             Bucket="examplebucket",
    #             Key="HappyFace.jpg",
    #         )
    #     except Exception:
    #         pass
    #     self._end_request(500)
    # else:
    #     print("general = " + self.path)
    #     s3_client = boto3.client("s3", region_name="us-west-2", endpoint_url="http://s3.localstack:4566")
    #     s3_client.list_buckets()
    #     self._end_request(200)
