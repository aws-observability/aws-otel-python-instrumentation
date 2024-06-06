# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import json
import os
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import List

import boto3
import requests
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
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
        if self.in_path("secretsmanager"):
            self._handle_secretsmanager_request()
        if self.in_path("stepfunctions"):
            self._handle_stepsfunction_request()

        self._end_request(self.main_status)

    # pylint: disable=invalid-name
    def do_POST(self):
        if self.in_path("sqserror"):
            self.send_response(self.main_status)
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
                error_client.send_message(QueueUrl="http://error.test:8080/000000000000/sqserror", MessageBody=_ERROR)
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client = boto3.client(
                    "sqs", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION, config=_NO_RETRY_CONFIG
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

    def _handle_secretsmanager_request(self) -> None:
        secretsmanager_client = boto3.client("secretsmanager", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_ERROR):
            set_main_status(400)
            try:
                error_client = boto3.client("secretsmanager", endpoint_url=_ERROR_ENDPOINT, region_name=_AWS_REGION)
                error_client.describe_secret(
                    SecretId="arn:aws:secretsmanager:us-west-2:000000000000:secret:unExistSecret"
                )
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client = boto3.client(
                    "secretsmanager", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION, config=_NO_RETRY_CONFIG
                )
                fault_client.get_secret_value(
                    SecretId="arn:aws:secretsmanager:us-west-2:000000000000:secret:nonexistent-secret"
                )
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("describesecret/my-secret"):
            set_main_status(200)
            secretsmanager_client.describe_secret(SecretId="testSecret")
        else:
            set_main_status(404)

    def _handle_stepsfunction_request(self) -> None:
        sfn_client = boto3.client("stepfunctions", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_ERROR):
            set_main_status(400)
            try:
                error_client = boto3.client("stepfunctions", endpoint_url=_ERROR_ENDPOINT, region_name=_AWS_REGION)
                error_client.describe_state_machine(
                    stateMachineArn="arn:aws:states:us-west-2:000000000000:stateMachine:unExistStateMachine"
                )
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client = boto3.client(
                    "stepfunctions", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION, config=_NO_RETRY_CONFIG
                )
                fault_client.meta.events.register(
                    "before-call.stepfunctions.ListStateMachineVersions",
                    lambda **kwargs: inject_500_error("ListStateMachineVersions", **kwargs),
                )
                fault_client.list_state_machine_versions(
                    stateMachineArn="arn:aws:states:us-west-2:000000000000:stateMachine:invalid-state-machine",
                )
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("describestatemachine/my-state-machine"):
            set_main_status(200)
            sfn_client.describe_state_machine(
                stateMachineArn="arn:aws:states:us-west-2:000000000000:stateMachine:testStateMachine"
            )
        else:
            set_main_status(404)

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


# pylint: disable=too-many-locals, too-many-statements
def prepare_aws_server() -> None:
    requests.Request(method="POST", url="http://localhost:4566/_localstack/state/reset")
    try:
        # Set up S3 so tests can access buckets and retrieve a file.
        s3_client: BaseClient = boto3.client("s3", endpoint_url=_AWS_SDK_S3_ENDPOINT, region_name=_AWS_REGION)
        bucket_names: List[str] = [bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]]
        put_bucket_name: str = "test-put-object-bucket-name"
        if put_bucket_name not in bucket_names:
            s3_client.create_bucket(
                Bucket=put_bucket_name, CreateBucketConfiguration={"LocationConstraint": _AWS_REGION}
            )

        get_bucket_name: str = "test-get-object-bucket-name"
        if get_bucket_name not in bucket_names:
            s3_client.create_bucket(
                Bucket=get_bucket_name, CreateBucketConfiguration={"LocationConstraint": _AWS_REGION}
            )
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            temp_file_name: str = temp_file.name
            temp_file.write(b"This is temp file for S3 upload")
            temp_file.flush()
            s3_client.upload_file(temp_file_name, "test-get-object-bucket-name", "test_object")

        # Set up DDB so tests can access a table.
        ddb_client: BaseClient = boto3.client("dynamodb", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        table_names: List[str] = ddb_client.list_tables()["TableNames"]

        table_name: str = "put_test_table"
        if table_name not in table_names:
            ddb_client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

        # Set up SQS so tests can access a queue.
        sqs_client: BaseClient = boto3.client("sqs", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        queue_name: str = "test_put_get_queue"
        queues_response = sqs_client.list_queues(QueueNamePrefix=queue_name)
        queues: List[str] = queues_response["QueueUrls"] if "QueueUrls" in queues_response else []
        if not queues:
            sqs_client.create_queue(QueueName=queue_name)

        # Set up Kinesis so tests can access a stream.
        kinesis_client: BaseClient = boto3.client("kinesis", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        stream_name: str = "test_stream"
        stream_response = kinesis_client.list_streams()
        if not stream_response["StreamNames"]:
            kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
            kinesis_client.register_stream_consumer(
                StreamARN="arn:aws:kinesis:us-west-2:000000000000:stream/" + stream_name, ConsumerName="test_consumer"
            )

        # Set up Secrets Manager so tests can access a stream.
        secretsmanager_client = boto3.client("secretsmanager", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)

        secretsmanager_response = secretsmanager_client.list_secrets()
        secret = next((s for s in secretsmanager_response["SecretList"] if s["Name"] == "testSecret"), None)
        if not secret:
            secretsmanager_client.create_secret(
                Name="testSecret", SecretString="secretValue", Description="This is a test secret"
            )

        # Set up IAM and create a role so StepFunctions use it to create a state machine.
        iam_client = boto3.client("iam", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        role_name = "StepFunctionsExecutionTestRole"
        iam_response = iam_client.list_roles()
        role = next((r for r in iam_response["Roles"] if r["RoleName"] == role_name), None)
        if not role:
            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
                ],
            }
            iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role_policy))
            iam_client.attach_role_policy(
                RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
            )

        # Set up StepFucntion so tests can access a state machine.
        sfn_client = boto3.client("stepfunctions", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        state_machine_name = "testStateMachine"
        state_machine_response = sfn_client.list_state_machines()
        state_machine = next(
            (st for st in state_machine_response["stateMachines"] if st["name"] == state_machine_name), None
        )
        if not state_machine:
            definition = {
                "Comment": "A simple AWS Step Functions state machine",
                "StartAt": "SimpleState",
                "States": {"SimpleState": {"Type": "Pass", "Result": "Hello, State Machine!", "End": True}},
            }

            sfn_client.create_state_machine(
                name=state_machine_name,
                definition=json.dumps(definition),
                roleArn="arn:aws:iam::000000000000:role/StepFunctionsExecutionTestRole",
            )
            # arn:aws:states:us-west-2:000000000000:stateMachine:testStateMachine
    except Exception as exception:
        print("Unexpected exception occurred", exception)


def inject_500_error(api_name, **kwargs):
    raise ClientError(
        {
            "Error": {"Code": "InternalServerError", "Message": "Internal Server Error"},
            "ResponseMetadata": {"HTTPStatusCode": 500, "RequestId": "mock-request-id"},
        },
        api_name,
    )


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
