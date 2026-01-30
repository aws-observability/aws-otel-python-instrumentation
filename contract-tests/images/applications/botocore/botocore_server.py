# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=too-many-lines
import atexit
import json
import os
import tempfile
from collections import namedtuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import PurePath
from threading import Thread

import boto3
import requests
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from typing_extensions import Tuple, override

_PORT: int = 8080
_ERROR: str = "error"
_FAULT: str = "fault"

_AWS_SDK_S3_ENDPOINT: str = os.environ.get("AWS_SDK_S3_ENDPOINT")
_AWS_SDK_ENDPOINT: str = os.environ.get("AWS_SDK_ENDPOINT")
_AWS_REGION: str = os.environ.get("AWS_REGION")
_AWS_ACCOUNT_ID: str = "123456789012"
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
        if self.in_path("bedrock"):
            if self.in_path("bedrock-agentcore"):
                self._handle_bedrock_agentcore_request()
            else:
                self._handle_bedrock_request()
        if self.in_path("secretsmanager"):
            self._handle_secretsmanager_request()
        if self.in_path("stepfunctions"):
            self._handle_stepfunctions_request()
        if self.in_path("sns"):
            self._handle_sns_request()
        if self.in_path("lambda"):
            self._handle_lambda_request()
        if self.in_path("cross-account"):
            self._handle_cross_account_request()

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

    def _handle_cross_account_request(self) -> None:
        s3_client = boto3.client(
            "s3",
            endpoint_url=_AWS_SDK_S3_ENDPOINT,
            region_name="eu-central-1",
            aws_access_key_id="account_b_access_key_id",
            aws_secret_access_key="account_b_secret_access_key",
            aws_session_token="account_b_token",
        )
        if self.in_path("createbucket/account_b"):
            set_main_status(200)
            s3_client.create_bucket(
                Bucket="cross-account-bucket", CreateBucketConfiguration={"LocationConstraint": "eu-central-1"}
            )
        else:
            set_main_status(404)

    # pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
    def _handle_bedrock_agentcore_request(self) -> None:
        bedrock_agentcore_client = boto3.client(
            "bedrock-agentcore", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION
        )
        bedrock_agentcore_control_client = boto3.client(
            "bedrock-agentcore-control", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION
        )
        # Parse URL structure: /bedrock-agentcore/{service}/{operation}/{resource_id}
        path = PurePath(self.path)
        path_parts = path.parts[1:]  # Remove leading '/'

        service = path_parts[1] if len(path_parts) > 1 else None
        operation = path_parts[2] if len(path_parts) > 2 else None
        resource_id = path_parts[3] if len(path_parts) > 3 else None

        set_main_status(200)
        if service == "runtime":
            agent_id = resource_id
            if operation == "createagentruntime":
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateAgentRuntime",
                    lambda **kwargs: inject_200_success(
                        agentRuntimeArn=f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:runtime/{agent_id}",
                        agentRuntimeId=agent_id,
                        agentRuntimeVersion="1.0",
                        createdAt="2024-01-01T00:00:00Z",
                        status="ACTIVE",
                        workloadIdentityDetails={
                            "workloadIdentityArn": (
                                f"arn:aws:iam::{_AWS_ACCOUNT_ID}:role/service-role/"
                                "AmazonBedrockAgentCoreRuntimeDefaultServiceRole"
                            )
                        },
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_agent_runtime(
                    agentRuntimeName="completeAgent",
                    description="Complete agent with all components",
                    agentRuntimeArtifact={
                        "containerConfiguration": {
                            "containerUri": f"{_AWS_ACCOUNT_ID}.dkr.ecr.{_AWS_REGION}.amazonaws.com/test-agent:latest"
                        }
                    },
                    roleArn=(
                        f"arn:aws:iam::{_AWS_ACCOUNT_ID}:role/service-role/"
                        "AmazonBedrockAgentCoreRuntimeDefaultServiceRole"
                    ),
                    networkConfiguration={"networkMode": "PUBLIC"},
                    protocolConfiguration={"serverProtocol": "HTTP"},
                )
                return
            if operation == "createendpoint":
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateAgentRuntimeEndpoint",
                    lambda **kwargs: inject_200_success(
                        agentRuntimeArn=f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:runtime/{agent_id}",
                        agentRuntimeEndpointArn=(
                            f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:endpoint/invokeEndpoint"
                        ),
                        agentRuntimeId=agent_id,
                        createdAt="2024-01-01T00:00:00Z",
                        endpointName="invokeEndpoint",
                        status="ACTIVE",
                        targetVersion="1.0",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_agent_runtime_endpoint(
                    agentRuntimeId=agent_id,
                    name="invokeEndpoint",
                    description="Endpoint for invoking agent runtime",
                )
                return
            if operation == "invokeagentruntime":
                bedrock_agentcore_client.meta.events.register(
                    "before-call.bedrock-agentcore.InvokeAgentRuntime",
                    inject_200_success,
                )
                bedrock_agentcore_client.invoke_agent_runtime(
                    agentRuntimeArn=f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:runtime/{agent_id}",
                    payload=b'{"message": "Hello, test message"}',
                )
                return
        if service == "browser":
            if operation == "startbrowsersession":
                browser_id = resource_id
                bedrock_agentcore_client.meta.events.register(
                    "before-call.bedrock-agentcore.StartBrowserSession",
                    lambda **kwargs: inject_200_success(
                        browserIdentifier=browser_id,
                        createdAt="2024-01-01T00:00:00Z",
                        sessionId="testBrowserSession",
                        streams={
                            "automationStream": {
                                "streamEndpoint": "wss://example.com/automation",
                                "streamStatus": "ENABLED",
                            },
                            "liveViewStream": {"streamEndpoint": "wss://example.com/liveview"},
                        },
                        **kwargs,
                    ),
                )
                bedrock_agentcore_client.start_browser_session(
                    browserIdentifier=browser_id,
                    name="testBrowserSession",
                    viewPort={"width": 1920, "height": 1080},
                )
                return
        if service == "codeinterpreter":
            if operation == "startcodeinterpretersession":
                code_interpreter_id = resource_id
                bedrock_agentcore_client.meta.events.register(
                    "before-call.bedrock-agentcore.StartCodeInterpreterSession",
                    lambda **kwargs: inject_200_success(
                        codeInterpreterIdentifier=code_interpreter_id,
                        createdAt="2024-01-01T00:00:00Z",
                        sessionId="testCodeInterpreterSession",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_client.start_code_interpreter_session(
                    codeInterpreterIdentifier=code_interpreter_id,
                )
                return
        if service == "memory":
            if operation == "createevent":
                memory_id = resource_id
                bedrock_agentcore_client.meta.events.register(
                    "before-call.bedrock-agentcore.CreateEvent",
                    lambda **kwargs: inject_200_success(
                        memoryId=memory_id,
                        eventId="test-event-123",
                        createdAt="2024-01-01T00:00:00Z",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_client.create_event(
                    memoryId=memory_id,
                    actorId="test-actor-123",
                    eventTimestamp=1704067200,
                    payload=[
                        {
                            "conversational": {
                                "content": {"text": "Test memory event for testing"},
                                "role": "USER",
                            }
                        }
                    ],
                )
                return
            if operation == "creatememory":
                memory_id = resource_id
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateMemory",
                    lambda **kwargs: inject_200_success(
                        memory={
                            "id": memory_id,
                            "arn": f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:memory/{memory_id}",
                            "name": "testMemory",
                            "status": "ACTIVE",
                            "createdAt": 1704067200,
                            "updatedAt": 1704067200,
                            "eventExpiryDuration": 30,
                        },
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_memory(
                    name="testMemory",
                    eventExpiryDuration=30,
                    description="Test memory for testing",
                )
                return
        if service == "gateway":
            if operation == "creategateway":
                gateway_id = resource_id
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateGateway",
                    lambda **kwargs: inject_200_success(
                        gatewayId=gateway_id,
                        gatewayArn=f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:gateway/{gateway_id}",
                        name="agentGateway",
                        status="ACTIVE",
                        createdAt="2024-01-01T00:00:00Z",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_gateway(
                    name="workingGateway",
                    description="Working test gateway",
                    authorizerType="CUSTOM_JWT",
                    protocolType="MCP",
                    roleArn=(
                        f"arn:aws:iam::{_AWS_ACCOUNT_ID}:role/service-role/"
                        "AmazonBedrockAgentCoreRuntimeDefaultServiceRole-swsx9"
                    ),
                    authorizerConfiguration={
                        "customJWTAuthorizer": {
                            "discoveryUrl": "https://example.com/.well-known/openid-configuration",
                            "allowedAudience": ["test-audience"],
                            "allowedClients": ["test-client"],
                        }
                    },
                )
                return
            if operation == "creategatewaytarget":
                gateway_id = resource_id
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateGatewayTarget",
                    lambda **kwargs: inject_200_success(
                        targetId="testTarget-123",
                        gatewayArn=f"arn:aws:bedrock-agentcore:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:gateway/{gateway_id}",
                        name="testTarget",
                        status="ACTIVE",
                        createdAt="2024-01-01T00:00:00Z",
                        description="Test gateway target",
                        lastSynchronizedAt="2024-01-01T00:00:00Z",
                        updatedAt="2024-01-01T00:00:00Z",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_gateway_target(
                    gatewayIdentifier=gateway_id,
                    name="testTarget",
                    description="Test gateway target",
                    targetConfiguration={
                        "mcp": {
                            "openApiSchema": {
                                "inlinePayload": (
                                    '{"openapi": "3.0.0", "info": ' '{"title": "Test API", "version": "1.0.0"}}'
                                )
                            }
                        }
                    },
                    credentialProviderConfigurations=[
                        {
                            "credentialProviderType": "API_KEY",
                            "credentialProvider": {
                                "apiKeyCredentialProvider": {
                                    "providerArn": (
                                        f"arn:aws:bedrock:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:"
                                        "api-key-credential-provider/test-provider"
                                    ),
                                    "credentialParameterName": "api-key",
                                    "credentialLocation": "HEADER",
                                }
                            },
                        }
                    ],
                )
                return
        if service == "identity":
            if operation == "createoauth2credentialprovider":
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateOauth2CredentialProvider",
                    lambda **kwargs: inject_200_success(
                        credentialProviderId="test-oauth2-provider-123",
                        credentialProviderArn=(
                            f"arn:aws:acps:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:"
                            "token-vault/default/oauth2credentialprovider/test-oauth2-provider-123"
                        ),
                        name="testOAuth2Provider",
                        status="ACTIVE",
                        createdAt="2024-01-01T00:00:00Z",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_oauth2_credential_provider(
                    name="testOAuth2Provider",
                    credentialProviderVendor="CUSTOM",
                    oauth2ProviderConfigInput={
                        "customOauth2ProviderConfig": {
                            "oauthDiscovery": {"discoveryUrl": "https://example.com/.well-known/openid-configuration"},
                            "clientId": "test-client-id",
                            "clientSecret": "test-client-secret",
                        }
                    },
                )
                return
            if operation == "createapikeycredentialprovider":
                bedrock_agentcore_control_client.meta.events.register(
                    "before-call.bedrock-agentcore-control.CreateApiKeyCredentialProvider",
                    lambda **kwargs: inject_200_success(
                        credentialProviderId="test-apikey-provider-123",
                        credentialProviderArn=(
                            f"arn:aws:acps:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:"
                            "token-vault/default/apikeycredentialprovider/test-apikey-provider-123"
                        ),
                        name="testAPIKeyProvider",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_control_client.create_api_key_credential_provider(
                    name="testAPIKeyProvider",
                    apiKey="test-api-key-value",
                )
                return
            if operation == "getresourceoauth2token":
                bedrock_agentcore_client.meta.events.register(
                    "before-call.bedrock-agentcore.GetResourceOauth2Token",
                    lambda **kwargs: inject_200_success(
                        accessToken="mock-access-token-12345",
                        sessionStatus="IN_PROGRESS",
                        sessionUri="urn:ietf:params:oauth:request_uri:test-session-123",
                        authorizationUrl="https://example.com/oauth/authorize?client_id=test&state=test",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_client.get_resource_oauth2_token(
                    oauth2Flow="USER_FEDERATION",
                    resourceCredentialProviderName="test-oauth2-provider-123",
                    scopes=["read", "write"],
                    workloadIdentityToken="mock-workload-identity-token",
                    customState="test-csrf-state",
                    forceAuthentication=False,
                    resourceOauth2ReturnUrl="https://example.com/callback",
                    sessionUri="urn:ietf:params:oauth:request_uri:test-session-123",
                )
                return
            if operation == "getresourceapikey":
                bedrock_agentcore_client.meta.events.register(
                    "before-call.bedrock-agentcore.GetResourceApiKey",
                    lambda **kwargs: inject_200_success(
                        apiKey="mock-api-key-value-12345",
                        **kwargs,
                    ),
                )
                bedrock_agentcore_client.get_resource_api_key(
                    resourceCredentialProviderName="test-apikey-provider-123",
                    workloadIdentityToken="mock-workload-identity-token",
                )
                return

        set_main_status(404)

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
        elif self.in_path("describetable/some-table"):
            set_main_status(200)
            ddb_client.describe_table(
                TableName="put_test_table",
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
        elif self.in_path("describestream/my-stream"):
            set_main_status(200)
            kinesis_client.describe_stream(
                StreamName="test_stream", StreamARN="arn:aws:kinesis:us-west-2:000000000000:stream/test_stream"
            )
        else:
            set_main_status(404)

    def _handle_bedrock_request(self) -> None:
        # Localstack does not support Bedrock related services.
        # we inject inject_200_success directly into the API call
        # to make sure we receive http response with expected status code and attributes.
        bedrock_client: BaseClient = boto3.client("bedrock", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        bedrock_agent_client: BaseClient = boto3.client(
            "bedrock-agent", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION
        )
        bedrock_runtime_client: BaseClient = boto3.client(
            "bedrock-runtime", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION
        )
        bedrock_agent_runtime_client: BaseClient = boto3.client(
            "bedrock-agent-runtime", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION
        )
        if self.in_path("getknowledgebase/get_knowledge_base"):
            set_main_status(200)
            bedrock_agent_client.meta.events.register(
                "before-call.bedrock-agent.GetKnowledgeBase",
                inject_200_success,
            )
            bedrock_agent_client.get_knowledge_base(knowledgeBaseId="invalid-knowledge-base-id")
        elif self.in_path("getdatasource/get_data_source"):
            set_main_status(200)
            bedrock_agent_client.meta.events.register(
                "before-call.bedrock-agent.GetDataSource",
                inject_200_success,
            )
            bedrock_agent_client.get_data_source(knowledgeBaseId="TESTKBSEID", dataSourceId="DATASURCID")
        elif self.in_path("getagent/get-agent"):
            set_main_status(200)
            bedrock_agent_client.meta.events.register(
                "before-call.bedrock-agent.GetAgent",
                inject_200_success,
            )
            bedrock_agent_client.get_agent(agentId="TESTAGENTID")
        elif self.in_path("getguardrail/get-guardrail"):
            set_main_status(200)
            bedrock_client.meta.events.register(
                "before-call.bedrock.GetGuardrail",
                lambda **kwargs: inject_200_success(
                    guardrailId="bt4o77i015cu",
                    guardrailArn="arn:aws:bedrock:us-east-1:000000000000:guardrail/bt4o77i015cu",
                    **kwargs,
                ),
            )
            bedrock_client.get_guardrail(
                guardrailIdentifier="arn:aws:bedrock:us-east-1:000000000000:guardrail/bt4o77i015cu"
            )
        elif self.in_path("invokeagent/invoke_agent"):
            set_main_status(200)
            bedrock_agent_runtime_client.meta.events.register(
                "before-call.bedrock-agent-runtime.InvokeAgent",
                inject_200_success,
            )
            bedrock_agent_runtime_client.invoke_agent(
                agentId="Q08WFRPHVL",
                agentAliasId="testAlias",
                sessionId="testSessionId",
                inputText="Invoke agent sample input text",
            )
        elif self.in_path("retrieve/retrieve"):
            set_main_status(200)
            bedrock_agent_runtime_client.meta.events.register(
                "before-call.bedrock-agent-runtime.Retrieve",
                inject_200_success,
            )
            bedrock_agent_runtime_client.retrieve(
                knowledgeBaseId="test-knowledge-base-id",
                retrievalQuery={
                    "text": "an example of retrieve query",
                },
            )
        elif self.in_path("invokemodel/invoke-model"):
            model_id, request_body, response_body = get_model_request_response(self.path)

            set_main_status(200)
            bedrock_runtime_client.meta.events.register(
                "before-call.bedrock-runtime.InvokeModel",
                lambda **kwargs: inject_200_success(
                    modelId=model_id,
                    body=response_body,
                    **kwargs,
                ),
            )
            accept = "application/json"
            content_type = "application/json"
            bedrock_runtime_client.invoke_model(
                body=request_body, modelId=model_id, accept=accept, contentType=content_type
            )
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

    def _handle_stepfunctions_request(self) -> None:
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
                fault_client = boto3.client("stepfunctions", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION)
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
        elif self.in_path("describeactivity/my-activity"):
            set_main_status(200)
            sfn_client.describe_activity(activityArn="arn:aws:states:us-west-2:000000000000:activity:testActivity")
        else:
            set_main_status(404)

    def _handle_sns_request(self) -> None:
        sns_client = boto3.client("sns", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path(_FAULT):
            set_main_status(500)
            try:
                fault_client = boto3.client("sns", endpoint_url=_FAULT_ENDPOINT, region_name=_AWS_REGION)
                fault_client.meta.events.register(
                    "before-call.sns.GetTopicAttributes",
                    lambda **kwargs: inject_500_error("GetTopicAttributes", **kwargs),
                )
                fault_client.get_topic_attributes(TopicArn="arn:aws:sns:us-west-2:000000000000:invalid-topic")
            except Exception as exception:
                print("Expected exception occurred", exception)
        elif self.in_path("gettopicattributes/test-topic"):
            set_main_status(200)
            sns_client.get_topic_attributes(
                TopicArn="arn:aws:sns:us-west-2:000000000000:test-topic",
            )
        elif self.in_path("publish/test-topic"):
            set_main_status(200)
            sns_client.publish(
                TopicArn="arn:aws:sns:us-west-2:000000000000:test-topic",
                Message="test message",
            )
        else:
            set_main_status(404)

    def _handle_lambda_request(self) -> None:
        lambda_client = boto3.client("lambda", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        if self.in_path("invoke/test-function"):
            set_main_status(200)
            lambda_client.meta.events.register(
                "before-call.lambda.Invoke",
                inject_200_success,
            )
            lambda_client.invoke(FunctionName="test-function")
        else:
            set_main_status(404)

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def get_model_request_response(path):
    prompt = "Describe the purpose of a 'hello world' program in one line."
    model_id = ""
    request_body = {}
    response_body = {}

    if "amazon.titan" in path:
        model_id = "amazon.titan-text-premier-v1:0"

        request_body = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 3072,
                "stopSequences": [],
                "temperature": 0.7,
                "topP": 0.9,
            },
        }

        response_body = {
            "inputTextTokenCount": 15,
            "results": [
                {
                    "tokenCount": 13,
                    "outputText": "text-test-response",
                    "completionReason": "CONTENT_FILTERED",
                },
            ],
        }

    if "amazon.nova" in path:
        model_id = "amazon.nova-pro-v1:0"

        request_body = {
            "messages": [{"role": "user", "content": [{"text": "A camping trip"}]}],
            "inferenceConfig": {
                "max_new_tokens": 800,
                "temperature": 0.9,
                "topP": 0.7,
            },
        }

        response_body = {
            "output": {"message": {"content": [{"text": ""}], "role": "assistant"}},
            "stopReason": "max_tokens",
            "usage": {"inputTokens": 432, "outputTokens": 681},
        }

    if "anthropic.claude" in path:
        model_id = "anthropic.claude-v2:1"

        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "temperature": 0.99,
            "top_p": 1,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}],
                },
            ],
        }

        response_body = {
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 15,
                "output_tokens": 13,
            },
        }

    if "meta.llama" in path:
        model_id = "meta.llama2-13b-chat-v1"

        request_body = {"prompt": prompt, "max_gen_len": 512, "temperature": 0.5, "top_p": 0.9}

        response_body = {"prompt_token_count": 31, "generation_token_count": 49, "stop_reason": "stop"}

    if "cohere.command" in path:
        model_id = "cohere.command-r-v1:0"

        request_body = {
            "chat_history": [],
            "message": prompt,
            "max_tokens": 512,
            "temperature": 0.5,
            "p": 0.65,
        }

        response_body = {
            "chat_history": [
                {"role": "USER", "message": prompt},
                {"role": "CHATBOT", "message": "test-text-output"},
            ],
            "finish_reason": "COMPLETE",
            "text": "test-generation-text",
        }

    if "mistral" in path:
        model_id = "mistral.mistral-7b-instruct-v0:2"

        request_body = {
            "prompt": prompt,
            "max_tokens": 4096,
            "temperature": 0.75,
            "top_p": 0.99,
        }

        response_body = {
            "outputs": [
                {
                    "text": "test-output-text",
                    "stop_reason": "stop",
                },
            ]
        }

    json_bytes = json.dumps(response_body).encode("utf-8")

    return model_id, json.dumps(request_body), StreamingBody(BytesIO(json_bytes), len(json_bytes))


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


# pylint: disable=too-many-locals
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

        # Set up Secrets Manager so tests can access a secret.
        secretsmanager_client: BaseClient = boto3.client(
            "secretsmanager", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION
        )
        secretsmanager_response = secretsmanager_client.list_secrets()
        secret = next((s for s in secretsmanager_response["SecretList"] if s["Name"] == "testSecret"), None)
        if not secret:
            secretsmanager_client.create_secret(
                Name="testSecret", SecretString="secretValue", Description="This is a test secret"
            )

        # Set up SNS so tests can access a topic.
        sns_client: BaseClient = boto3.client("sns", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        create_topic_response = sns_client.create_topic(Name="test-topic")
        print("Created topic successfully:", create_topic_response)

        # Set up Step Functions so tests can access a state machine and activity.
        sfn_client: BaseClient = boto3.client("stepfunctions", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
        sfn_response = sfn_client.list_state_machines()
        state_machine_name = "testStateMachine"
        activity_name = "testActivity"
        state_machine = next((st for st in sfn_response["stateMachines"] if st["name"] == state_machine_name), None)
        if not state_machine:
            # create state machine needs an iam role so we create it here
            iam_client: BaseClient = boto3.client("iam", endpoint_url=_AWS_SDK_ENDPOINT, region_name=_AWS_REGION)
            iam_role_name = "testRole"
            iam_role_arn = None
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
                ],
            }
            try:
                iam_response = iam_client.create_role(
                    RoleName=iam_role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
                )
                iam_client.attach_role_policy(
                    RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"
                )
                print(f"IAM Role '{iam_role_name}' create successfully.")
                iam_role_arn = iam_response["Role"]["Arn"]
                sfn_defintion = {
                    "Comment": "A simple sequential workflow",
                    "StartAt": "FirstState",
                    "States": {"FirstState": {"Type": "Pass", "Result": "Hello, World!", "End": True}},
                }
                definition_string = json.dumps(sfn_defintion)
                sfn_client.create_state_machine(
                    name=state_machine_name, definition=definition_string, roleArn=iam_role_arn
                )
                sfn_client.create_activity(name=activity_name)
            except Exception as exception:
                print("Something went wrong with Step Functions setup", exception)

    except Exception as exception:
        print("Unexpected exception occurred", exception)


def inject_200_success(**kwargs):
    response_metadata = {
        "HTTPStatusCode": 200,
        "RequestId": "mock-request-id",
    }

    response_body = {
        "Message": "Request succeeded",
        "ResponseMetadata": response_metadata,
    }

    for key, value in kwargs.items():
        if key not in ["headers", "body"]:
            response_body[key] = value

    HTTPResponse = namedtuple("HTTPResponse", ["status_code", "headers", "body"])
    headers = kwargs.get("headers", {})
    body = kwargs.get("body", "")
    if body:
        response_body["body"] = body
    http_response = HTTPResponse(200, headers=headers, body=body)

    return http_response, response_body


def inject_500_error(api_name: str, **kwargs):
    raise ClientError(
        {
            "Error": {"Code": "InternalServerError", "Message": "Internal Server Error"},
            "ResponseMetadata": {"HTTPStatusCode": 500, "RequestId": "mock-request-id"},
        },
        api_name,
    )


def main() -> None:
    prepare_aws_server()
    server_address: Tuple[str, int] = ("0.0.0.0", _PORT)
    request_handler_class: type = RequestHandler
    requests_server: ThreadingHTTPServer = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(requests_server.shutdown)
    server_thread: Thread = Thread(target=requests_server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()


if __name__ == "__main__":
    main()
