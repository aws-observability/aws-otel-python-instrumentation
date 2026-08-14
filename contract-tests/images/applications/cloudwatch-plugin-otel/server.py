# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from abc import ABC, abstractmethod
from concurrent import futures

import boto3
import fakeredis
import grpc
from botocore.stub import Stubber
from flask import Flask
from requests import get
from sqlalchemy import create_engine, text

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient, GrpcInstrumentorServer
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME, OTEL_TRACES_SAMPLER
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON
from opentelemetry.semconv._incubating.attributes.db_attributes import DB_OPERATION, DB_SQL_TABLE, DB_SYSTEM
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME
from opentelemetry.trace import SpanKind, Status, StatusCode

SERVICE = os.environ.get(OTEL_SERVICE_NAME, "cloudwatch-plugin-otel-contract-test")
ENDPOINT = os.environ.get(OTEL_EXPORTER_OTLP_ENDPOINT, "http://collector:4315")
SAMPLER = os.environ.get(OTEL_TRACES_SAMPLER, "always_on")
AWS_REGION = "us-east-1"
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:orders"
DYNAMODB_TABLE = "users"
DATABASE_URL = "sqlite:////tmp/span-metrics-contract.db"
GRPC_SERVICE = "contract.Health"
GRPC_METHOD = "Check"


class SpanMetricsApplication(ABC):
    @staticmethod
    def create_providers():
        resource = Resource.create({SERVICE_NAME: SERVICE})
        sampler = ALWAYS_OFF if SAMPLER == "always_off" else ALWAYS_ON

        tracer_provider = TracerProvider(resource=resource, sampler=sampler)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=ENDPOINT, insecure=True),
                schedule_delay_millis=50,
            )
        )
        trace.set_tracer_provider(tracer_provider)

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=ENDPOINT, insecure=True),
                    export_interval_millis=100,
                )
            ],
        )
        metrics.set_meter_provider(meter_provider)
        return tracer_provider, meter_provider

    @staticmethod
    def instrument_libraries(app, tracer_provider, meter_provider):
        RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
        BotocoreInstrumentor().instrument(tracer_provider=tracer_provider)
        SQLAlchemyInstrumentor().instrument(tracer_provider=tracer_provider)
        RedisInstrumentor().instrument(tracer_provider=tracer_provider)
        GrpcInstrumentorServer().instrument(tracer_provider=tracer_provider)
        GrpcInstrumentorClient().instrument(tracer_provider=tracer_provider)
        FlaskInstrumentor().instrument_app(
            app,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        )

    @abstractmethod
    def configure_instrumentation(self, app):
        raise NotImplementedError

    def run(self):
        app = Flask(__name__)
        self.configure_instrumentation(app)

        database = Database(DATABASE_URL)
        aws_clients = AwsClients(AWS_REGION)
        redis_cache = RedisCache()
        grpc_service = GrpcService()
        grpc_service.start()
        FlaskServer(app, database, aws_clients, redis_cache, grpc_service).run()


class Database:
    def __init__(self, url):
        self._engine = create_engine(url)
        with self._engine.begin() as connection:
            connection.execute(text("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"))

    def select_users(self):
        with self._engine.connect() as connection:
            connection.execute(text("SELECT name FROM users WHERE id = 1")).fetchall()


class AwsClients:
    def __init__(self, region):
        self.s3 = self._client("s3", region)
        self.sqs = self._client("sqs", region)
        self.sns = self._client("sns", region)
        self.dynamodb = self._client("dynamodb", region)

    @staticmethod
    def _client(service, region):
        return boto3.client(
            service,
            region_name=region,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    def list_buckets(self):
        with Stubber(self.s3) as stubber:
            stubber.add_response("list_buckets", {"Buckets": [{"Name": "contract-test"}]})
            self.s3.list_buckets()

    def send_message(self, queue_url):
        with Stubber(self.sqs) as stubber:
            stubber.add_response("send_message", {"MessageId": "message-1", "MD5OfMessageBody": "abc"})
            self.sqs.send_message(QueueUrl=queue_url, MessageBody="contract test")

    def publish(self, topic_arn):
        with Stubber(self.sns) as stubber:
            stubber.add_response("publish", {"MessageId": "message-2"})
            self.sns.publish(TopicArn=topic_arn, Message="contract test")

    def get_item(self, table_name):
        with Stubber(self.dynamodb) as stubber:
            stubber.add_response("get_item", {"Item": {"id": {"S": "1"}, "name": {"S": "contract-test"}}})
            self.dynamodb.get_item(TableName=table_name, Key={"id": {"S": "1"}})


class RedisCache:
    def __init__(self):
        self._client = fakeredis.FakeStrictRedis()

    def exercise(self):
        self._client.set("contract-test", "ok")
        self._client.get("contract-test")


class GrpcService:
    def __init__(self):
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        self._server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    GRPC_SERVICE,
                    {
                        GRPC_METHOD: grpc.unary_unary_rpc_method_handler(
                            lambda request, context: b"ok",
                            request_deserializer=lambda payload: payload,
                            response_serializer=lambda payload: payload,
                        )
                    },
                ),
            )
        )
        self._port = self._server.add_insecure_port("127.0.0.1:0")

    def start(self):
        self._server.start()

    def exercise(self):
        with grpc.insecure_channel(f"127.0.0.1:{self._port}") as channel:
            channel.unary_unary(
                f"/{GRPC_SERVICE}/{GRPC_METHOD}",
                request_serializer=lambda payload: payload,
                response_deserializer=lambda payload: payload,
            )(b"ping")


class FlaskServer:
    def __init__(self, app, database, aws_clients, redis_cache, grpc_service):
        self._database = database
        self._aws_clients = aws_clients
        self._redis_cache = redis_cache
        self._grpc_service = grpc_service
        self._tracer = trace.get_tracer(__name__)
        self.app = app
        self.app.add_url_rule("/health", view_func=self._health, methods=["GET"])
        self.app.add_url_rule("/downstream", view_func=self._downstream, methods=["GET"])
        self.app.add_url_rule("/exercise", view_func=self._exercise, methods=["GET"])
        self.app.add_url_rule("/error", view_func=self._error, methods=["GET"])

    def run(self):
        self.app.run(host="0.0.0.0", port=8080, threaded=True)

    @staticmethod
    def _health():
        return {"status": "ok"}

    @staticmethod
    def _downstream():
        return {"status": "ok"}

    def _exercise(self):
        with self._tracer.start_as_current_span("internal-work"):
            pass

        response = get("http://127.0.0.1:8080/downstream", timeout=5)
        response.raise_for_status()

        self._database.select_users()
        with self._tracer.start_as_current_span(
            "SELECT users",
            kind=SpanKind.CLIENT,
            attributes={
                DB_SYSTEM: "sqlite",
                DB_OPERATION: "SELECT",
                DB_SQL_TABLE: "users",
            },
        ):
            pass

        self._aws_clients.list_buckets()
        self._aws_clients.send_message(QUEUE_URL)
        self._aws_clients.publish(TOPIC_ARN)
        self._aws_clients.get_item(DYNAMODB_TABLE)

        self._redis_cache.exercise()
        self._grpc_service.exercise()

        with self._tracer.start_as_current_span(
            "orders receive",
            kind=SpanKind.CONSUMER,
            attributes={
                MESSAGING_SYSTEM: "contract-broker",
                MESSAGING_OPERATION_NAME: "receive",
                MESSAGING_OPERATION_TYPE: "receive",
                MESSAGING_DESTINATION_NAME: "orders",
            },
        ):
            pass

        return {"status": "ok"}

    @staticmethod
    def _error():
        span = trace.get_current_span()
        span.set_attribute(ERROR_TYPE, "RuntimeError")
        span.set_status(Status(StatusCode.ERROR))
        raise RuntimeError("expected contract-test error")
