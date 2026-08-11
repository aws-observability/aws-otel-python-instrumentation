# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

import boto3
from botocore.stub import Stubber
from flask import Flask, current_app
from plugins.opentelemetry.cloudwatch.span_metrics.instrumentor import SpanMetricsInstrumentor
from requests import get
from sqlalchemy import create_engine, text

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_SERVICE_NAME,
    OTEL_TRACES_SAMPLER,
)
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


def main():
    create_app().run(host="0.0.0.0", port=8080, threaded=True)


def create_app():
    settings = _settings()
    providers = _configure_manual_instrumentation(settings) if settings["SPAN_METRICS_MODE"] == "manual" else None

    application = Flask(__name__)
    application.config.update(settings)
    if providers is not None:
        FlaskInstrumentor().instrument_app(
            application,
            tracer_provider=providers[0],
            meter_provider=providers[1],
        )

    application.extensions["tracer"] = trace.get_tracer(__name__)
    application.extensions["engine"] = _create_engine()
    application.extensions["s3"] = _create_aws_client("s3", settings["AWS_REGION"])
    application.extensions["sqs"] = _create_aws_client("sqs", settings["AWS_REGION"])
    application.extensions["sns"] = _create_aws_client("sns", settings["AWS_REGION"])

    application.add_url_rule("/health", view_func=health, methods=["GET"])
    application.add_url_rule("/downstream", view_func=downstream, methods=["GET"])
    application.add_url_rule("/exercise", view_func=exercise, methods=["GET"])
    application.add_url_rule("/error", view_func=error, methods=["GET"])
    return application


def health():
    return {"status": "ok"}


def downstream():
    return {"status": "ok"}


def exercise():
    tracer = current_app.extensions["tracer"]
    engine = current_app.extensions["engine"]
    s3 = current_app.extensions["s3"]
    sqs = current_app.extensions["sqs"]
    sns = current_app.extensions["sns"]

    with tracer.start_as_current_span("internal-work"):
        pass

    response = get("http://127.0.0.1:8080/downstream", timeout=5)
    response.raise_for_status()

    with engine.connect() as connection:
        connection.execute(text("SELECT name FROM users WHERE id = 1")).fetchall()

    with tracer.start_as_current_span(
        "SELECT users",
        kind=SpanKind.CLIENT,
        attributes={
            DB_SYSTEM: "sqlite",
            DB_OPERATION: "SELECT",
            DB_SQL_TABLE: "users",
        },
    ):
        pass

    with Stubber(s3) as stubber:
        stubber.add_response("list_buckets", {"Buckets": [{"Name": "contract-test"}]})
        s3.list_buckets()

    with Stubber(sqs) as stubber:
        stubber.add_response("send_message", {"MessageId": "message-1", "MD5OfMessageBody": "abc"})
        sqs.send_message(QueueUrl=current_app.config["QUEUE_URL"], MessageBody="contract test")

    with Stubber(sns) as stubber:
        stubber.add_response("publish", {"MessageId": "message-2"})
        sns.publish(TopicArn=current_app.config["TOPIC_ARN"], Message="contract test")

    with tracer.start_as_current_span(
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


def error():
    span = trace.get_current_span()
    span.set_attribute(ERROR_TYPE, "RuntimeError")
    span.set_status(Status(StatusCode.ERROR))
    raise RuntimeError("expected contract-test error")


def _settings():
    return {
        "SPAN_METRICS_MODE": os.environ.get("SPAN_METRICS_MODE", "auto"),
        OTEL_SERVICE_NAME: os.environ.get(OTEL_SERVICE_NAME, "cloudwatch-plugin-otel-contract-test"),
        OTEL_EXPORTER_OTLP_ENDPOINT: os.environ.get(OTEL_EXPORTER_OTLP_ENDPOINT, "http://collector:4315"),
        OTEL_TRACES_SAMPLER: os.environ.get(OTEL_TRACES_SAMPLER, "always_on"),
        "AWS_REGION": "us-east-1",
        "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
        "TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:orders",
    }


def _configure_manual_instrumentation(settings):
    resource = Resource.create({SERVICE_NAME: settings[OTEL_SERVICE_NAME]})
    sampler = ALWAYS_OFF if settings[OTEL_TRACES_SAMPLER] == "always_off" else ALWAYS_ON

    tracer_provider = TracerProvider(resource=resource, sampler=sampler)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=settings[OTEL_EXPORTER_OTLP_ENDPOINT], insecure=True),
            schedule_delay_millis=50,
        )
    )
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=settings[OTEL_EXPORTER_OTLP_ENDPOINT], insecure=True),
        export_interval_millis=100,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    SpanMetricsInstrumentor().instrument(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
    RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
    BotocoreInstrumentor().instrument(tracer_provider=tracer_provider)
    SQLAlchemyInstrumentor().instrument(tracer_provider=tracer_provider)
    return tracer_provider, meter_provider


def _create_engine():
    engine = create_engine("sqlite:////tmp/span-metrics-contract.db")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"))
    return engine


def _create_aws_client(service, region):
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


if __name__ == "__main__":
    main()
