# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

import boto3
from botocore.stub import Stubber
from flask import Flask
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
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON
from opentelemetry.trace import SpanKind, Status, StatusCode

_MODE = os.environ.get("SPAN_METRICS_MODE", "auto")
_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "cloudwatch-plugin-otel-contract-test")
_OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4315")
_AWS_REGION = "us-east-1"
_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:orders"


def _configure_manual_instrumentation():
    resource = Resource.create({"service.name": _SERVICE_NAME})
    sampler = ALWAYS_OFF if os.environ.get("OTEL_TRACES_SAMPLER") == "always_off" else ALWAYS_ON

    tracer_provider = TracerProvider(resource=resource, sampler=sampler)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=_OTLP_ENDPOINT, insecure=True),
            schedule_delay_millis=50,
        )
    )
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=_OTLP_ENDPOINT, insecure=True),
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


_MANUAL_PROVIDERS = _configure_manual_instrumentation() if _MODE == "manual" else None

app = Flask(__name__)
if _MANUAL_PROVIDERS is not None:
    FlaskInstrumentor().instrument_app(
        app,
        tracer_provider=_MANUAL_PROVIDERS[0],
        meter_provider=_MANUAL_PROVIDERS[1],
    )

_tracer = trace.get_tracer(__name__)
_engine = create_engine("sqlite:////tmp/span-metrics-contract.db")
with _engine.begin() as _connection:
    _connection.execute(text("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"))

_s3 = boto3.client(
    "s3",
    region_name=_AWS_REGION,
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)
_sqs = boto3.client(
    "sqs",
    region_name=_AWS_REGION,
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)
_sns = boto3.client(
    "sns",
    region_name=_AWS_REGION,
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/downstream")
def downstream():
    return {"status": "ok"}


@app.get("/exercise")
def exercise():
    with _tracer.start_as_current_span("internal-work"):
        pass

    response = get("http://127.0.0.1:8080/downstream", timeout=5)
    response.raise_for_status()

    with _engine.connect() as connection:
        connection.execute(text("SELECT name FROM users WHERE id = 1")).fetchall()

    with _tracer.start_as_current_span(
        "SELECT users",
        kind=SpanKind.CLIENT,
        attributes={
            "db.system": "sqlite",
            "db.operation": "SELECT",
            "db.sql.table": "users",
        },
    ):
        pass

    with Stubber(_s3) as stubber:
        stubber.add_response("list_buckets", {"Buckets": [{"Name": "contract-test"}]})
        _s3.list_buckets()

    with Stubber(_sqs) as stubber:
        stubber.add_response("send_message", {"MessageId": "message-1", "MD5OfMessageBody": "abc"})
        _sqs.send_message(QueueUrl=_QUEUE_URL, MessageBody="contract test")

    with Stubber(_sns) as stubber:
        stubber.add_response("publish", {"MessageId": "message-2"})
        _sns.publish(TopicArn=_TOPIC_ARN, Message="contract test")

    with _tracer.start_as_current_span(
        "orders receive",
        kind=SpanKind.CONSUMER,
        attributes={
            "messaging.system": "contract-broker",
            "messaging.operation.name": "receive",
            "messaging.operation.type": "receive",
            "messaging.destination.name": "orders",
        },
    ):
        pass

    return {"status": "ok"}


@app.get("/error")
def error():
    span = trace.get_current_span()
    span.set_attribute("error.type", "RuntimeError")
    span.set_status(Status(StatusCode.ERROR))
    raise RuntimeError("expected contract-test error")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
