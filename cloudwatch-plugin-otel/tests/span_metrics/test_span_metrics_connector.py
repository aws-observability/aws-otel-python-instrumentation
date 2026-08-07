# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import botocore.session
import requests
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from flask import Flask
from plugins.opentelemetry.cloudwatch.span_metrics._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.span_metrics.connector import SpanMetricsConnector
from plugins.opentelemetry.cloudwatch.version import __version__

from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_CASSANDRA_TABLE,
    DB_COLLECTION_NAME,
    DB_COSMOSDB_CONTAINER,
    DB_MONGODB_COLLECTION,
    DB_OPERATION,
    DB_OPERATION_NAME,
    DB_SQL_TABLE,
    DB_SYSTEM,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv._incubating.attributes.http_attributes import HTTP_METHOD, HTTP_STATUS_CODE
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_ANONYMOUS,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_TEMPORARY,
    MESSAGING_OPERATION,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_METHOD, RPC_SERVICE, RPC_SYSTEM

try:
    from opentelemetry.semconv._incubating.attributes.rpc_attributes import RPC_SYSTEM_NAME
except ImportError:
    RPC_SYSTEM_NAME = "rpc.system.name"
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.http_attributes import HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE, HTTP_ROUTE
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind, Status, StatusCode


class SpanMetricsConnectorTestBase(TestBase):
    def setUp(self):
        super().setUp()
        self.processor = SpanMetricsConnector(meter_provider=self.meter_provider)
        self.tracer_provider.add_span_processor(self.processor)

    def record_span(self, name, *, kind=SpanKind.INTERNAL, attributes=None, status=None):
        tracer = self.tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span(name, kind=kind, attributes=attributes) as span:
            if status is not None:
                span.set_status(status)
        return self.get_metric_data_point(_SpanMetrics.CALLS_NAME, name)

    def get_metric_data_point(self, metric_name, span_name):
        data_point = None
        for metric in self.get_sorted_metrics():
            if metric.name != metric_name:
                continue
            for point in metric.data.data_points:
                if point.attributes.get(_SpanMetrics.SPAN_NAME) == span_name:
                    self.assertIsNone(data_point, f"found multiple {metric_name} points for span {span_name!r}")
                    data_point = point
        self.assertIsNotNone(data_point, f"found no {metric_name} point for span {span_name!r}")
        return data_point

    def assert_span_metrics(self, span_name, *, span_kind, status_code, service_name="unknown_service"):
        calls = self.get_metric_data_point(_SpanMetrics.CALLS_NAME, span_name)
        self.assertEqual(calls.value, 1)
        self.assertEqual(calls.attributes[SERVICE_NAME], service_name)
        self.assertEqual(calls.attributes[_SpanMetrics.SPAN_NAME], span_name)
        self.assertEqual(calls.attributes[_SpanMetrics.SPAN_KIND], span_kind)
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], status_code)
        self.assertEqual(calls.attributes[_SpanMetrics.SCHEMA], _SpanMetrics.SCHEMA_VERSION)
        self.assertEqual(calls.attributes[_SpanMetrics.LIB_VERSION], __version__)

        span = self.get_finished_spans().by_name(span_name)
        expected_seconds = (span.end_time - span.start_time) / _SpanMetrics.NANOS_PER_SECOND
        duration = self.get_metric_data_point(_SpanMetrics.DURATION_NAME, span_name)
        self.assertEqual(duration.count, 1)
        self.assertEqual(duration.sum, expected_seconds)
        self.assertEqual(duration.min, expected_seconds)
        self.assertEqual(duration.max, expected_seconds)
        self.assertEqual(tuple(duration.explicit_bounds), tuple(_SpanMetrics.DURATION_BUCKET_BOUNDARIES))
        self.assertEqual(dict(duration.attributes), dict(calls.attributes))
        return calls, duration


class TestSpanMetricsConnector(SpanMetricsConnectorTestBase):
    def test_canonical_attributes_take_precedence_over_legacy(self):
        calls = self.record_span(
            "precedence",
            kind=SpanKind.CLIENT,
            attributes={
                HTTP_REQUEST_METHOD: "GET",
                HTTP_METHOD: "POST",
                HTTP_RESPONSE_STATUS_CODE: 200,
                HTTP_STATUS_CODE: 500,
                DB_SYSTEM_NAME: "postgresql",
                DB_SYSTEM: "mysql",
                DB_OPERATION_NAME: "SELECT",
                DB_OPERATION: "INSERT",
                DB_COLLECTION_NAME: "users",
                DB_SQL_TABLE: "accounts",
            },
        )
        self.assertEqual(calls.attributes[HTTP_REQUEST_METHOD], "GET")
        self.assertEqual(calls.attributes[HTTP_RESPONSE_STATUS_CODE], 200)
        self.assertEqual(calls.attributes[DB_SYSTEM_NAME], "postgresql")
        self.assertEqual(calls.attributes[DB_OPERATION_NAME], "SELECT")
        self.assertEqual(calls.attributes[DB_COLLECTION_NAME], "users")

    def test_all_allowlisted_attributes_copied(self):
        attributes = {
            HTTP_REQUEST_METHOD: "GET",
            HTTP_RESPONSE_STATUS_CODE: 200,
            HTTP_ROUTE: "/items/<item_id>",
            ERROR_TYPE: "timeout",
            RPC_SYSTEM_NAME: "grpc",
            RPC_SERVICE: "Greeter",
            RPC_METHOD: "SayHello",
            DB_SYSTEM_NAME: "postgresql",
            DB_OPERATION_NAME: "SELECT",
            DB_COLLECTION_NAME: "users",
            MESSAGING_SYSTEM: "kafka",
            MESSAGING_OPERATION_NAME: "publish",
            MESSAGING_OPERATION_TYPE: "send",
            MESSAGING_DESTINATION_NAME: "orders",
        }
        calls = self.record_span("all-attrs", kind=SpanKind.CLIENT, attributes=attributes)
        for key, value in attributes.items():
            self.assertEqual(calls.attributes[key], value)

    def test_messaging_destination_kept_when_normal(self):
        calls = self.record_span(
            "messaging-normal",
            kind=SpanKind.PRODUCER,
            attributes={MESSAGING_SYSTEM: "kafka", MESSAGING_DESTINATION_NAME: "orders"},
        )
        self.assertEqual(calls.attributes[MESSAGING_SYSTEM], "kafka")
        self.assertEqual(calls.attributes[MESSAGING_DESTINATION_NAME], "orders")

    def test_messaging_destination_dropped_when_temporary(self):
        calls = self.record_span(
            "messaging-temporary",
            kind=SpanKind.PRODUCER,
            attributes={
                MESSAGING_SYSTEM: "kafka",
                MESSAGING_DESTINATION_NAME: "orders",
                MESSAGING_DESTINATION_TEMPORARY: True,
            },
        )
        self.assertEqual(calls.attributes[MESSAGING_SYSTEM], "kafka")
        self.assertNotIn(MESSAGING_DESTINATION_NAME, calls.attributes)

    def test_messaging_destination_dropped_when_anonymous(self):
        calls = self.record_span(
            "messaging-anonymous",
            kind=SpanKind.PRODUCER,
            attributes={
                MESSAGING_SYSTEM: "kafka",
                MESSAGING_DESTINATION_NAME: "orders",
                MESSAGING_DESTINATION_ANONYMOUS: True,
            },
        )
        self.assertEqual(calls.attributes[MESSAGING_SYSTEM], "kafka")
        self.assertNotIn(MESSAGING_DESTINATION_NAME, calls.attributes)

    def test_legacy_messaging_destination_normalized(self):
        calls = self.record_span(
            "messaging-legacy-dest",
            kind=SpanKind.PRODUCER,
            attributes={MESSAGING_SYSTEM: "kafka", SpanAttributes.MESSAGING_DESTINATION: "orders"},
        )
        self.assertEqual(calls.attributes[MESSAGING_DESTINATION_NAME], "orders")

    def test_canonical_messaging_destination_takes_precedence_over_legacy(self):
        calls = self.record_span(
            "messaging-dest-precedence",
            kind=SpanKind.PRODUCER,
            attributes={
                MESSAGING_SYSTEM: "kafka",
                MESSAGING_DESTINATION_NAME: "orders",
                SpanAttributes.MESSAGING_DESTINATION: "legacy-orders",
            },
        )
        self.assertEqual(calls.attributes[MESSAGING_DESTINATION_NAME], "orders")

    def test_legacy_messaging_destination_dropped_when_temporary(self):
        calls = self.record_span(
            "messaging-legacy-temporary",
            kind=SpanKind.PRODUCER,
            attributes={
                MESSAGING_SYSTEM: "kafka",
                SpanAttributes.MESSAGING_DESTINATION: "orders",
                MESSAGING_DESTINATION_TEMPORARY: True,
            },
        )
        self.assertNotIn(MESSAGING_DESTINATION_NAME, calls.attributes)

    def test_legacy_messaging_operation_normalized(self):
        calls = self.record_span(
            "messaging-legacy-op",
            kind=SpanKind.PRODUCER,
            attributes={MESSAGING_SYSTEM: "kafka", MESSAGING_OPERATION: "publish"},
        )
        self.assertEqual(calls.attributes[MESSAGING_OPERATION_TYPE], "publish")
        self.assertNotIn(MESSAGING_OPERATION_NAME, calls.attributes)

    def test_canonical_messaging_operation_type_takes_precedence_over_legacy(self):
        calls = self.record_span(
            "messaging-op-precedence",
            kind=SpanKind.PRODUCER,
            attributes={
                MESSAGING_SYSTEM: "kafka",
                MESSAGING_OPERATION_TYPE: "send",
                MESSAGING_OPERATION: "publish",
            },
        )
        self.assertEqual(calls.attributes[MESSAGING_OPERATION_TYPE], "send")

    def test_messaging_operation_name_and_type_are_independent(self):
        calls = self.record_span(
            "messaging-op-both",
            kind=SpanKind.PRODUCER,
            attributes={
                MESSAGING_SYSTEM: "kafka",
                MESSAGING_OPERATION_NAME: "publish",
                MESSAGING_OPERATION_TYPE: "send",
            },
        )
        self.assertEqual(calls.attributes[MESSAGING_OPERATION_NAME], "publish")
        self.assertEqual(calls.attributes[MESSAGING_OPERATION_TYPE], "send")

    def test_legacy_rpc_system_normalized(self):
        calls = self.record_span(
            "rpc-legacy-system",
            kind=SpanKind.CLIENT,
            attributes={RPC_SYSTEM: "grpc", RPC_METHOD: "SayHello"},
        )
        self.assertEqual(calls.attributes[RPC_SYSTEM_NAME], "grpc")
        self.assertNotIn(RPC_SYSTEM, calls.attributes)

    def test_canonical_rpc_system_name_takes_precedence_over_legacy(self):
        calls = self.record_span(
            "rpc-system-precedence",
            kind=SpanKind.CLIENT,
            attributes={RPC_SYSTEM_NAME: "grpc", RPC_SYSTEM: "apache_dubbo"},
        )
        self.assertEqual(calls.attributes[RPC_SYSTEM_NAME], "grpc")

    def test_cassandra_table_normalized(self):
        calls = self.record_span(
            "cassandra-select",
            kind=SpanKind.CLIENT,
            attributes={DB_SYSTEM: "cassandra", DB_CASSANDRA_TABLE: "users"},
        )
        self.assertEqual(calls.attributes[DB_COLLECTION_NAME], "users")

    def test_cosmosdb_container_normalized(self):
        calls = self.record_span(
            "cosmos-read",
            kind=SpanKind.CLIENT,
            attributes={DB_SYSTEM: "cosmosdb", DB_COSMOSDB_CONTAINER: "users"},
        )
        self.assertEqual(calls.attributes[DB_COLLECTION_NAME], "users")

    def test_db_collection_legacy_precedence_order(self):
        calls = self.record_span(
            "collection-precedence",
            kind=SpanKind.CLIENT,
            attributes={
                DB_SQL_TABLE: "sql_table",
                DB_MONGODB_COLLECTION: "mongo_coll",
                DB_CASSANDRA_TABLE: "cassandra_table",
                DB_COSMOSDB_CONTAINER: "cosmos_container",
            },
        )
        self.assertEqual(calls.attributes[DB_COLLECTION_NAME], "sql_table")

    def test_mongodb_collection_normalized(self):
        calls = self.record_span(
            "mongo-find",
            kind=SpanKind.CLIENT,
            attributes={DB_SYSTEM: "mongodb", DB_MONGODB_COLLECTION: "users"},
        )
        self.assertEqual(calls.attributes[DB_COLLECTION_NAME], "users")

    def test_absent_attributes_not_copied(self):
        calls = self.record_span("bare", kind=SpanKind.INTERNAL)
        self.assertEqual(
            set(calls.attributes),
            {
                SERVICE_NAME,
                _SpanMetrics.SPAN_NAME,
                _SpanMetrics.SPAN_KIND,
                _SpanMetrics.STATUS_CODE,
                _SpanMetrics.SCHEMA,
                _SpanMetrics.LIB_VERSION,
            },
        )

    def test_all_span_kinds_mapped(self):
        for kind in (SpanKind.SERVER, SpanKind.CLIENT, SpanKind.INTERNAL, SpanKind.PRODUCER, SpanKind.CONSUMER):
            calls = self.record_span(f"kind-{kind.name}", kind=kind)
            self.assertEqual(calls.attributes[_SpanMetrics.SPAN_KIND], kind.name)

    def test_status_ok_recorded(self):
        calls = self.record_span("status-ok", status=Status(StatusCode.OK))
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "OK")

    def test_status_error_recorded(self):
        calls = self.record_span("status-error", status=Status(StatusCode.ERROR))
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "ERROR")

    def test_status_unset_by_default(self):
        calls = self.record_span("status-unset")
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "UNSET")

    def test_counter_accumulates_for_repeated_span(self):
        self.record_span("repeat", kind=SpanKind.CLIENT, attributes={HTTP_REQUEST_METHOD: "GET"})
        calls = self.record_span("repeat", kind=SpanKind.CLIENT, attributes={HTTP_REQUEST_METHOD: "GET"})
        self.assertEqual(calls.value, 2)
        duration = self.get_metric_data_point(_SpanMetrics.DURATION_NAME, "repeat")
        self.assertEqual(duration.count, 2)

    def test_resource_service_name_used(self):
        tracer_provider, _ = self.create_tracer_provider(resource=Resource.create({SERVICE_NAME: "orders-service"}))
        tracer_provider.add_span_processor(self.processor)
        with tracer_provider.get_tracer(__name__).start_as_current_span("resource-span", kind=SpanKind.CLIENT):
            pass
        calls = self.get_metric_data_point(_SpanMetrics.CALLS_NAME, "resource-span")
        self.assertEqual(calls.attributes[SERVICE_NAME], "orders-service")

    def test_default_meter_uses_global_provider_with_package_scope(self):
        self.tracer_provider.add_span_processor(SpanMetricsConnector())
        with self.tracer_provider.get_tracer(__name__).start_as_current_span("default-meter"):
            pass

        metrics_data = self.memory_metrics_reader.get_metrics_data()
        scope_metrics = [
            scope_metric
            for resource_metric in metrics_data.resource_metrics
            for scope_metric in resource_metric.scope_metrics
            if scope_metric.scope.name == _SpanMetrics.SCOPE_NAME
        ]

        self.assertEqual(len(scope_metrics), 1)
        self.assertEqual(scope_metrics[0].scope.version, __version__)
        self.assertIn(_SpanMetrics.CALLS_NAME, {metric.name for metric in scope_metrics[0].metrics})

    def test_shutdown_and_force_flush(self):
        self.assertIsNone(self.processor.shutdown())
        self.assertTrue(self.processor.force_flush())

    def test_shutdown_disables_recording(self):
        self.assertTrue(self.processor.enabled)

        self.processor.shutdown()

        self.assertFalse(self.processor.enabled)
        tracer = self.tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("after_shutdown"):
            pass

        self.assertEqual(self.get_sorted_metrics(), [])
        span = self.get_finished_spans().by_name("after_shutdown")
        self.assertNotIn(_SpanMetrics.SCHEMA, span.attributes or {})

    def test_malformed_span_never_raises(self):
        self.processor.on_start(object())
        self.processor.on_end(object())
        self.assertEqual(self.get_sorted_metrics(), [])


class TestSpanMetricsConnectorHttpClient(SpanMetricsConnectorTestBase):
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            status = 500 if self.path.startswith("/boom") else 200
            self.send_response(status)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            return

    def setUp(self):
        super().setUp()
        self.server = HTTPServer(("127.0.0.1", 0), self._Handler)
        self.port = self.server.server_address[1]
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        RequestsInstrumentor().instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        RequestsInstrumentor().uninstrument()
        self.server.shutdown()
        self.server_thread.join(timeout=5)
        super().tearDown()

    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_successful_request_records_calls_and_duration(self):
        response = requests.get(self.url("/ok"), timeout=5)
        self.assertEqual(response.status_code, 200)

        span = self.get_finished_spans().by_name("GET")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertSpanHasAttributes(
            span,
            {
                _SpanMetrics.SCHEMA: _SpanMetrics.SCHEMA_VERSION,
                _SpanMetrics.LIB_VERSION: __version__,
            },
        )

        calls, _ = self.assert_span_metrics("GET", span_kind="CLIENT", status_code="UNSET")
        self.assertEqual(calls.attributes[HTTP_REQUEST_METHOD], "GET")
        self.assertEqual(calls.attributes[HTTP_RESPONSE_STATUS_CODE], 200)

    def test_server_error_marks_status_error(self):
        response = requests.get(self.url("/boom"), timeout=5)
        self.assertEqual(response.status_code, 500)

        calls = self.get_metric_data_point(_SpanMetrics.CALLS_NAME, "GET")
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "ERROR")
        self.assertEqual(calls.attributes[HTTP_REQUEST_METHOD], "GET")
        self.assertEqual(calls.attributes[HTTP_RESPONSE_STATUS_CODE], 500)

    def test_legacy_http_attributes_normalized(self):
        calls = self.record_span(
            "legacy-http",
            kind=SpanKind.CLIENT,
            attributes={HTTP_METHOD: "POST", HTTP_STATUS_CODE: 201},
        )
        self.assertEqual(calls.attributes[HTTP_REQUEST_METHOD], "POST")
        self.assertEqual(calls.attributes[HTTP_RESPONSE_STATUS_CODE], 201)
        self.assertNotIn(HTTP_METHOD, calls.attributes)
        self.assertNotIn(HTTP_STATUS_CODE, calls.attributes)


class TestSpanMetricsConnectorHttpServer(SpanMetricsConnectorTestBase):
    def setUp(self):
        super().setUp()
        self.app = Flask(__name__)

        @self.app.route("/items/<item_id>")
        def get_item(item_id):
            return {"status": "ok"}

        FlaskInstrumentor().instrument_app(
            self.app,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )
        self.client = self.app.test_client()

    def tearDown(self):
        FlaskInstrumentor().uninstrument_app(self.app)
        super().tearDown()

    def test_server_request_records_route_and_kind(self):
        response = self.client.get("/items/42")
        self.assertEqual(response.status_code, 200)

        span = self.get_finished_spans().by_name("GET /items/<item_id>")
        self.assertEqual(span.kind, SpanKind.SERVER)

        calls, _ = self.assert_span_metrics("GET /items/<item_id>", span_kind="SERVER", status_code="UNSET")
        self.assertEqual(calls.attributes[HTTP_REQUEST_METHOD], "GET")
        self.assertEqual(calls.attributes[HTTP_RESPONSE_STATUS_CODE], 200)
        self.assertEqual(calls.attributes[HTTP_ROUTE], "/items/<item_id>")


class TestSpanMetricsConnectorDb(SpanMetricsConnectorTestBase):
    def setUp(self):
        super().setUp()
        SQLite3Instrumentor().instrument(tracer_provider=self.tracer_provider)
        self.connection = sqlite3.connect(":memory:")
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.close()
        self.memory_exporter.clear()

    def tearDown(self):
        self.connection.close()
        SQLite3Instrumentor().uninstrument()
        super().tearDown()

    def test_select_records_db_attributes(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM users WHERE id = 1")
        cursor.fetchall()
        cursor.close()

        span = self.get_finished_spans().by_name("SELECT")
        self.assertEqual(span.kind, SpanKind.CLIENT)

        calls, _ = self.assert_span_metrics("SELECT", span_kind="CLIENT", status_code="UNSET")
        self.assertEqual(calls.attributes[DB_SYSTEM_NAME], "sqlite")

    def test_operations_recorded_under_distinct_span_names(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO users (name) VALUES ('alice')")
        cursor.execute("SELECT name FROM users")
        cursor.fetchall()
        cursor.close()

        insert = self.get_metric_data_point(_SpanMetrics.CALLS_NAME, "INSERT")
        select = self.get_metric_data_point(_SpanMetrics.CALLS_NAME, "SELECT")
        self.assertEqual(insert.value, 1)
        self.assertEqual(select.value, 1)
        self.assertEqual(insert.attributes[DB_SYSTEM_NAME], "sqlite")
        self.assertEqual(select.attributes[DB_SYSTEM_NAME], "sqlite")

    def test_legacy_db_attributes_normalized(self):
        calls = self.record_span(
            "legacy-db",
            kind=SpanKind.CLIENT,
            attributes={
                DB_SYSTEM: "postgresql",
                DB_OPERATION: "SELECT",
                DB_SQL_TABLE: "users",
            },
        )
        self.assertEqual(calls.attributes[DB_SYSTEM_NAME], "postgresql")
        self.assertEqual(calls.attributes[DB_OPERATION_NAME], "SELECT")
        self.assertEqual(calls.attributes[DB_COLLECTION_NAME], "users")


class TestSpanMetricsConnectorRpc(SpanMetricsConnectorTestBase):
    def setUp(self):
        super().setUp()
        BotocoreInstrumentor().instrument(tracer_provider=self.tracer_provider)
        self.client = botocore.session.get_session().create_client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    def tearDown(self):
        BotocoreInstrumentor().uninstrument()
        super().tearDown()

    def test_successful_call_records_rpc_attributes(self):
        stubber = Stubber(self.client)
        stubber.add_response("list_buckets", {"Buckets": [{"Name": "my-bucket"}]})
        with stubber:
            self.client.list_buckets()

        span = self.get_finished_spans().by_name("S3.ListBuckets")
        self.assertEqual(span.kind, SpanKind.CLIENT)

        calls, _ = self.assert_span_metrics("S3.ListBuckets", span_kind="CLIENT", status_code="UNSET")
        self.assertEqual(span.attributes[RPC_SYSTEM], "aws-api")
        self.assertEqual(calls.attributes[RPC_SYSTEM_NAME], "aws-api")
        self.assertNotIn(RPC_SYSTEM, calls.attributes)
        self.assertEqual(calls.attributes[RPC_SERVICE], "S3")
        self.assertEqual(calls.attributes[RPC_METHOD], "ListBuckets")

    def test_error_call_marks_status_error(self):
        stubber = Stubber(self.client)
        stubber.add_client_error("list_buckets", service_error_code="AccessDenied", http_status_code=403)
        with stubber:
            with self.assertRaises(ClientError):
                self.client.list_buckets()

        calls = self.get_metric_data_point(_SpanMetrics.CALLS_NAME, "S3.ListBuckets")
        self.assertEqual(calls.attributes[_SpanMetrics.STATUS_CODE], "ERROR")
        self.assertEqual(calls.attributes[RPC_SERVICE], "S3")
        self.assertEqual(calls.attributes[RPC_METHOD], "ListBuckets")
