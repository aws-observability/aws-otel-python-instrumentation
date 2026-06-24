# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.opentelemetry_lite_sdk import (
    BatchingSpanProcessor,
    Event,
    InstrumentationScope,
    Span,
    SpanProcessor,
    Tracer,
    TracerProvider,
    UdpExporter,
    UdpSpanExporter,
    _build_lambda_resource,
    _encode_export_trace_request,
    _resolve_remote_operation,
    _resolve_remote_service,
)
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

_TEST_SERVICE_NAME = "test-service"
_TEST_FUNCTION_NAME = "test-function"
_TEST_LOG_GROUP = "/aws/lambda/test-function"


class TestBuildLambdaResource(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2,cloud.platform=aws_lambda",
            "OTEL_SERVICE_NAME": _TEST_SERVICE_NAME,
        },
    )
    def test_builds_resource_from_env_vars(self):
        resource = _build_lambda_resource()
        self.assertEqual(resource["service.name"], _TEST_SERVICE_NAME)
        self.assertEqual(resource["cloud.region"], "us-west-2")
        self.assertEqual(resource["cloud.platform"], "aws_lambda")
        self.assertEqual(resource["telemetry.sdk.language"], "python")
        self.assertEqual(resource["telemetry.sdk.name"], "opentelemetry")
        from opentelemetry import version as otel_version

        self.assertEqual(resource["telemetry.sdk.version"], otel_version.__version__)

    @patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "", "OTEL_SERVICE_NAME": ""})
    def test_handles_empty_env_vars(self):
        resource = _build_lambda_resource()
        self.assertNotIn("service.name", resource)
        self.assertEqual(resource["telemetry.sdk.language"], "python")

    @patch.dict(
        os.environ,
        {
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=from-resource,cloud.region=us-west-2",
            "OTEL_SERVICE_NAME": "",
        },
    )
    def test_service_name_from_resource_attrs_when_env_empty(self):
        resource = _build_lambda_resource()
        self.assertEqual(resource["service.name"], "from-resource")

    @patch.dict(
        os.environ,
        {
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=from-resource",
            "OTEL_SERVICE_NAME": "from-env",
        },
    )
    def test_otel_service_name_takes_precedence(self):
        resource = _build_lambda_resource()
        self.assertEqual(resource["service.name"], "from-env")


class TestInstrumentationScope(unittest.TestCase):

    def test_properties(self):
        scope = InstrumentationScope("my-module", "1.0.0", "https://schema.url")
        self.assertEqual(scope.name, "my-module")
        self.assertEqual(scope.version, "1.0.0")
        self.assertEqual(scope.schema_url, "https://schema.url")

    def test_defaults(self):
        scope = InstrumentationScope("my-module")
        self.assertEqual(scope.name, "my-module")
        self.assertIsNone(scope.version)
        self.assertEqual(scope.schema_url, "")
        self.assertEqual(scope.attributes, {})

    def test_attributes(self):
        scope = InstrumentationScope("my-module", "1.0.0", attributes={"foo": "bar", "count": 42})
        self.assertEqual(scope.attributes, {"foo": "bar", "count": 42})


class TestTracerProvider(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "OTEL_RESOURCE_ATTRIBUTES": "cloud.region=us-west-2",
            "OTEL_SERVICE_NAME": _TEST_SERVICE_NAME,
        },
    )
    def test_creates_resource_from_env(self):
        provider = TracerProvider()
        self.assertEqual(provider.resource["service.name"], _TEST_SERVICE_NAME)

    def test_uses_provided_resource(self):
        custom_resource = {"service.name": "custom"}
        provider = TracerProvider(resource=custom_resource)
        self.assertEqual(provider.resource["service.name"], "custom")

    @patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "", "OTEL_SERVICE_NAME": "test"})
    def test_get_tracer_returns_tracer(self):
        provider = TracerProvider()
        tracer = provider.get_tracer("test-module", "1.0.0")
        self.assertIsInstance(tracer, Tracer)

    @patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "", "OTEL_SERVICE_NAME": "test"})
    def test_get_tracer_empty_name(self):
        provider = TracerProvider()
        tracer = provider.get_tracer("")
        self.assertIsInstance(tracer, Tracer)

    @patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "", "OTEL_SERVICE_NAME": "test"})
    def test_add_span_processor(self):
        provider = TracerProvider()
        processor = MagicMock(spec=SpanProcessor)
        provider.add_span_processor(processor)
        self.assertEqual(len(provider._span_processors), 1)

    @patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "", "OTEL_SERVICE_NAME": "test"})
    def test_force_flush_calls_processors(self):
        provider = TracerProvider()
        processor = MagicMock(spec=SpanProcessor)
        provider.add_span_processor(processor)
        provider.force_flush()
        processor.force_flush.assert_called_once()

    @patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "", "OTEL_SERVICE_NAME": "test"})
    def test_shutdown_calls_processors(self):
        provider = TracerProvider()
        processor = MagicMock(spec=SpanProcessor)
        provider.add_span_processor(processor)
        provider.shutdown()
        processor.shutdown.assert_called_once()


class TestTracer(unittest.TestCase):

    def setUp(self):
        self.provider = TracerProvider(resource={"service.name": _TEST_SERVICE_NAME})
        self.processor = MagicMock(spec=SpanProcessor)
        self.provider.add_span_processor(self.processor)
        self.tracer = self.provider.get_tracer("test-module")

    def test_start_span_creates_span(self):
        span = self.tracer.start_span("test-span", kind=SpanKind.SERVER)
        self.assertIsInstance(span, Span)
        self.assertEqual(span.name, "test-span")
        self.assertEqual(span.kind, SpanKind.SERVER)
        span.end()

    def test_start_span_sets_resource(self):
        span = self.tracer.start_span("test-span")
        self.assertEqual(span.resource["service.name"], _TEST_SERVICE_NAME)
        span.end()

    def test_start_span_root_generates_trace_id(self):
        span = self.tracer.start_span("test-span")
        ctx = span.get_span_context()
        self.assertIsNotNone(ctx.trace_id)
        self.assertNotEqual(ctx.trace_id, 0)
        span.end()

    def test_start_span_root_is_always_sampled(self):
        span = self.tracer.start_span("test-span")
        ctx = span.get_span_context()
        self.assertTrue(ctx.trace_flags.sampled)
        span.end()

    def test_start_span_inherits_parent_trace_id(self):
        with self.tracer.start_as_current_span("parent") as parent:
            parent_ctx = parent.get_span_context()
            with self.tracer.start_as_current_span("child") as child:
                child_ctx = child.get_span_context()
                self.assertEqual(child_ctx.trace_id, parent_ctx.trace_id)
                self.assertNotEqual(child_ctx.span_id, parent_ctx.span_id)

    def test_start_span_inherits_parent_trace_flags(self):
        parent = self.tracer.start_span("parent")
        with self.tracer.start_as_current_span("child") as child:
            child_ctx = child.get_span_context()
            self.assertTrue(child_ctx.trace_flags.sampled)
        parent.end()

    def test_start_as_current_span_context_manager(self):
        with self.tracer.start_as_current_span("test-span", kind=SpanKind.CLIENT) as span:
            self.assertIsInstance(span, Span)
            self.assertEqual(span.name, "test-span")
            self.assertTrue(span.is_recording())
        self.assertFalse(span.is_recording())

    def test_start_span_calls_on_start(self):
        span = self.tracer.start_span("test-span")
        self.processor.on_start.assert_called_once()
        span.end()


class TestSpan(unittest.TestCase):

    def setUp(self):
        self.provider = TracerProvider(resource={"service.name": _TEST_SERVICE_NAME})
        self.processor = MagicMock(spec=SpanProcessor)
        self.provider.add_span_processor(self.processor)
        self.tracer = self.provider.get_tracer("test-module")

    def test_set_attribute(self):
        span = self.tracer.start_span("test")
        span.set_attribute("key", "value")
        self.assertEqual(span.attributes["key"], "value")
        span.end()

    def test_set_attribute_after_end_ignored(self):
        span = self.tracer.start_span("test")
        span.end()
        span.set_attribute("key", "value")
        self.assertNotIn("key", span.attributes)

    def test_set_attributes(self):
        span = self.tracer.start_span("test")
        span.set_attributes({"k1": "v1", "k2": 42})
        self.assertEqual(span.attributes["k1"], "v1")
        self.assertEqual(span.attributes["k2"], 42)
        span.end()

    def test_add_event(self):
        span = self.tracer.start_span("test")
        span.add_event("my-event", attributes={"detail": "info"})
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "my-event")
        self.assertEqual(span.events[0].attributes["detail"], "info")
        span.end()

    def test_update_name(self):
        span = self.tracer.start_span("original")
        span.update_name("updated")
        self.assertEqual(span.name, "updated")
        span.end()

    def test_set_status_error(self):
        span = self.tracer.start_span("test")
        span.set_status(StatusCode.ERROR, "something failed")
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.status.description, "something failed")
        span.end()

    def test_set_status_ok_cannot_be_overridden(self):
        span = self.tracer.start_span("test")
        span.set_status(StatusCode.OK)
        span.set_status(StatusCode.ERROR, "should not override")
        self.assertEqual(span.status.status_code, StatusCode.OK)
        span.end()

    def test_set_status_error_can_be_upgraded_to_ok(self):
        span = self.tracer.start_span("test")
        span.set_status(StatusCode.ERROR, "failed")
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        span.set_status(StatusCode.OK)
        self.assertEqual(span.status.status_code, StatusCode.OK)
        span.end()

    def test_is_recording(self):
        span = self.tracer.start_span("test")
        self.assertTrue(span.is_recording())
        span.end()
        self.assertFalse(span.is_recording())

    def test_end_calls_on_end(self):
        span = self.tracer.start_span("test")
        span.end()
        self.processor.on_end.assert_called_once_with(span)

    def test_end_twice_ignored(self):
        span = self.tracer.start_span("test")
        span.end()
        span.end()
        self.processor.on_end.assert_called_once()

    def test_start_time_and_end_time(self):
        span = self.tracer.start_span("test")
        self.assertIsNotNone(span.start_time)
        self.assertIsNone(span.end_time)
        span.end()
        self.assertIsNotNone(span.end_time)
        self.assertGreaterEqual(span.end_time, span.start_time)

    def test_record_exception(self):
        span = self.tracer.start_span("test")
        try:
            raise ValueError("test error")
        except ValueError as exc:
            span.record_exception(exc)
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")
        self.assertEqual(span.events[0].attributes["exception.type"], "ValueError")
        self.assertEqual(span.events[0].attributes["exception.message"], "test error")
        span.end()

    def test_links_always_empty(self):
        span = self.tracer.start_span("test")
        self.assertEqual(span.links, ())
        span.end()

    def test_parent_property(self):
        with self.tracer.start_as_current_span("parent") as parent:
            with self.tracer.start_as_current_span("child") as child:
                self.assertIsNotNone(child.parent)
                self.assertEqual(child.parent.span_id, parent.get_span_context().span_id)

    def test_context_manager_records_exception(self):
        with self.assertRaises(RuntimeError):
            with self.tracer.start_as_current_span("test") as span:
                raise RuntimeError("boom")
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")


class TestBatchingSpanProcessor(unittest.TestCase):

    def setUp(self):
        self.exporter = MagicMock()
        self.exporter.export.return_value = True
        self.processor = BatchingSpanProcessor(self.exporter)

    def test_on_end_batches_spans(self):
        span1 = MagicMock()
        span2 = MagicMock()
        self.processor.on_end(span1)
        self.processor.on_end(span2)
        self.assertEqual(len(self.processor._spans), 2)
        self.exporter.export.assert_not_called()

    def test_force_flush_exports_all(self):
        span1 = MagicMock()
        span2 = MagicMock()
        self.processor.on_end(span1)
        self.processor.on_end(span2)
        self.processor.force_flush()
        self.exporter.export.assert_called_once_with([span1, span2])
        self.assertEqual(len(self.processor._spans), 0)

    def test_force_flush_empty_does_not_export(self):
        self.processor.force_flush()
        self.exporter.export.assert_not_called()

    def test_shutdown_flushes_and_shuts_down_exporter(self):
        span = MagicMock()
        self.processor.on_end(span)
        self.processor.shutdown()
        self.exporter.export.assert_called_once_with([span])
        self.exporter.shutdown.assert_called_once()


class TestUdpSpanExporter(unittest.TestCase):

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    def test_export_sends_otlp(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "test"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.end()

        result = exporter.export([span])
        self.assertTrue(result)
        exporter._udp_exporter.send_otlp.assert_called_once()

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "true", "AWS_LAMBDA_FUNCTION_NAME": "my-func"})
    def test_export_injects_app_signals_attributes_when_enabled(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "my-service"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.end()

        exporter.export([span])
        self.assertEqual(span._attributes["aws.local.service"], "my-service")
        self.assertEqual(span._attributes["aws.local.operation"], "my-func/FunctionHandler")
        self.assertEqual(span._attributes["aws.local.environment"], "lambda:default")

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "true", "AWS_LAMBDA_FUNCTION_NAME": "my-func"})
    def test_export_injects_remote_attributes_for_client_spans(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "my-service"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("S3.ListBuckets", kind=SpanKind.CLIENT)
        span.set_attribute("rpc.service", "S3")
        span.set_attribute("rpc.system", "aws-api")
        span.set_attribute("rpc.method", "ListBuckets")
        span.end()

        exporter.export([span])
        self.assertEqual(span._attributes["aws.remote.service"], "AWS::S3")
        self.assertEqual(span._attributes["aws.remote.operation"], "ListBuckets")

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    def test_export_uses_t1s_prefix_for_sampled(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "test"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.end()

        exporter.export([span])
        call_kwargs = exporter._udp_exporter.send_otlp.call_args
        self.assertEqual(call_kwargs[1]["prefix"], "T1S")

    def test_export_uses_t1u_prefix_for_unsampled(self):
        from opentelemetry.trace import SpanContext, TraceFlags

        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "test"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("test-span", kind=SpanKind.SERVER)
        span._context = SpanContext(
            trace_id=span.get_span_context().trace_id,
            span_id=span.get_span_context().span_id,
            is_remote=False,
            trace_flags=TraceFlags(0x00),
        )
        span.end()

        exporter.export([span])
        call_kwargs = exporter._udp_exporter.send_otlp.call_args
        self.assertEqual(call_kwargs[1]["prefix"], "T1U")

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    def test_export_does_not_inject_app_signals_when_disabled(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "test"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.end()

        exporter.export([span])
        self.assertNotIn("aws.local.service", span._attributes)
        self.assertNotIn("aws.local.operation", span._attributes)

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    def test_export_empty_spans_returns_true(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        result = exporter.export([])
        self.assertTrue(result)
        exporter._udp_exporter.send_otlp.assert_not_called()

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    @patch("amazon.opentelemetry.distro.opentelemetry_lite_sdk._encode_export_trace_request")
    def test_export_error_path_returns_false_and_logs(self, mock_encode):
        mock_encode.side_effect = RuntimeError("encoding failed")
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()

        provider = TracerProvider(resource={"service.name": "test"})
        tracer = provider.get_tracer("test")
        span = tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.end()

        with self.assertLogs("amazon.opentelemetry.distro.opentelemetry_lite_sdk", level="ERROR") as cm:
            result = exporter.export([span])

        self.assertFalse(result)
        self.assertTrue(any("Failed to export" in msg for msg in cm.output))
        exporter._udp_exporter.send_otlp.assert_not_called()


class TestResolveRemoteService(unittest.TestCase):

    def test_aws_api_service(self):
        attrs = {"rpc.service": "S3", "rpc.system": "aws-api"}
        self.assertEqual(_resolve_remote_service(attrs), "AWS::S3")

    def test_non_aws_rpc_service(self):
        attrs = {"rpc.service": "MyService", "rpc.system": "grpc"}
        self.assertEqual(_resolve_remote_service(attrs), "MyService")

    def test_http_url_fallback(self):
        attrs = {"http.url": "https://example.com/api/data"}
        self.assertEqual(_resolve_remote_service(attrs), "example.com")

    def test_unknown_fallback(self):
        attrs = {}
        self.assertEqual(_resolve_remote_service(attrs), "UnknownRemoteService")


class TestResolveRemoteOperation(unittest.TestCase):

    def test_rpc_method(self):
        attrs = {"rpc.method": "ListBuckets"}
        self.assertEqual(_resolve_remote_operation(attrs), "ListBuckets")

    def test_http_method_and_url(self):
        attrs = {"http.method": "GET", "http.url": "https://example.com/api/data"}
        self.assertEqual(_resolve_remote_operation(attrs), "GET /api/data")

    def test_http_method_only(self):
        attrs = {"http.method": "POST"}
        self.assertEqual(_resolve_remote_operation(attrs), "POST")

    def test_unknown_fallback(self):
        attrs = {}
        self.assertEqual(_resolve_remote_operation(attrs), "UnknownRemoteOperation")


class TestOtlpEncoding(unittest.TestCase):

    def setUp(self):
        self.provider = TracerProvider(resource={"service.name": "test", "cloud.region": "us-west-2"})
        self.tracer = self.provider.get_tracer("test-module", "1.0.0")

    def test_encode_single_span(self):
        span = self.tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.set_attribute("key", "value")
        span.end()

        data = _encode_export_trace_request([span])
        self.assertIsInstance(data, bytes)
        self.assertGreater(len(data), 0)

    def test_encode_multiple_spans(self):
        span1 = self.tracer.start_span("span1", kind=SpanKind.SERVER)
        span1.end()
        span2 = self.tracer.start_span("span2", kind=SpanKind.CLIENT)
        span2.end()

        data = _encode_export_trace_request([span1, span2])
        self.assertIsInstance(data, bytes)
        self.assertGreater(len(data), 0)

    def test_encode_empty_spans_returns_empty(self):
        data = _encode_export_trace_request([])
        self.assertEqual(data, b"")

    def test_encode_span_with_events(self):
        span = self.tracer.start_span("test-span")
        span.add_event("my-event", attributes={"detail": "info"})
        span.end()

        data = _encode_export_trace_request([span])
        self.assertGreater(len(data), 0)

    def test_encode_span_with_error_status(self):
        span = self.tracer.start_span("test-span")
        span.set_status(StatusCode.ERROR, "failed")
        span.end()

        data = _encode_export_trace_request([span])
        self.assertGreater(len(data), 0)

    def test_encoded_data_is_valid_protobuf(self):
        span = self.tracer.start_span("test-span", kind=SpanKind.SERVER)
        span.set_attribute("test.key", "test.value")
        span.end()

        data = _encode_export_trace_request([span])
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

            req = ExportTraceServiceRequest()
            req.ParseFromString(data)
            self.assertEqual(len(req.resource_spans), 1)
            self.assertEqual(len(req.resource_spans[0].scope_spans[0].spans), 1)
            self.assertEqual(req.resource_spans[0].scope_spans[0].spans[0].name, "test-span")
        except ImportError:
            self.skipTest("protobuf library not available for validation")

    def test_encode_groups_spans_by_instrumentation_scope(self):
        provider = TracerProvider(resource={"service.name": "test-svc"})
        tracer_a = provider.get_tracer("scope.a", "1.0")
        tracer_b = provider.get_tracer("scope.b", "2.0")

        span_a1 = tracer_a.start_span("a1", kind=SpanKind.SERVER)
        span_a1.end()
        span_b1 = tracer_b.start_span("b1", kind=SpanKind.CLIENT)
        span_b1.end()
        span_a2 = tracer_a.start_span("a2", kind=SpanKind.CLIENT)
        span_a2.end()

        data = _encode_export_trace_request([span_a1, span_b1, span_a2])
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

            req = ExportTraceServiceRequest()
            req.ParseFromString(data)
            self.assertEqual(len(req.resource_spans), 1)
            rs = req.resource_spans[0]
            self.assertEqual(len(rs.scope_spans), 2)
            scope_map = {ss.scope.name: [s.name for s in ss.spans] for ss in rs.scope_spans}
            self.assertEqual(sorted(scope_map["scope.a"]), ["a1", "a2"])
            self.assertEqual(scope_map["scope.b"], ["b1"])
        except ImportError:
            self.skipTest("protobuf library not available for validation")

    def test_scope_attributes_encoded_and_grouped(self):
        provider = TracerProvider(resource={"service.name": "test-svc"})
        tracer_v1 = provider.get_tracer("my-lib", "1.0", attributes={"env": "prod"})
        tracer_v1_alt = provider.get_tracer("my-lib", "1.0", attributes={"env": "staging"})

        span1 = tracer_v1.start_span("s1", kind=SpanKind.SERVER)
        span1.end()
        span2 = tracer_v1_alt.start_span("s2", kind=SpanKind.CLIENT)
        span2.end()

        data = _encode_export_trace_request([span1, span2])
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

            req = ExportTraceServiceRequest()
            req.ParseFromString(data)
            rs = req.resource_spans[0]
            self.assertEqual(len(rs.scope_spans), 2)

            scope_attrs = {}
            for ss in rs.scope_spans:
                attrs = {kv.key: kv.value.string_value for kv in ss.scope.attributes}
                span_names = [s.name for s in ss.spans]
                scope_attrs[attrs.get("env")] = span_names

            self.assertEqual(scope_attrs["prod"], ["s1"])
            self.assertEqual(scope_attrs["staging"], ["s2"])
        except ImportError:
            self.skipTest("protobuf library not available for validation")


class TestEvent(unittest.TestCase):

    def test_event_properties(self):
        event = Event("test-event", attributes={"key": "val"}, timestamp=12345)
        self.assertEqual(event.name, "test-event")
        self.assertEqual(event.attributes["key"], "val")
        self.assertEqual(event.timestamp, 12345)

    def test_event_default_timestamp(self):
        event = Event("test-event")
        self.assertIsNotNone(event.timestamp)
        self.assertGreater(event.timestamp, 0)


class TestUdpExporter(unittest.TestCase):

    @patch("socket.socket")
    def test_send_otlp(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        exporter = UdpExporter(endpoint="127.0.0.1:2000")
        exporter.send_otlp(b"test-data", prefix="T1S")

        mock_socket.sendto.assert_called_once()
        sent_data = mock_socket.sendto.call_args[0][0]
        self.assertIn(b"T1S", sent_data)

    @patch("socket.socket")
    def test_shutdown_closes_socket(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        exporter = UdpExporter(endpoint="127.0.0.1:2000")
        exporter.shutdown()
        mock_socket.close.assert_called_once()


class TestLiteVsFullSdkParity(unittest.TestCase):
    """Compare lite SDK OTLP output against the full OTel SDK to ensure no data loss."""

    def _extract_any_value(self, val):
        which = val.WhichOneof("value")
        if which == "string_value":
            return val.string_value
        if which == "int_value":
            return val.int_value
        if which == "double_value":
            return val.double_value
        if which == "bool_value":
            return val.bool_value
        if which == "array_value":
            return [self._extract_any_value(v) for v in val.array_value.values]
        return str(val)

    def _parse_otlp(self, data):
        """Parse OTLP bytes into a normalized dict for comparison."""
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

        req = ExportTraceServiceRequest()
        req.ParseFromString(data)
        spans = []
        for rs in req.resource_spans:
            resource = {kv.key: kv.value.string_value for kv in rs.resource.attributes}
            for ss in rs.scope_spans:
                scope_name = ss.scope.name
                scope_version = ss.scope.version
                for span in ss.spans:
                    attrs = {}
                    for kv in span.attributes:
                        attrs[kv.key] = self._extract_any_value(kv.value)
                    spans.append(
                        {
                            "name": span.name,
                            "kind": span.kind,
                            "status_code": span.status.code,
                            "status_message": span.status.message,
                            "attributes": attrs,
                            "resource": resource,
                            "scope_name": scope_name,
                            "scope_version": scope_version,
                            "has_parent": bool(span.parent_span_id),
                            "events": [e.name for e in span.events],
                            "flags": span.flags,
                        }
                    )
        return spans

    def _create_full_sdk_spans(self):
        """Create spans using the full OTel SDK and export to OTLP bytes."""
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

        collected = []

        class CollectingExporter(SpanExporter):
            def export(self, spans):
                collected.extend(spans)
                return SpanExportResult.SUCCESS

        resource = Resource.create({"service.name": "test-service", "cloud.region": "us-west-2"})
        provider = SdkTracerProvider(resource=resource)
        provider.add_span_processor(SimpleSpanProcessor(CollectingExporter()))

        tracer = provider.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")
        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER) as server_span:
            server_span.set_attribute("faas.invocation_id", "req-123")
            server_span.set_attribute("http.status_code", 200)
            server_span.set_attribute("score", 3.14)
            server_span.set_attribute("is_cold_start", True)
            server_span.set_attribute("tags", ["prod", "us-west-2"])
            server_span.add_event("checkpoint", attributes={"step": "init"})
            server_span.set_status(StatusCode.OK)

            botocore_tracer = provider.get_tracer("opentelemetry.instrumentation.botocore", "0.2.0")
            with botocore_tracer.start_as_current_span("S3.ListBuckets", kind=SpanKind.CLIENT) as client_span:
                client_span.set_attribute("rpc.service", "S3")
                client_span.set_attribute("rpc.system", "aws-api")
                client_span.set_attribute("rpc.method", "ListBuckets")

        provider.shutdown()
        return collected

    def _create_lite_sdk_spans(self):
        """Create equivalent spans using the lite SDK."""
        lite_provider = TracerProvider(resource={"service.name": "test-service", "cloud.region": "us-west-2"})

        tracer = lite_provider.get_tracer("opentelemetry.instrumentation.aws_lambda", "0.1.0")
        with tracer.start_as_current_span("handler", kind=SpanKind.SERVER) as server_span:
            server_span.set_attribute("faas.invocation_id", "req-123")
            server_span.set_attribute("http.status_code", 200)
            server_span.set_attribute("score", 3.14)
            server_span.set_attribute("is_cold_start", True)
            server_span.set_attribute("tags", ["prod", "us-west-2"])
            server_span.add_event("checkpoint", attributes={"step": "init"})
            server_span.set_status(StatusCode.OK)

            botocore_tracer = lite_provider.get_tracer("opentelemetry.instrumentation.botocore", "0.2.0")
            with botocore_tracer.start_as_current_span("S3.ListBuckets", kind=SpanKind.CLIENT) as client_span:
                client_span.set_attribute("rpc.service", "S3")
                client_span.set_attribute("rpc.system", "aws-api")
                client_span.set_attribute("rpc.method", "ListBuckets")

        return [server_span, client_span]

    def test_span_structure_matches_full_sdk(self):
        """Verify lite SDK produces the same span structure as the full SDK."""
        try:
            from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider  # noqa: F401
        except ImportError:
            self.skipTest("Full OTel SDK not available")

        full_sdk_spans = sorted(self._create_full_sdk_spans(), key=lambda s: s.name)
        lite_spans = sorted(self._create_lite_sdk_spans(), key=lambda s: s.name)

        self.assertEqual(len(full_sdk_spans), len(lite_spans))

        for full_span, lite_span in zip(full_sdk_spans, lite_spans):
            self.assertEqual(full_span.name, lite_span.name)
            self.assertEqual(full_span.kind, lite_span.kind)
            for key, value in full_span.attributes.items():
                lite_value = lite_span.attributes[key]
                if isinstance(value, tuple):
                    self.assertEqual(list(value), list(lite_value))
                else:
                    self.assertEqual(value, lite_value)
            self.assertEqual(
                [e.name for e in full_span.events],
                [e.name for e in lite_span.events],
            )
            self.assertEqual(full_span.status.status_code, lite_span.status.status_code)

    def test_otlp_encoding_matches_full_sdk(self):
        """Verify lite SDK OTLP output decodes to the same logical data as the full SDK."""
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest  # noqa: F401
        except ImportError:
            self.skipTest("protobuf library not available")

        full_sdk_spans = sorted(self._create_full_sdk_spans(), key=lambda s: s.name)
        lite_spans = sorted(self._create_lite_sdk_spans(), key=lambda s: s.name)

        lite_otlp = _encode_export_trace_request(lite_spans)
        lite_parsed = sorted(self._parse_otlp(lite_otlp), key=lambda s: s["name"])

        self.assertEqual(len(lite_parsed), len(full_sdk_spans))

        kind_map = {SpanKind.INTERNAL: 1, SpanKind.SERVER: 2, SpanKind.CLIENT: 3}
        for full_span, lite_span_data in zip(full_sdk_spans, lite_parsed):
            self.assertEqual(full_span.name, lite_span_data["name"])
            self.assertEqual(kind_map[full_span.kind], lite_span_data["kind"])
            self.assertEqual(lite_span_data["scope_name"], full_span.instrumentation_scope.name)
            self.assertEqual(lite_span_data["scope_version"], full_span.instrumentation_scope.version)
            for key, value in full_span.attributes.items():
                self.assertIn(key, lite_span_data["attributes"])
                if isinstance(value, (list, tuple)):
                    self.assertEqual(list(value), lite_span_data["attributes"][key])
                else:
                    self.assertEqual(value, lite_span_data["attributes"][key])
            self.assertEqual([e.name for e in full_span.events], lite_span_data["events"])
            self.assertTrue(lite_span_data["flags"] & 0x100, "HAS_IS_REMOTE bit not set")

    def test_resource_attributes_match_full_sdk(self):
        """Verify resource attributes are encoded identically."""
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest  # noqa: F401
        except ImportError:
            self.skipTest("protobuf library not available")

        lite_spans = self._create_lite_sdk_spans()
        lite_otlp = _encode_export_trace_request(lite_spans)
        lite_parsed = self._parse_otlp(lite_otlp)

        self.assertEqual(lite_parsed[0]["resource"]["service.name"], "test-service")
        self.assertEqual(lite_parsed[0]["resource"]["cloud.region"], "us-west-2")

    def test_parent_child_relationship_matches_full_sdk(self):
        """Verify parent-child span IDs are correctly encoded like the full SDK."""
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest  # noqa: F401
        except ImportError:
            self.skipTest("protobuf library not available")

        lite_spans = self._create_lite_sdk_spans()
        lite_otlp = _encode_export_trace_request(lite_spans)
        lite_parsed = self._parse_otlp(lite_otlp)

        server = lite_parsed[0]
        client = lite_parsed[1]
        self.assertFalse(server["has_parent"])
        self.assertTrue(client["has_parent"])

    def test_multi_scope_grouping_matches_full_sdk(self):
        """Verify spans from different scopes are grouped correctly like the full SDK."""
        try:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest  # noqa: F401
        except ImportError:
            self.skipTest("protobuf library not available")

        lite_spans = self._create_lite_sdk_spans()
        lite_otlp = _encode_export_trace_request(lite_spans)

        req = ExportTraceServiceRequest()
        req.ParseFromString(lite_otlp)

        self.assertEqual(len(req.resource_spans), 1)
        scope_names = {ss.scope.name for ss in req.resource_spans[0].scope_spans}
        self.assertIn("opentelemetry.instrumentation.aws_lambda", scope_names)
        self.assertIn("opentelemetry.instrumentation.botocore", scope_names)


class TestAddCodeAttributesNoOpInLiteMode(unittest.TestCase):
    """Tests that add_code_attributes_to_span is a no-op in lite mode."""

    _LAMBDA_LAYER_DIR = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "lambda-layer", "src")
    )
    _HAS_LAMBDA_LAYER = os.path.isfile(os.path.join(_LAMBDA_LAYER_DIR, "otel_wrapper.py"))

    def setUp(self):
        if not self._HAS_LAMBDA_LAYER:
            self.skipTest("lambda-layer/src not available")
        import sys

        sys.path.insert(0, self._LAMBDA_LAYER_DIR)

    def tearDown(self):
        import sys

        if self._LAMBDA_LAYER_DIR in sys.path:
            sys.path.remove(self._LAMBDA_LAYER_DIR)

    @patch.dict(
        "os.environ",
        {
            "OTEL_AWS_LAMBDA_FAST_START": "true",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
        },
    )
    def test_add_code_attributes_is_noop_in_lite_mode(self):
        import importlib

        import opentelemetry.instrumentation.aws_lambda as aws_lambda_mod

        importlib.reload(aws_lambda_mod)

        mock_span = MagicMock()
        mock_func = MagicMock()
        aws_lambda_mod.add_code_attributes_to_span(mock_span, mock_func)

        mock_span.set_attribute.assert_not_called()

    @patch.dict(
        "os.environ",
        {
            "OTEL_AWS_LAMBDA_FAST_START": "false",
            "AWS_LAMBDA_FUNCTION_NAME": "my-function",
        },
    )
    def test_add_code_attributes_not_noop_when_disabled(self):
        import importlib

        import opentelemetry.instrumentation.aws_lambda as aws_lambda_mod

        importlib.reload(aws_lambda_mod)

        from amazon.opentelemetry.distro.code_correlation import add_code_attributes_to_span

        self.assertIs(aws_lambda_mod.add_code_attributes_to_span, add_code_attributes_to_span)
