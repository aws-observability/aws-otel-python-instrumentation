# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Regression coverage for additive trace export and destination isolation."""

import gzip
import json
import os
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import TestCase
from unittest.mock import MagicMock, patch

from botocore.session import Session as BotocoreSession
from requests import Response, Session

from amazon.opentelemetry.distro.additional_otlp_span_processor import (
    OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE,
    OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION,
    OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED,
    OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT,
    OTEL_AWS_ADDITIONAL_TRACES_HEADERS,
    OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED,
    OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT,
    add_additional_span_processors,
)
from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from amazon.opentelemetry.distro.attribute_redacting_span_processor import AttributeRedactingSpanProcessor
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from amazon.opentelemetry.distro.aws_opentelemetry_configurator import _init_tracing
from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import OTLPAwsSpanExporter
from amazon.opentelemetry.distro.llo_handler import LLOHandler
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import Span as OtlpSpan
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import Event, ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON, ParentBased
from opentelemetry.sdk.util import BoundedList
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, SpanKind, TraceFlags, set_span_in_context

MODULE = "amazon.opentelemetry.distro.additional_otlp_span_processor"
CONFIGURATOR = "amazon.opentelemetry.distro.aws_opentelemetry_configurator"


@contextmanager
def receiver(status=200):
    deliveries = []

    class Handler(BaseHTTPRequestHandler):
        # pylint: disable=invalid-name
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if self.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            request = ExportTraceServiceRequest()
            request.ParseFromString(body)
            deliveries.append({"headers": dict(self.headers), "request": request})
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/traces", deliveries
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def span_ids(deliveries):
    return [
        (span.trace_id, span.span_id)
        for delivery in deliveries
        for resource in delivery["request"].resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


# pylint: disable=too-many-public-methods
class TestAdditionalOtlpSpanProcessor(TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.provider = TracerProvider()
        self.addCleanup(self.provider.shutdown)

    def test_unset_endpoint_does_not_create_an_exporter(self):
        with patch(f"{MODULE}.OTLPSpanExporter") as exporter:
            add_additional_span_processors(self.provider)
        exporter.assert_not_called()

    def test_invalid_configuration_is_reported_without_values_or_new_processors(self):
        cases = [
            {OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: ""},
            {OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "file:///private"},
            {OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://user:secret@example.com/v1/traces"},
            {OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://example.com:99999/v1/traces"},
            {OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "authorization=secret;malformed"},
            {OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "authorization=secret%0D%0Ainjected"},
            {OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "content-type=text/plain"},
            {OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "Content%2DType=text/plain"},
            {OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "bad%0Aname=value"},
            {OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "x-value=non-latin-%E2%82%AC"},
            {OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT: "nan"},
            {OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT: "0"},
            {OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT: "inf"},
            {OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION: "unsupported"},
            {OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED: "invalid"},
            {OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED: "invalid"},
        ]
        for case in cases:
            with self.subTest(case=case), patch.dict(
                os.environ, {OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://example.com/v1/traces", **case}
            ), patch(f"{MODULE}.OTLPSpanExporter") as exporter, self.assertLogs(MODULE, level="WARNING") as logs:
                add_additional_span_processors(self.provider)
                exporter.assert_not_called()
                self.assertNotIn("secret", "".join(logs.output))

    def test_timeout_and_tls_configuration_are_independent(self):
        os.environ.update(
            {
                OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://example.com/v1/traces",
                OTEL_AWS_ADDITIONAL_TRACES_TIMEOUT: "2500",
                "OTEL_EXPORTER_OTLP_CERTIFICATE": "primary-ca.pem",
                "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "primary-client.pem",
                "OTEL_EXPORTER_OTLP_CLIENT_KEY": "primary-client-key.pem",
            }
        )

        def exporter_factory(**kwargs):
            kwargs["session"].verify = "primary-ca.pem"
            kwargs["session"].cert = ("primary-client.pem", "primary-client-key.pem")
            return InMemorySpanExporter()

        with patch(f"{MODULE}.OTLPSpanExporter", side_effect=exporter_factory) as exporter:
            add_additional_span_processors(self.provider)
        kwargs = exporter.call_args.kwargs
        self.assertEqual(kwargs["timeout"], 2.5)
        self.assertIs(kwargs["session"].verify, True)
        self.assertIsNone(kwargs["session"].cert)

    def test_additional_ca_does_not_change_primary_environment(self):
        os.environ.update(
            {
                OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://example.com/v1/traces",
                OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE: "additional-ca.pem",
                "OTEL_EXPORTER_OTLP_CERTIFICATE": "primary-ca.pem",
            }
        )
        with patch(f"{MODULE}.OTLPSpanExporter", return_value=InMemorySpanExporter()) as exporter:
            add_additional_span_processors(self.provider)
        self.assertEqual(exporter.call_args.kwargs["session"].verify, "additional-ca.pem")
        self.assertEqual(os.environ["OTEL_EXPORTER_OTLP_CERTIFICATE"], "primary-ca.pem")

    def test_actual_transport_uses_only_additional_tls_and_headers(self):
        os.environ.update(
            {
                OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://example.com/v1/traces",
                OTEL_AWS_ADDITIONAL_TRACES_CERTIFICATE: "additional-ca.pem",
                "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": "primary-ca.pem",
                "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE": "primary-client.pem",
                "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY": "primary-key.pem",
                "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "authorization=primary-token",
            }
        )
        response = Response()
        response.status_code = 200
        response._content = b""
        with patch.object(Session, "send", return_value=response) as send:
            add_additional_span_processors(self.provider)
            with self.provider.get_tracer("test").start_as_current_span("tls-isolation"):
                pass
            self.assertTrue(self.provider.force_flush())
        self.assertEqual(send.call_count, 1)
        self.assertEqual(send.call_args.kwargs["verify"], "additional-ca.pem")
        self.assertIsNone(send.call_args.kwargs["cert"])
        self.assertFalse(send.call_args.kwargs["allow_redirects"])
        self.assertNotIn("Authorization", send.call_args.args[0].headers)

    def test_primary_credentials_are_not_inherited_when_additional_headers_are_unset(self):
        with receiver() as (endpoint, deliveries):
            os.environ.update(
                {
                    OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: endpoint,
                    "OTEL_EXPORTER_OTLP_HEADERS": "authorization=primary-secret,x-primary=private",
                    "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "authorization=primary-trace-secret,x-primary=private",
                    "OTEL_PYTHON_EXPORTER_OTLP_HTTP_TRACES_CREDENTIAL_PROVIDER": "invalid-primary-provider",
                }
            )
            add_additional_span_processors(self.provider)
            with self.provider.get_tracer("test").start_as_current_span("additional-only"):
                pass
            self.provider.force_flush()
            self.assertEqual(len(span_ids(deliveries)), 1)
            headers = {key.lower(): value for key, value in deliveries[0]["headers"].items()}
            self.assertNotIn("authorization", headers)
            self.assertNotIn("x-primary", headers)

    def test_real_signed_primary_and_additional_receive_identical_spans(self):
        with receiver() as (primary_endpoint, primary), receiver() as (additional_endpoint, additional):
            session = BotocoreSession()
            session.set_credentials("test-access-key", "test-secret-key")
            primary_exporter = OTLPAwsSpanExporter(session=session, aws_region="us-east-1", endpoint=primary_endpoint)
            self.provider.add_span_processor(BatchSpanProcessor(primary_exporter))
            os.environ.update(
                {
                    OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: additional_endpoint,
                    OTEL_AWS_ADDITIONAL_TRACES_HEADERS: "authorization=Bearer%20test-token,x-extra=one%3Dtwo",
                    OTEL_AWS_ADDITIONAL_TRACES_COMPRESSION: "gzip",
                    "OTEL_EXPORTER_OTLP_HEADERS": "x-primary=private",
                }
            )
            add_additional_span_processors(self.provider)
            for index in range(2):
                with self.provider.get_tracer("test").start_as_current_span(f"span-{index}"):
                    pass
                self.provider.force_flush()
            self.assertEqual(span_ids(primary), span_ids(additional))
            self.assertEqual(len(span_ids(primary)), 2)
            self.assertTrue(primary[0]["headers"]["Authorization"].startswith("AWS4-HMAC-SHA256"))
            for delivery in additional:
                headers = {key.lower(): value for key, value in delivery["headers"].items()}
                self.assertEqual(headers["authorization"], "Bearer test-token")
                self.assertEqual(headers["x-extra"], "one=two")
                self.assertNotIn("x-primary", headers)
                self.assertEqual(headers["content-encoding"], "gzip")

    def test_record_only_spans_are_mirrored_once_when_enabled(self):
        self.provider.shutdown()
        self.provider = TracerProvider(sampler=AlwaysRecordSampler(ALWAYS_OFF))
        self.addCleanup(self.provider.shutdown)
        with receiver() as (primary_endpoint, primary), receiver() as (additional_endpoint, additional):
            self.provider.add_span_processor(BatchUnsampledSpanProcessor(OTLPSpanExporter(endpoint=primary_endpoint)))
            os.environ[OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT] = additional_endpoint
            add_additional_span_processors(self.provider, include_unsampled=True)
            with self.provider.get_tracer("test").start_as_current_span("record-only"):
                pass
            self.provider.force_flush()
            self.assertEqual(len(span_ids(primary)), 1)
            self.assertEqual(span_ids(primary), span_ids(additional))

    def test_sampled_and_record_only_spans_are_each_exported_once(self):
        with receiver() as (endpoint, deliveries):
            provider = TracerProvider(sampler=AlwaysRecordSampler(ParentBased(ALWAYS_ON)))
            self.addCleanup(provider.shutdown)
            os.environ[OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT] = endpoint
            add_additional_span_processors(provider, include_unsampled=True)
            tracer = provider.get_tracer("test")
            with tracer.start_as_current_span("sampled"):
                pass
            parent = NonRecordingSpan(SpanContext(1, 2, True, TraceFlags(0)))
            with tracer.start_as_current_span("record-only", context=set_span_in_context(parent)):
                pass
            self.assertTrue(provider.force_flush())
            ids = span_ids(deliveries)
            self.assertEqual(len(ids), 2)
            self.assertEqual(len(set(ids)), 2)

    def test_aws_content_extraction_leaves_additional_content_intact(self):
        with receiver() as (primary_endpoint, primary), receiver() as (additional_endpoint, additional):
            os.environ.update(
                {
                    "AGENT_OBSERVABILITY_ENABLED": "true",
                    "AWS_GENAI_CONTENT_EXTRACTION_OPT_OUT": "false",
                    OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: additional_endpoint,
                }
            )
            session = BotocoreSession()
            session.set_credentials("test-access-key", "test-secret-key")
            logger_provider = LoggerProvider()
            self.addCleanup(logger_provider.shutdown)
            primary_exporter = OTLPAwsSpanExporter(
                session=session, aws_region="us-east-1", endpoint=primary_endpoint, logger_provider=logger_provider
            )
            primary_processor = BatchSpanProcessor(primary_exporter)
            self.provider.add_span_processor(primary_processor)
            add_additional_span_processors(self.provider)
            with patch.object(LLOHandler, "_emit_llo_attributes") as emit:
                with self.provider.get_tracer("test").start_as_current_span(
                    "content", attributes={"gen_ai.prompt": "fixture-prompt", "keep": "value"}
                ) as span:
                    span.add_event("event", {"gen_ai.completion": "fixture-output", "keep": "event"})
                self.assertTrue(primary_processor.force_flush())
                self.assertTrue(self.provider.force_flush())
            emit.assert_called_once()
            self.assertEqual(span_ids(primary), span_ids(additional))
            primary_span = primary[0]["request"].resource_spans[0].scope_spans[0].spans[0]
            additional_span = additional[0]["request"].resource_spans[0].scope_spans[0].spans[0]
            self.assertNotIn("gen_ai.prompt", [attr.key for attr in primary_span.attributes])
            self.assertNotIn("gen_ai.completion", [attr.key for attr in primary_span.events[0].attributes])
            self.assertEqual(
                next(attr.value.string_value for attr in additional_span.attributes if attr.key == "gen_ai.prompt"),
                "fixture-prompt",
            )
            self.assertEqual(
                next(
                    attr.value.string_value
                    for attr in additional_span.events[0].attributes
                    if attr.key == "gen_ai.completion"
                ),
                "fixture-output",
            )

    def test_failed_additional_delivery_does_not_prevent_primary_delivery(self):
        with receiver(400) as (additional_endpoint, additional):
            primary_exporter = InMemorySpanExporter()
            self.provider.add_span_processor(BatchSpanProcessor(primary_exporter))
            os.environ[OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT] = additional_endpoint
            add_additional_span_processors(self.provider)
            for index in range(2):
                with self.provider.get_tracer("test").start_as_current_span(f"failure-{index}"):
                    pass
                self.provider.force_flush()
            self.assertEqual(len(primary_exporter.get_finished_spans()), 2)
            self.assertEqual(len(span_ids(additional)), 2)

    def test_slow_additional_export_does_not_block_primary_worker(self):
        started = threading.Event()
        release = threading.Event()
        primary_exporter = InMemorySpanExporter()
        primary_processor = BatchSpanProcessor(primary_exporter)
        self.provider.add_span_processor(primary_processor)
        additional_exporter = MagicMock()

        def export(_spans):
            started.set()
            release.wait(timeout=5)
            return SpanExportResult.FAILURE

        additional_exporter.export.side_effect = export
        os.environ.update(
            {
                OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: "https://example.com/v1/traces",
                "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "1",
            }
        )
        with patch(f"{MODULE}.OTLPSpanExporter", return_value=additional_exporter):
            add_additional_span_processors(self.provider)
        try:
            with self.provider.get_tracer("test").start_as_current_span("independent-workers"):
                pass
            self.assertTrue(started.wait(timeout=2))
            self.assertTrue(primary_processor.force_flush(timeout_millis=1000))
            self.assertEqual(len(primary_exporter.get_finished_spans()), 1)
        finally:
            release.set()
            self.provider.force_flush()

    def test_configurator_wires_additional_export_before_primary_redaction(self):
        os.environ[OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT] = "https://example.com/v1/traces"
        registered_processors = []

        def capture_registration(provider, include_unsampled):
            registered_processors.extend(provider._active_span_processor._span_processors)

        with patch(f"{CONFIGURATOR}.set_tracer_provider") as set_provider, patch(
            f"{CONFIGURATOR}.add_additional_span_processors", side_effect=capture_registration
        ) as add:
            _init_tracing({"memory": InMemorySpanExporter}, sampler=ALWAYS_ON)
        provider = set_provider.call_args.args[0]
        self.addCleanup(provider.shutdown)
        add.assert_called_once_with(provider, include_unsampled=False)
        self.assertFalse(any(isinstance(p, AttributeRedactingSpanProcessor) for p in registered_processors))
        self.assertTrue(
            any(isinstance(p, BatchSpanProcessor) for p in provider._active_span_processor._span_processors)
        )

    def test_disabled_primary_exporters_do_not_enable_additional_export(self):
        os.environ[OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT] = "https://example.com/v1/traces"
        with patch(f"{CONFIGURATOR}.set_tracer_provider") as set_provider, patch(
            f"{CONFIGURATOR}.add_additional_span_processors"
        ) as add:
            _init_tracing({}, sampler=ALWAYS_ON)
        self.addCleanup(set_provider.call_args.args[0].shutdown)
        add.assert_not_called()

    def test_configurator_redacts_spans_events_and_links_before_both_exports(self):
        with receiver() as (endpoint, deliveries):
            os.environ.update(
                {
                    OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: endpoint,
                    OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED: "true",
                    "AGENT_OBSERVABILITY_ENABLED": "true",
                    "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false",
                    "AWS_REDACT_SPAN_ATTRIBUTES": "private",
                }
            )
            primary_exporter = InMemorySpanExporter()
            with patch(f"{CONFIGURATOR}.set_tracer_provider") as set_provider:
                _init_tracing({"memory": lambda: primary_exporter}, sampler=ALWAYS_ON)
            provider = set_provider.call_args.args[0]
            self.addCleanup(provider.shutdown)
            tracer = provider.get_tracer("test")
            attributes = {"gen_ai.operation.name": "chat", "private": "span-content"}
            with tracer.start_as_current_span("outer", kind=SpanKind.CLIENT, attributes=attributes) as span:
                span.add_event("event", {"private": "event-content"})
                span.add_link(SpanContext(1, 2, False, TraceFlags(1)), {"private": "link-content"})
                with tracer.start_as_current_span("inner", kind=SpanKind.CLIENT, attributes=attributes):
                    pass
            self.assertTrue(provider.force_flush())
            primary_spans = primary_exporter.get_finished_spans()
            self.assertEqual(len(primary_spans), 2)
            outer = next(span for span in primary_spans if span.name == "outer")
            self.assertEqual(outer.kind, SpanKind.INTERNAL)
            self.assertEqual(outer.attributes["private"], "REDACTED")
            self.assertEqual(outer.events[0].attributes["private"], "REDACTED")
            self.assertEqual(outer.links[0].attributes["private"], "REDACTED")
            wire_spans = [
                span
                for delivery in deliveries
                for resource in delivery["request"].resource_spans
                for scope in resource.scope_spans
                for span in scope.spans
            ]
            self.assertEqual(len(wire_spans), 2)
            wire_outer = next(span for span in wire_spans if span.name == "outer")
            self.assertEqual(wire_outer.kind, OtlpSpan.SPAN_KIND_INTERNAL)
            self.assertEqual(
                next(attr.value.string_value for attr in wire_outer.attributes if attr.key == "private"), "REDACTED"
            )
            self.assertEqual(wire_outer.events[0].attributes[0].value.string_value, "REDACTED")
            self.assertEqual(wire_outer.links[0].attributes[0].value.string_value, "REDACTED")

    # pylint: disable=too-many-locals
    def test_additional_filter_flags_do_not_change_primary_filtering(self):
        cases = [(None, None), ("false", "false"), ("true", "false"), ("false", "true"), ("TRUE", " true ")]
        for redaction, content_filtering in cases:
            with self.subTest(redaction=redaction, content_filtering=content_filtering), receiver() as (
                endpoint,
                deliveries,
            ):
                environment = {
                    OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT: endpoint,
                    "OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false",
                    "AWS_REDACT_SPAN_ATTRIBUTES": "private",
                }
                if redaction is not None:
                    environment[OTEL_AWS_ADDITIONAL_TRACES_REDACTION_ENABLED] = redaction
                if content_filtering is not None:
                    environment[OTEL_AWS_ADDITIONAL_TRACES_CONTENT_FILTERING_ENABLED] = content_filtering
                primary_exporter = InMemorySpanExporter()
                with patch.dict(os.environ, environment), patch(f"{CONFIGURATOR}.set_tracer_provider") as set_provider:
                    _init_tracing({"memory": lambda exporter=primary_exporter: exporter}, sampler=ALWAYS_ON)
                provider = set_provider.call_args.args[0]
                try:
                    with patch.object(LLOHandler, "_emit_llo_attributes") as emit:
                        with provider.get_tracer("test").start_as_current_span(
                            "content-flags", attributes={"private": "recorded", "gen_ai.prompt": "fixture-prompt"}
                        ) as span:
                            span.add_event(
                                "event", {"private": "recorded-event", "gen_ai.completion": "fixture-output"}
                            )
                            span.add_event("gen_ai.user.message", {"content": "fixture-message"})
                            span.add_link(SpanContext(1, 2, False, TraceFlags(1)), {"private": "recorded-link"})
                        self.assertTrue(provider.force_flush())
                    emit.assert_not_called()
                    primary_span = primary_exporter.get_finished_spans()[0]
                    self.assertEqual(primary_span.attributes["private"], "REDACTED")
                    self.assertEqual(primary_span.attributes["gen_ai.prompt"], "fixture-prompt")
                    self.assertEqual(primary_span.events[0].attributes["private"], "REDACTED")
                    self.assertEqual(primary_span.links[0].attributes["private"], "REDACTED")
                    wire_span = deliveries[0]["request"].resource_spans[0].scope_spans[0].spans[0]
                    attributes = {attr.key: attr.value.string_value for attr in wire_span.attributes}
                    event_attributes = {attr.key: attr.value.string_value for attr in wire_span.events[0].attributes}
                    redact = redaction is not None and redaction.lower() == "true"
                    remove_content = content_filtering is not None and content_filtering.strip().lower() == "true"
                    self.assertEqual(attributes["private"], "REDACTED" if redact else "recorded")
                    self.assertEqual(event_attributes["private"], "REDACTED" if redact else "recorded-event")
                    self.assertEqual(
                        wire_span.links[0].attributes[0].value.string_value, "REDACTED" if redact else "recorded-link"
                    )
                    self.assertEqual("gen_ai.prompt" in attributes, not remove_content)
                    self.assertEqual("gen_ai.completion" in event_attributes, not remove_content)
                    self.assertEqual(len(wire_span.events), 1 if remove_content else 2)
                finally:
                    provider.shutdown()

    def test_snapshot_preserves_attribute_limits_drop_counts_and_nested_values(self):
        exporter = InMemorySpanExporter()
        os.environ[OTEL_AWS_ADDITIONAL_TRACES_ENDPOINT] = "https://example.com/v1/traces"
        with patch(f"{MODULE}.OTLPSpanExporter", return_value=exporter):
            add_additional_span_processors(self.provider)
        attributes = BoundedAttributes(maxlen=2, attributes={"private": "recorded"}, max_value_len=100)
        attributes.dropped = 3
        events = BoundedList(1)
        events.append(Event("first"))
        events.append(Event("retained", {"private": "event"}))
        links = BoundedList(1)
        links.append(Link(SpanContext(3, 4, False, TraceFlags(1))))
        links.append(Link(SpanContext(5, 6, False, TraceFlags(1)), {"private": "link"}))
        span = ReadableSpan(
            "snapshot", SpanContext(1, 2, False, TraceFlags(1)), attributes=attributes, events=events, links=links
        )
        self.provider._active_span_processor.on_end(span)
        AttributeRedactingSpanProcessor(["private"]).on_end(span)
        self.assertTrue(self.provider.force_flush())
        snapshot = exporter.get_finished_spans()[0]
        self.assertEqual(snapshot.dropped_attributes, 3)
        self.assertEqual(snapshot.dropped_events, 1)
        self.assertEqual(snapshot.dropped_links, 1)
        self.assertEqual(snapshot._attributes.maxlen, 2)
        self.assertEqual(snapshot._attributes.max_value_len, 100)
        self.assertEqual(snapshot.attributes["private"], "recorded")
        self.assertEqual(snapshot.events[0].attributes["private"], "event")
        self.assertEqual(snapshot.links[0].attributes["private"], "link")
        nested = {"content": {"parts": ["recorded"]}}
        span = ReadableSpan("nested", SpanContext(7, 8, False, TraceFlags(1)), attributes=nested)
        self.provider._active_span_processor.on_end(span)
        nested["content"]["parts"][0] = "mutated"
        self.assertTrue(self.provider.force_flush())
        self.assertEqual(exporter.get_finished_spans()[1].attributes["content"]["parts"], ["recorded"])

    def test_content_extraction_does_not_mutate_spans_shared_with_another_exporter(self):
        context = SpanContext(1, 2, False, TraceFlags(1))
        span = ReadableSpan(
            "shared",
            context=context,
            attributes={"gen_ai.input.messages": json.dumps([{"role": "user", "content": "test"}]), "keep": "value"},
            events=[Event("event", attributes={"gen_ai.prompt": "test", "keep": "event"})],
        )
        logger_provider = LoggerProvider()
        self.addCleanup(logger_provider.shutdown)
        handler = LLOHandler(logger_provider)
        with patch.object(handler, "_emit_llo_attributes"):
            result = handler.process_spans([span])[0]
        self.assertIsNot(result, span)
        self.assertIn("gen_ai.input.messages", span.attributes)
        self.assertIn("gen_ai.prompt", span.events[0].attributes)
        self.assertNotIn("gen_ai.input.messages", result.attributes)
        self.assertNotIn("gen_ai.prompt", result.events[0].attributes)
        self.assertEqual(result.context, span.context)
