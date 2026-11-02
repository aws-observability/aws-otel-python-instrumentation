# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import base64
import os
import re
import subprocess
import sys
import textwrap
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._udp_span_payload import (
    MAX_UDP_DATAGRAM_SIZE,
    TRUNCATED_ATTRIBUTE_KEY,
    build_payloads,
    max_payload_size,
    truncate_span_payload,
)
from amazon.opentelemetry.distro.opentelemetry_lite_sdk import TracerProvider as LiteTracerProvider
from amazon.opentelemetry.distro.opentelemetry_lite_sdk import UdpSpanExporter
from amazon.opentelemetry.distro.otlp_udp_exporter import (
    FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX,
    PROTOCOL_HEADER,
    OTLPUdpSpanExporter,
)
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_MAX_SIZE = max_payload_size(len(PROTOCOL_HEADER) + len(FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX))


def _make_spans(statement_sizes, other_attributes=None):
    memory_exporter = InMemorySpanExporter()
    provider = TracerProvider(span_limits=SpanLimits(max_span_attributes=1_000))
    provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    tracer = provider.get_tracer("test")
    for index, size in enumerate(statement_sizes):
        with tracer.start_as_current_span(f"INSERT {index}") as span:
            span.set_attribute("db.statement", "x" * size)
            for key, value in (other_attributes or {}).items():
                span.set_attribute(key, value)
    return memory_exporter.get_finished_spans()


def _encode(spans) -> bytes:
    return encode_spans(spans).SerializeToString()


def _decode_spans(data: bytes):
    request = ExportTraceServiceRequest()
    request.ParseFromString(data)
    return [span for rs in request.resource_spans for ss in rs.scope_spans for span in ss.spans]


def _attributes(span):
    return {kv.key: kv.value for kv in span.attributes}


def _marker_paths(attributes):
    return [value.string_value for value in attributes[TRUNCATED_ATTRIBUTE_KEY].array_value.values]


def _assert_suffix(test, value: str, original_size: int) -> int:
    """Assert value ends with the truncation suffix for original_size and return the kept size."""
    match = re.search(r"\.\.\.\[truncated: (\d+) -> (\d+) bytes\]$", value)
    test.assertIsNotNone(match, value[-80:])
    test.assertEqual(int(match.group(1)), original_size)
    kept_size = int(match.group(2))
    test.assertEqual(len(value[: match.start()].encode("utf-8")), kept_size)
    return kept_size


def _datagram_size(data: bytes) -> int:
    message = PROTOCOL_HEADER + FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX + base64.b64encode(data).decode("utf-8")
    return len(message.encode("utf-8"))


class TestMaxPayloadSize(TestCase):
    def test_largest_payload_fits_in_datagram(self):
        self.assertLessEqual(_datagram_size(b"\0" * _MAX_SIZE), MAX_UDP_DATAGRAM_SIZE)
        self.assertGreater(_datagram_size(b"\0" * (_MAX_SIZE + 3)), MAX_UDP_DATAGRAM_SIZE)


class TestBuildPayloads(TestCase):
    def test_small_batch_is_encoded_once(self):
        spans = _make_spans([100] * 10)
        encode = MagicMock(side_effect=_encode)
        payloads = list(build_payloads(list(spans), encode, _MAX_SIZE))
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0][0], 10)
        self.assertEqual(payloads[0][1], _encode(spans))
        encode.assert_called_once()

    def test_oversized_batch_is_split_without_losing_spans(self):
        # Matches the reported case: 10 SQL spans of 6,000 characters encode to ~84KB
        spans = _make_spans([6_000] * 10)
        self.assertGreater(_datagram_size(_encode(spans)), MAX_UDP_DATAGRAM_SIZE)

        payloads = list(build_payloads(list(spans), _encode, _MAX_SIZE))

        self.assertGreater(len(payloads), 1)
        self.assertEqual(sum(count for count, _ in payloads), 10)
        exported = [span for _, data in payloads for span in _decode_spans(data)]
        self.assertEqual(len(exported), 10)
        for _, data in payloads:
            self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        for span in exported:
            self.assertEqual(len(_attributes(span)["db.statement"].string_value), 6_000)
            self.assertNotIn(TRUNCATED_ATTRIBUTE_KEY, _attributes(span))

    def test_only_oversized_span_is_truncated(self):
        spans = _make_spans([100, 100_000, 100])
        payloads = list(build_payloads(list(spans), _encode, _MAX_SIZE))

        exported = [span for _, data in payloads for span in _decode_spans(data)]
        self.assertEqual(len(exported), 3)
        truncated = [span for span in exported if TRUNCATED_ATTRIBUTE_KEY in _attributes(span)]
        self.assertEqual([span.name for span in truncated], ["INSERT 1"])
        for _, data in payloads:
            self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)


class TestTruncateSpanPayload(TestCase):
    def test_truncates_longest_value_and_keeps_others(self):
        spans = _make_spans([100_000], {"short": "keep me", "medium": "m" * 1_000, "count": 3})
        with self.assertLogs("amazon.opentelemetry.distro._udp_span_payload", level="WARNING") as logs:
            data = truncate_span_payload(_encode(spans), _MAX_SIZE)

        self.assertIsNotNone(data)
        self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        attributes = _attributes(_decode_spans(data)[0])
        self.assertEqual(_marker_paths(attributes), ["db.statement"])
        statement = attributes["db.statement"].string_value
        kept_size = _assert_suffix(self, statement, 100_000)
        self.assertGreater(kept_size, _MAX_SIZE - 2_000)
        self.assertEqual(attributes["short"].string_value, "keep me")
        self.assertEqual(attributes["medium"].string_value, "m" * 1_000)
        self.assertEqual(attributes["count"].int_value, 3)
        self.assertTrue(any("Truncated db.statement" in message for message in logs.output))

    def test_truncates_event_attributes(self):
        memory_exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
        with provider.get_tracer("test").start_as_current_span("failing") as span:
            span.add_event("exception", {"exception.stacktrace": "s" * 100_000})
        data = truncate_span_payload(_encode(memory_exporter.get_finished_spans()), _MAX_SIZE)

        self.assertIsNotNone(data)
        self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        span = _decode_spans(data)[0]
        self.assertEqual(_marker_paths(_attributes(span)), ["events[0].exception.stacktrace"])
        event_attributes = {kv.key: kv.value for kv in span.events[0].attributes}
        _assert_suffix(self, event_attributes["exception.stacktrace"].string_value, 100_000)

    def test_truncates_string_arrays(self):
        spans = _make_spans([10], {"queries": ("q" * 50_000, "r" * 50_000)})
        data = truncate_span_payload(_encode(spans), _MAX_SIZE)

        self.assertIsNotNone(data)
        self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        attributes = _attributes(_decode_spans(data)[0])
        self.assertEqual(sorted(_marker_paths(attributes)), ["queries[0]", "queries[1]"])
        for item in attributes["queries"].array_value.values:
            _assert_suffix(self, item.string_value, 50_000)

    def test_marks_every_truncated_value(self):
        spans = _make_spans([60_000], {"http.url": "u" * 60_000, "short": "keep me"})
        data = truncate_span_payload(_encode(spans), _MAX_SIZE)

        self.assertIsNotNone(data)
        self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        attributes = _attributes(_decode_spans(data)[0])
        self.assertEqual(sorted(_marker_paths(attributes)), ["db.statement", "http.url"])
        _assert_suffix(self, attributes["db.statement"].string_value, 60_000)
        _assert_suffix(self, attributes["http.url"].string_value, 60_000)

    def test_cuts_oversized_values_to_a_common_size(self):
        spans = _make_spans([60_000], {"http.url": "u" * 40_000, "stack": "s" * 50_000, "small": "x" * 5_000})
        data = truncate_span_payload(_encode(spans), _MAX_SIZE)

        self.assertIsNotNone(data)
        self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        attributes = _attributes(_decode_spans(data)[0])
        self.assertEqual(sorted(_marker_paths(attributes)), ["db.statement", "http.url", "stack"])
        kept_sizes = [
            _assert_suffix(self, attributes["db.statement"].string_value, 60_000),
            _assert_suffix(self, attributes["http.url"].string_value, 40_000),
            _assert_suffix(self, attributes["stack"].string_value, 50_000),
        ]
        self.assertEqual(len(set(kept_sizes)), 1)
        # The common size is as large as fits: roughly the payload limit shared by the three values
        self.assertGreater(kept_sizes[0], (_MAX_SIZE - 5_000) // 3 - 500)
        self.assertEqual(attributes["small"].string_value, "x" * 5_000)

    def test_cuts_on_utf8_character_boundary(self):
        spans = _make_spans([0], {"db.statement": "中" * 40_000})
        data = truncate_span_payload(_encode(spans), _MAX_SIZE)

        self.assertIsNotNone(data)
        value = _attributes(_decode_spans(data)[0])["db.statement"].string_value
        kept_size = _assert_suffix(self, value, 120_000)
        kept = value[: value.index("...[truncated:")]
        self.assertEqual(set(kept), {"中"})
        self.assertEqual(len(kept.encode("utf-8")), kept_size)

    def test_returns_none_when_many_small_attributes_cannot_fit(self):
        attributes = {f"key.{i}": "v" * 200 for i in range(500)}
        spans = _make_spans([0], attributes)
        with self.assertLogs("amazon.opentelemetry.distro._udp_span_payload", level="WARNING") as logs:
            self.assertIsNone(truncate_span_payload(_encode(spans), _MAX_SIZE))
        self.assertTrue(any("Dropping 1 span(s)" in message for message in logs.output))


class TestOTLPUdpSpanExporterSplitting(TestCase):
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_oversized_batch_is_sent_in_multiple_datagrams(self, mock_udp_exporter):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        exporter = OTLPUdpSpanExporter()

        result = exporter.export(_make_spans([6_000] * 10))

        self.assertEqual(result, SpanExportResult.SUCCESS)
        calls = mock_udp_exporter_instance.send_data.call_args_list
        self.assertGreater(len(calls), 1)
        self.assertEqual(sum(len(_decode_spans(call.kwargs["data"])) for call in calls), 10)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_send_failure_does_not_stop_remaining_datagrams(self, mock_udp_exporter):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        mock_udp_exporter_instance.send_data.side_effect = [OSError("boom"), None, None, None]
        exporter = OTLPUdpSpanExporter()

        result = exporter.export(_make_spans([6_000] * 10))

        self.assertEqual(result, SpanExportResult.FAILURE)
        self.assertGreater(mock_udp_exporter_instance.send_data.call_count, 1)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_unfittable_span_reports_failure_and_sends_others(self, mock_udp_exporter):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        exporter = OTLPUdpSpanExporter()
        spans = list(_make_spans([10])) + list(_make_spans([0], {f"key.{i}": "v" * 200 for i in range(500)}))

        result = exporter.export(spans)

        self.assertEqual(result, SpanExportResult.FAILURE)
        mock_udp_exporter_instance.send_data.assert_called_once()


class TestLiteUdpSpanExporterSplitting(TestCase):
    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    def test_oversized_flush_is_sent_in_multiple_datagrams(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()
        tracer = LiteTracerProvider(resource={"service.name": "test"}).get_tracer("test")
        spans = []
        for index in range(30):
            span = tracer.start_span(f"INSERT {index}")
            span.set_attribute("db.statement", "x" * 6_000)
            span.end()
            spans.append(span)
        spans.append(tracer.start_span("huge"))
        spans[-1].set_attribute("db.statement", "y" * 200_000)
        spans[-1].end()

        self.assertTrue(exporter.export(spans))

        calls = exporter._udp_exporter.send_otlp.call_args_list
        self.assertGreater(len(calls), 1)
        exported = [span for call in calls for span in _decode_spans(call.args[0])]
        self.assertEqual(len(exported), 31)
        for call in calls:
            self.assertLessEqual(_datagram_size(call.args[0]), MAX_UDP_DATAGRAM_SIZE)
        truncated = [span.name for span in exported if TRUNCATED_ATTRIBUTE_KEY in _attributes(span)]
        self.assertEqual(truncated, ["huge"])


class TestLiteTruncation(TestCase):
    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "false"})
    def test_truncates_copy_without_touching_original_span(self):
        exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
        exporter._udp_exporter = MagicMock()
        tracer = LiteTracerProvider(resource={"service.name": "test"}).get_tracer("test")
        span = tracer.start_span("huge")
        span.set_attribute("db.statement", "y" * 100_000)
        span.set_attribute("queries", ("q" * 30_000, "short"))
        span.add_event("exception", {"exception.stacktrace": "s" * 50_000})
        span.end()

        with patch("amazon.opentelemetry.distro._udp_span_payload.truncate_span_payload") as protobuf_truncate:
            self.assertTrue(exporter.export([span]))
        protobuf_truncate.assert_not_called()

        data = exporter._udp_exporter.send_otlp.call_args.args[0]
        self.assertLessEqual(_datagram_size(data), MAX_UDP_DATAGRAM_SIZE)
        exported = _decode_spans(data)[0]
        attributes = _attributes(exported)
        self.assertEqual(
            sorted(_marker_paths(attributes)), ["db.statement", "events[0].exception.stacktrace", "queries[0]"]
        )
        _assert_suffix(self, attributes["db.statement"].string_value, 100_000)
        self.assertEqual(attributes["queries"].array_value.values[1].string_value, "short")
        event_attributes = {kv.key: kv.value for kv in exported.events[0].attributes}
        _assert_suffix(self, event_attributes["exception.stacktrace"].string_value, 50_000)
        # The span itself is unchanged
        self.assertEqual(span.attributes["db.statement"], "y" * 100_000)
        self.assertEqual(span.events[0].attributes["exception.stacktrace"], "s" * 50_000)
        self.assertNotIn(TRUNCATED_ATTRIBUTE_KEY, span.attributes)

    def test_lite_truncation_does_not_import_protobuf(self):
        script = textwrap.dedent(
            """
            import os, sys
            from unittest.mock import MagicMock
            os.environ["OTEL_AWS_APPLICATION_SIGNALS_ENABLED"] = "false"
            from amazon.opentelemetry.distro.opentelemetry_lite_sdk import TracerProvider, UdpSpanExporter
            assert "google.protobuf" not in sys.modules, "imported before export"
            exporter = UdpSpanExporter(endpoint="127.0.0.1:2000")
            exporter._udp_exporter = MagicMock()
            span = TracerProvider().get_tracer("test").start_span("huge")
            span.set_attribute("db.statement", "y" * 200_000)
            span.end()
            assert exporter.export([span])
            assert exporter._udp_exporter.send_otlp.call_count == 1
            print("protobuf imported:", "google.protobuf" in sys.modules)
            """
        )
        output = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True).stdout
        self.assertIn("protobuf imported: False", output)
