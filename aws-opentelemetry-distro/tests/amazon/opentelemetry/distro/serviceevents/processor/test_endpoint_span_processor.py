# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor import (
    ServiceEventsSpanProcessor,
    _exception_from_span_event,
    _get_http_method,
    _get_status_code,
    _is_request_boundary,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import INVALID_SPAN_CONTEXT, SpanContext, SpanKind, TraceFlags

_MONITOR_PATH = "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor._ServiceEventsMonitorState"

# A valid, non-remote parent context (so the span is NOT a local root).
_LOCAL_PARENT = SpanContext(trace_id=0x1, span_id=0x2, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))
# A remote parent context (the span IS a local root: parent is from another process).
_REMOTE_PARENT = SpanContext(trace_id=0x1, span_id=0x2, is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED))
# A valid span context for the span itself (so trace_id/span_id are extractable).
_SELF_CONTEXT = SpanContext(trace_id=0xAA, span_id=0xBB, is_remote=False, trace_flags=TraceFlags(TraceFlags.SAMPLED))


def _build_span(
    attributes: dict,
    kind: SpanKind = SpanKind.SERVER,
    parent: Optional[SpanContext] = None,
    name: str = "GET /users/<int:uid>",
    start_time: int = 0,
    end_time: int = 5_000_000,
    span_context: SpanContext = _SELF_CONTEXT,
    events: Optional[list] = None,
) -> ReadableSpan:
    span: ReadableSpan = MagicMock()
    span.attributes = attributes
    span.kind = kind
    span.parent = parent
    span.name = name
    span.start_time = start_time
    span.end_time = end_time
    span.events = events if events is not None else []
    span.instrumentation_scope = InstrumentationScope("opentelemetry.instrumentation.flask", "1.0")
    span.get_span_context.return_value = span_context
    return span


def _exception_event(exc_type="ValueError", message="bad input", stacktrace="Traceback..."):
    """A span event shaped like OTel's recorded ``exception`` event."""
    event = MagicMock()
    event.name = "exception"
    attrs = {}
    if exc_type is not None:
        attrs[SpanAttributes.EXCEPTION_TYPE] = exc_type
    if message is not None:
        attrs[SpanAttributes.EXCEPTION_MESSAGE] = message
    if stacktrace is not None:
        attrs[SpanAttributes.EXCEPTION_STACKTRACE] = stacktrace
    event.attributes = attrs
    return event


class TestRequestBoundary(TestCase):
    def test_server_span_is_boundary(self):
        span = _build_span({}, kind=SpanKind.SERVER, parent=_LOCAL_PARENT)
        self.assertTrue(_is_request_boundary(span))

    def test_local_root_internal_span_is_boundary(self):
        # INTERNAL kind, but parent is remote -> local root (covers Coral-style inbound).
        span = _build_span({}, kind=SpanKind.INTERNAL, parent=_REMOTE_PARENT)
        self.assertTrue(_is_request_boundary(span))

    def test_no_parent_is_boundary(self):
        span = _build_span({}, kind=SpanKind.INTERNAL, parent=None)
        self.assertTrue(_is_request_boundary(span))

    def test_internal_child_span_is_not_boundary(self):
        # INTERNAL kind with a valid local (non-remote) parent -> not a boundary.
        span = _build_span({}, kind=SpanKind.INTERNAL, parent=_LOCAL_PARENT)
        self.assertFalse(_is_request_boundary(span))

    def test_client_child_span_is_not_boundary(self):
        span = _build_span({}, kind=SpanKind.CLIENT, parent=_LOCAL_PARENT)
        self.assertFalse(_is_request_boundary(span))


class TestAttributeReaders(TestCase):
    def test_method_prefers_stable_key(self):
        span = _build_span({SpanAttributes.HTTP_REQUEST_METHOD: "POST", SpanAttributes.HTTP_METHOD: "GET"})
        self.assertEqual(_get_http_method(span), "POST")

    def test_method_falls_back_to_legacy_key(self):
        span = _build_span({SpanAttributes.HTTP_METHOD: "GET"})
        self.assertEqual(_get_http_method(span), "GET")

    def test_method_none_when_absent(self):
        self.assertIsNone(_get_http_method(_build_span({})))

    def test_status_prefers_stable_key(self):
        span = _build_span({SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 503, SpanAttributes.HTTP_STATUS_CODE: 200})
        self.assertEqual(_get_status_code(span), 503)

    def test_status_falls_back_to_legacy_key(self):
        span = _build_span({SpanAttributes.HTTP_STATUS_CODE: 404})
        self.assertEqual(_get_status_code(span), 404)

    def test_status_zero_when_absent(self):
        self.assertEqual(_get_status_code(_build_span({})), 0)

    def test_status_zero_when_unparseable(self):
        span = _build_span({SpanAttributes.HTTP_RESPONSE_STATUS_CODE: "not-a-number"})
        self.assertEqual(_get_status_code(span), 0)


class TestOnStart(TestCase):
    def setUp(self):
        self.endpoint_collector = MagicMock()
        self.incident_collector = MagicMock()
        self.config = MagicMock()
        self.processor = ServiceEventsSpanProcessor(
            self.endpoint_collector, self.incident_collector, self.config
        )

    def test_begin_investigation_fired_for_boundary(self):
        span = _build_span({}, kind=SpanKind.SERVER, parent=_LOCAL_PARENT)
        with patch(_MONITOR_PATH) as monitor_cls:
            self.processor.on_start(span)
            monitor_cls.get_instance.return_value.begin_investigation.assert_called_once()

    def test_begin_investigation_skipped_for_child(self):
        span = _build_span({}, kind=SpanKind.INTERNAL, parent=_LOCAL_PARENT)
        with patch(_MONITOR_PATH) as monitor_cls:
            self.processor.on_start(span)
            monitor_cls.get_instance.return_value.begin_investigation.assert_not_called()

    def test_on_start_never_raises(self):
        span = _build_span({}, kind=SpanKind.SERVER, parent=_LOCAL_PARENT)
        with patch(_MONITOR_PATH) as monitor_cls:
            monitor_cls.get_instance.side_effect = RuntimeError("boom")
            # Must not propagate.
            self.processor.on_start(span)


class TestOnEnd(TestCase):
    def setUp(self):
        self.endpoint_collector = MagicMock()
        self.incident_collector = MagicMock()
        self.incident_collector.process_potential_incident.return_value = None
        self.config = MagicMock()
        self.config.should_track_endpoint.return_value = True
        self.processor = ServiceEventsSpanProcessor(
            self.endpoint_collector, self.incident_collector, self.config
        )

    def _run(self, span):
        with patch(_MONITOR_PATH) as monitor_cls, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor._extract_error_from_call_path"
        ) as extract_mock:
            extract_mock.return_value = None
            self.processor.on_end(span)
            return monitor_cls, extract_mock

    def test_matched_route_records_request(self):
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/users/<int:uid>",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
            },
            name="GET /users/<int:uid>",
        )
        self._run(span)
        self.endpoint_collector.record_request.assert_called_once()
        kwargs = self.endpoint_collector.record_request.call_args.kwargs
        self.assertEqual(kwargs["route"], "/users/<int:uid>")
        self.assertEqual(kwargs["method"], "GET")
        self.assertEqual(kwargs["status_code"], 200)
        self.assertEqual(kwargs["duration_ns"], 5_000_000)
        self.assertIsNone(kwargs["error_info"])

    def test_operation_backed_out_to_route_matches_span_name(self):
        # span.name is the App Signals operation; route must be name minus "METHOD ".
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "POST",
                SpanAttributes.HTTP_ROUTE: "/api/orders/{id}",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 201,
            },
            name="POST /api/orders/{id}",
        )
        self._run(span)
        self.assertEqual(self.endpoint_collector.record_request.call_args.kwargs["route"], "/api/orders/{id}")

    def test_unmatched_route_uses_first_segment_via_ingress_op(self):
        # Bare "GET" span name -> get_ingress_operation collapses url.path to first segment.
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_TARGET: "/wp-admin/setup.php",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 404,
            },
            name="GET",
        )
        self._run(span)
        self.assertEqual(self.endpoint_collector.record_request.call_args.kwargs["route"], "/wp-admin")

    def test_no_method_skips(self):
        span = _build_span({SpanAttributes.HTTP_ROUTE: "/x"}, name="GET /x")
        self._run(span)
        self.endpoint_collector.record_request.assert_not_called()

    def test_internal_operation_skipped(self):
        # A local-root non-SERVER span with no HTTP attributes yields InternalOperation,
        # which has no "METHOD " prefix -> skipped.
        span = _build_span({}, kind=SpanKind.INTERNAL, parent=_REMOTE_PARENT, name="InternalOperation")
        self._run(span)
        self.endpoint_collector.record_request.assert_not_called()

    def test_child_span_skipped_entirely(self):
        span = _build_span(
            {SpanAttributes.HTTP_REQUEST_METHOD: "GET", SpanAttributes.HTTP_ROUTE: "/x"},
            kind=SpanKind.INTERNAL,
            parent=_LOCAL_PARENT,
        )
        self._run(span)
        self.endpoint_collector.record_request.assert_not_called()
        self.incident_collector.process_potential_incident.assert_not_called()

    def test_endpoint_filter_excludes(self):
        self.config.should_track_endpoint.return_value = False
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/health",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
            },
            name="GET /health",
        )
        self._run(span)
        self.endpoint_collector.record_request.assert_not_called()

    def test_error_extracted_for_4xx_and_5xx(self):
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/boom",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 500,
            },
            name="GET /boom",
        )
        _, extract_mock = self._run(span)
        extract_mock.assert_called_once_with(None, "/boom", "GET")

    def test_error_not_extracted_for_2xx(self):
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/ok",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
            },
            name="GET /ok",
        )
        _, extract_mock = self._run(span)
        extract_mock.assert_not_called()

    def test_incident_driven_with_none_exception(self):
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/boom",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 500,
            },
            name="GET /boom",
        )
        self._run(span)
        self.incident_collector.process_potential_incident.assert_called_once()
        kwargs = self.incident_collector.process_potential_incident.call_args.kwargs
        self.assertEqual(kwargs["route"], "/boom")
        self.assertEqual(kwargs["status_code"], 500)
        self.assertIsNone(kwargs["exception"])
        # Trace correlation carried from the span context.
        self.assertEqual(kwargs["request_data"]["trace_id"], 0xAA)
        self.assertEqual(kwargs["request_data"]["span_id"], 0xBB)

    def test_exemplar_recorded_when_incident_returns_one(self):
        self.incident_collector.process_potential_incident.return_value = {
            "operation": "GET /boom",
            "snapshot_id": "snap_1",
        }
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/boom",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 500,
            },
            name="GET /boom",
        )
        self._run(span)
        self.endpoint_collector.record_incident_exemplar.assert_called_once()
        op_arg = self.endpoint_collector.record_incident_exemplar.call_args.args[0]
        self.assertEqual(op_arg, "GET /boom")

    def test_context_cleared_in_finally(self):
        span = _build_span(
            {SpanAttributes.HTTP_REQUEST_METHOD: "GET", SpanAttributes.HTTP_ROUTE: "/x"},
            name="GET /x",
        )
        with patch(_MONITOR_PATH) as monitor_cls, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor.clear_current_operation"
        ) as clear_op, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor._extract_error_from_call_path"
        ) as extract_mock:
            extract_mock.return_value = None
            self.processor.on_end(span)
            clear_op.assert_called_once()
            monitor_cls.get_instance.return_value.clear_investigation_data.assert_called_once()

    def test_context_cleared_even_when_recording_raises(self):
        self.endpoint_collector.record_request.side_effect = RuntimeError("collector down")
        span = _build_span(
            {SpanAttributes.HTTP_REQUEST_METHOD: "GET", SpanAttributes.HTTP_ROUTE: "/x"},
            name="GET /x",
        )
        with patch(_MONITOR_PATH) as monitor_cls, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor.clear_current_operation"
        ) as clear_op, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor._extract_error_from_call_path"
        ) as extract_mock:
            extract_mock.return_value = None
            # record_request raised, but on_end swallows and still clears context.
            self.processor.on_end(span)
            clear_op.assert_called_once()
            monitor_cls.get_instance.return_value.clear_investigation_data.assert_called_once()

    def test_on_end_never_raises_on_child(self):
        # Defensive: even a totally broken span must not propagate.
        span = MagicMock()
        span.kind = SpanKind.SERVER
        span.parent = _LOCAL_PARENT
        span.attributes = None
        span.name = None
        # _get_http_method tolerates None attributes -> returns None -> early return.
        self.processor.on_end(span)


class TestExceptionFromSpanEvent(TestCase):
    """_exception_from_span_event recovers a fault from the span's own ``exception`` event."""

    def test_returns_none_when_no_events(self):
        span = _build_span({}, events=[])
        self.assertIsNone(_exception_from_span_event(span))

    def test_returns_none_when_no_exception_event(self):
        other = MagicMock()
        other.name = "some.other.event"
        other.attributes = {}
        span = _build_span({}, events=[other])
        self.assertIsNone(_exception_from_span_event(span))

    def test_parses_exception_event(self):
        span = _build_span({}, events=[_exception_event("ValueError", "bad input", "Traceback...")])
        result = _exception_from_span_event(span)
        self.assertEqual(result["name"], "ValueError")
        self.assertEqual(result["message"], "bad input")
        self.assertEqual(result["traceback_info"], "Traceback...")
        # The span event carries no origin function.
        self.assertEqual(result["function_name"], "unknown")

    def test_last_exception_event_wins(self):
        span = _build_span(
            {},
            events=[_exception_event("FirstError"), _exception_event("LastError")],
        )
        self.assertEqual(_exception_from_span_event(span)["name"], "LastError")

    def test_event_without_type_is_skipped(self):
        # An exception event missing exception.type is not a usable fault source.
        span = _build_span({}, events=[_exception_event(exc_type=None)])
        self.assertIsNone(_exception_from_span_event(span))

    def test_missing_message_and_stacktrace_default_to_empty(self):
        span = _build_span({}, events=[_exception_event("KeyError", message=None, stacktrace=None)])
        result = _exception_from_span_event(span)
        self.assertEqual(result["name"], "KeyError")
        self.assertEqual(result["message"], "")
        self.assertEqual(result["traceback_info"], "")


class TestSeedExceptionFromSpan(TestCase):
    """_seed_exception_from_span fills investigation data from the span event (first-writer-wins)."""

    def setUp(self):
        self.processor = ServiceEventsSpanProcessor(MagicMock(), MagicMock(), MagicMock())

    def test_seeds_when_investigation_has_no_exception(self):
        # The 5xx unwound through uninstrumented code: AST monitor captured nothing, but the
        # span has an exception event. It must be seeded so the breakdown/snapshot recover it.
        inv_data = {"call_path": [], "exception": None, "start_time": 0.0}
        span = _build_span({}, events=[_exception_event("RuntimeError", "boom")])
        with patch(_MONITOR_PATH) as monitor_cls:
            monitor_cls.get_instance.return_value.peek_investigation_data.return_value = inv_data
            self.processor._seed_exception_from_span(span)
        self.assertIsNotNone(inv_data["exception"])
        self.assertEqual(inv_data["exception"]["name"], "RuntimeError")
        self.assertEqual(inv_data["exception"]["message"], "boom")

    def test_does_not_overwrite_ast_captured_exception(self):
        # First-writer-wins: a real instrumented throw (with the true origin function) must win
        # over the span event (which only knows "unknown").
        captured = {"name": "ValueError", "message": "real", "function_name": "handler"}
        inv_data = {"call_path": [], "exception": captured, "start_time": 0.0}
        span = _build_span({}, events=[_exception_event("RuntimeError", "from span")])
        with patch(_MONITOR_PATH) as monitor_cls:
            monitor_cls.get_instance.return_value.peek_investigation_data.return_value = inv_data
            self.processor._seed_exception_from_span(span)
        self.assertEqual(inv_data["exception"]["name"], "ValueError")
        self.assertEqual(inv_data["exception"]["function_name"], "handler")

    def test_noop_when_no_investigation_context(self):
        span = _build_span({}, events=[_exception_event("RuntimeError")])
        with patch(_MONITOR_PATH) as monitor_cls:
            monitor_cls.get_instance.return_value.peek_investigation_data.return_value = None
            # Must not raise.
            self.processor._seed_exception_from_span(span)

    def test_noop_when_span_has_no_exception_event(self):
        inv_data = {"call_path": [], "exception": None, "start_time": 0.0}
        span = _build_span({}, events=[])
        with patch(_MONITOR_PATH) as monitor_cls:
            monitor_cls.get_instance.return_value.peek_investigation_data.return_value = inv_data
            self.processor._seed_exception_from_span(span)
        self.assertIsNone(inv_data["exception"])

    def test_on_end_seeds_for_5xx_only(self):
        # The seed runs only for faults (>= 500); a 2xx/4xx span event is not seeded by on_end.
        inv_data = {"call_path": [], "exception": None, "start_time": 0.0}
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/boom",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
            },
            name="GET /boom",
            events=[_exception_event("RuntimeError", "boom")],
        )
        with patch(_MONITOR_PATH) as monitor_cls, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor._extract_error_from_call_path"
        ) as extract_mock:
            extract_mock.return_value = None
            monitor_cls.get_instance.return_value.peek_investigation_data.return_value = inv_data
            self.processor.on_end(span)
        # 200 status -> seed skipped, investigation exception untouched.
        self.assertIsNone(inv_data["exception"])

    def test_on_end_seeds_for_5xx(self):
        inv_data = {"call_path": [], "exception": None, "start_time": 0.0}
        span = _build_span(
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/boom",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 500,
            },
            name="GET /boom",
            events=[_exception_event("RuntimeError", "boom")],
        )
        with patch(_MONITOR_PATH) as monitor_cls, patch(
            "amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor._extract_error_from_call_path"
        ) as extract_mock:
            extract_mock.return_value = None
            monitor_cls.get_instance.return_value.peek_investigation_data.return_value = inv_data
            self.processor.on_end(span)
        # 500 status -> span exception seeded into investigation data.
        self.assertIsNotNone(inv_data["exception"])
        self.assertEqual(inv_data["exception"]["name"], "RuntimeError")


class TestLifecycle(TestCase):
    def test_force_flush_and_shutdown(self):
        processor = ServiceEventsSpanProcessor(MagicMock(), MagicMock(), MagicMock())
        self.assertTrue(processor.force_flush())
        self.assertIsNone(processor.shutdown())
