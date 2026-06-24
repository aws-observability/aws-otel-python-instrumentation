# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end integration tests for the framework-agnostic endpoint span processor.

Unlike test_endpoint_span_processor.py — which feeds hand-built ReadableSpans and mocks the
monitor state — these tests register the REAL processor on a REAL OTel SDK ``TracerProvider`` and
drive REAL spans through the full pipeline (``provider.get_tracer().start_as_current_span`` →
the SDK fires ``on_start``/``on_end`` → ``get_ingress_operation`` → route back-out → the real
``EndpointMetricCollector``). This exercises the now-DEFAULT span-processor path the way framework
instrumentation actually produces it, including the real ``_ServiceEventsMonitorState`` and the
span ``exception`` event fault-recovery, so the architectural assumptions (on_start/on_end fire on
the same span, operation parity, exception seeding) are validated rather than mocked away.
"""

import os
from unittest import TestCase

from amazon.opentelemetry.distro.serviceevents.collectors.endpoint_collector import EndpointMetricCollector
from amazon.opentelemetry.distro.serviceevents.processor.endpoint_span_processor import (
    EndpointServiceEventsSpanProcessor,
)
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind


class _Config:
    """Minimal config: track every endpoint (no include/exclude filtering)."""

    @staticmethod
    def should_track_endpoint(route, method):  # pylint: disable=unused-argument
        return True


class TestEndpointSpanProcessorIntegration(TestCase):
    def setUp(self):
        # get_ingress_operation forces "<fn>/FunctionHandler" when AWS_LAMBDA_FUNCTION_NAME is set;
        # other suites leak it, so neutralize for these tests and restore after.
        self._saved_lambda = os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)

        # Large flush interval + never started: we read the in-memory aggregations directly, so
        # no background flush thread or emitter is needed.
        self.endpoint_collector = EndpointMetricCollector(flush_interval_ms=3_600_000, otlp_emitter=None)
        # No incident collector needed for endpoint-metric assertions; pass None.
        self.processor = EndpointServiceEventsSpanProcessor(
            endpoint_collector=self.endpoint_collector,
            incident_snapshot_collector=None,
            config=_Config(),
        )
        self.provider = TracerProvider()
        self.provider.add_span_processor(self.processor)
        self.tracer = self.provider.get_tracer("integration-test")

        # Reset the real monitor state so a leaked investigation can't bleed across tests.
        _ServiceEventsMonitorState.get_instance().clear_investigation_data()

    def tearDown(self):
        _ServiceEventsMonitorState.get_instance().clear_investigation_data()
        if self._saved_lambda is None:
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
        else:
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = self._saved_lambda

    def _emit_request_span(self, name, attributes, events=None):
        """Emit a real SERVER span exactly as framework instrumentation would."""
        with self.tracer.start_as_current_span(name, kind=SpanKind.SERVER, attributes=attributes) as span:
            for event_name, event_attrs in events or []:
                span.add_event(event_name, attributes=event_attrs)

    def _aggregation(self, operation):
        return self.endpoint_collector._aggregations.get(operation)

    def test_matched_route_2xx_recorded_end_to_end(self):
        self._emit_request_span(
            "GET /users/{id}",
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/users/{id}",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
            },
        )
        agg = self._aggregation("GET /users/{id}")
        self.assertIsNotNone(agg)
        self.assertEqual(agg["count"], 1)
        self.assertEqual(agg["route"], "/users/{id}")
        self.assertEqual(agg["method"], "GET")
        self.assertEqual(agg["faults"], 0)
        self.assertEqual(agg["errors"], 0)

    def test_unmatched_route_collapses_to_first_segment_end_to_end(self):
        # Bare "GET" span name (no http.route) -> get_ingress_operation collapses the path.
        self._emit_request_span(
            "GET",
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_TARGET: "/wp-admin/setup.php",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 404,
            },
        )
        agg = self._aggregation("GET /wp-admin")
        self.assertIsNotNone(agg)
        self.assertEqual(agg["count"], 1)
        self.assertEqual(agg["errors"], 1)  # 4xx counts as an error, not a fault

    def test_5xx_fault_recovered_from_span_exception_event_end_to_end(self):
        # The defining case for the span-processor default: a 5xx whose exception was NEVER seen by
        # an AST-instrumented frame (raised in library code / converted by a global handler) still
        # appears in the error breakdown because the processor recovers it from the span's own
        # OTel `exception` event and seeds the investigation data.
        self._emit_request_span(
            "POST /checkout",
            {
                SpanAttributes.HTTP_REQUEST_METHOD: "POST",
                SpanAttributes.HTTP_ROUTE: "/checkout",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 500,
            },
            events=[
                (
                    "exception",
                    {
                        SpanAttributes.EXCEPTION_TYPE: "RuntimeError",
                        SpanAttributes.EXCEPTION_MESSAGE: "payment gateway down",
                        SpanAttributes.EXCEPTION_STACKTRACE: "Traceback...",
                    },
                )
            ],
        )
        agg = self._aggregation("POST /checkout")
        self.assertIsNotNone(agg)
        self.assertEqual(agg["faults"], 1)
        # The fault was attributed to the span-event exception type (origin function unknown).
        breakdown = agg["error_breakdown"]["500"]
        self.assertIn("RuntimeError:unknown", breakdown)
        self.assertEqual(breakdown["RuntimeError:unknown"]["error_type"], "RuntimeError")

    def test_child_span_does_not_produce_an_endpoint(self):
        # An INTERNAL child span under the SERVER span must NOT be recorded as its own endpoint.
        with self.tracer.start_as_current_span(
            "GET /parent",
            kind=SpanKind.SERVER,
            attributes={
                SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                SpanAttributes.HTTP_ROUTE: "/parent",
                SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
            },
        ):
            with self.tracer.start_as_current_span("db.query", kind=SpanKind.INTERNAL):
                pass
        # Exactly one endpoint aggregation (the SERVER span); the INTERNAL child is excluded.
        operations = list(self.endpoint_collector._aggregations.keys())
        self.assertEqual(operations, ["GET /parent"])

    def test_multiple_requests_aggregate_on_the_same_operation(self):
        for _ in range(3):
            self._emit_request_span(
                "GET /health",
                {
                    SpanAttributes.HTTP_REQUEST_METHOD: "GET",
                    SpanAttributes.HTTP_ROUTE: "/health",
                    SpanAttributes.HTTP_RESPONSE_STATUS_CODE: 200,
                },
            )
        agg = self._aggregation("GET /health")
        self.assertIsNotNone(agg)
        self.assertEqual(agg["count"], 3)
