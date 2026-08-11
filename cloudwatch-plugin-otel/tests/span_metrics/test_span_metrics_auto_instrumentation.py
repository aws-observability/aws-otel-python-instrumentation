# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest.mock import patch

from plugins.opentelemetry.cloudwatch.span_metrics._constants import _SpanMetrics
from plugins.opentelemetry.cloudwatch.span_metrics.instrumentor import SpanMetricsInstrumentor

from opentelemetry.instrumentation.auto_instrumentation import _load
from opentelemetry.instrumentation.distro import DefaultDistro
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from opentelemetry.test.globals_test import reset_trace_globals
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import set_tracer_provider
from opentelemetry.util._importlib_metadata import EntryPoint, entry_points

_ENTRY_POINT_NAME = "span_metrics"
_ENTRY_POINT_VALUE = "plugins.opentelemetry.cloudwatch.span_metrics.instrumentor:SpanMetricsInstrumentor"


class TestSpanMetricsAutoInstrumentation(TestBase):
    def setUp(self):
        super().setUp()
        SpanMetricsInstrumentor().uninstrument()

    def tearDown(self):
        SpanMetricsInstrumentor().uninstrument()
        super().tearDown()

    def _run_auto_instrumentation(self):
        entry_point = EntryPoint(_ENTRY_POINT_NAME, _ENTRY_POINT_VALUE, "opentelemetry_instrumentor")

        def only_span_metrics(group):
            return (entry_point,) if group == "opentelemetry_instrumentor" else ()

        with patch.object(_load, "entry_points", side_effect=only_span_metrics), patch.object(
            _load, "get_dist_dependency_conflicts", return_value=None
        ):
            _load._load_instrumentors(DefaultDistro())

    def _calls_for(self, span_name):
        return [
            point
            for metric in self.get_sorted_metrics()
            if metric.name == _SpanMetrics.CALLS_NAME
            for point in metric.data.data_points
            if point.attributes.get(_SpanMetrics.SPAN_NAME) == span_name
        ]

    def test_entry_point_registered_for_auto_instrumentation(self):
        matches = [ep for ep in entry_points(group="opentelemetry_instrumentor") if ep.name == _ENTRY_POINT_NAME]
        if not matches:
            self.skipTest("cloudwatch-plugin-otel is not installed as a distribution; entry points are unavailable")
        self.assertEqual(matches[0].value, _ENTRY_POINT_VALUE)
        self.assertIs(matches[0].load(), SpanMetricsInstrumentor)

    def test_auto_instrumentation_loader_derives_metrics_for_dropped_spans(self):
        self.tracer_provider, self.memory_exporter = self.create_tracer_provider(sampler=ALWAYS_OFF)
        reset_trace_globals()
        set_tracer_provider(self.tracer_provider)

        self._run_auto_instrumentation()

        span = self.tracer_provider.get_tracer("probe").start_span("auto_span")
        self.assertTrue(span.is_recording())
        span.end()

        calls = self._calls_for("auto_span")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].value, 1)
        exported = [s for s in self.memory_exporter.get_finished_spans() if s.name == "auto_span"]
        self.assertEqual(exported, [])

    def test_auto_instrumentation_loader_respects_disabled_list(self):
        with patch.dict("os.environ", {"OTEL_PYTHON_DISABLED_INSTRUMENTATIONS": _ENTRY_POINT_NAME}):
            self._run_auto_instrumentation()

        with self.tracer_provider.get_tracer("probe").start_as_current_span("auto_span"):
            pass

        self.assertEqual(self._calls_for("auto_span"), [])
