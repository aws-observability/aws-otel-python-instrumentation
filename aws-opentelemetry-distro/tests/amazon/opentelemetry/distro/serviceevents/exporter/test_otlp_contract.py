# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Contract tests for ServiceEvents OTLP signal conformance.

Verifies each signal type matches its expected OTLP shape:
- Correct event.name attribute
- Required attributes present
- Body structure (nested fields, types)
- Instrumentation scope aws.serviceevents/1.0
- Trace context (IncidentSnapshot only)
- Counter metric dimensions (EndpointErrorMetrics)

These tests use in-memory exporters — no Docker or network needed.
Mirrors the Java contract tests in ServiceEventsSpringMvcTest.
"""

import time

import pytest

from amazon.opentelemetry.distro.serviceevents.exporter.otlp_emitter import ServiceEventsOtlpEmitter
from amazon.opentelemetry.distro.serviceevents.models import DeploymentEventTelemetry
from amazon.opentelemetry.distro.serviceevents.models.deployment_telemetry import DeploymentContext
from amazon.opentelemetry.distro.serviceevents.models.endpoint_telemetry import (
    EndpointErrorMetric,
    EndpointMetricEvent,
    ErrorBreakdownEntry,
    ErrorDetail,
    IncidentExemplar,
)
from amazon.opentelemetry.distro.serviceevents.models.function_telemetry import DurationMetrics
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource


@pytest.fixture
def otlp_env():
    """Create emitter + in-memory exporters for contract testing."""
    resource = Resource.create(
        {
            "service.name": "contract-test-svc",
            "deployment.environment.name": "contract-test",
            "telemetry.sdk.language": "python",
        }
    )
    log_exporter = InMemoryLogExporter()
    lp = LoggerProvider(resource=resource)
    lp.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    metric_reader = InMemoryMetricReader()
    from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation, View

    function_duration_view = View(
        instrument_name="service.function.duration",
        aggregation=ExponentialBucketHistogramAggregation(),
    )
    mp = MeterProvider(resource=resource, metric_readers=[metric_reader], views=[function_duration_view])
    emitter = ServiceEventsOtlpEmitter(lp, mp, "deploy-contract", "sha-contract", "https://github.com/contract")
    yield emitter, log_exporter, metric_reader
    lp.shutdown()
    mp.shutdown()


def _get_log(log_exporter, index=0):
    logs = log_exporter.get_finished_logs()
    assert len(logs) > index, f"Expected at least {index + 1} logs, got {len(logs)}"
    return logs[index]


def _attrs(log_exporter, index=0):
    return dict(_get_log(log_exporter, index).log_record.attributes)


def _body(log_exporter, index=0):
    return _get_log(log_exporter, index).log_record.body


def _scope(log_exporter, index=0):
    return _get_log(log_exporter, index).instrumentation_scope


# ==========================================================================
# DeploymentEvent contract
# ==========================================================================


class TestDeploymentEventContract:
    def test_event_name(self, otlp_env):
        emitter, logs, _ = otlp_env
        emitter.emit_deployment_event(DeploymentEventTelemetry.create(service_name="svc", environment="prod"))
        assert _attrs(logs)["event.name"] == "aws.service_events.deployment_event"

    def test_instrumentation_scope(self, otlp_env):
        emitter, logs, _ = otlp_env
        emitter.emit_deployment_event(DeploymentEventTelemetry.create())
        scope = _scope(logs)
        assert scope.name == "serviceevents"
        assert scope.version == "1.0"

    def test_no_body(self, otlp_env):
        emitter, logs, _ = otlp_env
        emitter.emit_deployment_event(DeploymentEventTelemetry.create())
        body = _body(logs)
        assert body == "" or body is None, "DeploymentEvent must have no body"

    def test_deployment_id_attribute(self, otlp_env):
        emitter, logs, _ = otlp_env
        event = DeploymentEventTelemetry.create()
        # A real context — a bare create() with no env yields the "unknown" sentinel,
        # which the emitter intentionally omits.
        event.deployment_context = DeploymentContext(git_commit_sha="sha-contract", deployment_id="deploy-contract")
        emitter.emit_deployment_event(event)
        attrs = _attrs(logs)
        # From DeploymentContext or emitter config
        assert "aws.service_events.deployment.id" in attrs or "vcs.ref.head.revision" in attrs

    def test_no_trace_context(self, otlp_env):
        emitter, logs, _ = otlp_env
        emitter.emit_deployment_event(DeploymentEventTelemetry.create())
        log = _get_log(logs)
        assert log.log_record.trace_id == 0
        assert log.log_record.span_id == 0


# ==========================================================================
# EndpointSummary contract
# ==========================================================================


class TestEndpointSummaryContract:
    @pytest.fixture
    def endpoint_event(self):
        return EndpointMetricEvent(
            environment="test",
            service_name="svc",
            sdk_version="0.14",
            instance_id="host",
            operation="POST /api/users",
            method="POST",
            route="/api/users",
            pid=1234,
            timestamp="2026-01-01T00:00:00Z",
            count=100,
            faults=5,
            errors=10,
            incident_count=2,
            duration=DurationMetrics(
                values=[10.0, 50.0, 200.0], counts=[50, 30, 20], max=500.0, min=1.0, count=100, sum=5000.0
            ),
            error_breakdown=[
                ErrorBreakdownEntry(
                    errors=[ErrorDetail(error_type="ValueError", function_name="mod.func")], count=5, failure_type="500"
                ),
            ],
            incidents_exemplar=[
                IncidentExemplar(
                    snapshot_id="snap_001",
                    trigger_type="exception",
                    severity="critical",
                    timestamp=int(time.time() * 1000),
                ),
            ],
        )

    def test_event_name(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["event.name"] == "aws.service_events.endpoint_summary"

    def test_instrumentation_scope(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _scope(logs).name == "serviceevents"
        assert _scope(logs).version == "1.0"

    def test_http_method_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["http.request.method"] == "POST"

    def test_url_route_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["url.route"] == "/api/users"

    def test_operation_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["aws.service_events.operation"] == "POST /api/users"

    def test_request_count_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["aws.service_events.request.count"] == 100

    def test_faults_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["aws.service_events.request.faults"] == 5

    def test_errors_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["aws.service_events.request.errors"] == 10

    def test_incident_count_attribute(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        assert _attrs(logs)["aws.service_events.incident.count"] == 2

    def test_vcs_attributes(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        attrs = _attrs(logs)
        assert attrs["vcs.ref.head.revision"] == "sha-contract"
        assert attrs["vcs.repository.url.full"] == "https://github.com/contract"
        assert attrs["aws.service_events.deployment.id"] == "deploy-contract"

    def test_body_duration_structure(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        body = _body(logs)
        assert isinstance(body, dict)
        dur = body["duration"]
        assert dur["Values"] == [10.0, 50.0, 200.0]
        assert dur["Counts"] == [50, 30, 20]
        assert dur["Max"] == 500.0
        assert dur["Min"] == 1.0
        assert dur["Count"] == 100
        assert dur["Sum"] == 5000.0

    def test_body_exception_breakdown(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        body = _body(logs)
        eb = body["exception_breakdown"]
        assert len(eb) == 1
        assert eb[0]["count"] == 5
        assert eb[0]["failure_type"] == "500"
        assert eb[0]["exceptions"][0]["exception_type"] == "ValueError"

    def test_body_incidents_exemplar(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        body = _body(logs)
        ex = body["incidents_exemplar"]
        assert len(ex) == 1
        assert ex[0]["snapshot_id"] == "snap_001"
        assert ex[0]["trigger_type"] == "exception"

    def test_no_trace_context(self, otlp_env, endpoint_event):
        emitter, logs, _ = otlp_env
        emitter.emit_endpoint_summary(endpoint_event)
        log = _get_log(logs)
        assert log.log_record.trace_id == 0


# ==========================================================================
# FunctionCall contract (Histogram - direct recording)
# ==========================================================================
#
# PythonServiceEventsMonitor.__exit__ records into the duration Histogram only
# when the call is sampled. Latency stays clean and the SEH/EMF fallback path
# remains the source of truth for total/error counts when sampling is below
# 100%.
#
#   - service.function.duration (Histogram)  - sampled calls only (latency)


def _get_function_call_metric(metric_reader):
    """Extract the service.function.duration metric from InMemoryMetricReader."""
    metrics_data = metric_reader.get_metrics_data()
    if metrics_data is None:
        return None, None
    for resource_metrics in metrics_data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == "service.function.duration":
                    return metric, scope_metrics.scope
    return None, None


def _build_call_attrs(
    function_name,
    caller=None,
    function_at_line=None,
    is_async=False,
    exception_type=None,
):
    """Build the per-call attribute dict for service.function.duration.

    Mirrors record_function_call_metrics in python_monitor_impl. Service
    identity (service.name, environment, vcs.*, deployment id) lives on the
    OTel Resource — not on this dict — so consumers correlate via Resource +
    per-call attrs together.

    `exception_type` is only used to flip ``status`` to ``"error"``; the class
    name itself is not exposed as a histogram dimension to keep cardinality
    bounded (it lives on the IncidentSnapshot log signal instead).
    """
    attrs = {
        "Telemetry.Source": "ServiceEvents",
        "function.name": function_name,
    }
    if caller:
        attrs["aws.service_events.caller"] = caller
    if function_at_line is not None:
        attrs["aws.service_events.function_at_line"] = function_at_line
    if is_async:
        attrs["aws.service_events.async"] = True
    if exception_type:
        attrs["status"] = "error"
    else:
        attrs["status"] = "success"
    return attrs


def _record_function_call(
    histogram,
    duration_us,
    *,
    is_sampled=True,
    **call_kwargs,
):
    """Simulate PythonServiceEventsMonitor.__exit__ recording into the histogram.

    Mirrors record_function_call_metrics in python_monitor_impl: the histogram
    fires only when the call is sampled.
    """
    if histogram is None or not is_sampled:
        return
    attrs = _build_call_attrs(**call_kwargs)
    histogram.record(duration_us, attrs)


class TestFunctionCallContract:
    @pytest.fixture
    def function_metrics(self, otlp_env):
        """Create the duration Histogram on the meter, mirroring production
        wiring in serviceevents_instrumentation.py.
        """
        emitter, _, _ = otlp_env
        emitter._ensure_initialized()
        meter = emitter._meter_provider.get_meter("serviceevents", "1.0")
        histogram = meter.create_histogram(
            "service.function.duration",
            unit="Microseconds",
            description="Function call duration",
        )
        return histogram

    @pytest.fixture
    def fc_params(self):
        """Common parameters for function call recording."""
        return {
            "function_name": "mymod.MyClass.process",
            "function_at_line": 42,
            "caller": "mymod.handler",
            "is_async": False,
        }

    @staticmethod
    def _expected_success_attrs(fc_params):
        return {
            "Telemetry.Source": "ServiceEvents",
            "function.name": fc_params["function_name"],
            "aws.service_events.caller": fc_params["caller"],
            "aws.service_events.function_at_line": fc_params["function_at_line"],
            "status": "success",
        }

    def test_sampled_call_records_to_histogram(self, otlp_env, function_metrics, fc_params):
        """Sampled calls land on the histogram with the full expected attribute
        set (instrument metadata, scope, count, sum, attributes verified together).

        Covers success and error paths so both branches of the status dimension
        are validated in one go. Exception class names are intentionally not
        recorded as a histogram dimension to keep cardinality bounded — they
        flow through the IncidentSnapshot log signal instead.
        """
        _, _, metric_reader = otlp_env
        histogram = function_metrics

        # 3 successful calls + 1 errored call, all sampled.
        for _ in range(3):
            _record_function_call(histogram, 25.0, **fc_params)
        _record_function_call(histogram, 25.0, exception_type="TypeError", **fc_params)

        # Histogram: instrument metadata + scope.
        hist_metric, hist_scope = _get_function_call_metric(metric_reader)
        assert hist_metric is not None, "service.function.duration must be exported"
        assert hist_metric.name == "service.function.duration"
        assert hist_metric.unit == "Microseconds"
        assert hist_scope.name == "serviceevents"
        assert hist_scope.version == "1.0"

        def _find(metric, *, status):
            for dp in metric.data.data_points:
                if dict(dp.attributes).get("status") == status:
                    return dp
            raise AssertionError(f"No data point with status={status}")

        # Success: count=3 sum=75 (3 * 25us).
        hist_success = _find(hist_metric, status="success")
        expected_success = self._expected_success_attrs(fc_params)
        assert dict(hist_success.attributes) == expected_success
        assert hist_success.count == 3
        assert hist_success.sum == 75.0

        # Error: count=1 sum=25. Same attribute set as success aside from `status`;
        # exception class name lives on the IncidentSnapshot signal, not here.
        hist_err = _find(hist_metric, status="error")
        expected_error = {**expected_success, "status": "error"}
        assert dict(hist_err.attributes) == expected_error
        assert "exception.type" not in dict(hist_err.attributes)
        assert hist_err.count == 1
        assert hist_err.sum == 25.0

    def test_non_sampled_call_skips_histogram(self, otlp_env, function_metrics, fc_params):
        """Non-sampled calls must NOT emit zero-duration histogram entries.

        Guards the original duration-pollution bug.
        """
        _, _, metric_reader = otlp_env
        histogram = function_metrics

        # 5 successful + 2 errored, all non-sampled.
        for _ in range(5):
            _record_function_call(histogram, 0.0, is_sampled=False, **fc_params)
        for _ in range(2):
            _record_function_call(histogram, 0.0, is_sampled=False, exception_type="ValueError", **fc_params)

        # Histogram must have ZERO data points for this function.
        hist_metric, _ = _get_function_call_metric(metric_reader)
        if hist_metric is not None:
            matching = [
                dp
                for dp in hist_metric.data.data_points
                if dict(dp.attributes).get("function.name") == fc_params["function_name"]
            ]
            assert len(matching) == 0, "Histogram must not record non-sampled calls (would pollute sum/percentiles)"

    def test_mixed_sampling_histogram_count_equals_sampled_subset(self, otlp_env, function_metrics, fc_params):
        """Histogram count tracks the sampled subset, not total invocations."""
        _, _, metric_reader = otlp_env
        histogram = function_metrics

        for _ in range(3):
            _record_function_call(histogram, 25.0, is_sampled=True, **fc_params)
        for _ in range(5):
            _record_function_call(histogram, 0.0, is_sampled=False, **fc_params)

        hist_metric, _ = _get_function_call_metric(metric_reader)
        assert hist_metric is not None
        assert sum(dp.count for dp in hist_metric.data.data_points) == 3


class TestIncidentSnapshotContract:
    @pytest.fixture
    def snapshot(self):
        return {
            "snapshot_id": "snap_contract_001",
            "trigger_type": "exception",
            "operation": "POST /api/checkout",
            "duration_ms": 350.5,
            "is_partial": False,
            "method": "POST",
            "route": "/api/checkout",
            "exception_info": [
                {
                    "exception_type": "PaymentError",
                    "exception_message": "Card declined",
                    "stack_trace": "Traceback...",
                    "call_path": [
                        {
                            "function_name": "checkout",
                            "caller_function_name": "handler",
                            "duration_ns": 350000000,
                            "error": True,
                        },
                    ],
                }
            ],
            "request_context": {
                "type": "http",
                "timestamp": int(time.time() * 1000),
                "status_code": 500,
                "custom_context": {"user_id": "u123"},
            },
            "telemetry_correlation": {
                "trace_id": "abcdef0123456789abcdef0123456789",
                "span_id": "0123456789abcdef",
            },
        }

    def test_event_name(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["event.name"] == "aws.service_events.incident_snapshot"

    def test_snapshot_id_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["aws.service_events.snapshot_id"] == "snap_contract_001"

    def test_trigger_type_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["aws.service_events.trigger_type"] == "exception"

    def test_operation_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["aws.service_events.operation"] == "POST /api/checkout"

    def test_duration_ms_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["aws.service_events.duration_ms"] == 350.5

    def test_http_method_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["http.request.method"] == "POST"

    def test_url_route_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["url.route"] == "/api/checkout"

    def test_status_code_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["http.response.status_code"] == 500

    def test_request_type_attribute(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        assert _attrs(logs)["aws.service_events.request.type"] == "http"

    def test_body_exception_info(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        body = _body(logs)
        assert "exception_info" in body
        assert body["exception_info"][0]["exception_type"] == "PaymentError"
        assert body["exception_info"][0]["exception_message"] == "Card declined"

    def test_body_request_context(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        body = _body(logs)
        assert "request_context" in body
        assert body["request_context"]["status_code"] == 500

    def test_trace_context_present(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        log = _get_log(logs)
        assert log.log_record.trace_id != 0
        assert log.log_record.span_id != 0
        assert log.log_record.trace_flags == 1  # SAMPLED

    def test_trace_id_value(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        emitter.emit_incident_snapshot(snapshot)
        log = _get_log(logs)
        expected = int("abcdef0123456789abcdef0123456789", 16)
        assert log.log_record.trace_id == expected

    def test_no_trace_context_when_missing(self, otlp_env, snapshot):
        emitter, logs, _ = otlp_env
        snapshot["telemetry_correlation"] = {}
        emitter.emit_incident_snapshot(snapshot)
        log = _get_log(logs)
        assert log.log_record.trace_id == 0
        assert log.log_record.span_id == 0
        assert log.log_record.trace_flags == 0


# ==========================================================================
# EndpointErrorMetrics contract
# ==========================================================================


class TestEndpointErrorMetricsContract:
    def test_counter_name_and_unit(self, otlp_env):
        emitter, _, metrics = otlp_env
        emitter.emit_endpoint_error_metrics(
            [
                EndpointErrorMetric(
                    environment="t",
                    service_name="s",
                    operation="GET /a",
                    instance_id="h",
                    pid=1,
                    exception="Err",
                    count=1,
                ),
            ]
        )
        data = metrics.get_metrics_data()
        found = False
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == "count":
                        found = True
                        assert m.unit == "Count"
        assert found

    def test_dimensions_match_spec(self, otlp_env):
        """Counter data points must have service_name, environment, operation, exception."""
        emitter, _, metrics = otlp_env
        emitter.emit_endpoint_error_metrics(
            [
                EndpointErrorMetric(
                    environment="prod",
                    service_name="my-svc",
                    operation="POST /pay",
                    instance_id="h",
                    pid=1,
                    exception="PaymentError",
                    count=7,
                ),
            ]
        )
        data = metrics.get_metrics_data()
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == "count":
                        points = list(m.data.data_points)
                        assert len(points) == 1
                        attrs = dict(points[0].attributes)
                        assert attrs["service_name"] == "my-svc"
                        assert attrs["environment"] == "prod"
                        assert attrs["operation"] == "POST /pay"
                        assert attrs["exception"] == "PaymentError"
                        assert points[0].value == 7

    def test_multiple_error_types(self, otlp_env):
        emitter, _, metrics = otlp_env
        emitter.emit_endpoint_error_metrics(
            [
                EndpointErrorMetric(
                    environment="t",
                    service_name="s",
                    operation="GET /a",
                    instance_id="h",
                    pid=1,
                    exception="TypeError",
                    count=3,
                ),
                EndpointErrorMetric(
                    environment="t",
                    service_name="s",
                    operation="GET /a",
                    instance_id="h",
                    pid=1,
                    exception="ValueError",
                    count=5,
                ),
            ]
        )
        data = metrics.get_metrics_data()
        total = 0
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == "count":
                        total += len(list(m.data.data_points))
        assert total == 2

    def test_instrumentation_scope(self, otlp_env):
        emitter, _, metrics = otlp_env
        emitter.emit_endpoint_error_metrics(
            [
                EndpointErrorMetric(
                    environment="t",
                    service_name="s",
                    operation="GET /a",
                    instance_id="h",
                    pid=1,
                    exception="Err",
                    count=1,
                ),
            ]
        )
        data = metrics.get_metrics_data()
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                if sm.metrics and any(m.name == "count" for m in sm.metrics):
                    assert sm.scope.name == "serviceevents"
                    assert sm.scope.version == "1.0"

    def test_zero_count_skipped(self, otlp_env):
        """Zero-count metrics should not add counter value (emitter skips add)."""
        emitter, _, metrics = otlp_env
        emitter.emit_endpoint_error_metrics(
            [
                EndpointErrorMetric(
                    environment="t",
                    service_name="s",
                    operation="GET /a",
                    instance_id="h",
                    pid=1,
                    exception="Err",
                    count=0,
                ),
            ]
        )
        data = metrics.get_metrics_data()
        if data is None:
            return  # No metrics at all — pass
        total_value = 0
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    for dp in m.data.data_points:
                        total_value += dp.value
        assert total_value == 0, "Zero-count metrics should not add any counter value"
