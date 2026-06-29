# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for ServiceEventsOtlpEmitter."""

import time
from unittest.mock import MagicMock

from amazon.opentelemetry.serviceevents.exporter.otlp_emitter import ServiceEventsOtlpEmitter
from amazon.opentelemetry.serviceevents.models import DeploymentEventTelemetry
from amazon.opentelemetry.serviceevents.models.deployment_telemetry import DeploymentContext
from amazon.opentelemetry.serviceevents.models.endpoint_telemetry import (
    EndpointErrorMetric,
    EndpointMetricEvent,
    ErrorBreakdownEntry,
    ErrorDetail,
    IncidentExemplar,
)
from amazon.opentelemetry.serviceevents.models.function_telemetry import DurationMetrics
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource


def _make_emitter():
    """Create emitter with in-memory exporters for testing."""
    resource = Resource.create({"service.name": "test-svc"})
    log_exporter = InMemoryLogExporter()
    lp = LoggerProvider(resource=resource)
    lp.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    metric_reader = InMemoryMetricReader()
    mp = MeterProvider(resource=resource, metric_readers=[metric_reader])
    emitter = ServiceEventsOtlpEmitter(lp, mp, "deploy-1", "sha-abc", "https://github.com/test")
    return emitter, log_exporter, metric_reader, lp, mp


class TestDeploymentEvent:
    def test_event_name(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        emitter.emit_deployment_event(event)
        logs = log_exporter.get_finished_logs()
        assert len(logs) == 1
        attrs = dict(logs[0].log_record.attributes)
        assert attrs["event.name"] == "aws.service_events.deployment_event"

    def test_no_body(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        emitter.emit_deployment_event(event)
        body = log_exporter.get_finished_logs()[0].log_record.body
        assert body == "" or body is None

    def test_deployment_context_attributes(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        # Populate a real context so the attributes are genuinely present (a bare
        # create() with no env yields the "unknown" sentinel, which is now omitted).
        event.deployment_context = DeploymentContext(git_commit_sha="abc123", deployment_id="99")
        emitter.emit_deployment_event(event)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        # DeploymentContext attributes should be present
        assert "aws.service_events.deployment.id" in attrs or "vcs.ref.head.revision" in attrs

    def test_instrumentation_scope(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        emitter.emit_deployment_event(event)
        log = log_exporter.get_finished_logs()[0]
        assert log.instrumentation_scope.name == "serviceevents"
        assert log.instrumentation_scope.version == "1.0"

    def test_unset_context_fields_omitted(self):
        """DeploymentContext fields left unset (empty) must NOT be emitted.

        Unset fields default to empty, so each guarded attribute is omitted rather
        than shipping a placeholder onto the wire.
        """
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        event.deployment_context = DeploymentContext()  # all fields empty (unset)
        emitter.emit_deployment_event(event)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        for key in (
            "vcs.ref.head.revision",
            "vcs.repository.url.full",
            "aws.service_events.deployment.id",
            "aws.service_events.deployment.url",
            "aws.service_events.deployment.timestamp",
        ):
            assert key not in attrs, f"{key} should be omitted when its context value is unset"
        # No placeholder sentinel should leak onto the wire either.
        assert "unknown" not in attrs.values()
        # The trigger attribute is still emitted normally.
        assert attrs["aws.service_events.deployment.trigger"] == "periodic"

    def test_real_context_fields_emitted(self):
        """Populated DeploymentContext values are emitted as attributes."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        event.deployment_context = DeploymentContext(
            git_repo_url="https://github.com/org/repo",
            git_commit_sha="abc123",
            deployment_url="https://github.com/org/repo/actions/runs/99",
            deployment_timestamp="2026-02-04T00:00:00Z",
            deployment_id="99",
        )
        emitter.emit_deployment_event(event)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["vcs.ref.head.revision"] == "abc123"
        assert attrs["vcs.repository.url.full"] == "https://github.com/org/repo"
        assert attrs["aws.service_events.deployment.id"] == "99"
        assert attrs["aws.service_events.deployment.url"] == "https://github.com/org/repo/actions/runs/99"
        assert attrs["aws.service_events.deployment.timestamp"] == "2026-02-04T00:00:00Z"


class TestEndpointSummary:
    def _make_event(self):
        return EndpointMetricEvent(
            environment="test",
            service_name="test-svc",
            sdk_version="0.14",
            instance_id="localhost",
            operation="GET /success",
            method="GET",
            route="/success",
            pid=1234,
            timestamp="2026-01-01T00:00:00Z",
            count=10,
            faults=2,
            errors=1,
            incident_count=0,
            duration=DurationMetrics(values=[100.0, 200.0], counts=[5, 5], max=250.0, min=50.0, count=10, sum=1500.0),
        )

    def test_event_name(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_endpoint_summary(self._make_event())
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["event.name"] == "aws.service_events.endpoint_summary"

    def test_attributes(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_endpoint_summary(self._make_event())
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["http.request.method"] == "GET"
        assert attrs["url.route"] == "/success"
        assert attrs["aws.service_events.operation"] == "GET /success"
        assert attrs["aws.service_events.request.count"] == 10
        assert attrs["aws.service_events.request.faults"] == 2
        assert attrs["aws.service_events.request.errors"] == 1

    def test_body_has_duration(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_endpoint_summary(self._make_event())
        body = log_exporter.get_finished_logs()[0].log_record.body
        assert isinstance(body, dict)
        assert "duration" in body
        dur = body["duration"]
        assert dur["Values"] == [100.0, 200.0]
        assert dur["Counts"] == [5, 5]
        assert dur["Max"] == 250.0
        assert dur["Count"] == 10

    def test_body_has_exception_breakdown(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_endpoint_summary(self._make_event())
        body = log_exporter.get_finished_logs()[0].log_record.body
        assert "exception_breakdown" in body
        assert isinstance(body["exception_breakdown"], list)

    def test_body_has_incidents_exemplar(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_endpoint_summary(self._make_event())
        body = log_exporter.get_finished_logs()[0].log_record.body
        assert "incidents_exemplar" in body

    def test_vcs_attributes(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_endpoint_summary(self._make_event())
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["vcs.ref.head.revision"] == "sha-abc"
        assert attrs["vcs.repository.url.full"] == "https://github.com/test"
        assert attrs["aws.service_events.deployment.id"] == "deploy-1"


class TestIncidentSnapshot:
    def _make_snapshot(self):
        return {
            "snapshot_id": "snap_001",
            "trigger_type": "exception",
            "operation": "GET /error",
            "duration_ms": 150.0,
            "is_partial": False,
            "method": "GET",
            "route": "/error",
            "exception_info": [{"exception_type": "RuntimeError", "exception_message": "fail"}],
            "request_context": {
                "type": "http",
                "timestamp": int(time.time() * 1000),
                "status_code": 500,
            },
            "telemetry_correlation": {
                "trace_id": "0af7651916cd43dd8448eb211c80319c",
                "span_id": "b7ad6b7169203331",
            },
        }

    def test_event_name(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_incident_snapshot(self._make_snapshot())
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["event.name"] == "aws.service_events.incident_snapshot"

    def test_attributes(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_incident_snapshot(self._make_snapshot())
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["aws.service_events.snapshot_id"] == "snap_001"
        assert attrs["aws.service_events.trigger_type"] == "exception"
        assert attrs["aws.service_events.operation"] == "GET /error"
        assert attrs["http.request.method"] == "GET"
        assert attrs["url.route"] == "/error"
        assert attrs["http.response.status_code"] == 500

    def test_body_has_exception_info(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_incident_snapshot(self._make_snapshot())
        body = log_exporter.get_finished_logs()[0].log_record.body
        assert isinstance(body, dict)
        assert "exception_info" in body
        assert body["exception_info"][0]["exception_type"] == "RuntimeError"

    def test_body_has_request_context(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_incident_snapshot(self._make_snapshot())
        body = log_exporter.get_finished_logs()[0].log_record.body
        assert "request_context" in body
        assert body["request_context"]["status_code"] == 500

    def test_trace_context(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter.emit_incident_snapshot(self._make_snapshot())
        log = log_exporter.get_finished_logs()[0]
        assert log.log_record.trace_id != 0
        assert log.log_record.span_id != 0
        assert log.log_record.trace_flags == 1  # SAMPLED

    def test_no_trace_context_when_missing(self):
        emitter, log_exporter, _, lp, mp = _make_emitter()
        snapshot = self._make_snapshot()
        snapshot["telemetry_correlation"] = {}
        emitter.emit_incident_snapshot(snapshot)
        log = log_exporter.get_finished_logs()[0]
        assert log.log_record.trace_id == 0
        assert log.log_record.span_id == 0


class TestEndpointErrorMetrics:
    def test_counter_emitted(self):
        emitter, _, metric_reader, lp, mp = _make_emitter()
        metrics = [
            EndpointErrorMetric(
                environment="test",
                service_name="test-svc",
                operation="GET /error",
                instance_id="localhost",
                pid=1234,
                exception="RuntimeError",
                count=5,
            ),
        ]
        emitter.emit_endpoint_error_metrics(metrics)
        # Force metric collection
        data = metric_reader.get_metrics_data()
        assert data is not None
        # Find the counter
        found = False
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == "count":
                        found = True
                        assert m.unit == "Count"
                        points = list(m.data.data_points)
                        assert len(points) == 1
                        assert points[0].value == 5
                        attrs = dict(points[0].attributes)
                        assert attrs["Telemetry.Source"] == "ServiceEvents"
                        assert attrs["service_name"] == "test-svc"
                        assert attrs["environment"] == "test"
                        assert attrs["operation"] == "GET /error"
                        assert attrs["exception"] == "RuntimeError"
        assert found, "count metric not found"

    def test_multiple_metrics(self):
        emitter, _, metric_reader, lp, mp = _make_emitter()
        metrics = [
            EndpointErrorMetric(
                environment="test",
                service_name="svc",
                operation="GET /a",
                instance_id="h",
                pid=1,
                exception="TypeError",
                count=2,
            ),
            EndpointErrorMetric(
                environment="test",
                service_name="svc",
                operation="GET /b",
                instance_id="h",
                pid=1,
                exception="ValueError",
                count=7,
            ),
        ]
        emitter.emit_endpoint_error_metrics(metrics)
        data = metric_reader.get_metrics_data()
        total_points = 0
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == "count":
                        total_points += len(list(m.data.data_points))
        assert total_points == 2


class TestEdgeCases:
    def test_emitter_not_initialized_no_crash(self):
        """Emitter with None providers should not crash on emit."""
        emitter = ServiceEventsOtlpEmitter(None, None)
        # Should not raise
        emitter.emit_deployment_event(DeploymentEventTelemetry.create())

    def test_vcs_attributes_omitted_when_empty_on_non_deployment(self):
        """VCS/deployment attributes omitted on EndpointSummary when emitter has empty strings."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        # Create emitter with empty VCS fields
        emitter_empty = ServiceEventsOtlpEmitter(lp, mp, "", "", "")
        event = EndpointMetricEvent(
            environment="t",
            service_name="s",
            sdk_version="0.14",
            instance_id="h",
            operation="GET /x",
            method="GET",
            route="/x",
            pid=1,
            timestamp="2026-01-01T00:00:00Z",
            count=1,
            faults=0,
            errors=0,
            incident_count=0,
            duration=DurationMetrics(values=[1.0], counts=[1], max=1.0, min=1.0, count=1, sum=1.0),
        )
        emitter_empty.emit_endpoint_summary(event)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert "vcs.ref.head.revision" not in attrs
        assert "vcs.repository.url.full" not in attrs
        assert "aws.service_events.deployment.id" not in attrs

    def test_shutdown_safe(self):
        """Shutdown should not crash even when called multiple times."""
        emitter, _, _, lp, mp = _make_emitter()
        emitter.shutdown()
        emitter.shutdown()  # Should not raise


class TestInitialization:
    def test_initialization_is_idempotent(self):
        """A second _ensure_initialized call returns early without re-creating instruments."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        emitter._ensure_initialized()
        first_logger = emitter._otel_logger
        first_counter = emitter._error_counter
        emitter._ensure_initialized()  # second call hits the early-return guard
        assert emitter._otel_logger is first_logger
        assert emitter._error_counter is first_counter

    def test_init_failure_is_swallowed(self):
        """A provider that raises during get_logger must not crash and leaves logger unset."""
        lp = MagicMock()
        lp.get_logger.side_effect = RuntimeError("boom")
        mp = MagicMock()
        emitter = ServiceEventsOtlpEmitter(lp, mp)
        emitter._ensure_initialized()  # should swallow the exception
        assert emitter._initialized is False
        assert emitter._otel_logger is None


class TestEmitGuards:
    def test_emit_endpoint_summary_no_logger_returns(self):
        """emit_endpoint_summary returns silently when logger init fails."""
        lp = MagicMock()
        lp.get_logger.side_effect = RuntimeError("boom")
        emitter = ServiceEventsOtlpEmitter(lp, MagicMock())
        event = EndpointMetricEvent(
            environment="t",
            service_name="s",
            sdk_version="0.14",
            instance_id="h",
            operation="GET /x",
            method="GET",
            route="/x",
            pid=1,
            timestamp="2026-01-01T00:00:00Z",
            count=1,
        )
        emitter.emit_endpoint_summary(event)  # should not raise

    def test_emit_incident_snapshot_no_logger_returns(self):
        """emit_incident_snapshot returns silently when logger init fails."""
        lp = MagicMock()
        lp.get_logger.side_effect = RuntimeError("boom")
        emitter = ServiceEventsOtlpEmitter(lp, MagicMock())
        emitter.emit_incident_snapshot({"snapshot_id": "x"})  # should not raise

    def test_emit_error_metrics_no_counter_returns(self):
        """emit_endpoint_error_metrics returns silently when counter init fails."""
        mp = MagicMock()
        mp.get_meter.side_effect = RuntimeError("boom")
        emitter = ServiceEventsOtlpEmitter(MagicMock(), mp)
        emitter.emit_endpoint_error_metrics(
            [
                EndpointErrorMetric(
                    environment="t",
                    service_name="s",
                    operation="GET /x",
                    instance_id="h",
                    pid=1,
                    exception="E",
                    count=1,
                )
            ]
        )  # should not raise


class TestIncidentSnapshotInferredHttp:
    def test_method_route_inferred_from_operation(self):
        """When method is absent, http method/route are parsed from operation."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        snapshot = {
            "snapshot_id": "snap_002",
            "trigger_type": "exception",
            "operation": "POST /orders",
            "telemetry_correlation": {},
        }
        emitter.emit_incident_snapshot(snapshot)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["http.request.method"] == "POST"
        assert attrs["url.route"] == "/orders"

    def test_empty_operation_yields_empty_http_attrs(self):
        """An empty operation with no method leaves http method/route empty."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        snapshot = {
            "snapshot_id": "snap_003",
            "trigger_type": "latency",
            "operation": "",
            "telemetry_correlation": {},
        }
        emitter.emit_incident_snapshot(snapshot)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["http.request.method"] == ""
        assert attrs["url.route"] == ""


class TestDeploymentFallback:
    def test_no_context_falls_back_to_emitter_config(self):
        """With no deployment_context, emitter-level VCS/deploy config is used."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        event.deployment_context = None  # force the fallback branch
        emitter.emit_deployment_event(event)
        attrs = dict(log_exporter.get_finished_logs()[0].log_record.attributes)
        assert attrs["vcs.ref.head.revision"] == "sha-abc"
        assert attrs["vcs.repository.url.full"] == "https://github.com/test"
        assert attrs["aws.service_events.deployment.id"] == "deploy-1"


class TestErrorMetricsFiltering:
    def test_non_positive_counts_are_skipped(self):
        """Metrics with count <= 0 emit no data points."""
        emitter, _, metric_reader, lp, mp = _make_emitter()
        metrics = [
            EndpointErrorMetric(
                environment="t",
                service_name="s",
                operation="GET /a",
                instance_id="h",
                pid=1,
                exception="E",
                count=0,
            ),
            EndpointErrorMetric(
                environment="t",
                service_name="s",
                operation="GET /b",
                instance_id="h",
                pid=1,
                exception="E",
                count=-3,
            ),
        ]
        emitter.emit_endpoint_error_metrics(metrics)
        data = metric_reader.get_metrics_data()
        total_points = 0
        # No positive counts means the counter never records, so collection yields no data.
        for rm in getattr(data, "resource_metrics", []) or []:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    if m.name == "count":
                        total_points += len(list(m.data.data_points))
        assert total_points == 0


class TestShutdownErrorHandling:
    def test_logger_provider_flush_error_swallowed(self):
        """A logger provider that raises on flush must not crash shutdown."""
        lp = MagicMock()
        lp.force_flush.side_effect = RuntimeError("flush boom")
        mp = MagicMock()
        emitter = ServiceEventsOtlpEmitter(lp, mp)
        emitter.shutdown()  # should not raise
        mp.shutdown.assert_called_once()

    def test_meter_provider_shutdown_error_swallowed(self):
        """A meter provider that raises on shutdown must not crash shutdown."""
        lp = MagicMock()
        mp = MagicMock()
        mp.shutdown.side_effect = RuntimeError("shutdown boom")
        emitter = ServiceEventsOtlpEmitter(lp, mp)
        emitter.shutdown()  # should not raise
        lp.shutdown.assert_called_once()


class TestEmitLogErrorHandling:
    def test_invalid_trace_context_is_ignored(self):
        """Non-hex trace ids fall through the ValueError guard, leaving ids at 0."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        snapshot = {
            "snapshot_id": "snap_004",
            "trigger_type": "exception",
            "operation": "GET /x",
            "telemetry_correlation": {"trace_id": "not-hex", "span_id": "also-bad"},
        }
        emitter.emit_incident_snapshot(snapshot)
        log = log_exporter.get_finished_logs()[0]
        assert log.log_record.trace_id == 0
        assert log.log_record.span_id == 0
        assert log.log_record.trace_flags == 0

    def test_emit_failure_is_swallowed(self):
        """An exception from the underlying logger.emit must not propagate."""
        emitter, _, _, lp, mp = _make_emitter()
        emitter._ensure_initialized()
        emitter._otel_logger = MagicMock()
        emitter._otel_logger.emit.side_effect = RuntimeError("emit boom")
        event = DeploymentEventTelemetry.create(service_name="test", environment="prod")
        emitter.emit_deployment_event(event)  # should not raise


class TestBodyConversionHelpers:
    def test_error_breakdown_converted_to_body(self):
        """Populated error_breakdown is rendered into the emitted body."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = EndpointMetricEvent(
            environment="t",
            service_name="s",
            sdk_version="0.14",
            instance_id="h",
            operation="GET /x",
            method="GET",
            route="/x",
            pid=1,
            timestamp="2026-01-01T00:00:00Z",
            count=3,
            error_breakdown=[
                ErrorBreakdownEntry(
                    errors=[ErrorDetail(error_type="ValueError", function_name="mod.func")],
                    count=2,
                    failure_type="500",
                ),
                ErrorBreakdownEntry(errors=[], count=1, failure_type="404"),
            ],
        )
        emitter.emit_endpoint_summary(event)
        body = log_exporter.get_finished_logs()[0].log_record.body
        breakdown = body["exception_breakdown"]
        assert breakdown[0]["count"] == 2
        assert breakdown[0]["failure_type"] == "500"
        assert breakdown[0]["exceptions"][0]["exception_type"] == "ValueError"
        assert breakdown[0]["exceptions"][0]["function_name"] == "mod.func"
        # Entry with no errors omits the exceptions key.
        assert "exceptions" not in breakdown[1]

    def test_incidents_exemplar_converted_to_body(self):
        """Populated incidents_exemplar is rendered into the emitted body."""
        emitter, log_exporter, _, lp, mp = _make_emitter()
        event = EndpointMetricEvent(
            environment="t",
            service_name="s",
            sdk_version="0.14",
            instance_id="h",
            operation="GET /x",
            method="GET",
            route="/x",
            pid=1,
            timestamp="2026-01-01T00:00:00Z",
            count=1,
            incidents_exemplar=[
                IncidentExemplar(
                    snapshot_id="snap_xyz",
                    trigger_type="exception",
                    severity="critical",
                    timestamp=1706745600000,
                ),
            ],
        )
        emitter.emit_endpoint_summary(event)
        body = log_exporter.get_finished_logs()[0].log_record.body
        exemplars = body["incidents_exemplar"]
        assert exemplars[0]["snapshot_id"] == "snap_xyz"
        assert exemplars[0]["trigger_type"] == "exception"
        assert exemplars[0]["timestamp"] == 1706745600000
