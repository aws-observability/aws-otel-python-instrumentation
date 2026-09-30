# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.serviceevents.models.deployment_telemetry import (
    DeploymentContext,
    DeploymentEventTelemetry,
)
from amazon.opentelemetry.distro.serviceevents.models.endpoint_telemetry import (
    EndpointMetricEvent,
    ErrorBreakdownEntry,
    ErrorDetail,
)
from amazon.opentelemetry.distro.serviceevents.models.function_telemetry import DurationMetrics
from amazon.opentelemetry.distro.serviceevents.models.incident_telemetry import (
    CallPathEntry,
    ExceptionInfo,
    IncidentSnapshot,
    RequestContext,
    TelemetryCorrelation,
)
from amazon.opentelemetry.distro.serviceevents.models.resource_attributes import ResourceAttributes


class TestDurationMetrics(TestCase):
    """Test the DurationMetrics dataclass."""

    def test_all_fields_accessible(self):
        """Test that all fields are accessible."""
        dm = DurationMetrics(
            values=[100.0, 200.0],
            counts=[1, 2],
            max=200.0,
            min=100.0,
            count=3,
            sum=500.0,
        )

        self.assertEqual(dm.values, [100.0, 200.0])
        self.assertEqual(dm.counts, [1, 2])
        self.assertEqual(dm.max, 200.0)
        self.assertEqual(dm.min, 100.0)
        self.assertEqual(dm.count, 3)
        self.assertEqual(dm.sum, 500.0)

    def test_accepts_float_counts(self):
        """Test that counts can be floats (from SEH aggregation)."""
        dm = DurationMetrics(
            values=[100.0],
            counts=[1.5],
            max=100.0,
            min=100.0,
            count=1.5,
            sum=150.0,
        )
        self.assertEqual(dm.counts, [1.5])


class TestErrorDetail(TestCase):
    """Test the ErrorDetail dataclass."""

    def test_field_access(self):
        """Test that ErrorDetail fields are accessible."""
        detail = ErrorDetail(error_type="ValueError", function_name="func_123")
        self.assertEqual(detail.error_type, "ValueError")
        self.assertEqual(detail.function_name, "func_123")


class TestErrorBreakdownEntry(TestCase):
    """Test the ErrorBreakdownEntry dataclass."""

    def test_field_access(self):
        """Test that ErrorBreakdownEntry fields are accessible."""
        entry = ErrorBreakdownEntry(
            errors=[ErrorDetail(error_type="ValueError", function_name="func_1")],
            count=5,
            failure_type="500",
        )
        self.assertEqual(len(entry.errors), 1)
        self.assertEqual(entry.count, 5)
        self.assertEqual(entry.failure_type, "500")


class TestEndpointMetricEvent(TestCase):
    """Test the EndpointMetricEvent dataclass."""

    def test_instantiation(self):
        """Test basic instantiation."""
        event = EndpointMetricEvent(
            environment="production",
            service_name="api-svc",
            sdk_version="0.14.2",
            instance_id="host-1",
            operation="GET /api/users",
            pid=100,
            timestamp="2026-01-01T00:00:00+00:00",
            count=42,
            method="GET",
            route="/api/users",
        )

        self.assertEqual(event.environment, "production")
        self.assertEqual(event.operation, "GET /api/users")
        self.assertEqual(event.method, "GET")
        self.assertEqual(event.route, "/api/users")
        self.assertEqual(event.count, 42)
        self.assertEqual(event.telemetry_type, "EndpointSummary")
        self.assertEqual(event.error_breakdown, [])
        self.assertEqual(event.faults, 0)
        self.assertEqual(event.errors, 0)
        self.assertIsNone(event.duration)


class TestCallPathEntry(TestCase):
    """Test the CallPathEntry dataclass."""

    def test_error_defaults_to_false(self):
        """Test that error defaults to False."""
        entry = CallPathEntry(
            function_name="func_a",
            caller_function_name="func_b",
            duration_ns=1000,
        )
        self.assertFalse(entry.error)

    def test_error_can_be_true(self):
        """Test that error can be set to True."""
        entry = CallPathEntry(
            function_name="func_a",
            caller_function_name=None,
            duration_ns=5000,
            error=True,
        )
        self.assertTrue(entry.error)
        self.assertIsNone(entry.caller_function_name)


class TestExceptionInfo(TestCase):
    """Test the ExceptionInfo dataclass."""

    def test_fields_accessible(self):
        """Test that all ExceptionInfo fields are accessible."""
        info = ExceptionInfo(
            exception_type="ValueError",
            exception_message="invalid value",
            stack_trace="Traceback...",
            call_path=[
                CallPathEntry(function_name="f1", caller_function_name=None, duration_ns=100),
            ],
        )

        self.assertEqual(info.exception_type, "ValueError")
        self.assertEqual(info.exception_message, "invalid value")
        self.assertEqual(info.stack_trace, "Traceback...")
        self.assertEqual(len(info.call_path), 1)


class TestIncidentSnapshot(TestCase):
    """Test the IncidentSnapshot dataclass."""

    def test_to_dict_with_all_fields(self):
        """Test to_dict with all fields populated."""
        snapshot = IncidentSnapshot(
            snapshot_id="snap_123",
            timestamp=1706745600000,
            severity="critical",
            trigger_type="exception",
            service="user-service",
            environment="production",
            instance_id="host-1",
            operation="GET /api/users",
            sdk_version="0.14.2",
            pid=12345,
            duration_ms=150.5,
            exception_info=[
                ExceptionInfo(
                    exception_type="ValueError",
                    exception_message="bad input",
                    stack_trace="Traceback...",
                    call_path=[
                        CallPathEntry(
                            function_name="func_a",
                            caller_function_name=None,
                            duration_ns=1000,
                            error=True,
                        ),
                    ],
                ),
            ],
            request_context=RequestContext(
                type="http",
                timestamp=1706745600000,
                status_code=500,
            ),
            telemetry_correlation=TelemetryCorrelation(
                trace_id="trace-xyz",
                request_id="req-123",
            ),
        )

        result = snapshot.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result["snapshot_id"], "snap_123")
        self.assertEqual(result["severity"], "critical")
        self.assertEqual(result["trigger_type"], "exception")
        self.assertEqual(result["telemetry_type"], "IncidentSnapshot")
        self.assertEqual(result["sdk_lang"], "python")
        self.assertEqual(result["duration_ms"], 150.5)
        self.assertEqual(len(result["exception_info"]), 1)
        self.assertEqual(result["exception_info"][0]["exception_type"], "ValueError")
        self.assertEqual(result["request_context"]["type"], "http")
        self.assertEqual(result["telemetry_correlation"]["trace_id"], "trace-xyz")

    def test_default_telemetry_type(self):
        """Test that telemetry_type defaults to 'IncidentSnapshot'."""
        snapshot = IncidentSnapshot(
            snapshot_id="snap_1",
            timestamp=0,
            severity="low",
            trigger_type="latency",
            service="svc",
            environment="dev",
            instance_id="host",
            operation="GET /test",
            sdk_version="0.14.2",
            pid=1,
            duration_ms=100.0,
            exception_info=[],
            request_context=RequestContext(type="http", timestamp=0, status_code=200),
            telemetry_correlation=TelemetryCorrelation(),
        )
        self.assertEqual(snapshot.telemetry_type, "IncidentSnapshot")


class TestRequestContext(TestCase):
    """Test the RequestContext dataclass."""

    def test_default_fields(self):
        """Test default field values."""
        ctx = RequestContext(type="http", timestamp=1000, status_code=200)
        self.assertEqual(ctx.custom_context, {})
        self.assertIsNone(ctx.request_body)
        self.assertIsNone(ctx.query_params)
        self.assertIsNone(ctx.path_params)
        self.assertIsNone(ctx.request_headers)

    def test_all_fields_populated(self):
        """Test with all optional fields populated."""
        ctx = RequestContext(
            type="http",
            timestamp=1000,
            status_code=500,
            custom_context={"user_id": "u123"},
            request_body={"data": "value"},
            query_params={"page": "1"},
            path_params={"id": "42"},
            request_headers={"Content-Type": "application/json"},
        )
        self.assertEqual(ctx.custom_context["user_id"], "u123")
        self.assertEqual(ctx.request_body["data"], "value")


class TestTelemetryCorrelation(TestCase):
    """Test the TelemetryCorrelation dataclass."""

    def test_default_fields(self):
        """Test default field values."""
        tc = TelemetryCorrelation()
        self.assertIsNone(tc.trace_id)
        self.assertIsNone(tc.session_id)
        self.assertIsNone(tc.span_id)
        self.assertIsNone(tc.request_id)
        self.assertIsNone(tc.correlation_ids)


class TestDeploymentEventSimplified(TestCase):
    """Test that DeploymentEvent has no functions list."""

    def test_no_functions_field(self):
        """DeploymentEvent should not have a functions field."""
        from amazon.opentelemetry.distro.serviceevents.models import deployment_telemetry

        self.assertFalse(hasattr(deployment_telemetry, "FunctionInfo"))
        self.assertFalse(hasattr(deployment_telemetry, "FunctionMappingTelemetry"))


class TestDeploymentContext(TestCase):
    """Test the DeploymentContext dataclass."""

    def test_defaults(self):
        """Unset fields default to empty (not a sentinel) so emitters omit them."""
        gm = DeploymentContext()
        self.assertEqual(gm.git_repo_url, "")
        self.assertEqual(gm.git_commit_sha, "")
        self.assertEqual(gm.deployment_url, "")
        self.assertEqual(gm.deployment_timestamp, "")
        self.assertEqual(gm.deployment_id, "")

    @patch.dict(
        "os.environ",
        {
            "OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL": "https://github.com/org/repo",
            "OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA": "abc123",
            "OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_URL": "https://github.com/org/repo/actions/runs/99",
            "OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_TIMESTAMP": "2026-02-04T00:00:00Z",
            "OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID": "99",
        },
    )
    def test_from_environment(self):
        """Test creating DeploymentContext from environment variables."""
        gm = DeploymentContext.from_environment()
        self.assertEqual(gm.git_repo_url, "https://github.com/org/repo")
        self.assertEqual(gm.git_commit_sha, "abc123")
        self.assertEqual(gm.deployment_url, "https://github.com/org/repo/actions/runs/99")
        self.assertEqual(gm.deployment_timestamp, "2026-02-04T00:00:00Z")
        self.assertEqual(gm.deployment_id, "99")

    def test_to_dict(self):
        """Test to_dict conversion."""
        gm = DeploymentContext(git_repo_url="repo", git_commit_sha="sha")
        result = gm.to_dict()
        self.assertEqual(result["git_repo_url"], "repo")
        self.assertEqual(result["git_commit_sha"], "sha")


class TestDeploymentEventTelemetry(TestCase):
    """Test the DeploymentEventTelemetry dataclass."""

    def test_create(self):
        """Test creating a DeploymentEventTelemetry instance."""
        evt = DeploymentEventTelemetry.create(
            service_name="test-svc",
            environment="dev",
            instance_id="host-1",
            pid=42,
            include_deployment_context=False,
        )

        self.assertEqual(evt.service_name, "test-svc")
        self.assertEqual(evt.telemetry_type, "DeploymentEvent")

    def test_to_dict(self):
        """Test to_dict conversion."""
        evt = DeploymentEventTelemetry(
            service_name="test-svc",
            environment="dev",
            instance_id="host-1",
            sdk_version="0.14.2",
            pid=42,
        )

        result = evt.to_dict()
        self.assertEqual(result["telemetry_type"], "DeploymentEvent")
        self.assertEqual(result["service_name"], "test-svc")
        self.assertNotIn("functions", result)
        self.assertNotIn("total_functions", result)
        self.assertEqual(result["sdk_lang"], "python")
        self.assertNotIn("deployment_context", result)

    @patch.dict(
        "os.environ",
        {
            "OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL": "https://github.com/org/repo",
            "OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA": "abc123",
        },
    )
    def test_to_dict_with_deployment_context(self):
        """Test to_dict includes deployment_context when present."""
        evt = DeploymentEventTelemetry.create(
            service_name="test-svc",
            environment="dev",
            instance_id="host-1",
            pid=42,
            include_deployment_context=True,
        )

        result = evt.to_dict()
        self.assertIn("deployment_context", result)
        self.assertEqual(result["deployment_context"]["git_repo_url"], "https://github.com/org/repo")

    def test_timestamp_auto_set(self):
        """Test that timestamp is auto-set if not provided."""
        evt = DeploymentEventTelemetry(
            service_name="svc",
            environment="dev",
            instance_id="host",
            sdk_version="0.14.2",
            pid=1,
        )
        self.assertTrue(len(evt.timestamp) > 0)

    def test_json_serializable(self):
        """Test that to_dict output is JSON serializable."""
        evt = DeploymentEventTelemetry(
            service_name="test-svc",
            environment="dev",
            instance_id="host-1",
            sdk_version="0.14.2",
            pid=42,
        )

        json_str = json.dumps(evt.to_dict())
        self.assertIsInstance(json_str, str)


class TestResourceAttributes(TestCase):
    """Test the ResourceAttributes dataclass."""

    def test_empty_defaults(self):
        """Test that all fields default to None."""
        ra = ResourceAttributes()
        self.assertIsNone(ra.cloud_provider)
        self.assertIsNone(ra.cloud_region)
        self.assertIsNone(ra.host_id)
        self.assertTrue(ra.is_empty())

    def test_to_dict_sparse(self):
        """Test that to_dict only includes non-None fields with OTel keys."""
        ra = ResourceAttributes(
            cloud_provider="aws",
            cloud_region="us-east-1",
            host_id="i-0abc123",
        )
        result = ra.to_dict()
        self.assertEqual(
            result,
            {
                "cloud.provider": "aws",
                "cloud.region": "us-east-1",
                "host.id": "i-0abc123",
            },
        )
        # None fields should not appear
        self.assertNotIn("cloud.platform", result)
        self.assertNotIn("container.id", result)

    def test_to_dict_empty(self):
        """Test that empty ResourceAttributes produces empty dict."""
        ra = ResourceAttributes()
        self.assertEqual(ra.to_dict(), {})

    def test_is_empty_false_when_set(self):
        """Test is_empty returns False when any field is set."""
        ra = ResourceAttributes(cloud_provider="aws")
        self.assertFalse(ra.is_empty())

    def test_from_otel_resource(self):
        """Test creating from a mock OTel Resource."""

        class MockResource:
            attributes = {
                "cloud.provider": "aws",
                "cloud.platform": "aws_ec2",
                "cloud.region": "us-west-2",
                "host.id": "i-abc123",
                "host.type": "t3.medium",
                "telemetry.auto.version": "0.14.2",  # Should be ignored
                "custom.attr": "value",  # Should be ignored
            }

        ra = ResourceAttributes.from_otel_resource(MockResource())
        self.assertEqual(ra.cloud_provider, "aws")
        self.assertEqual(ra.cloud_platform, "aws_ec2")
        self.assertEqual(ra.cloud_region, "us-west-2")
        self.assertEqual(ra.host_id, "i-abc123")
        self.assertEqual(ra.host_type, "t3.medium")
        # Unknown attributes should not be included
        self.assertIsNone(ra.container_id)
        self.assertIsNone(ra.k8s_cluster_name)

    def test_from_otel_resource_none(self):
        """Test creating from None resource returns empty."""
        ra = ResourceAttributes.from_otel_resource(None)
        self.assertTrue(ra.is_empty())

    def test_from_otel_resource_empty_values_skipped(self):
        """Test that empty string values are skipped."""

        class MockResource:
            attributes = {
                "cloud.provider": "aws",
                "cloud.region": "",
                "host.id": "  ",
            }

        ra = ResourceAttributes.from_otel_resource(MockResource())
        self.assertEqual(ra.cloud_provider, "aws")
        self.assertIsNone(ra.cloud_region)
        self.assertIsNone(ra.host_id)

    def test_all_otel_keys_roundtrip(self):
        """Test that all 11 attributes roundtrip through to_dict."""
        ra = ResourceAttributes(
            cloud_provider="aws",
            cloud_platform="aws_ecs",
            cloud_region="eu-west-1",
            cloud_account_id="123456789012",
            cloud_availability_zone="eu-west-1a",
            host_id="i-def456",
            host_type="m5.xlarge",
            container_id="abc123container",
            k8s_cluster_name="prod-cluster",
            k8s_pod_name="my-pod-xyz",
            k8s_namespace_name="default",
        )
        result = ra.to_dict()
        self.assertEqual(len(result), 11)
        self.assertEqual(result["cloud.provider"], "aws")
        self.assertEqual(result["cloud.platform"], "aws_ecs")
        self.assertEqual(result["cloud.region"], "eu-west-1")
        self.assertEqual(result["cloud.account.id"], "123456789012")
        self.assertEqual(result["cloud.availability_zone"], "eu-west-1a")
        self.assertEqual(result["host.id"], "i-def456")
        self.assertEqual(result["host.type"], "m5.xlarge")
        self.assertEqual(result["container.id"], "abc123container")
        self.assertEqual(result["k8s.cluster.name"], "prod-cluster")
        self.assertEqual(result["k8s.pod.name"], "my-pod-xyz")
        self.assertEqual(result["k8s.namespace.name"], "default")


class TestResourceAttributesInModels(TestCase):
    """Test that resource_attributes integrates correctly in telemetry models."""

    def _make_resource_attrs(self):
        return ResourceAttributes(
            cloud_provider="aws",
            cloud_region="us-east-1",
            host_id="i-test123",
        )

    def test_incident_snapshot_with_resource_attributes(self):
        """Test IncidentSnapshot includes resource_attributes in output."""
        ra = self._make_resource_attrs()
        snapshot = IncidentSnapshot(
            snapshot_id="snap_1",
            timestamp=0,
            severity="low",
            trigger_type="latency",
            service="svc",
            environment="dev",
            instance_id="host",
            operation="GET /test",
            sdk_version="0.14.2",
            pid=1,
            duration_ms=100.0,
            exception_info=[],
            request_context=RequestContext(type="http", timestamp=0, status_code=200),
            telemetry_correlation=TelemetryCorrelation(),
            resource_attributes=ra,
        )
        result = snapshot.to_dict()
        self.assertEqual(result["resource_attributes"]["cloud.region"], "us-east-1")

    def test_deployment_event_with_resource_attributes(self):
        """Test DeploymentEventTelemetry includes resource_attributes when non-empty."""
        ra = self._make_resource_attrs()
        evt = DeploymentEventTelemetry.create(
            service_name="svc",
            environment="dev",
            instance_id="host-1",
            pid=1,
            include_deployment_context=False,
            resource_attributes=ra,
        )
        result = evt.to_dict()
        self.assertIn("resource_attributes", result)
        self.assertEqual(result["resource_attributes"]["cloud.provider"], "aws")

    def test_deployment_event_without_resource_attributes(self):
        """Test DeploymentEventTelemetry omits resource_attributes when empty."""
        evt = DeploymentEventTelemetry.create(
            service_name="svc",
            environment="dev",
            instance_id="host-1",
            pid=1,
            include_deployment_context=False,
        )
        result = evt.to_dict()
        self.assertNotIn("resource_attributes", result)
