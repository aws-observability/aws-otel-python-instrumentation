# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
from typing import Dict

from typing_extensions import override

from amazon.serviceevents.serviceevents_contract_test_base import (
    SERVICE_EVENTS_FLUSH_INTERVAL_MS,
    ServiceEventsContractTestBase,
    ServiceEventsTestInfrastructure,
)

_APP_IMAGE = "aws-application-signals-tests-serviceevents-flask-app"


class FlaskServiceEventsTest(ServiceEventsContractTestBase):
    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Running on"

    # -------------------------------------------------------------------------
    # Flask-only tests (framework-agnostic feature coverage)
    # -------------------------------------------------------------------------

    def test_endpoint_summary_error_count(self) -> None:
        """3x GET /error (400), verify errors >= 3, faults == 0."""
        for _ in range(3):
            response = self.send_request("GET", "error")
            self.assertEqual(400, response.status_code)

        logs = self.wait_for_endpoint_summary("GET", "/error")
        total_errors = sum(self.attrs(log).get("aws.service_events.request.errors", 0) for log in logs)
        total_faults = sum(self.attrs(log).get("aws.service_events.request.faults", 0) for log in logs)
        self.assertGreaterEqual(total_errors, 3, "Expected errors >= 3")
        self.assertEqual(total_faults, 0)

    # test_endpoint_error_metrics_counter lives in the shared base now (it runs across
    # all frameworks — async and WSGI-fork are exactly where a metrics-pipeline
    # regression is most likely), so it is intentionally not duplicated here.

    def test_endpoint_summary_incident_exemplar(self) -> None:
        """GET /exception, verify incidents_exemplar body entry with snapshot_id."""
        response = self.send_request("GET", "exception")
        self.assertEqual(500, response.status_code)

        logs = self.wait_for_endpoint_summary("GET", "/exception")
        found_exemplar = False
        for log in logs:
            body = self.body(log)
            exemplars = body.get("incidents_exemplar", []) if isinstance(body, dict) else []
            if exemplars and exemplars[0].get("snapshot_id"):
                found_exemplar = True
                exemplar = exemplars[0]
                # The exemplar carries the snapshot_id plus the trigger_type/timestamp
                # of the incident it points to. /exception triggers an "exception".
                self.assertEqual(exemplar.get("trigger_type"), "exception")
                self.assertGreater(exemplar.get("timestamp", 0), 0, "Expected a non-zero exemplar timestamp")
                # The incident.count attribute tracks the same incidents as the body
                # exemplar list, so it must be >= 1 on the log that carries one.
                self.assert_endpoint_summary(log, min_incident_count=1)
                break
        self.assertTrue(found_exemplar, "Expected at least one incidents_exemplar entry with snapshot_id")

    def test_function_call_exception_tracking(self) -> None:
        """GET /exception — verify the function-duration histogram has a data point
        tagged status=error.

        Exception class names are intentionally NOT exposed on the histogram
        (cardinality control); the ValueError class is asserted on the
        IncidentSnapshot log signal in test_incident_snapshot_value_error.
        """
        response = self.send_request("GET", "exception")
        self.assertEqual(500, response.status_code)

        data_points = self.wait_for_function_duration_metric()
        self.assertGreater(len(data_points), 0)

        found_error = False
        for dp in data_points:
            attrs = self.dp_attrs(dp)
            if attrs.get("status") == "error":
                # exception.type must NOT be on the histogram.
                self.assertNotIn("exception.type", attrs)
                found_error = True
                break
        self.assertTrue(
            found_error,
            "Expected at least one service.function.duration data point with status=error",
        )

    def test_incident_snapshot_deduplication(self) -> None:
        """5x GET /exception, verify snapshot count is capped (deduplication)."""
        for _ in range(5):
            self.send_request("GET", "exception")

        # Wait for at least one snapshot to flush, then give dedup time to settle.
        self.wait_for_otlp_logs("aws.service_events.incident_snapshot")
        time.sleep(float(SERVICE_EVENTS_FLUSH_INTERVAL_MS) / 1000 + 1)

        logs = self.get_otlp_logs_by_event_name("aws.service_events.incident_snapshot")
        self.assertGreater(len(logs), 0, "Expected at least one IncidentSnapshot log")
        self.assertLessEqual(len(logs), 5, "Expected deduplication to cap snapshot count")


class FlaskEndpointFilterTest(ServiceEventsTestInfrastructure):
    """Test that endpoint exclude patterns filter out matching endpoints."""

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Running on"

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {"OTEL_AWS_SERVICE_EVENTS_ENDPOINT_EXCLUDE_PATTERNS": "GET /success"}

    def test_endpoint_exclude_pattern(self) -> None:
        """Verify /success is excluded from EndpointSummary when filtered."""
        for _ in range(3):
            self.send_request("GET", "success")
        for _ in range(2):
            self.send_request("GET", "fault")

        # Wait for /fault (which should appear)
        self.wait_for_endpoint_summary("GET", "/fault")

        # /success should be absent from OTLP
        success_logs = self.get_endpoint_summary_logs("GET", "/success")
        self.assertEqual(len(success_logs), 0, "Expected /success to be excluded by filter pattern")


class FlaskDisabledTest(ServiceEventsTestInfrastructure):
    """Test that disabling serviceevents produces no telemetry OTLP logs."""

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Running on"

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {"OTEL_AWS_SERVICE_EVENTS_ENABLED": "false"}

    def test_serviceevents_disabled_produces_no_records(self) -> None:
        """Verify zero serviceevents OTLP logs when disabled."""
        for _ in range(3):
            self.send_request("GET", "success")
        self.send_request("GET", "exception")

        time.sleep(float(SERVICE_EVENTS_FLUSH_INTERVAL_MS) / 1000 + 2)

        for event_name in [
            "aws.service_events.endpoint_summary",
            "aws.service_events.incident_snapshot",
            "aws.service_events.deployment_event",
        ]:
            logs = self.get_otlp_logs_by_event_name(event_name)
            self.assertEqual(len(logs), 0, f"Expected no {event_name} logs when disabled, found {len(logs)}")

        # The function-duration histogram should also be silent when disabled.
        data_points = self._peek_function_duration_data_points()
        self.assertEqual(
            len(data_points),
            0,
            f"Expected no service.function.duration data points when disabled, found {len(data_points)}",
        )


class FlaskAppSignalsBundledTest(ServiceEventsTestInfrastructure):
    """Bundled mode: when OTEL_AWS_APPLICATION_SIGNALS_ENABLED=true, EndpointSummary
    is suppressed (App Signals carries equivalent per-endpoint metrics), while
    FunctionCall / IncidentSnapshot / DeploymentEvent continue to flow.
    """

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Running on"

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "true"}

    def test_endpoint_summary_suppressed_when_app_signals_on(self) -> None:
        for _ in range(3):
            self.send_request("GET", "success")
        self.send_request("GET", "exception")

        # Wait for other serviceevents signals to confirm the pipeline is alive,
        # then assert EndpointSummary never arrived.
        self.wait_for_function_duration_metric()
        time.sleep(float(SERVICE_EVENTS_FLUSH_INTERVAL_MS) / 1000 + 2)

        summary_logs = self.get_otlp_logs_by_event_name("aws.service_events.endpoint_summary")
        self.assertEqual(
            len(summary_logs),
            0,
            f"Expected EndpointSummary suppressed under App Signals, found {len(summary_logs)}",
        )

        # Other serviceevents signals still flow.
        self.assertGreater(len(self._peek_function_duration_data_points()), 0)
        self.assertGreater(len(self.get_otlp_logs_by_event_name("aws.service_events.incident_snapshot")), 0)
        self.assertGreaterEqual(len(self.get_otlp_logs_by_event_name("aws.service_events.deployment_event")), 1)
