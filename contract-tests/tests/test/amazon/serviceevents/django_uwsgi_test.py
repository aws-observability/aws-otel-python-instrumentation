# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
from typing import Dict, List

from typing_extensions import override

from amazon.serviceevents.serviceevents_contract_test_base import (
    OTLP_POLL_INTERVAL,
    OTLP_POLL_TIMEOUT,
    ServiceEventsContractTestBase,
)

_APP_IMAGE = "aws-application-signals-tests-serviceevents-django-uwsgi-app"


class DjangoUwsgiServiceEventsTest(ServiceEventsContractTestBase):
    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "spawned uWSGI worker"

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {
            "DJANGO_SETTINGS_MODULE": "serviceevents_django.settings",
            "OTEL_AWS_PYTHON_DEFER_TO_WORKERS_ENABLED": "true",
        }

    @override
    def get_application_start_timeout(self) -> int:
        return 60

    # -------------------------------------------------------------------------
    # uWSGI aggregation helper
    # -------------------------------------------------------------------------

    def _wait_for_endpoint_count(self, method: str, route: str, min_total: int) -> List:
        """Poll until the summed count across matching EndpointSummary OTLP logs reaches min_total."""
        start = time.time()
        while time.time() - start < OTLP_POLL_TIMEOUT:
            logs = self.get_endpoint_summary_logs(method, route)
            total = sum(self.attrs(log).get("aws.service_events.request.count", 0) for log in logs)
            if total >= min_total:
                return logs
            time.sleep(OTLP_POLL_INTERVAL)
        logs = self.get_endpoint_summary_logs(method, route)
        total = sum(self.attrs(log).get("aws.service_events.request.count", 0) for log in logs)
        self.assertGreaterEqual(total, min_total, f"Timed out: total count {total} < {min_total} for {method} {route}")
        return logs

    # -------------------------------------------------------------------------
    # Overridden EndpointSummary tests (aggregate across workers)
    # -------------------------------------------------------------------------

    @override
    def test_endpoint_summary_success(self) -> None:
        for _ in range(3):
            response = self.send_request("GET", "success")
            self.assertEqual(200, response.status_code)

        logs = self._wait_for_endpoint_count("GET", "/success", 3)
        total_faults = sum(self.attrs(log).get("aws.service_events.request.faults", 0) for log in logs)
        self.assertEqual(total_faults, 0)

    @override
    def test_endpoint_summary_fault(self) -> None:
        for _ in range(2):
            response = self.send_request("GET", "fault")
            self.assertEqual(500, response.status_code)

        logs = self._wait_for_endpoint_count("GET", "/fault", 2)
        total_faults = sum(self.attrs(log).get("aws.service_events.request.faults", 0) for log in logs)
        self.assertGreater(total_faults, 0, "Expected faults > 0")

    # -------------------------------------------------------------------------
    # uWSGI-specific tests
    # -------------------------------------------------------------------------

    def test_uwsgi_multiprocess_telemetry(self) -> None:
        """Verify serviceevents collectors work in uWSGI forked worker processes."""
        for _ in range(5):
            self.send_request("GET", "success")

        # If collectors failed to re-initialize in workers, no records would appear.
        # FunctionCall now flows through the service.function.duration histogram metric.
        data_points = self.wait_for_function_duration_metric()
        self.assertGreater(len(data_points), 0)

        # Each uWSGI worker has its own collector; counts may split across multiple
        # EndpointSummary logs. Wait until total reaches 5.
        self._wait_for_endpoint_count("GET", "/success", 5)
