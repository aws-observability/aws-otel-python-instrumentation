from typing import List

from requests import Response, request
from typing_extensions import override
from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan

from amazon.base.contract_test_base import ContractTestBase

from amazon.utils.app_signals_constants import (
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_SERVICE,
    AWS_SPAN_KIND,
    ERROR_METRIC,
    FAULT_METRIC,
    LATENCY_METRIC,
)

class Psychopg2Test(ContractTestBase):
    @override
    def get_application_image_name(self) -> str:
        return "aws-appsignals-tests-psychopg2-app"

    def test_success(self) -> None:
        self.do_test_requests("success", "GET", 200, 0, 0)

    def do_test_requests(
            self, path: str, method: str, status_code: int, expected_error: int, expected_fault: int
    ) -> None:
        address: str = self.application.get_container_host_ip()
        port: str = self.application.get_exposed_port(self.get_application_port())
        url: str = f"http://{address}:{port}/{path}"
        response: Response = request(method, url, timeout=20)

        self.assertEqual(status_code, response.status_code)

        resource_scope_spans: List[ResourceScopeSpan] = self.mock_collector_client.get_traces()
        self._assert_aws_span_attributes(resource_scope_spans, method, path)
        self._assert_semantic_conventions_span_attributes(resource_scope_spans, method, path, status_code)

        metrics: List[ResourceScopeMetric] = self.mock_collector_client.get_metrics(
            {LATENCY_METRIC, ERROR_METRIC, FAULT_METRIC}
        )
        self._assert_metric_attributes(metrics, method, path, LATENCY_METRIC, 5000)
        self._assert_metric_attributes(metrics, method, path, ERROR_METRIC, expected_error)
        self._assert_metric_attributes(metrics, method, path, FAULT_METRIC, expected_fault)