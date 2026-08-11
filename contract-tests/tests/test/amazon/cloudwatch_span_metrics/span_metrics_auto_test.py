# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from amazon.cloudwatch_span_metrics.span_metrics_contract_test_base import SpanMetricsContractTestBase


class SpanMetricsAutoTest(SpanMetricsContractTestBase):
    __test__ = True

    def get_mode(self) -> str:
        return "auto"
