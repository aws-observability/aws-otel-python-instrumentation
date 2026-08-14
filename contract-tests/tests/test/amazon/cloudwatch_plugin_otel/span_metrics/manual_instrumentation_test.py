# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from amazon.cloudwatch_plugin_otel.span_metrics.contract_test_base import SpanMetricsContractTestBase


class SpanMetricsManualInstrumentationTest(SpanMetricsContractTestBase):
    __test__ = True

    def get_mode(self) -> str:
        return "manual"

    def command(self) -> str:
        return "python -u ./server.py"
