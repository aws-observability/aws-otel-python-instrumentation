# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from amazon.cloudwatch_plugin_otel.span_metrics import InstrumentationMode
from amazon.cloudwatch_plugin_otel.span_metrics.contract_test_base import SpanMetricsContractTestBase


class SpanMetricsAutoInstrumentationTest(SpanMetricsContractTestBase):
    __test__ = True

    def get_mode(self) -> InstrumentationMode:
        return InstrumentationMode.AUTO

    def command(self) -> str:
        return "opentelemetry-instrument python -u ./server.py"
