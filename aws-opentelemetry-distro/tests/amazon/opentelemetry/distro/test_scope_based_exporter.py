# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import unittest
from time import time_ns
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.scope_based_exporter import ScopeBasedPeriodicExportingMetricReader
from opentelemetry.sdk.metrics.export import (
    Metric,
    MetricExporter,
    MetricsData,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope


class TestScopeBasedPeriodicExportingMetricReader(unittest.TestCase):
    def setUp(self):
        self.metric_exporter: MetricExporter = MagicMock()

    def _create_periodic_reader(self, metrics, interval=60000, timeout=30000):
        pmr = ScopeBasedPeriodicExportingMetricReader(
            self.metric_exporter,
            export_interval_millis=interval,
            export_timeout_millis=timeout,
            registered_scope_names={"io.test.retained"},
        )

        def _collect(reader, timeout_millis):
            pmr._receive_metrics(metrics, timeout_millis)

        pmr._set_collect_callback(_collect)
        return pmr

    def test_scope_based_metric_filter(self):
        scope_metrics = _scope_metrics(5, "io.test.retained") + _scope_metrics(3, "io.test.dropped")
        md = MetricsData(
            resource_metrics=[
                ResourceMetrics(
                    schema_url="",
                    resource=Resource.create(),
                    scope_metrics=scope_metrics,
                )
            ]
        )
        pmr = self._create_periodic_reader(md)
        pmr.collect()
        args, _ = self.metric_exporter.export.call_args

        exporting_metric_data: MetricsData = args[0]
        self.assertEqual(len(exporting_metric_data.resource_metrics[0].scope_metrics), 5)

    def test_empty_metrics(self):
        md = MetricsData(
            resource_metrics=[
                ResourceMetrics(
                    schema_url="",
                    resource=Resource.create(),
                    scope_metrics=[],
                )
            ]
        )
        pmr = self._create_periodic_reader(md)
        pmr.collect()
        self.metric_exporter.export.assert_not_called()


def _scope_metrics(num: int, scope_name: str):
    scope_metrics = []
    for _ in range(num):
        scope_metrics.append(
            ScopeMetrics(
                schema_url="",
                scope=InstrumentationScope(name=scope_name),
                metrics=[
                    Metric(
                        name="sum_name",
                        description="",
                        unit="",
                        data=Sum(
                            data_points=[
                                NumberDataPoint(
                                    attributes={},
                                    start_time_unix_nano=time_ns(),
                                    time_unix_nano=time_ns(),
                                    value=2,
                                )
                            ],
                            aggregation_temporality=1,
                            is_monotonic=True,
                        ),
                    )
                ],
            ),
        )
    return scope_metrics
