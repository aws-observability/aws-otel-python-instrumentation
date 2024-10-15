# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import Logger, getLogger
from typing import Optional, Set

from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY, attach, detach, set_value
from opentelemetry.sdk.metrics.export import MetricExporter, MetricsData, PeriodicExportingMetricReader, ResourceMetrics

_logger: Logger = getLogger(__name__)


class ScopeBasedPeriodicExportingMetricReader(PeriodicExportingMetricReader):

    def __init__(
        self,
        exporter: MetricExporter,
        export_interval_millis: Optional[float] = None,
        export_timeout_millis: Optional[float] = None,
        registered_scope_names: Set[str] = None,
    ):
        super().__init__(exporter, export_interval_millis, export_timeout_millis)
        self._registered_scope_names = registered_scope_names

    def _receive_metrics(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs,
    ) -> None:

        token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        # pylint: disable=broad-exception-caught,invalid-name
        try:
            with self._export_lock:
                exporting_resource_metrics = []
                for metric in metrics_data.resource_metrics:
                    exporting_scope_metrics = []
                    for scope_metric in metric.scope_metrics:
                        if scope_metric.scope.name in self._registered_scope_names:
                            exporting_scope_metrics.append(scope_metric)
                    if len(exporting_scope_metrics) > 0:
                        exporting_resource_metrics.append(
                            ResourceMetrics(
                                resource=metric.resource,
                                scope_metrics=exporting_scope_metrics,
                                schema_url=metric.schema_url,
                            )
                        )
                if len(exporting_resource_metrics) > 0:
                    new_metrics_data = MetricsData(resource_metrics=exporting_resource_metrics)
                    self._exporter.export(new_metrics_data, timeout_millis=timeout_millis)
        except Exception as e:
            _logger.exception("Exception while exporting metrics %s", str(e))
        detach(token)
