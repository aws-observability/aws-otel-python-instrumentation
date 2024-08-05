# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import socket
from typing import Dict, Optional, Sequence

from typing_extensions import override

from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.metrics._internal.aggregation import AggregationTemporality
from opentelemetry.sdk.metrics._internal.export import MetricExportResult
from opentelemetry.sdk.metrics._internal.point import MetricsData
from opentelemetry.sdk.metrics.export import MetricExporter
from opentelemetry.sdk.metrics.view import Aggregation
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from logging import getLogger, Logger

DEFAULT_ENDPOINT = "127.0.0.1:2000"
PROTOCOL_HEADER = '{"format":"json","version":1}\n'
PROTOCOL_DELIMITER = "\n"

_logger: Logger = getLogger(__name__)

class UdpExporter:
    def __init__(self, endpoint: Optional[str] = None):
        self._endpoint = endpoint or DEFAULT_ENDPOINT # TODO: read from some env var??
        self._host, self._port = self._parse_endpoint(self._endpoint)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)

    def send_data(self, data: str, signal_format: str):
        udp_data = f'{{"format":"{signal_format}","data":{data}}}'
        message = PROTOCOL_HEADER + udp_data

        try:
            print("Sending UDP data: ", message)  # TODO: remove
            self._socket.sendto(message.encode("utf-8"), (self._host, int(self._port)))
        except Exception as exc:  # pylint: disable=broad-except
            _logger.error("Error sending UDP data: %s", exc)

    def shutdown(self):
        self._socket.close()

    # pylint: disable=no-self-use
    def _parse_endpoint(self, endpoint: str) -> tuple[str, int]:
        try:
            vals = endpoint.split(":")
            host = vals[0]
            port = int(vals[1])
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError(f"Invalid endpoint: {endpoint}") from exc

        return host, port


class OtlpUdpMetricExporter(MetricExporter):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        preferred_temporality: Dict[type, AggregationTemporality] = None,
        preferred_aggregation: Dict[type, Aggregation] = None,
    ):
        super().__init__(
            preferred_temporality=preferred_temporality,
            preferred_aggregation=preferred_aggregation,
        )
        self._udp_exporter = UdpExporter(endpoint=endpoint)

    @override
    def export(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs,
    ) -> MetricExportResult:
        serialized_data = encode_metrics(metrics_data).SerializeToString()
        self._udp_exporter.send_data(data=serialized_data, signal_format="OTEL_V1_METRICS")  # TODO: Convert to constant
        return MetricExportResult.SUCCESS  # TODO: send appropriate status back. Need to??

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        self._udp_exporter.shutdown()


class OtlpUdpSpanExporter(SpanExporter):
    def __init__(self, endpoint: Optional[str] = None):
        self._udp_exporter = UdpExporter(endpoint=endpoint)

    @override
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        serialized_data = encode_spans(spans).SerializeToString()
        self._udp_exporter.send_data(data=serialized_data, signal_format="OTEL_V1_TRACES")  # TODO: Convert to constant
        return SpanExportResult.SUCCESS  # TODO: send appropriate status back. Need to??

    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    @override
    def shutdown(self) -> None:
        self._udp_exporter.shutdown()
