# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import base64
import socket
from logging import Logger, getLogger
from typing import Dict, Optional, Sequence, Tuple

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

DEFAULT_ENDPOINT = "127.0.0.1:2000"
PROTOCOL_HEADER = '{"format":"json","version":1}\n'
FORMAT_OTEL_METRICS_BINARY_PREFIX = "M1"

FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX = "T1S"
FORMAT_OTEL_UNSAMPLED_TRACES_BINARY_PREFIX = "T1U"

_logger: Logger = getLogger(__name__)


class UdpExporter:
    def __init__(self, endpoint: Optional[str] = None):
        self._endpoint = endpoint or DEFAULT_ENDPOINT
        self._host, self._port = self._parse_endpoint(self._endpoint)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)

    def send_data(self, data: bytes, signal_format_prefix: str):
        # base64 encoding and then converting to string with utf-8
        base64_encoded_string: str = base64.b64encode(data).decode("utf-8")
        message = f"{PROTOCOL_HEADER}{signal_format_prefix}{base64_encoded_string}"

        try:
            _logger.debug("Sending UDP data: %s", message)
            self._socket.sendto(message.encode("utf-8"), (self._host, int(self._port)))
        except Exception as exc:  # pylint: disable=broad-except
            _logger.error("Error sending UDP data: %s", exc)
            raise

    def shutdown(self):
        self._socket.close()

    # pylint: disable=no-self-use
    def _parse_endpoint(self, endpoint: str) -> Tuple[str, int]:
        try:
            vals = endpoint.split(":")
            host = vals[0]
            port = int(vals[1])
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError(f"Invalid endpoint: {endpoint}") from exc

        return host, port


class OTLPUdpMetricExporter(MetricExporter):
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

        try:
            self._udp_exporter.send_data(data=serialized_data, signal_format_prefix=FORMAT_OTEL_METRICS_BINARY_PREFIX)
            return MetricExportResult.SUCCESS
        except Exception as exc:  # pylint: disable=broad-except
            _logger.error("Error exporting metrics: %s", exc)
            return MetricExportResult.FAILURE

    # pylint: disable=no-self-use
    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        # TODO: implement force flush
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        self._udp_exporter.shutdown()


class OTLPUdpSpanExporter(SpanExporter):
    def __init__(self, endpoint: Optional[str] = None, sampled: bool = True):
        self._udp_exporter = UdpExporter(endpoint=endpoint)
        self._sampled = sampled

    @override
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        serialized_data = encode_spans(spans).SerializeToString()

        try:
            prefix = (
                FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX
                if self._sampled
                else FORMAT_OTEL_UNSAMPLED_TRACES_BINARY_PREFIX
            )
            self._udp_exporter.send_data(data=serialized_data, signal_format_prefix=prefix)
            return SpanExportResult.SUCCESS
        except Exception as exc:  # pylint: disable=broad-except
            _logger.error("Error exporting spans: %s", exc)
            return SpanExportResult.FAILURE

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        # TODO: implement force flush
        return True

    @override
    def shutdown(self) -> None:
        self._udp_exporter.shutdown()
