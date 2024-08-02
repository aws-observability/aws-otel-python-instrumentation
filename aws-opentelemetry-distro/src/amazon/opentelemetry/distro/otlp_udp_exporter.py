import typing
from typing import Optional, Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.metrics.export import MetricExporter
from typing_extensions import override
from opentelemetry.sdk.metrics._internal.point import MetricsData
from opentelemetry.sdk.metrics._internal.export import MetricExportResult
from opentelemetry.exporter.otlp.proto.common.metrics_encoder import (
    encode_metrics,
)
from opentelemetry.exporter.otlp.proto.common.trace_encoder import (
    encode_spans,
)

import socket

PROTOCOL_HEADER = "{\"format\":\"json\",\"version\":1}"
PROTOCOL_DELIMITER = '\n'


class OtlpUdpExporterCommon:
    def __init__(
            self,
            endpoint: Optional[str] = None
    ):
        self._endpoint = endpoint or "http://127.0.0.1:2000"
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  #TODO: What does this mean?
        self._socket.setblocking(False)  #TODO: Is this correct that we don't want to block?

    def send_data(self, data, format):
        udp_data = {
            "format": format,
            "data": data
        }

        message = "%s%s%s" % (PROTOCOL_HEADER,
                              PROTOCOL_DELIMITER,
                              udp_data)

        print("Sending UDP data: ", message)
        self._socket.sendto(message.encode('utf-8'), self._endpoint)
        pass


class OtlpUdpMetricExporter(OtlpUdpExporterCommon, MetricExporter):
    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        pass

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        pass

    def __init__(self, endpoint=None, temporality_dict=None):
        OtlpUdpExporterCommon.__init__(self, endpoint)
        MetricExporter.__init__(self, preferred_temporality=temporality_dict)

    @override
    def export(
            self,
            metrics_data: MetricsData,
            timeout_millis: float = 10_000,
            **kwargs,
    ) -> MetricExportResult:
        # serialized_data = encode_metrics(metrics_data).SerializeToString()
        # self.send_data(data=serialized_data, format="OTEL_V1_METRICS")
        return MetricExportResult.SUCCESS


class OtlpUdpTraceExporter(SpanExporter):
    def __init__(self, endpoint=None):
        self._exp = OtlpUdpExporterCommon(endpoint=endpoint)

    @override
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        print("SPAN EXPORTER CALLED")
        serialized_data = encode_spans(spans).SerializeToString()
        self._exp.send_data(data=serialized_data, format="OTEL_V1_TRACES")
        return SpanExportResult.SUCCESS

    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    @override
    def shutdown(self) -> None:
        pass
