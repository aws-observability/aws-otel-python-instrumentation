# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import base64
import socket
import unittest
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.otlp_udp_exporter import (
    DEFAULT_ENDPOINT,
    FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX,
    FORMAT_OTEL_UNSAMPLED_TRACES_BINARY_PREFIX,
    PROTOCOL_HEADER,
    OTLPUdpMetricExporter,
    OTLPUdpSpanExporter,
    UdpExporter,
)
from opentelemetry.sdk.metrics._internal.export import MetricExportResult
from opentelemetry.sdk.trace.export import SpanExportResult


class TestUdpExporter(TestCase):

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.socket.socket")
    def test_udp_exporter_init_default(self, mock_socket):
        exporter = UdpExporter()
        self.assertEqual(exporter._endpoint, DEFAULT_ENDPOINT)
        self.assertEqual(exporter._host, "127.0.0.1")
        self.assertEqual(exporter._port, 2000)
        mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_DGRAM)
        mock_socket().setblocking.assert_called_once_with(False)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.socket.socket")
    def test_udp_exporter_init_with_endpoint(self, mock_socket):
        exporter = UdpExporter(endpoint="localhost:5000")
        self.assertNotEqual(exporter._endpoint, DEFAULT_ENDPOINT)
        self.assertEqual(exporter._host, "localhost")
        self.assertEqual(exporter._port, 5000)
        mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_DGRAM)
        mock_socket().setblocking.assert_called_once_with(False)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.socket.socket")
    def test_udp_exporter_init_invalid_endpoint(self, mock_socket):
        with self.assertRaises(ValueError):
            UdpExporter(endpoint="invalidEndpoint:port")

    # pylint: disable=no-self-use
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.socket.socket")
    def test_send_data(self, mock_socket):
        mock_socket_instance = mock_socket.return_value
        exporter = UdpExporter()
        input_bytes: bytes = b"hello"
        encoded_bytes: bytes = base64.b64encode(input_bytes)
        exporter.send_data(input_bytes, "signal_prefix")
        expected_message = PROTOCOL_HEADER + "signal_prefix" + encoded_bytes.decode("utf-8")
        mock_socket_instance.sendto.assert_called_once_with(expected_message.encode("utf-8"), ("127.0.0.1", 2000))

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.socket.socket")
    def test_shutdown(self, mock_socket):
        mock_socket_instance = mock_socket.return_value
        exporter = UdpExporter()
        exporter.shutdown()
        mock_socket_instance.close.assert_called_once()


class TestOTLPUdpMetricExporter(unittest.TestCase):

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.encode_metrics")
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_export(self, mock_udp_exporter, mock_encode_metrics):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        mock_encoded_data = MagicMock()
        mock_encode_metrics.return_value.SerializeToString.return_value = mock_encoded_data
        exporter = OTLPUdpMetricExporter()
        result = exporter.export(MagicMock())
        mock_udp_exporter_instance.send_data.assert_called_once_with(data=mock_encoded_data, signal_format_prefix="M1")
        self.assertEqual(result, MetricExportResult.SUCCESS)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.encode_metrics")
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_export_with_exception(self, mock_udp_exporter, mock_encode_metrics):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        mock_encoded_data = MagicMock()
        mock_encode_metrics.return_value.SerializeToString.return_value = mock_encoded_data
        mock_udp_exporter_instance.send_data.side_effect = Exception("Something went wrong")
        exporter = OTLPUdpMetricExporter()
        result = exporter.export(MagicMock())
        self.assertEqual(result, MetricExportResult.FAILURE)

    # pylint: disable=no-self-use
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_shutdown(self, mock_udp_exporter):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        exporter = OTLPUdpMetricExporter()
        exporter.force_flush()
        exporter.shutdown()
        mock_udp_exporter_instance.shutdown.assert_called_once()


class TestOTLPUdpSpanExporter(unittest.TestCase):

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.encode_spans")
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_export_unsampled_span(self, mock_udp_exporter, mock_encode_spans):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        mock_encoded_data = MagicMock()
        mock_encode_spans.return_value.SerializeToString.return_value = mock_encoded_data
        exporter = OTLPUdpSpanExporter(sampled=False)
        result = exporter.export(MagicMock())
        mock_udp_exporter_instance.send_data.assert_called_once_with(
            data=mock_encoded_data, signal_format_prefix=FORMAT_OTEL_UNSAMPLED_TRACES_BINARY_PREFIX
        )
        self.assertEqual(result, SpanExportResult.SUCCESS)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.encode_spans")
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_export_sampled_span(self, mock_udp_exporter, mock_encode_spans):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        mock_encoded_data = MagicMock()
        mock_encode_spans.return_value.SerializeToString.return_value = mock_encoded_data
        exporter = OTLPUdpSpanExporter()
        result = exporter.export(MagicMock())
        mock_udp_exporter_instance.send_data.assert_called_once_with(
            data=mock_encoded_data, signal_format_prefix=FORMAT_OTEL_SAMPLED_TRACES_BINARY_PREFIX
        )
        self.assertEqual(result, SpanExportResult.SUCCESS)

    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.encode_spans")
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_export_with_exception(self, mock_udp_exporter, mock_encode_spans):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        mock_encoded_data = MagicMock()
        mock_encode_spans.return_value.SerializeToString.return_value = mock_encoded_data
        mock_udp_exporter_instance.send_data.side_effect = Exception("Something went wrong")
        exporter = OTLPUdpSpanExporter()
        result = exporter.export(MagicMock())
        self.assertEqual(result, SpanExportResult.FAILURE)

    # pylint: disable=no-self-use
    @patch("amazon.opentelemetry.distro.otlp_udp_exporter.UdpExporter")
    def test_shutdown(self, mock_udp_exporter):
        mock_udp_exporter_instance = mock_udp_exporter.return_value
        exporter = OTLPUdpSpanExporter()
        exporter.shutdown()
        exporter.force_flush()
        mock_udp_exporter_instance.shutdown.assert_called_once()
