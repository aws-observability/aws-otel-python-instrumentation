# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
from os import environ
from typing import Dict, Optional

import requests

from opentelemetry.exporter.otlp.proto.common._internal.metrics_encoder import OTLPMetricExporterMixin, _logger
from opentelemetry.exporter.otlp.proto.http import Compression as HttpCompression
from opentelemetry.exporter.otlp.proto.http.metric_exporter import DEFAULT_ENDPOINT, DEFAULT_TIMEOUT
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as HttpOTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import _append_metrics_path, _compression_from_env
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_CERTIFICATE,
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_HEADERS,
    OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE,
    OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
    OTEL_EXPORTER_OTLP_METRICS_HEADERS,
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL,
    OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE,
    OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TIMEOUT,
)
from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics._internal.aggregation import Aggregation as InternalAggregation
from opentelemetry.sdk.metrics.export import AggregationTemporality, MetricExporter
from opentelemetry.sdk.metrics.view import Aggregation as ViewAggregation
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, ExponentialBucketHistogramAggregation
from opentelemetry.util.re import parse_env_headers


# The OpenTelemetry Authors code
def _apply_otlp_metric_exporter_patches() -> None:
    """OTLP Metrics Exporter patches for getting the following change in the upstream:
    https://github.com/open-telemetry/opentelemetry-python/commit/12f449074e80fa88b59468a48b7a4b99dbcda34d
    """

    def patch_otlp_metric_exporter_mixin_common_configuration(
        self,
        preferred_temporality: Dict[type, AggregationTemporality] = None,
        preferred_aggregation: Dict[type, ViewAggregation] = None,
    ) -> None:

        MetricExporter.__init__(
            self,
            preferred_temporality=self._get_temporality(preferred_temporality),
            preferred_aggregation=self._get_aggregation(preferred_aggregation),
        )

    def patch_otlp_metric_exporter_mixin_get_temporality(
        self, preferred_temporality: Dict[type, AggregationTemporality]
    ) -> Dict[type, AggregationTemporality]:

        otel_exporter_otlp_metrics_temporality_preference = (
            environ.get(
                OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE,
                "CUMULATIVE",
            )
            .upper()
            .strip()
        )

        if otel_exporter_otlp_metrics_temporality_preference == "DELTA":
            instrument_class_temporality = {
                Counter: AggregationTemporality.DELTA,
                UpDownCounter: AggregationTemporality.CUMULATIVE,
                Histogram: AggregationTemporality.DELTA,
                ObservableCounter: AggregationTemporality.DELTA,
                ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            }

        elif otel_exporter_otlp_metrics_temporality_preference == "LOWMEMORY":
            instrument_class_temporality = {
                Counter: AggregationTemporality.DELTA,
                UpDownCounter: AggregationTemporality.CUMULATIVE,
                Histogram: AggregationTemporality.DELTA,
                ObservableCounter: AggregationTemporality.CUMULATIVE,
                ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            }

        else:
            if otel_exporter_otlp_metrics_temporality_preference != (
                    "CUMULATIVE"
            ):
                # pylint: disable=logging-fstring-interpolation
                _logger.warning(
                    "Unrecognized OTEL_EXPORTER_METRICS_TEMPORALITY_PREFERENCE"
                    " value found: "
                    f"{otel_exporter_otlp_metrics_temporality_preference}, "
                    "using CUMULATIVE"
                )
            instrument_class_temporality = {
                Counter: AggregationTemporality.CUMULATIVE,
                UpDownCounter: AggregationTemporality.CUMULATIVE,
                Histogram: AggregationTemporality.CUMULATIVE,
                ObservableCounter: AggregationTemporality.CUMULATIVE,
                ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            }

        instrument_class_temporality.update(preferred_temporality or {})

        return instrument_class_temporality

    def patch_otlp_metric_exporter_mixin_get_aggregation(
        self,
        preferred_aggregation: Dict[type, ViewAggregation],
    ) -> Dict[type, ViewAggregation]:

        otel_exporter_otlp_metrics_default_histogram_aggregation = environ.get(
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
            "explicit_bucket_histogram",
        )

        if otel_exporter_otlp_metrics_default_histogram_aggregation == (
                "base2_exponential_bucket_histogram"
        ):

            instrument_class_aggregation = {
                Histogram: ExponentialBucketHistogramAggregation(),
            }

        else:

            if otel_exporter_otlp_metrics_default_histogram_aggregation != (
                    "explicit_bucket_histogram"
            ):

                # pylint: disable=implicit-str-concat
                _logger.warning(
                    (
                        "Invalid value for %s: %s, using explicit bucket "
                        "histogram aggregation"
                    ),
                    OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
                    otel_exporter_otlp_metrics_default_histogram_aggregation,
                )

            instrument_class_aggregation = {
                Histogram: ExplicitBucketHistogramAggregation(),
            }

        instrument_class_aggregation.update(preferred_aggregation or {})

        return instrument_class_aggregation

    def patch_http_otlp_metric_exporter_init(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[HttpCompression] = None,
        session: Optional[requests.Session] = None,
        preferred_temporality: Dict[type, AggregationTemporality] = None,
        preferred_aggregation: Dict[type, InternalAggregation] = None,
    ):
        self._endpoint = endpoint or environ.get(
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
            _append_metrics_path(
                environ.get(OTEL_EXPORTER_OTLP_ENDPOINT, DEFAULT_ENDPOINT)
            ),
        )
        self._certificate_file = certificate_file or environ.get(
            OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE,
            environ.get(OTEL_EXPORTER_OTLP_CERTIFICATE, True),
        )
        headers_string = environ.get(
            OTEL_EXPORTER_OTLP_METRICS_HEADERS,
            environ.get(OTEL_EXPORTER_OTLP_HEADERS, ""),
        )
        self._headers = headers or parse_env_headers(headers_string)
        self._timeout = timeout or int(
            environ.get(
                OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
                environ.get(OTEL_EXPORTER_OTLP_TIMEOUT, DEFAULT_TIMEOUT),
            )
        )
        self._compression = compression or _compression_from_env()
        self._session = session or requests.Session()
        self._session.headers.update(self._headers)
        self._session.headers.update(
            {"Content-Type": "application/x-protobuf"}
        )
        if self._compression is not HttpCompression.NoCompression:
            self._session.headers.update(
                {"Content-Encoding": self._compression.value}
            )

        self._common_configuration(
            preferred_temporality, preferred_aggregation
        )

    OTLPMetricExporterMixin._common_configuration = patch_otlp_metric_exporter_mixin_common_configuration
    OTLPMetricExporterMixin._get_temporality = patch_otlp_metric_exporter_mixin_get_temporality
    OTLPMetricExporterMixin._get_aggregation = patch_otlp_metric_exporter_mixin_get_aggregation
    HttpOTLPMetricExporter.__init__ = patch_http_otlp_metric_exporter_init

    protocol = environ.get(
        OTEL_EXPORTER_OTLP_METRICS_PROTOCOL, environ.get(OTEL_EXPORTER_OTLP_PROTOCOL, "http/protobuf")
    )
    if protocol == "grpc":
        _apply_grpc_otlp_metric_exporter_patches()


def _apply_grpc_otlp_metric_exporter_patches():
    # pylint: disable=import-outside-toplevel
    # Delay import to only occur if gRPC specifically requested. Vended Docker image will not have gRPC bundled,
    # so importing it at the class level can cause runtime failures.
    from typing import Sequence as TypingSequence
    from typing import Tuple, Union

    from grpc import ChannelCredentials
    from grpc import Compression as GrpcCompression

    from opentelemetry.exporter.otlp.proto.grpc.exporter import (
        OTLPExporterMixin,
        _get_credentials,
        environ_to_compression,
    )
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as GrpcOTLPMetricExporter
    from opentelemetry.sdk.environment_variables import (
        OTEL_EXPORTER_OTLP_METRICS_COMPRESSION,
        OTEL_EXPORTER_OTLP_METRICS_INSECURE,
    )

    def patch_http_grpc_metric_exporter_init(
        self,
        endpoint: Optional[str] = None,
        insecure: Optional[bool] = None,
        credentials: Optional[ChannelCredentials] = None,
        headers: Optional[
            Union[TypingSequence[Tuple[str, str]], Dict[str, str], str]
        ] = None,
        timeout: Optional[int] = None,
        compression: Optional[GrpcCompression] = None,
        preferred_temporality: Dict[type, AggregationTemporality] = None,
        preferred_aggregation: Dict[type, InternalAggregation] = None,
        max_export_batch_size: Optional[int] = None,
    ):

        if insecure is None:
            insecure = environ.get(OTEL_EXPORTER_OTLP_METRICS_INSECURE)
            if insecure is not None:
                insecure = insecure.lower() == "true"

        if (
                not insecure
                and environ.get(OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE) is not None
        ):
            credentials = _get_credentials(
                credentials, OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE
            )

        environ_timeout = environ.get(OTEL_EXPORTER_OTLP_METRICS_TIMEOUT)
        environ_timeout = (
            int(environ_timeout) if environ_timeout is not None else None
        )

        compression = (
            environ_to_compression(OTEL_EXPORTER_OTLP_METRICS_COMPRESSION)
            if compression is None
            else compression
        )

        self._common_configuration(
            preferred_temporality, preferred_aggregation
        )

        OTLPExporterMixin.__init__(
            self,
            endpoint=endpoint
                     or environ.get(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT),
            insecure=insecure,
            credentials=credentials,
            headers=headers or environ.get(OTEL_EXPORTER_OTLP_METRICS_HEADERS),
            timeout=timeout or environ_timeout,
            compression=compression,
        )

        self._max_export_batch_size: Optional[int] = max_export_batch_size

    GrpcOTLPMetricExporter.__init__ = patch_http_grpc_metric_exporter_init


# END The OpenTelemetry Authors code
