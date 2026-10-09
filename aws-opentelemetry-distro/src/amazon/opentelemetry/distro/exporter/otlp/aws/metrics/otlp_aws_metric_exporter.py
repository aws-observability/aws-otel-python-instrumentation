# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Dict, Optional

from botocore.session import Session

from amazon.opentelemetry.distro.exporter.otlp.aws.common._aws_http_headers import _OTLP_AWS_HTTP_HEADERS
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from amazon.opentelemetry.distro.exporter.otlp.aws.environment_variables import (
    OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE,
    OTEL_EXPORTER_OTLP_SIGV4_SERVICE,
)
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._common import _resolve_endpoint
from opentelemetry.exporter.otlp.proto.http.metric_exporter import DEFAULT_METRICS_EXPORT_PATH, OTLPMetricExporter
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
from opentelemetry.sdk.metrics.export import AggregationTemporality
from opentelemetry.sdk.metrics.view import Aggregation


class OTLPAwsMetricExporter(OTLPMetricExporter):
    """
    This exporter extends the functionality of the OTLPMetricExporter to allow metrics to be exported
    to the CloudWatch Metrics OTLP endpoint https://monitoring.[AWSRegion].amazonaws.com/v1/metrics.
    Utilizes the AwsAuthSession to sign and directly inject SigV4 Authentication to the exported
    request's headers. The signing service uses an explicit ``aws_service`` first, then
    ``OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE``, then ``OTEL_EXPORTER_OTLP_SIGV4_SERVICE``,
    and defaults to ``monitoring``.

    ``preferred_temporality`` and ``preferred_aggregation`` are forwarded to the upstream exporter
    rather than defaulted here. Overriding them would silently change the meaning of exported
    metrics, so the caller's (or upstream's env-derived) configuration is preserved.

    See: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

    def __init__(
        self,
        aws_region: Optional[str] = None,
        aws_service: Optional[str] = None,
        session: Optional[Session] = None,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
        preferred_temporality: Optional[Dict[type, AggregationTemporality]] = None,
        preferred_aggregation: Optional[Dict[type, Aggregation]] = None,
    ):
        # Compression is passed through unchanged. Unlike OTLPAwsLogRecordExporter, this exporter
        # does not force gzip: the measured SigV4 signature covers content-type, host and x-amz-date
        # but not Content-Encoding, and whether an unsigned Content-Encoding is acceptable for the
        # metrics endpoint is still open with the CloudWatch service team.
        #
        # The upstream client sends its own User-Agent as a per-request header, which takes precedence
        # over session headers, so the ADOT User-Agent must be passed in via ``headers``.
        endpoint = endpoint or _resolve_endpoint(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, DEFAULT_METRICS_EXPORT_PATH)
        self._session = AwsAuthSession(
            session=session,
            aws_region=aws_region,
            service=aws_service
            or os.environ.get(OTEL_EXPORTER_OTLP_METRICS_SIGV4_SERVICE)
            or os.environ.get(OTEL_EXPORTER_OTLP_SIGV4_SERVICE)
            or "monitoring",
            endpoint=endpoint,
        )
        self._aws_region = self._session._aws_region  # pylint: disable=protected-access
        OTLPMetricExporter.__init__(
            self,
            endpoint=endpoint,
            certificate_file=certificate_file,
            client_key_file=client_key_file,
            client_certificate_file=client_certificate_file,
            headers={**(headers or {}), **_OTLP_AWS_HTTP_HEADERS},
            timeout=timeout,
            compression=compression,
            session=self._session,
            preferred_temporality=preferred_temporality,
            preferred_aggregation=preferred_aggregation,
        )
