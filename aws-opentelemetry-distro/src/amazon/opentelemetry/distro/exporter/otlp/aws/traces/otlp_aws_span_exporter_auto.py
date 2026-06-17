# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from amazon.opentelemetry.distro._utils import IS_BOTOCORE_INSTALLED, get_aws_region, get_sigv4_traces_service
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

_logger = logging.getLogger(__name__)


class AutoOTLPAwsSpanExporter(OTLPSpanExporter):
    """Entry point for ``OTEL_TRACES_EXPORTER=otlp/sigv4``."""

    def __new__(cls):
        # SigV4 signing requires botocore, an optional dependency (the 'patch' extra).
        if not IS_BOTOCORE_INSTALLED:
            _logger.warning(
                "OTEL_TRACES_EXPORTER=otlp/sigv4 requires botocore to be installed; "
                "using the default OTLP span exporter without SigV4 signing."
            )
            return OTLPSpanExporter()

        region = get_aws_region()
        if region is None:
            _logger.warning(
                "OTEL_TRACES_EXPORTER=otlp/sigv4 but no AWS region is set; "
                "set AWS_REGION or AWS_DEFAULT_REGION. Using the default OTLP span exporter without SigV4 signing."
            )
            return OTLPSpanExporter()

        service = get_sigv4_traces_service()
        if service is None:
            _logger.info("OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE is not set; defaulting to xray.")
            service = "xray"

        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import (
            create_aws_otlp_span_exporter,
        )

        return create_aws_otlp_span_exporter(region=region, aws_service=service)
