# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amazon.opentelemetry.distro._utils import get_aws_region, get_sigv4_traces_service
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


class AutoOTLPAwsSpanExporter(OTLPSpanExporter):
    """Entry point for ``OTEL_TRACES_EXPORTER=otlp/sigv4``.

    The OTel SDK constructs exporters from entry points with no arguments, so this resolves the AWS
    region and SigV4 signing service from the environment (``OTEL_EXPORTER_OTLP_TRACES_SIGV4_SERVICE``,
    defaulting to ``xray``) and returns a fully configured SigV4 span exporter.

    ``create_aws_otlp_span_exporter`` is imported lazily so that merely loading this entry-point module
    does not import botocore, which is an optional dependency (the 'patch' extra).
    """

    def __new__(cls):
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.exporter.otlp.aws.traces.otlp_aws_span_exporter import (
            create_aws_otlp_span_exporter,
        )

        return create_aws_otlp_span_exporter(
            region=get_aws_region(),
            aws_service=get_sigv4_traces_service() or "xray",
        )
