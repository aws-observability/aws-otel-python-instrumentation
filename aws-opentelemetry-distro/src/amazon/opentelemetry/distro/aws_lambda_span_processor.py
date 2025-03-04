# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from typing_extensions import override

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import SpanKind


class AwsLambdaSpanProcessor(SpanProcessor):
    def __init__(self, instrumentation_names=None):
        self.instrumentation_names = set(instrumentation_names or ["opentelemetry.instrumentation.flask"])
        self.parent_lambda_span = None

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        scope = getattr(span, "instrumentation_scope", None)
        if span.kind == SpanKind.SERVER and scope.name == "opentelemetry.instrumentation.aws_lambda":
            self.parent_lambda_span = span

        if (
            scope.name in self.instrumentation_names
            and self.parent_lambda_span.get_span_context().span_id == span.parent.span_id
        ):
            if span.kind == SpanKind.INTERNAL:
                span._kind = SpanKind.SERVER
            self.parent_lambda_span.set_attribute(AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER, True)
        return

    @override
    def on_end(self, span: ReadableSpan) -> None:
        return

    @override
    def shutdown(self) -> None:
        self.force_flush()

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: int = None) -> bool:
        return True
