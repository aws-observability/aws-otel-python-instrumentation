# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from typing_extensions import override

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER
from opentelemetry.context import Context, get_value
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation import _SPAN_KEY


class AwsLambdaSpanProcessor(SpanProcessor):
    def __init__(self, instrumentation_names=None):
        self.instrumentation_names = set(instrumentation_names or ["opentelemetry.instrumentation.flask"])

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:

        scope = getattr(span, "instrumentation_scope", None)
        if scope.name in self.instrumentation_names:
            parent_span = get_value(_SPAN_KEY, context=parent_context)

            if parent_span is None:
                return

            parent_scope = getattr(parent_span, "instrumentation_scope", None)
            if parent_scope.name == "opentelemetry.instrumentation.aws_lambda":
                span._kind = SpanKind.SERVER
                parent_span.set_attribute(AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER, True)

        return

    # pylint: disable=no-self-use
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
