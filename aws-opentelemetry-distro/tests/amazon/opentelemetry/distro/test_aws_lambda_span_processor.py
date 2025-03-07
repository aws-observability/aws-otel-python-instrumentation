# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER
from amazon.opentelemetry.distro.aws_lambda_span_processor import AwsLambdaSpanProcessor
from opentelemetry.context import Context, set_value
from opentelemetry.trace import Span, SpanContext, SpanKind
from opentelemetry.trace.propagation import _SPAN_KEY


class TestAwsLambdaSpanProcessor(TestCase):

    def setUp(self):
        self.processor = AwsLambdaSpanProcessor()
        self.lambda_span: Span = MagicMock()
        self.lambda_span.instrumentation_scope.name = "opentelemetry.instrumentation.aws_lambda"
        self.lambda_span.kind = SpanKind.SERVER

        self.lambda_span_context: SpanContext = MagicMock()
        self.lambda_span_context.trace_id = "ABC"
        self.lambda_span_context.span_id = "lambda_id"

        self.lambda_context: Context = set_value(_SPAN_KEY, self.lambda_span)

        self.lambda_span.get_span_context.return_value = self.lambda_span_context
        self.processor.on_start(self.lambda_span)

    def tearDown(self):
        self.processor.on_end(self.lambda_span)
        self.processor.shutdown()

    @patch("opentelemetry.sdk.trace.Span")
    def test_lambda_span_multiple_server_flag_internal_api(self, mock_span_class):

        flask_span = mock_span_class.return_value
        flask_span.instrumentation_scope.name = "opentelemetry.instrumentation.flask"
        flask_span.kind = SpanKind.INTERNAL
        flask_span.parent = self.lambda_span_context

        self.processor.on_start(flask_span, self.lambda_context)

        self.assertEqual(flask_span._kind, SpanKind.SERVER)
        self.assertIn(AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER, self.lambda_span.set_attribute.call_args_list[0][0][0])

        self.processor.on_end(flask_span)
        self.processor.on_end(self.lambda_span)

        self.processor.shutdown()

    @patch("opentelemetry.sdk.trace.Span")
    def test_lambda_span_multiple_server_flag_server_api(self, mock_span_class):

        flask_span = mock_span_class.return_value
        flask_span.instrumentation_scope.name = "opentelemetry.instrumentation.flask"
        flask_span.kind = SpanKind.SERVER
        flask_span.parent = self.lambda_span_context

        self.processor.on_start(flask_span, self.lambda_context)

        self.assertEqual(flask_span.kind, SpanKind.SERVER)
        self.assertIn(AWS_TRACE_LAMBDA_FLAG_MULTIPLE_SERVER, self.lambda_span.set_attribute.call_args_list[0][0][0])

        self.processor.on_end(flask_span)
        self.processor.on_end(self.lambda_span)

        self.processor.shutdown()

    @patch("opentelemetry.sdk.trace.Span")
    def test_lambda_span_single_server_span(self, mock_span_class):

        flask_span = mock_span_class.return_value
        flask_span.instrumentation_scope.name = "opentelemetry.instrumentation.http"
        flask_span.kind = SpanKind.CLIENT
        flask_span.parent = self.lambda_span_context

        self.processor.on_start(flask_span, self.lambda_context)

        self.assertEqual(flask_span.kind, SpanKind.CLIENT)
        flask_span.set_attribute.assert_not_called()

        self.processor.on_end(flask_span)
        self.processor.on_end(self.lambda_span)

        self.processor.shutdown()
