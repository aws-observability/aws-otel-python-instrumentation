from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_TRACE_FLAG_UNSAMPLED
from amazon.opentelemetry.distro.aws_batch_unsampled_span_processor import BatchUnsampledSpanProcessor
from opentelemetry.trace import TraceFlags


class TestBatchUnsampledSpanProcessor(TestCase):

    def setUp(self):
        self.mock_exporter = MagicMock()
        self.processor = BatchUnsampledSpanProcessor(self.mock_exporter)

    @patch("opentelemetry.sdk.trace.Span")
    def test_on_end_sampled(self, mock_span_class):
        trace_flags = TraceFlags(TraceFlags.SAMPLED)

        mock_span = mock_span_class.return_value
        mock_span.context.trace_flags = trace_flags

        self.processor.on_start(mock_span)
        self.processor.on_end(mock_span)

        self.assertEqual(len(self.processor.queue), 0)
        mock_span.set_attribute.assert_not_called()

    @patch("opentelemetry.sdk.trace.Span")
    def test_on_end_not_sampled(self, mock_span_class):

        trace_flags = TraceFlags(0)
        mock_span = mock_span_class.return_value
        mock_span.context.trace_flags = trace_flags

        self.processor.on_start(mock_span)
        self.processor.on_end(mock_span)

        self.assertEqual(len(self.processor.queue), 1)
        self.assertIn(AWS_TRACE_FLAG_UNSAMPLED, mock_span.set_attribute.call_args_list[0][0][0])
