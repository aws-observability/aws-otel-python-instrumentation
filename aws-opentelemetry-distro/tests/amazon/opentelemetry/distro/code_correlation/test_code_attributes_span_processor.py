# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from types import FrameType
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor import CodeAttributesSpanProcessor
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.attributes.code_attributes import CODE_FUNCTION_NAME
from opentelemetry.trace import SpanKind


class TestIterStackFrames(TestCase):
    """Test the _iter_stack_frames private class method."""

    def test_iter_stack_frames_single_frame(self):
        """Test iterating over a single frame."""
        mock_frame = MagicMock(spec=FrameType)
        mock_frame.f_back = None

        frames = list(CodeAttributesSpanProcessor._iter_stack_frames(mock_frame))

        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0], mock_frame)

    def test_iter_stack_frames_multiple_frames(self):
        """Test iterating over multiple frames."""
        # Create a chain of frames
        frame3 = MagicMock(spec=FrameType)
        frame3.f_back = None

        frame2 = MagicMock(spec=FrameType)
        frame2.f_back = frame3

        frame1 = MagicMock(spec=FrameType)
        frame1.f_back = frame2

        frames = list(CodeAttributesSpanProcessor._iter_stack_frames(frame1))

        self.assertEqual(len(frames), 3)
        self.assertEqual(frames[0], frame1)
        self.assertEqual(frames[1], frame2)
        self.assertEqual(frames[2], frame3)

    def test_iter_stack_frames_empty_when_none(self):
        """Test that no frames are yielded when starting with None."""
        frames = list(CodeAttributesSpanProcessor._iter_stack_frames(None))
        self.assertEqual(len(frames), 0)


class TestCodeAttributesSpanProcessor(TestCase):
    """Test the CodeAttributesSpanProcessor class."""

    def setUp(self):
        """Set up test fixtures."""
        # Patch the initialization calls to avoid side effects
        self.build_package_mapping_patcher = patch(
            "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor._build_package_mapping"
        )
        self.load_third_party_packages_patcher = patch(
            "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor._load_third_party_packages"
        )

        self.mock_build_package_mapping = self.build_package_mapping_patcher.start()
        self.mock_load_third_party_packages = self.load_third_party_packages_patcher.start()

        self.processor = CodeAttributesSpanProcessor()

    def tearDown(self):
        """Clean up test fixtures."""
        self.build_package_mapping_patcher.stop()
        self.load_third_party_packages_patcher.stop()

    def test_initialization_calls_setup_functions(self):
        """Test that initialization calls the package mapping functions."""
        self.mock_build_package_mapping.assert_called_once()
        self.mock_load_third_party_packages.assert_called_once()

    def test_max_stack_frames_constant(self):
        """Test that MAX_STACK_FRAMES is set to expected value."""
        self.assertEqual(CodeAttributesSpanProcessor.MAX_STACK_FRAMES, 50)

    def create_mock_span(self, span_kind=SpanKind.CLIENT, attributes=None, instrumentation_scope_name=None):
        """Helper to create a mock span with specified properties."""
        mock_span = MagicMock(spec=Span)
        mock_span.kind = span_kind
        mock_span.attributes = attributes

        # Set up instrumentation scope
        if instrumentation_scope_name is not None:
            mock_scope = MagicMock(spec=InstrumentationScope)
            mock_scope.name = instrumentation_scope_name
            mock_span.instrumentation_scope = mock_scope
        else:
            mock_span.instrumentation_scope = None

        return mock_span

    def test_should_process_span_user_client_span_without_attributes(self):
        """Test that user CLIENT spans without code attributes should be processed."""
        span = self.create_mock_span(SpanKind.CLIENT, attributes=None, instrumentation_scope_name="my-app")
        result = self.processor._should_process_span(span)
        self.assertTrue(result)

    def test_should_process_span_user_client_span_with_empty_attributes(self):
        """Test that user CLIENT spans with empty attributes should be processed."""
        span = self.create_mock_span(SpanKind.CLIENT, attributes={}, instrumentation_scope_name="my-app")
        result = self.processor._should_process_span(span)
        self.assertTrue(result)

    def test_should_process_span_client_span_with_existing_code_attributes(self):
        """Test that spans with existing code attributes should not be processed."""
        attributes = {CODE_FUNCTION_NAME: "existing.function"}
        span = self.create_mock_span(SpanKind.CLIENT, attributes=attributes, instrumentation_scope_name="my-app")
        result = self.processor._should_process_span(span)
        self.assertFalse(result)

    def test_should_process_span_user_server_spans(self):
        """Test that user SERVER spans should be processed (new logic)."""
        span = self.create_mock_span(SpanKind.SERVER, attributes=None, instrumentation_scope_name="my-app")
        result = self.processor._should_process_span(span)
        self.assertTrue(result)

    def test_should_process_span_library_server_spans_not_processed(self):
        """Test that library instrumentation SERVER spans should not be processed."""
        span = self.create_mock_span(
            SpanKind.SERVER, attributes=None, instrumentation_scope_name="opentelemetry.instrumentation.flask"
        )
        result = self.processor._should_process_span(span)
        self.assertFalse(result)

    def test_should_process_span_library_internal_spans_not_processed(self):
        """Test that library instrumentation INTERNAL spans should not be processed."""
        span = self.create_mock_span(
            SpanKind.INTERNAL, attributes=None, instrumentation_scope_name="opentelemetry.instrumentation.botocore"
        )
        result = self.processor._should_process_span(span)
        self.assertFalse(result)

    def test_should_process_span_library_client_spans_processed(self):
        """Test that library instrumentation CLIENT spans should be processed."""
        span = self.create_mock_span(
            SpanKind.CLIENT, attributes=None, instrumentation_scope_name="opentelemetry.instrumentation.requests"
        )
        result = self.processor._should_process_span(span)
        self.assertTrue(result)

    def test_should_process_span_user_spans_all_kinds(self):
        """Test that user spans of all kinds should be processed."""
        test_cases = [
            SpanKind.CLIENT,
            SpanKind.SERVER,
            SpanKind.PRODUCER,
            SpanKind.CONSUMER,
            SpanKind.INTERNAL,
        ]

        for span_kind in test_cases:
            with self.subTest(span_kind=span_kind):
                span = self.create_mock_span(span_kind, attributes=None, instrumentation_scope_name="my-app")
                result = self.processor._should_process_span(span)
                self.assertTrue(result)

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.is_user_code")
    @patch(
        "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor."
        "add_code_attributes_to_span_from_frame"
    )
    def test_capture_code_attributes_with_user_code(self, mock_add_attributes, mock_is_user_code, mock_getframe):
        """Test capturing code attributes when user code is found."""
        # Create mock frames
        mock_code = MagicMock()
        mock_code.co_filename = "/user/code.py"

        mock_frame = MagicMock(spec=FrameType)
        mock_frame.f_code = mock_code
        mock_frame.f_back = None

        mock_getframe.return_value = mock_frame
        mock_is_user_code.return_value = True

        span = self.create_mock_span()

        self.processor._capture_code_attributes(span)

        mock_getframe.assert_called_once_with(1)
        mock_is_user_code.assert_called_once_with("/user/code.py")
        mock_add_attributes.assert_called_once_with(mock_frame, span)

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.is_user_code")
    @patch(
        "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor."
        "add_code_attributes_to_span_from_frame"
    )
    def test_capture_code_attributes_no_user_code(self, mock_add_attributes, mock_is_user_code, mock_getframe):
        """Test capturing code attributes when no user code is found."""
        # Create mock frames - all library code
        mock_code1 = MagicMock()
        mock_code1.co_filename = "/lib/code1.py"

        mock_code2 = MagicMock()
        mock_code2.co_filename = "/lib/code2.py"

        mock_frame2 = MagicMock(spec=FrameType)
        mock_frame2.f_code = mock_code2
        mock_frame2.f_back = None

        mock_frame1 = MagicMock(spec=FrameType)
        mock_frame1.f_code = mock_code1
        mock_frame1.f_back = mock_frame2

        mock_getframe.return_value = mock_frame1
        mock_is_user_code.return_value = False

        span = self.create_mock_span()

        self.processor._capture_code_attributes(span)

        mock_getframe.assert_called_once_with(1)
        # is_user_code should be called for both frames
        self.assertEqual(mock_is_user_code.call_count, 2)
        mock_add_attributes.assert_not_called()

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.is_user_code")
    @patch(
        "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor."
        "add_code_attributes_to_span_from_frame"
    )
    def test_capture_code_attributes_max_frames_limit(self, mock_add_attributes, mock_is_user_code, mock_getframe):
        """Test that frame iteration respects MAX_STACK_FRAMES limit."""
        # Create a deep stack that exceeds MAX_STACK_FRAMES
        frames = []
        for i in range(CodeAttributesSpanProcessor.MAX_STACK_FRAMES + 1):  # More than MAX_STACK_FRAMES
            mock_code = MagicMock()
            mock_code.co_filename = f"/frame{i}.py"

            mock_frame = MagicMock(spec=FrameType)
            mock_frame.f_code = mock_code
            frames.append(mock_frame)

        # Link frames together
        for i in range(len(frames) - 1):
            frames[i].f_back = frames[i + 1]
        frames[-1].f_back = None

        mock_getframe.return_value = frames[0]
        mock_is_user_code.return_value = False  # No user code found

        span = self.create_mock_span()

        self.processor._capture_code_attributes(span)

        # Should only check up to MAX_STACK_FRAMES
        self.assertEqual(mock_is_user_code.call_count, CodeAttributesSpanProcessor.MAX_STACK_FRAMES)
        mock_add_attributes.assert_not_called()

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    def test_capture_code_attributes_getframe_oserror(self, mock_getframe):
        """Test handling of OSError when sys._getframe is not available."""
        mock_getframe.side_effect = OSError("sys._getframe not available")

        span = self.create_mock_span()

        # Should not raise exception
        self.processor._capture_code_attributes(span)

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    def test_capture_code_attributes_getframe_valueerror(self, mock_getframe):
        """Test handling of ValueError when sys._getframe is called with invalid argument."""
        mock_getframe.side_effect = ValueError("invalid frame")

        span = self.create_mock_span()

        # Should not raise exception
        self.processor._capture_code_attributes(span)

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.is_user_code")
    @patch(
        "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor."
        "add_code_attributes_to_span_from_frame"
    )
    def test_capture_code_attributes_stops_at_first_user_code(
        self, mock_add_attributes, mock_is_user_code, mock_getframe
    ):
        """Test that processing stops at the first user code frame."""
        # Create mock frames where second frame is user code
        mock_code1 = MagicMock()
        mock_code1.co_filename = "/lib/code1.py"

        mock_code2 = MagicMock()
        mock_code2.co_filename = "/user/code2.py"

        mock_code3 = MagicMock()
        mock_code3.co_filename = "/user/code3.py"

        mock_frame3 = MagicMock(spec=FrameType)
        mock_frame3.f_code = mock_code3
        mock_frame3.f_back = None

        mock_frame2 = MagicMock(spec=FrameType)
        mock_frame2.f_code = mock_code2
        mock_frame2.f_back = mock_frame3

        mock_frame1 = MagicMock(spec=FrameType)
        mock_frame1.f_code = mock_code1
        mock_frame1.f_back = mock_frame2

        mock_getframe.return_value = mock_frame1

        def is_user_code_side_effect(filename):
            return filename in ["/user/code2.py", "/user/code3.py"]

        mock_is_user_code.side_effect = is_user_code_side_effect

        span = self.create_mock_span()

        self.processor._capture_code_attributes(span)

        # Should check first two frames, then stop at first user code
        self.assertEqual(mock_is_user_code.call_count, 2)
        mock_add_attributes.assert_called_once_with(mock_frame2, span)

    def test_on_start_should_not_process_span(self):
        """Test on_start when span should not be processed."""
        # Library instrumentation SERVER span should not be processed
        span = self.create_mock_span(SpanKind.SERVER, instrumentation_scope_name="opentelemetry.instrumentation.flask")

        with patch.object(self.processor, "_capture_code_attributes") as mock_capture:
            self.processor.on_start(span)
            mock_capture.assert_not_called()

    def test_on_start_should_process_span(self):
        """Test on_start when span should be processed."""
        span = self.create_mock_span(SpanKind.CLIENT)  # Client span without code attributes

        with patch.object(self.processor, "_capture_code_attributes") as mock_capture:
            self.processor.on_start(span)
            mock_capture.assert_called_once_with(span)

    def test_on_start_with_parent_context(self):
        """Test on_start with parent context parameter."""
        span = self.create_mock_span(SpanKind.CLIENT)
        parent_context = MagicMock(spec=Context)

        with patch.object(self.processor, "_capture_code_attributes") as mock_capture:
            self.processor.on_start(span, parent_context)
            mock_capture.assert_called_once_with(span)

    def test_on_end(self):
        """Test on_end method (empty implementation)."""
        mock_span = MagicMock(spec=ReadableSpan)

        # Should not raise exception
        self.processor.on_end(mock_span)

    def test_shutdown(self):
        """Test shutdown method (empty implementation)."""
        # Should not raise exception
        self.processor.shutdown()

    def test_force_flush_returns_true(self):
        """Test that force_flush always returns True."""
        result = self.processor.force_flush()
        self.assertTrue(result)

    def test_force_flush_with_timeout(self):
        """Test that force_flush accepts timeout parameter and returns True."""
        result = self.processor.force_flush(timeout_millis=5000)
        self.assertTrue(result)

    def test_force_flush_with_default_timeout(self):
        """Test that force_flush uses default timeout and returns True."""
        result = self.processor.force_flush()
        self.assertTrue(result)


class TestCodeAttributesSpanProcessorIntegration(TestCase):
    """Integration tests for CodeAttributesSpanProcessor."""

    def setUp(self):
        """Set up test fixtures."""
        # Patch the initialization calls
        with patch(
            "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor._build_package_mapping"
        ), patch(
            "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor._load_third_party_packages"
        ):
            self.processor = CodeAttributesSpanProcessor()

    def create_real_span_mock(self, span_kind=SpanKind.CLIENT, attributes=None):
        """Create a more realistic span mock."""
        span = MagicMock(spec=Span)
        span.kind = span_kind
        span.attributes = attributes
        span.is_recording.return_value = True
        return span

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.is_user_code")
    @patch(
        "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor."
        "add_code_attributes_to_span_from_frame"
    )
    def test_full_workflow_with_user_code(self, mock_add_attributes, mock_is_user_code, mock_getframe):
        """Test the complete workflow when user code is found."""
        # Setup mock frame
        mock_code = MagicMock()
        mock_code.co_filename = "/app/user_code.py"

        mock_frame = MagicMock(spec=FrameType)
        mock_frame.f_code = mock_code
        mock_frame.f_back = None

        mock_getframe.return_value = mock_frame
        mock_is_user_code.return_value = True

        span = self.create_real_span_mock(SpanKind.CLIENT)

        # Execute the full workflow
        self.processor.on_start(span)

        # Verify all components were called correctly
        mock_getframe.assert_called_once_with(1)
        mock_is_user_code.assert_called_once_with("/app/user_code.py")
        mock_add_attributes.assert_called_once_with(mock_frame, span)

    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.sys._getframe")
    @patch("amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor.is_user_code")
    @patch(
        "amazon.opentelemetry.distro.code_correlation.code_attributes_span_processor."
        "add_code_attributes_to_span_from_frame"
    )
    def test_full_workflow_no_user_code_found(self, mock_add_attributes, mock_is_user_code, mock_getframe):
        """Test the complete workflow when no user code is found."""
        # Setup mock frames - all library code
        frames_data = [
            ("/lib/instrumentation.py", False),
            ("/lib/framework.py", False),
            ("/lib/stdlib.py", False),
        ]

        frames = []
        for i, (filename, _) in enumerate(frames_data):
            mock_code = MagicMock()
            mock_code.co_filename = filename

            mock_frame = MagicMock(spec=FrameType)
            mock_frame.f_code = mock_code
            frames.append(mock_frame)

        # Link frames
        for i in range(len(frames) - 1):
            frames[i].f_back = frames[i + 1]
        frames[-1].f_back = None

        mock_getframe.return_value = frames[0]
        mock_is_user_code.return_value = False

        span = self.create_real_span_mock(SpanKind.CLIENT)

        # Execute the full workflow
        self.processor.on_start(span)

        # Verify components were called
        mock_getframe.assert_called_once_with(1)
        self.assertEqual(mock_is_user_code.call_count, len(frames_data))
        mock_add_attributes.assert_not_called()

    def test_processor_lifecycle(self):
        """Test the complete processor lifecycle."""
        span = self.create_real_span_mock(SpanKind.CLIENT)

        # Start processing
        with patch.object(self.processor, "_capture_code_attributes") as mock_capture:
            self.processor.on_start(span)
            mock_capture.assert_called_once()

        # End processing
        self.processor.on_end(span)  # Should not raise

        # Force flush
        result = self.processor.force_flush()
        self.assertTrue(result)

        # Shutdown
        self.processor.shutdown()  # Should not raise
