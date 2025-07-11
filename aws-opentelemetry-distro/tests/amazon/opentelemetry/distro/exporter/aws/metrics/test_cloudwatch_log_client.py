# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=too-many-public-methods

import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from botocore.exceptions import ClientError

from amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client import CloudWatchLogClient


class TestCloudWatchLogClient(unittest.TestCase):
    """Test CloudWatchLogClient class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the botocore session to avoid AWS calls
        with patch("botocore.session.Session") as mock_session:
            mock_client = Mock()
            mock_session_instance = Mock()
            mock_session.return_value = mock_session_instance
            mock_session_instance.create_client.return_value = mock_client

            self.log_client = CloudWatchLogClient(session=mock_session, log_group_name="test-log-group")

    def test_initialization(self):
        """Test log client initialization."""
        self.assertEqual(self.log_client.log_group_name, "test-log-group")
        self.assertIsNotNone(self.log_client.log_stream_name)
        self.assertTrue(self.log_client.log_stream_name.startswith("otel-python-"))

    @patch("botocore.session.Session")
    def test_initialization_with_custom_params(self, mock_session):
        """Test log client initialization with custom parameters."""
        # Mock the botocore session to avoid AWS calls
        mock_client = Mock()
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.create_client.return_value = mock_client

        log_client = CloudWatchLogClient(
            session=mock_session,
            log_group_name="custom-log-group",
            log_stream_name="custom-stream",
            aws_region="us-west-2",
        )
        self.assertEqual(log_client.log_group_name, "custom-log-group")
        self.assertEqual(log_client.log_stream_name, "custom-stream")

    def test_generate_log_stream_name(self):
        """Test log stream name generation."""
        name1 = self.log_client._generate_log_stream_name()
        name2 = self.log_client._generate_log_stream_name()

        # Should generate unique names
        self.assertNotEqual(name1, name2)
        self.assertTrue(name1.startswith("otel-python-"))
        self.assertTrue(name2.startswith("otel-python-"))

    def test_create_log_group_if_needed_success(self):
        """Test log group creation when needed."""
        # This method should not raise an exception
        self.log_client._create_log_group_if_needed()

    def test_create_log_group_if_needed_already_exists(self):
        """Test log group creation when it already exists."""
        # Mock the create_log_group to raise ResourceAlreadyExistsException
        self.log_client.logs_client.create_log_group.side_effect = ClientError(
            {"Error": {"Code": "ResourceAlreadyExistsException"}}, "CreateLogGroup"
        )

        # This should not raise an exception
        self.log_client._create_log_group_if_needed()

    def test_create_log_group_if_needed_failure(self):
        """Test log group creation failure."""
        # Mock the create_log_group to raise AccessDenied error
        self.log_client.logs_client.create_log_group.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "CreateLogGroup"
        )

        with self.assertRaises(ClientError):
            self.log_client._create_log_group_if_needed()

    def test_create_event_batch(self):
        """Test event batch creation."""
        batch = self.log_client._create_event_batch()

        self.assertEqual(batch.log_events, [])
        self.assertEqual(batch.byte_total, 0)
        self.assertEqual(batch.min_timestamp_ms, 0)
        self.assertEqual(batch.max_timestamp_ms, 0)
        self.assertIsInstance(batch.created_timestamp_ms, int)

    def test_validate_log_event_valid(self):
        """Test log event validation with valid event."""
        log_event = {"message": "test message", "timestamp": int(time.time() * 1000)}

        result = self.log_client._validate_log_event(log_event)
        self.assertTrue(result)

    def test_validate_log_event_empty_message(self):
        """Test log event validation with empty message."""
        log_event = {"message": "", "timestamp": int(time.time() * 1000)}

        result = self.log_client._validate_log_event(log_event)
        self.assertFalse(result)

        # Test whitespace-only message
        whitespace_event = {"message": "   ", "timestamp": int(time.time() * 1000)}
        result = self.log_client._validate_log_event(whitespace_event)
        self.assertFalse(result)

        # Test missing message key
        missing_message_event = {"timestamp": int(time.time() * 1000)}
        result = self.log_client._validate_log_event(missing_message_event)
        self.assertFalse(result)

    def test_validate_log_event_oversized_message(self):
        """Test log event validation with oversized message."""
        # Create a message larger than the maximum allowed size
        large_message = "x" * (self.log_client.CW_MAX_EVENT_PAYLOAD_BYTES + 100)
        log_event = {"message": large_message, "timestamp": int(time.time() * 1000)}

        result = self.log_client._validate_log_event(log_event)
        self.assertTrue(result)  # Should still be valid after truncation
        # Check that message was truncated
        self.assertLess(len(log_event["message"]), len(large_message))
        self.assertTrue(log_event["message"].endswith(self.log_client.CW_TRUNCATED_SUFFIX))

    def test_validate_log_event_old_timestamp(self):
        """Test log event validation with very old timestamp."""
        # Timestamp from 15 days ago
        old_timestamp = int(time.time() * 1000) - (15 * 24 * 60 * 60 * 1000)
        log_event = {"message": "test message", "timestamp": old_timestamp}

        result = self.log_client._validate_log_event(log_event)
        self.assertFalse(result)

    def test_validate_log_event_future_timestamp(self):
        """Test log event validation with future timestamp."""
        # Timestamp 3 hours in the future
        future_timestamp = int(time.time() * 1000) + (3 * 60 * 60 * 1000)
        log_event = {"message": "test message", "timestamp": future_timestamp}

        result = self.log_client._validate_log_event(log_event)
        self.assertFalse(result)

    def test_event_batch_exceeds_limit_by_count(self):
        """Test batch limit checking by event count."""
        batch = self.log_client._create_event_batch()
        # Simulate batch with maximum events
        for _ in range(self.log_client.CW_MAX_REQUEST_EVENT_COUNT):
            batch.add_event({"message": "test", "timestamp": int(time.time() * 1000)}, 10)

        result = self.log_client._event_batch_exceeds_limit(batch, 100)
        self.assertTrue(result)

    def test_event_batch_exceeds_limit_by_size(self):
        """Test batch limit checking by byte size."""
        batch = self.log_client._create_event_batch()
        # Manually set byte_total to near limit
        batch.byte_total = self.log_client.CW_MAX_REQUEST_PAYLOAD_BYTES - 50

        result = self.log_client._event_batch_exceeds_limit(batch, 100)
        self.assertTrue(result)

    def test_event_batch_within_limits(self):
        """Test batch limit checking within limits."""
        batch = self.log_client._create_event_batch()
        for _ in range(10):
            batch.add_event({"message": "test", "timestamp": int(time.time() * 1000)}, 100)

        result = self.log_client._event_batch_exceeds_limit(batch, 100)
        self.assertFalse(result)

    def test_is_batch_active_new_batch(self):
        """Test batch activity check for new batch."""
        batch = self.log_client._create_event_batch()
        current_time = int(time.time() * 1000)

        result = self.log_client._is_batch_active(batch, current_time)
        self.assertTrue(result)

    def test_is_batch_active_24_hour_span(self):
        """Test batch activity check for 24+ hour span."""
        batch = self.log_client._create_event_batch()
        current_time = int(time.time() * 1000)
        # Add an event to set the timestamps
        batch.add_event({"message": "test", "timestamp": current_time}, 10)

        # Test with timestamp 25 hours in the future
        future_timestamp = current_time + (25 * 60 * 60 * 1000)

        result = self.log_client._is_batch_active(batch, future_timestamp)
        self.assertFalse(result)

    def test_log_event_batch_add_event(self):
        """Test adding log event to batch."""
        batch = self.log_client._create_event_batch()
        log_event = {"message": "test message", "timestamp": int(time.time() * 1000)}
        event_size = 100

        batch.add_event(log_event, event_size)

        self.assertEqual(batch.size(), 1)
        self.assertEqual(batch.byte_total, event_size)
        self.assertEqual(batch.min_timestamp_ms, log_event["timestamp"])
        self.assertEqual(batch.max_timestamp_ms, log_event["timestamp"])

    def test_sort_log_events(self):
        """Test sorting log events by timestamp."""
        batch = self.log_client._create_event_batch()
        current_time = int(time.time() * 1000)

        # Add events with timestamps in reverse order
        events = [
            {"message": "third", "timestamp": current_time + 2000},
            {"message": "first", "timestamp": current_time},
            {"message": "second", "timestamp": current_time + 1000},
        ]

        # Add events to batch in unsorted order
        for event in events:
            batch.add_event(event, 10)

        self.log_client._sort_log_events(batch)

        # Check that events are now sorted by timestamp
        self.assertEqual(batch.log_events[0]["message"], "first")
        self.assertEqual(batch.log_events[1]["message"], "second")
        self.assertEqual(batch.log_events[2]["message"], "third")

    @patch.object(CloudWatchLogClient, "_send_log_batch")
    def test_flush_pending_events_with_pending_events(self, mock_send_batch):
        """Test flush pending events functionality with pending events."""
        # Create a batch with events
        self.log_client._event_batch = self.log_client._create_event_batch()
        self.log_client._event_batch.add_event({"message": "test", "timestamp": int(time.time() * 1000)}, 10)

        result = self.log_client.flush_pending_events()

        self.assertTrue(result)
        mock_send_batch.assert_called_once()

    def test_flush_pending_events_no_pending_events(self):
        """Test flush pending events functionality with no pending events."""
        # No batch exists
        self.assertIsNone(self.log_client._event_batch)

        result = self.log_client.flush_pending_events()

        self.assertTrue(result)

    def test_send_log_event_method_exists(self):
        """Test that send_log_event method exists and can be called."""
        # Just test that the method exists and doesn't crash with basic input
        log_event = {"message": "test message", "timestamp": 1234567890}

        # Mock the AWS client methods to avoid actual AWS calls
        with patch.object(self.log_client.logs_client, "put_log_events") as mock_put:
            mock_put.return_value = {"nextSequenceToken": "12345"}

            # Should not raise an exception
            try:
                self.log_client.send_log_event(log_event)
                # Method should complete without error
            except ClientError as error:
                self.fail(f"send_log_event raised an exception: {error}")

    def test_send_log_batch_with_resource_not_found(self):
        """Test lazy creation when put_log_events fails with ResourceNotFoundException."""
        batch = self.log_client._create_event_batch()
        batch.add_event({"message": "test message", "timestamp": int(time.time() * 1000)}, 10)

        # Mock put_log_events to fail first, then succeed
        mock_put = self.log_client.logs_client.put_log_events
        mock_put.side_effect = [
            ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "PutLogEvents"),
            {"nextSequenceToken": "12345"},
        ]

        # Mock the create methods
        mock_create_group = Mock()
        mock_create_stream = Mock()
        self.log_client._create_log_group_if_needed = mock_create_group
        self.log_client._create_log_stream_if_needed = mock_create_stream

        # Should not raise an exception and should create resources
        self.log_client._send_log_batch(batch)

        # Verify that creation methods were called
        mock_create_group.assert_called_once()
        mock_create_stream.assert_called_once()

        # Verify put_log_events was called twice (initial attempt + retry)
        self.assertEqual(mock_put.call_count, 2)

    def test_send_log_batch_with_other_error(self):
        """Test that non-ResourceNotFoundException errors are re-raised."""
        batch = self.log_client._create_event_batch()
        batch.add_event({"message": "test message", "timestamp": int(time.time() * 1000)}, 10)

        # Mock put_log_events to fail with different error
        self.log_client.logs_client.put_log_events.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "PutLogEvents"
        )

        # Should raise the original exception
        with self.assertRaises(ClientError):
            self.log_client._send_log_batch(batch)

    def test_create_log_stream_if_needed_success(self):
        """Test log stream creation when needed."""
        # This method should not raise an exception
        self.log_client._create_log_stream_if_needed()

    def test_create_log_stream_if_needed_already_exists(self):
        """Test log stream creation when it already exists."""
        # Mock the create_log_stream to raise ResourceAlreadyExistsException
        self.log_client.logs_client.create_log_stream.side_effect = ClientError(
            {"Error": {"Code": "ResourceAlreadyExistsException"}}, "CreateLogStream"
        )

        # This should not raise an exception
        self.log_client._create_log_stream_if_needed()

    def test_create_log_stream_if_needed_failure(self):
        """Test log stream creation failure."""
        # Mock the create_log_stream to raise AccessDenied error
        self.log_client.logs_client.create_log_stream.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "CreateLogStream"
        )

        with self.assertRaises(ClientError):
            self.log_client._create_log_stream_if_needed()

    def test_send_log_batch_success(self):
        """Test successful log batch sending."""
        batch = self.log_client._create_event_batch()
        batch.add_event({"message": "test message", "timestamp": int(time.time() * 1000)}, 10)

        # Mock successful put_log_events call
        self.log_client.logs_client.put_log_events.return_value = {"nextSequenceToken": "12345"}

        # Should not raise an exception
        result = self.log_client._send_log_batch(batch)
        self.assertEqual(result["nextSequenceToken"], "12345")

    def test_send_log_batch_empty_batch(self):
        """Test sending empty batch does nothing."""
        batch = self.log_client._create_event_batch()
        # Empty batch should return early without calling AWS

        result = self.log_client._send_log_batch(batch)
        self.assertIsNone(result)

        # Verify put_log_events was not called
        self.log_client.logs_client.put_log_events.assert_not_called()

    def test_is_batch_active_flush_interval_reached(self):
        """Test batch activity check when flush interval is reached."""
        batch = self.log_client._create_event_batch()
        current_time = int(time.time() * 1000)

        # Set the batch creation time to more than flush interval ago
        batch.created_timestamp_ms = current_time - (self.log_client.BATCH_FLUSH_INTERVAL + 1000)
        # Add an event to set the timestamps
        batch.add_event({"message": "test", "timestamp": current_time}, 10)

        result = self.log_client._is_batch_active(batch, current_time)
        self.assertFalse(result)

    def test_send_log_event_with_invalid_event(self):
        """Test send_log_event with an invalid event that fails validation."""
        # Create an event that will fail validation (empty message)
        log_event = {"message": "", "timestamp": int(time.time() * 1000)}

        # Should not raise an exception, but should not call put_log_events
        self.log_client.send_log_event(log_event)

        # Verify put_log_events was not called due to validation failure
        self.log_client.logs_client.put_log_events.assert_not_called()

    def test_send_log_event_batching_logic(self):
        """Test that send_log_event properly batches events."""
        log_event = {"message": "test message", "timestamp": int(time.time() * 1000)}

        # Mock put_log_events to not be called initially (batching)
        self.log_client.logs_client.put_log_events.return_value = {"nextSequenceToken": "12345"}

        # Send one event (should be batched, not sent immediately)
        self.log_client.send_log_event(log_event)

        # Verify event was added to batch
        self.assertIsNotNone(self.log_client._event_batch)
        self.assertEqual(self.log_client._event_batch.size(), 1)

        # put_log_events should not be called yet (event is batched)
        self.log_client.logs_client.put_log_events.assert_not_called()

    def test_send_log_event_force_batch_send(self):
        """Test that send_log_event sends batch when limits are exceeded."""
        # Mock put_log_events
        self.log_client.logs_client.put_log_events.return_value = {"nextSequenceToken": "12345"}

        # Create events to reach the maximum event count limit
        current_time = int(time.time() * 1000)

        # Send events up to the limit (should all be batched)
        for event_index in range(self.log_client.CW_MAX_REQUEST_EVENT_COUNT):
            log_event = {"message": f"test message {event_index}", "timestamp": current_time}
            self.log_client.send_log_event(log_event)

        # At this point, no batch should have been sent yet
        self.log_client.logs_client.put_log_events.assert_not_called()

        # Send one more event (should trigger batch send due to count limit)
        final_event = {"message": "final message", "timestamp": current_time}
        self.log_client.send_log_event(final_event)

        # put_log_events should have been called once
        self.log_client.logs_client.put_log_events.assert_called_once()

    def test_log_event_batch_clear(self):
        """Test clearing a log event batch."""
        batch = self.log_client._create_event_batch()
        batch.add_event({"message": "test", "timestamp": int(time.time() * 1000)}, 100)

        # Verify batch has content
        self.assertFalse(batch.is_empty())
        self.assertEqual(batch.size(), 1)

        # Clear and verify
        batch.clear()
        self.assertTrue(batch.is_empty())
        self.assertEqual(batch.size(), 0)
        self.assertEqual(batch.byte_total, 0)

    def test_log_event_batch_timestamp_tracking(self):
        """Test timestamp tracking in LogEventBatch."""
        batch = self.log_client._create_event_batch()
        current_time = int(time.time() * 1000)

        # Add first event
        batch.add_event({"message": "first", "timestamp": current_time}, 10)
        self.assertEqual(batch.min_timestamp_ms, current_time)
        self.assertEqual(batch.max_timestamp_ms, current_time)

        # Add earlier event
        earlier_time = current_time - 1000
        batch.add_event({"message": "earlier", "timestamp": earlier_time}, 10)
        self.assertEqual(batch.min_timestamp_ms, earlier_time)
        self.assertEqual(batch.max_timestamp_ms, current_time)

        # Add later event
        later_time = current_time + 1000
        batch.add_event({"message": "later", "timestamp": later_time}, 10)
        self.assertEqual(batch.min_timestamp_ms, earlier_time)
        self.assertEqual(batch.max_timestamp_ms, later_time)

    def test_generate_log_stream_name_format(self):
        """Test log stream name generation format and uniqueness."""
        name = self.log_client._generate_log_stream_name()
        self.assertTrue(name.startswith("otel-python-"))
        self.assertEqual(len(name), len("otel-python-") + 8)

        # Generate another and ensure they're different
        name2 = self.log_client._generate_log_stream_name()
        self.assertNotEqual(name, name2)

    @patch("botocore.session.Session")
    def test_initialization_with_custom_log_stream_name(self, mock_session):
        """Test initialization with custom log stream name."""
        # Mock the session and client
        mock_client = Mock()
        mock_session.return_value.create_client.return_value = mock_client

        custom_stream = "my-custom-stream"
        client = CloudWatchLogClient(session=mock_session, log_group_name="test-group", log_stream_name=custom_stream)
        self.assertEqual(client.log_stream_name, custom_stream)

    def test_send_log_batch_empty_batch_no_aws_call(self):
        """Test sending an empty batch returns None and doesn't call AWS."""
        batch = self.log_client._create_event_batch()
        result = self.log_client._send_log_batch(batch)
        self.assertIsNone(result)

        # Verify put_log_events is not called for empty batch
        self.log_client.logs_client.put_log_events.assert_not_called()

    def test_validate_log_event_missing_timestamp(self):
        """Test validation of log event with missing timestamp."""
        log_event = {"message": "test message"}  # No timestamp
        result = self.log_client._validate_log_event(log_event)

        # Should be invalid - timestamp defaults to 0 which is too old
        self.assertFalse(result)

    def test_validate_log_event_invalid_timestamp_past(self):
        """Test validation of log event with timestamp too far in the past."""
        # Create timestamp older than 14 days
        old_time = int(time.time() * 1000) - (15 * 24 * 60 * 60 * 1000)
        log_event = {"message": "test message", "timestamp": old_time}

        result = self.log_client._validate_log_event(log_event)
        self.assertFalse(result)

    def test_validate_log_event_invalid_timestamp_future(self):
        """Test validation of log event with timestamp too far in the future."""
        # Create timestamp more than 2 hours in the future
        future_time = int(time.time() * 1000) + (3 * 60 * 60 * 1000)
        log_event = {"message": "test message", "timestamp": future_time}

        result = self.log_client._validate_log_event(log_event)
        self.assertFalse(result)

    def test_send_log_event_validation_failure(self):
        """Test send_log_event when validation fails."""
        # Create invalid event (empty message)
        invalid_event = {"message": "", "timestamp": int(time.time() * 1000)}

        # Mock put_log_events to track calls
        self.log_client.logs_client.put_log_events.return_value = {"nextSequenceToken": "12345"}

        # Send invalid event
        self.log_client.send_log_event(invalid_event)

        # Should not call put_log_events or create batch
        self.log_client.logs_client.put_log_events.assert_not_called()
        self.assertIsNone(self.log_client._event_batch)

    def test_send_log_event_exception_handling(self):
        """Test exception handling in send_log_event."""
        # Mock _validate_log_event to raise an exception
        with patch.object(self.log_client, "_validate_log_event", side_effect=Exception("Test error")):
            log_event = {"message": "test", "timestamp": int(time.time() * 1000)}

            with self.assertRaises(Exception) as context:
                self.log_client.send_log_event(log_event)

            self.assertEqual(str(context.exception), "Test error")

    def test_flush_pending_events_no_batch(self):
        """Test flush pending events when no batch exists."""
        # Ensure no batch exists
        self.log_client._event_batch = None

        result = self.log_client.flush_pending_events()
        self.assertTrue(result)

        # Should not call send_log_batch
        self.log_client.logs_client.put_log_events.assert_not_called()

    def test_is_batch_active_edge_cases(self):
        """Test edge cases for batch activity checking."""
        batch = self.log_client._create_event_batch()
        current_time = int(time.time() * 1000)

        # Test exactly at 24 hour boundary (should still be active)
        batch.add_event({"message": "test", "timestamp": current_time}, 10)
        exactly_24h_future = current_time + (24 * 60 * 60 * 1000)
        result = self.log_client._is_batch_active(batch, exactly_24h_future)
        self.assertTrue(result)

        # Test just over 24 hour boundary (should be inactive)
        over_24h_future = current_time + (24 * 60 * 60 * 1000 + 1)
        result = self.log_client._is_batch_active(batch, over_24h_future)
        self.assertFalse(result)

        # Test exactly at flush interval boundary
        # Create a new batch for this test
        batch2 = self.log_client._create_event_batch()
        batch2.add_event({"message": "test", "timestamp": current_time}, 10)
        batch2.created_timestamp_ms = current_time - self.log_client.BATCH_FLUSH_INTERVAL
        result = self.log_client._is_batch_active(batch2, current_time)
        self.assertFalse(result)

    @patch("amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client.suppress_instrumentation")
    def test_create_log_group_uses_suppress_instrumentation(self, mock_suppress):
        """Test that _create_log_group_if_needed uses suppress_instrumentation."""
        # Configure the mock context manager
        mock_context = MagicMock()
        mock_suppress.return_value = mock_context
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None

        # Call the method
        self.log_client._create_log_group_if_needed()

        # Verify suppress_instrumentation was called
        mock_suppress.assert_called_once()
        mock_context.__enter__.assert_called_once()
        mock_context.__exit__.assert_called_once()

        # Verify the AWS call happened within the context
        self.log_client.logs_client.create_log_group.assert_called_once_with(logGroupName="test-log-group")

    @patch("amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client.suppress_instrumentation")
    def test_create_log_stream_uses_suppress_instrumentation(self, mock_suppress):
        """Test that _create_log_stream_if_needed uses suppress_instrumentation."""
        # Configure the mock context manager
        mock_context = MagicMock()
        mock_suppress.return_value = mock_context
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None

        # Call the method
        self.log_client._create_log_stream_if_needed()

        # Verify suppress_instrumentation was called
        mock_suppress.assert_called_once()
        mock_context.__enter__.assert_called_once()
        mock_context.__exit__.assert_called_once()

        # Verify the AWS call happened within the context
        self.log_client.logs_client.create_log_stream.assert_called_once()

    @patch("amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client.suppress_instrumentation")
    def test_send_log_batch_uses_suppress_instrumentation(self, mock_suppress):
        """Test that _send_log_batch uses suppress_instrumentation."""
        # Configure the mock context manager
        mock_context = MagicMock()
        mock_suppress.return_value = mock_context
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None

        # Create a batch with events
        batch = self.log_client._create_event_batch()
        batch.add_event({"message": "test", "timestamp": int(time.time() * 1000)}, 10)

        # Mock successful put_log_events
        self.log_client.logs_client.put_log_events.return_value = {"nextSequenceToken": "12345"}

        # Call the method
        self.log_client._send_log_batch(batch)

        # Verify suppress_instrumentation was called
        mock_suppress.assert_called_once()
        mock_context.__enter__.assert_called_once()
        mock_context.__exit__.assert_called_once()

        # Verify the AWS call happened within the context
        self.log_client.logs_client.put_log_events.assert_called_once()

    @patch("amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client.suppress_instrumentation")
    def test_send_log_batch_retry_uses_suppress_instrumentation(self, mock_suppress):
        """Test that _send_log_batch retry logic also uses suppress_instrumentation."""
        # Configure the mock context manager
        mock_context = MagicMock()
        mock_suppress.return_value = mock_context
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None

        # Create a batch with events
        batch = self.log_client._create_event_batch()
        batch.add_event({"message": "test", "timestamp": int(time.time() * 1000)}, 10)

        # Mock put_log_events to fail first with ResourceNotFoundException, then succeed
        self.log_client.logs_client.put_log_events.side_effect = [
            ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "PutLogEvents"),
            {"nextSequenceToken": "12345"},
        ]

        # Call the method
        self.log_client._send_log_batch(batch)

        # Verify suppress_instrumentation was called:
        # 1. Initial _send_log_batch context
        # 2. Nested context in the retry block
        # 3. _create_log_group_if_needed context
        # 4. _create_log_stream_if_needed context
        self.assertEqual(mock_suppress.call_count, 4)
        # Each context should have been properly entered and exited
        self.assertEqual(mock_context.__enter__.call_count, 4)
        self.assertEqual(mock_context.__exit__.call_count, 4)

        # Verify AWS calls happened
        self.assertEqual(self.log_client.logs_client.put_log_events.call_count, 2)
        self.log_client.logs_client.create_log_group.assert_called_once()
        self.log_client.logs_client.create_log_stream.assert_called_once()

    @patch("amazon.opentelemetry.distro.exporter.aws.metrics._cloudwatch_log_client.suppress_instrumentation")
    def test_create_log_group_exception_still_uses_suppress_instrumentation(self, mock_suppress):
        """Test that suppress_instrumentation is properly used even when exceptions occur."""
        # Configure the mock context manager
        mock_context = MagicMock()
        mock_suppress.return_value = mock_context
        mock_context.__enter__.return_value = mock_context
        mock_context.__exit__.return_value = None

        # Make create_log_group raise an exception
        self.log_client.logs_client.create_log_group.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "CreateLogGroup"
        )

        # Call should raise the exception
        with self.assertRaises(ClientError):
            self.log_client._create_log_group_if_needed()

        # Verify suppress_instrumentation was still properly used
        mock_suppress.assert_called_once()
        mock_context.__enter__.assert_called_once()
        # __exit__ should be called even though an exception was raised
        mock_context.__exit__.assert_called_once()


if __name__ == "__main__":
    unittest.main()
