# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.serviceevents.utils.instance_id import clear_instance_id_cache, get_instance_id


class TestGetInstanceId(TestCase):
    """Test the get_instance_id function."""

    def setUp(self):
        """Clear the cache before each test."""
        clear_instance_id_cache()

    def tearDown(self):
        """Clear the cache after each test."""
        clear_instance_id_cache()

    @patch.dict("os.environ", {"INSTANCE_ID": "my-instance-123"}, clear=False)
    def test_instance_id_env_var(self):
        """Test that INSTANCE_ID environment variable is used."""
        result = get_instance_id()
        self.assertEqual(result, "my-instance-123")

    @patch.dict("os.environ", {"HOSTNAME": "my-hostname"}, clear=False)
    @patch.dict("os.environ", {}, clear=False)
    def test_hostname_env_var(self):
        """Test that HOSTNAME environment variable is used as fallback."""
        # Make sure INSTANCE_ID is not set
        import os

        os.environ.pop("INSTANCE_ID", None)
        clear_instance_id_cache()

        result = get_instance_id()
        self.assertEqual(result, "my-hostname")

    @patch.dict("os.environ", {"INSTANCE_ID": "instance-1", "HOSTNAME": "host-1"}, clear=False)
    def test_instance_id_takes_priority_over_hostname(self):
        """Test that INSTANCE_ID takes priority over HOSTNAME."""
        result = get_instance_id()
        self.assertEqual(result, "instance-1")

    @patch(
        "amazon.opentelemetry.serviceevents.utils.instance_id.socket.gethostname", return_value="socket-hostname"
    )
    @patch.dict("os.environ", {}, clear=True)
    def test_falls_back_to_socket_gethostname(self, mock_gethostname):
        """Test fallback to socket.gethostname()."""
        result = get_instance_id()
        self.assertEqual(result, "socket-hostname")
        mock_gethostname.assert_called_once()

    @patch(
        "amazon.opentelemetry.serviceevents.utils.instance_id.socket.gethostname",
        side_effect=Exception("DNS error"),
    )
    @patch.dict("os.environ", {}, clear=True)
    def test_socket_error_returns_unknown(self, mock_gethostname):
        """Test that socket.gethostname() error returns 'unknown'."""
        result = get_instance_id()
        self.assertEqual(result, "unknown")

    @patch("amazon.opentelemetry.serviceevents.utils.instance_id.socket.gethostname", return_value="cached-host")
    @patch.dict("os.environ", {}, clear=True)
    def test_caching_behavior(self, mock_gethostname):
        """Test that result is cached - socket only called once."""
        result1 = get_instance_id()
        result2 = get_instance_id()

        self.assertEqual(result1, "cached-host")
        self.assertEqual(result2, "cached-host")
        mock_gethostname.assert_called_once()

    @patch("amazon.opentelemetry.serviceevents.utils.instance_id.socket.gethostname")
    @patch.dict("os.environ", {}, clear=True)
    def test_clear_cache_resets(self, mock_gethostname):
        """Test that clear_instance_id_cache resets the cache."""
        mock_gethostname.side_effect = ["first-host", "second-host"]

        result1 = get_instance_id()
        self.assertEqual(result1, "first-host")

        clear_instance_id_cache()

        result2 = get_instance_id()
        self.assertEqual(result2, "second-host")
        self.assertEqual(mock_gethostname.call_count, 2)
