# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._utils import get_aws_region


class TestGetAwsRegion(TestCase):
    def setUp(self):
        # Clear environment variables before each test
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("AWS_DEFAULT_REGION", None)

    def tearDown(self):
        # Clean up environment variables after each test
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("AWS_DEFAULT_REGION", None)

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    def test_get_aws_region_from_aws_region_env(self, mock_is_installed):
        mock_is_installed.return_value = True

        # Mock botocore module and session
        mock_botocore = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = "us-west-2"
        mock_botocore.session.Session.return_value = mock_session_instance
        sys.modules["botocore"] = mock_botocore
        sys.modules["botocore.session"] = mock_botocore.session

        os.environ["AWS_REGION"] = "us-west-2"

        try:
            self.assertEqual(get_aws_region(), "us-west-2")
        finally:
            # Clean up mock
            if "botocore" in sys.modules:
                del sys.modules["botocore"]
            if "botocore.session" in sys.modules:
                del sys.modules["botocore.session"]

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    def test_get_aws_region_from_aws_default_region_env(self, mock_is_installed):
        mock_is_installed.return_value = True

        # Mock botocore module and session
        mock_botocore = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = "eu-central-1"
        mock_botocore.session.Session.return_value = mock_session_instance
        sys.modules["botocore"] = mock_botocore
        sys.modules["botocore.session"] = mock_botocore.session

        os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"

        try:
            self.assertEqual(get_aws_region(), "eu-central-1")
        finally:
            # Clean up mock
            if "botocore" in sys.modules:
                del sys.modules["botocore"]
            if "botocore.session" in sys.modules:
                del sys.modules["botocore.session"]

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    def test_get_aws_region_prefers_aws_region_over_default(self, mock_is_installed):
        mock_is_installed.return_value = True

        # Mock botocore module and session
        mock_botocore = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = "us-east-1"
        mock_botocore.session.Session.return_value = mock_session_instance
        sys.modules["botocore"] = mock_botocore
        sys.modules["botocore.session"] = mock_botocore.session

        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

        try:
            self.assertEqual(get_aws_region(), "us-east-1")
        finally:
            # Clean up mock
            if "botocore" in sys.modules:
                del sys.modules["botocore"]
            if "botocore.session" in sys.modules:
                del sys.modules["botocore.session"]

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    def test_get_aws_region_from_botocore_session(self, mock_is_installed):
        mock_is_installed.return_value = True

        # Mock botocore module and session
        mock_botocore = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = "ap-southeast-1"
        mock_botocore.session.Session.return_value = mock_session_instance
        sys.modules["botocore"] = mock_botocore
        sys.modules["botocore.session"] = mock_botocore.session

        try:
            result = get_aws_region()
            self.assertEqual(result, "ap-southeast-1")
        finally:
            # Clean up mock
            if "botocore" in sys.modules:
                del sys.modules["botocore"]
            if "botocore.session" in sys.modules:
                del sys.modules["botocore.session"]

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    @patch("amazon.opentelemetry.distro._utils._logger")
    def test_get_aws_region_returns_none_when_no_region_found(self, mock_logger, mock_is_installed):
        mock_is_installed.return_value = False

        result = get_aws_region()

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once()

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    @patch("amazon.opentelemetry.distro._utils._logger")
    def test_get_aws_region_returns_none_when_botocore_has_no_region(self, mock_logger, mock_is_installed):
        mock_is_installed.return_value = True

        # Mock botocore module with no region
        mock_botocore = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = None
        mock_botocore.session.Session.return_value = mock_session_instance
        sys.modules["botocore"] = mock_botocore
        sys.modules["botocore.session"] = mock_botocore.session

        try:
            result = get_aws_region()
            self.assertIsNone(result)
            mock_logger.warning.assert_called_once()
        finally:
            # Clean up mock
            if "botocore" in sys.modules:
                del sys.modules["botocore"]
            if "botocore.session" in sys.modules:
                del sys.modules["botocore.session"]
