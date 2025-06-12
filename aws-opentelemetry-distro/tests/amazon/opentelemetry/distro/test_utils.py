# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
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

    def test_get_aws_region_from_aws_region_env(self):
        os.environ["AWS_REGION"] = "us-west-2"
        self.assertEqual(get_aws_region(), "us-west-2")

    def test_get_aws_region_from_aws_default_region_env(self):
        os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"
        self.assertEqual(get_aws_region(), "eu-central-1")

    def test_get_aws_region_prefers_aws_region_over_default(self):
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        self.assertEqual(get_aws_region(), "us-east-1")

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    @patch("botocore.session.Session")
    def test_get_aws_region_from_botocore_session(self, mock_session_class, mock_is_installed):
        mock_is_installed.return_value = True

        mock_session = MagicMock()
        mock_session.region_name = "ap-southeast-1"
        mock_session_class.return_value = mock_session

        result = get_aws_region()

        self.assertEqual(result, "ap-southeast-1")

    @patch("amazon.opentelemetry.distro._utils.is_installed")
    @patch("amazon.opentelemetry.distro._utils._logger")
    def test_get_aws_region_returns_none_when_no_region_found(self, mock_logger, mock_is_installed):
        mock_is_installed.return_value = False

        result = get_aws_region()

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once()
