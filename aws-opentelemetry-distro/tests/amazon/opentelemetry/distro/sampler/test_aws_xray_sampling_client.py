# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
from logging import getLogger
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.sampler._aws_xray_sampling_client import _AwsXRaySamplingClient

SAMPLING_CLIENT_LOGGER_NAME = "amazon.opentelemetry.distro.sampler._aws_xray_sampling_client"
_logger = getLogger(SAMPLING_CLIENT_LOGGER_NAME)

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(TEST_DIR, "data")


class TestAwsXRaySamplingClient(TestCase):
    @patch("requests.post")
    def test_get_no_sampling_rules(self, mock_post=None):
        mock_post.return_value.configure_mock(**{"json.return_value": {"SamplingRuleRecords": []}})
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_rules = client.get_sampling_rules()
        self.assertTrue(len(sampling_rules) == 0)

    @patch("requests.post")
    def test_get_invalid_response(self, mock_post=None):
        mock_post.return_value.configure_mock(**{"json.return_value": {}})
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        with self.assertLogs(_logger, level="ERROR"):
            sampling_rules = client.get_sampling_rules()
        self.assertTrue(len(sampling_rules) == 0)

    @patch("requests.post")
    def test_get_two_sampling_rules(self, mock_post=None):
        with open(f"{DATA_DIR}/get-sampling-rules-response-sample.json", encoding="UTF-8") as file:
            mock_post.return_value.configure_mock(**{"json.return_value": json.load(file)})
            file.close()
        client = _AwsXRaySamplingClient("http://127.0.0.1:2000")
        sampling_rules = client.get_sampling_rules()
        self.assertTrue(len(sampling_rules) == 3)
