# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import patch

import requests
from botocore.credentials import Credentials

from amazon.opentelemetry.distro._utils import get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession

AWS_OTLP_TRACES_ENDPOINT = "https://xray.us-east-1.amazonaws.com/v1/traces"
AWS_OTLP_LOGS_ENDPOINT = "https://logs.us-east-1.amazonaws.com/v1/logs"

AUTHORIZATION_HEADER = "Authorization"
X_AMZ_DATE_HEADER = "X-Amz-Date"
X_AMZ_SECURITY_TOKEN_HEADER = "X-Amz-Security-Token"

mock_credentials = Credentials(access_key="test_access_key", secret_key="test_secret_key", token="test_session_token")


class TestAwsAuthSession(TestCase):
    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=None)
    def test_aws_auth_session_no_credentials(self, _, __):
        """Tests that aws_auth_session will not inject SigV4 Headers if retrieving credentials returns None."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertNotIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertNotIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)

    @patch("requests.Session.request", return_value=requests.Response())
    @patch("botocore.session.Session.get_credentials", return_value=mock_credentials)
    def test_aws_auth_session(self, _, __):
        """Tests that aws_auth_session will inject SigV4 Headers if botocore is installed."""

        session = AwsAuthSession("us-east-1", "xray", get_aws_session())
        actual_headers = {"test": "test"}

        session.request("POST", AWS_OTLP_TRACES_ENDPOINT, data="", headers=actual_headers)

        self.assertIn(AUTHORIZATION_HEADER, actual_headers)
        self.assertIn(X_AMZ_DATE_HEADER, actual_headers)
        self.assertIn(X_AMZ_SECURITY_TOKEN_HEADER, actual_headers)
