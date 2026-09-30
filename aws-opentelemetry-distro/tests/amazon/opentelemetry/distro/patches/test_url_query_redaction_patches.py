# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for AWS SigV4 pre-signed URL query-parameter redaction patch."""

import unittest
from unittest.mock import patch

from amazon.opentelemetry.distro.patches._url_query_redaction_patches import (
    _AWS_SIGV4_SENSITIVE_QUERY_PARAMETERS,
    _apply_url_query_redaction_patches,
)
from opentelemetry.util import http as otel_util_http
from opentelemetry.util.http import redact_url


class TestUrlQueryRedactionPatches(unittest.TestCase):
    """Test cases for the URL query redaction patch."""

    def setUp(self):
        # Snapshot the module-level list so each test starts from a clean state and mutations do
        # not leak between tests (the patch mutates this list in place).
        self._original_params = list(otel_util_http.PARAMS_TO_REDACT)

    def tearDown(self):
        otel_util_http.PARAMS_TO_REDACT[:] = self._original_params

    def test_patch_appends_aws_sigv4_parameters(self):
        _apply_url_query_redaction_patches()

        for param in _AWS_SIGV4_SENSITIVE_QUERY_PARAMETERS:
            self.assertIn(param, otel_util_http.PARAMS_TO_REDACT)

    def test_patch_is_idempotent(self):
        _apply_url_query_redaction_patches()
        after_first = list(otel_util_http.PARAMS_TO_REDACT)

        _apply_url_query_redaction_patches()
        after_second = list(otel_util_http.PARAMS_TO_REDACT)

        self.assertEqual(after_first, after_second)

    def test_patch_preserves_existing_default_parameters(self):
        _apply_url_query_redaction_patches()

        for param in self._original_params:
            self.assertIn(param, otel_util_http.PARAMS_TO_REDACT)

    def test_presigned_url_credentials_blanked_after_patch(self):
        _apply_url_query_redaction_patches()

        url = (
            "https://examplebucket.s3.us-east-1.amazonaws.com/test.txt?"
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260724%2Fus-east-1%2Fs3%2Faws4_request&"
            "X-Amz-Date=20260724T000000Z&"
            "X-Amz-Expires=3600&"
            "X-Amz-SignedHeaders=host&"
            "X-Amz-Security-Token=FwoGZXIvYXdzEExampleToken&"
            "X-Amz-Signature=abcdef1234567890"
        )

        redacted = redact_url(url)

        # Sensitive values are blanked (but keys are retained).
        self.assertNotIn("abcdef1234567890", redacted)
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", redacted)
        self.assertNotIn("FwoGZXIvYXdzEExampleToken", redacted)
        self.assertIn("X-Amz-Signature=REDACTED", redacted)
        self.assertIn("X-Amz-Credential=REDACTED", redacted)
        self.assertIn("X-Amz-Security-Token=REDACTED", redacted)

    def test_non_sensitive_sigv4_parameters_preserved_after_patch(self):
        """The parameters pre-signed URL attribution relies on must survive redaction intact."""
        _apply_url_query_redaction_patches()

        url = (
            "https://examplebucket.s3.us-east-1.amazonaws.com/test.txt?"
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260724%2Fus-east-1%2Fs3%2Faws4_request&"
            "X-Amz-Date=20260724T000000Z&"
            "X-Amz-Expires=3600&"
            "X-Amz-SignedHeaders=host&"
            "X-Amz-Signature=abcdef1234567890"
        )

        redacted = redact_url(url)

        self.assertIn("X-Amz-Algorithm=AWS4-HMAC-SHA256", redacted)
        self.assertIn("X-Amz-Date=20260724T000000Z", redacted)
        self.assertIn("X-Amz-Expires=3600", redacted)
        self.assertIn("X-Amz-SignedHeaders=host", redacted)

    @patch("amazon.opentelemetry.distro.patches._url_query_redaction_patches.logger")
    def test_missing_params_to_redact_logs_warning(self, mock_logger):
        with patch.object(otel_util_http, "PARAMS_TO_REDACT", None):
            _apply_url_query_redaction_patches()

        mock_logger.warning.assert_called_once()
        self.assertIn("PARAMS_TO_REDACT", mock_logger.warning.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
