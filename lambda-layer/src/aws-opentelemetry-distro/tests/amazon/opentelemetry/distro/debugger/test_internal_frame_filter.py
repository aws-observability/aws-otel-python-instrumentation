# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for internal stack frame filtering in _function_wrapper.py."""

import unittest

from amazon.opentelemetry.distro.debugger._stack_utils import is_internal_frame as _is_internal_frame


class TestIsInternalFrame(unittest.TestCase):
    """Tests for _is_internal_frame helper function."""

    def test_filters_debugger_frames(self):
        self.assertTrue(_is_internal_frame("/site-packages/amazon/opentelemetry/distro/debugger/_function_wrapper.py"))

    def test_filters_distro_frames(self):
        self.assertTrue(
            _is_internal_frame("/site-packages/amazon/opentelemetry/distro/aws_opentelemetry_configurator.py")
        )

    def test_filters_otel_sdk_frames(self):
        self.assertTrue(_is_internal_frame("/site-packages/opentelemetry/sdk/trace/__init__.py"))

    def test_filters_otel_instrumentation_frames(self):
        self.assertTrue(_is_internal_frame("/site-packages/opentelemetry/instrumentation/flask/__init__.py"))

    def test_filters_otel_api_frames(self):
        self.assertTrue(_is_internal_frame("/site-packages/opentelemetry/trace/__init__.py"))

    def test_keeps_user_frames(self):
        self.assertFalse(_is_internal_frame("/app/myproject/services/order_service.py"))

    def test_keeps_stdlib_frames(self):
        self.assertFalse(_is_internal_frame("/usr/lib/python3.12/threading.py"))

    def test_keeps_third_party_frames(self):
        self.assertFalse(_is_internal_frame("/site-packages/flask/app.py"))
        self.assertFalse(_is_internal_frame("/site-packages/django/core/handlers/base.py"))

    def test_keeps_boto_frames(self):
        self.assertFalse(_is_internal_frame("/site-packages/botocore/client.py"))

    def test_handles_windows_paths(self):
        self.assertTrue(
            _is_internal_frame(
                "C:\\Python312\\Lib\\site-packages\\amazon\\opentelemetry\\distro\\debugger\\_function_wrapper.py"
            )
        )
        self.assertFalse(_is_internal_frame("C:\\Users\\dev\\myproject\\app.py"))

    def test_handles_empty_string(self):
        self.assertFalse(_is_internal_frame(""))

    def test_handles_none(self):
        self.assertFalse(_is_internal_frame(None))

    def test_keeps_user_opentelemetry_project_dirs(self):
        """User project directories containing 'opentelemetry' should not be filtered."""
        self.assertFalse(_is_internal_frame("/home/dev/opentelemetry-demo/app.py"))
        self.assertFalse(_is_internal_frame("/workspace/opentelemetry/custom_exporter.py"))


if __name__ == "__main__":
    unittest.main()
