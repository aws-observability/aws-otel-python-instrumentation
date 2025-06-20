# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from importlib.metadata import PackageNotFoundError, version
from unittest import TestCase


class TestAwsOpenTelemetryDistro(TestCase):
    def test_package_available(self):
        try:
            version("aws-opentelemetry-distro")
        except PackageNotFoundError:
            self.fail("aws-opentelemetry-distro not installed")
