# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from pkg_resources import DistributionNotFound, require


class TestAWSDistro(TestCase):
    def test_package_available(self):
        try:
            require(["aws-opentelemetry-distro"])
        except DistributionNotFound:
            self.fail("aws-opentelemetry-distro not installed")
