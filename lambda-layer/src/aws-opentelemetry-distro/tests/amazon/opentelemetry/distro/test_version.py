# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.version import __version__


class TestVersion(TestCase):
    def test_version_is_not_empty_and_not_none(self):
        self.assertIsNotNone(__version__)
        self.assertNotEqual(__version__, "")
