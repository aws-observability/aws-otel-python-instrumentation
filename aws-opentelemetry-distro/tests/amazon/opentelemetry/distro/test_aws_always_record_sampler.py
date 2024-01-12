# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from opentelemetry.sdk.trace.sampling import Sampler


class TestAlwaysRecordSampler(TestCase):
    def test_basic(self):
        sampler: Sampler = AlwaysRecordSampler(None)
        with self.assertRaises(ValueError):
            sampler.get_description()
