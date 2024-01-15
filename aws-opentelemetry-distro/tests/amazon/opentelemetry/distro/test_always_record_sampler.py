# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.always_record_sampler import AlwaysRecordSampler
from opentelemetry.sdk.trace.sampling import Decision, Sampler, StaticSampler


class TestAlwaysRecordSampler(TestCase):
    def test_basic(self):
        root_sampler: Sampler = StaticSampler(Decision.DROP)
        sampler: Sampler = AlwaysRecordSampler(root_sampler)
        self.assertIn("AlwaysRecordSampler", sampler.get_description())
