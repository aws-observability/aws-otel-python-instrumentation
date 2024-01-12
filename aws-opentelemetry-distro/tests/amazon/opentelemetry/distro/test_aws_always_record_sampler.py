# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.aws_always_record_sampler import AwsAlwaysRecordSampler
from opentelemetry.sdk.trace.sampling import Sampler


class TestAwsAlwaysRecordSampler(TestCase):
    def test_basic(self):
        sampler: Sampler = AwsAlwaysRecordSampler()
        self.assertIn("AwsAlwaysRecordSampler", sampler.get_description())
