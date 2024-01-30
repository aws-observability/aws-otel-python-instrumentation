# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import DEBUG
from unittest import TestCase

from amazon.opentelemetry.distro.sampler.aws_xray_remote_sampler import AwsXRayRemoteSampler
from opentelemetry.sdk.resources import Resource


class AwsXRayRemoteSamplerTest(TestCase):
    def test_create_remote_sampler_with_empty_resource(self):
        rs = AwsXRayRemoteSampler(resource=Resource.get_empty())
        self.assertIsNotNone(rs._timer)
        self.assertEqual(rs._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__resource)

    def test_create_remote_sampler_with_populated_resource(self):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"})
        )
        self.assertIsNotNone(rs._timer)
        self.assertEqual(rs._AwsXRayRemoteSampler__polling_interval, 300)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__resource)
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["service.name"], "test-service-name")
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["cloud.platform"], "test-cloud-platform")

    def test_create_remote_sampler_with_all_fields_populated(self):
        rs = AwsXRayRemoteSampler(
            resource=Resource.create({"service.name": "test-service-name", "cloud.platform": "test-cloud-platform"}),
            endpoint="http://abc.com",
            polling_interval=120,
            log_level=DEBUG,
        )
        self.assertIsNotNone(rs._timer)
        self.assertEqual(rs._AwsXRayRemoteSampler__polling_interval, 120)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__xray_client)
        self.assertIsNotNone(rs._AwsXRayRemoteSampler__resource)
        self.assertEqual(
            rs._AwsXRayRemoteSampler__xray_client._AwsXRaySamplingClient__getSamplingRulesEndpoint,
            "http://abc.com/GetSamplingRules",
        )  # "http://127.0.0.1:2000"
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["service.name"], "test-service-name")
        self.assertEqual(rs._AwsXRayRemoteSampler__resource.attributes["cloud.platform"], "test-cloud-platform")
