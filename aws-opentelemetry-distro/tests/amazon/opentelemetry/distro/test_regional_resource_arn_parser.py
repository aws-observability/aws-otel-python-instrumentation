# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.regional_resource_arn_parser import RegionalResourceArnParser


class TestRegionalResourceArnParser(TestCase):
    def test_get_account_id(self):
        # Test invalid ARN formats
        self.validate_get_account_id(None, None)
        self.validate_get_account_id("", None)
        self.validate_get_account_id(" ", None)
        self.validate_get_account_id(":", None)
        self.validate_get_account_id("::::::", None)
        self.validate_get_account_id("not:an:arn:string", None)
        self.validate_get_account_id("arn:aws:ec2:us-west-2:123456", None)
        self.validate_get_account_id("arn:aws:ec2:us-west-2:1234567xxxxx", None)
        self.validate_get_account_id("arn:aws:ec2:us-west-2:123456789012", None)

        # Test valid ARN formats
        self.validate_get_account_id("arn:aws:dynamodb:us-west-2:123456789012:table/test_table", "123456789012")
        self.validate_get_account_id("arn:aws:acm:us-east-1:123456789012:certificate:abc-123", "123456789012")

    def test_get_region(self):
        # Test invalid ARN formats
        self.validate_get_region(None, None)
        self.validate_get_region("", None)
        self.validate_get_region(" ", None)
        self.validate_get_region(":", None)
        self.validate_get_region("::::::", None)
        self.validate_get_region("not:an:arn:string", None)
        self.validate_get_region("arn:aws:ec2:us-west-2:123456", None)
        self.validate_get_region("arn:aws:ec2:us-west-2:1234567xxxxx", None)
        self.validate_get_region("arn:aws:ec2:us-west-2:123456789012", None)

        # Test valid ARN formats
        self.validate_get_region("arn:aws:dynamodb:us-west-2:123456789012:table/test_table", "us-west-2")
        self.validate_get_region("arn:aws:acm:us-east-1:123456789012:certificate:abc-123", "us-east-1")

    def test_extract_dynamodb_table_name_from_arn(self):
        # Test invalid ARN formats
        self.validate_dynamodb_table_name(None, None)
        self.validate_dynamodb_table_name("", None)
        self.validate_dynamodb_table_name(" ", None)
        self.validate_dynamodb_table_name(":", None)
        self.validate_dynamodb_table_name("::::::", None)
        self.validate_dynamodb_table_name("not:an:arn:string", None)

        # Test valid ARN formats
        self.validate_dynamodb_table_name("arn:aws:dynamodb:us-west-2:123456789012:table/test_table", "test_table")
        self.validate_dynamodb_table_name(
            "arn:aws:dynamodb:us-west-2:123456789012:table/my-table-name", "my-table-name"
        )

    def test_extract_kinesis_stream_name_from_arn(self):
        # Test invalid ARN formats
        self.validate_kinesis_stream_name(None, None)
        self.validate_kinesis_stream_name("", None)
        self.validate_kinesis_stream_name(" ", None)
        self.validate_kinesis_stream_name(":", None)
        self.validate_kinesis_stream_name("::::::", None)
        self.validate_kinesis_stream_name("not:an:arn:string", None)

        # Test valid ARN formats
        self.validate_kinesis_stream_name("arn:aws:kinesis:us-west-2:123456789012:stream/test_stream", "test_stream")
        self.validate_kinesis_stream_name(
            "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream-name", "my-stream-name"
        )

    def test_extract_resource_name_from_arn(self):
        # Test invalid ARN formats
        self.validate_resource_name(None, None)
        self.validate_resource_name("", None)
        self.validate_resource_name(" ", None)
        self.validate_resource_name(":", None)
        self.validate_resource_name("::::::", None)
        self.validate_resource_name("not:an:arn:string", None)

        # Test valid ARN formats
        self.validate_resource_name("arn:aws:dynamodb:us-west-2:123456789012:table/test_table", "table/test_table")
        self.validate_resource_name("arn:aws:kinesis:us-west-2:123456789012:stream/test_stream", "stream/test_stream")
        self.validate_resource_name("arn:aws:s3:us-west-2:123456789012:my-bucket", "my-bucket")

    def validate_dynamodb_table_name(self, arn, expected_name):
        self.assertEqual(RegionalResourceArnParser.extract_dynamodb_table_name_from_arn(arn), expected_name)

    def validate_kinesis_stream_name(self, arn, expected_name):
        self.assertEqual(RegionalResourceArnParser.extract_kinesis_stream_name_from_arn(arn), expected_name)

    def validate_resource_name(self, arn, expected_name):
        self.assertEqual(RegionalResourceArnParser.extract_resource_name_from_arn(arn), expected_name)

    def validate_get_region(self, arn, expected_region):
        self.assertEqual(RegionalResourceArnParser.get_region(arn), expected_region)

    def validate_get_account_id(self, arn, expected_account_id):
        self.assertEqual(RegionalResourceArnParser.get_account_id(arn), expected_account_id)
