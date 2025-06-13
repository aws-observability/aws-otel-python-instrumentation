from unittest import TestCase

from amazon.opentelemetry.distro.regional_resource_arn_parser import RegionalResourceArnParser


class TestRegionalResourceArnParser(TestCase):
    def test_get_account_id(self):
        # Test invalid ARN formats
        self.validateGetAccountId(None, None)
        self.validateGetAccountId("", None)
        self.validateGetAccountId(" ", None)
        self.validateGetAccountId(":", None)
        self.validateGetAccountId("::::::", None)
        self.validateGetAccountId("not:an:arn:string", None)
        self.validateGetAccountId("arn:aws:ec2:us-west-2:123456", None)
        self.validateGetAccountId("arn:aws:ec2:us-west-2:1234567xxxxx", None)
        self.validateGetAccountId("arn:aws:ec2:us-west-2:123456789012", None)

        # Test valid ARN formats
        self.validateGetAccountId("arn:aws:dynamodb:us-west-2:123456789012:table/test_table", "123456789012")
        self.validateGetAccountId("arn:aws:acm:us-east-1:123456789012:certificate:abc-123", "123456789012")

    def test_get_region(self):
        # Test invalid ARN formats
        self.validateGetRegion(None, None)
        self.validateGetRegion("", None)
        self.validateGetRegion(" ", None)
        self.validateGetRegion(":", None)
        self.validateGetRegion("::::::", None)
        self.validateGetRegion("not:an:arn:string", None)
        self.validateGetRegion("arn:aws:ec2:us-west-2:123456", None)
        self.validateGetRegion("arn:aws:ec2:us-west-2:1234567xxxxx", None)
        self.validateGetRegion("arn:aws:ec2:us-west-2:123456789012", None)

        # Test valid ARN formats
        self.validateGetRegion("arn:aws:dynamodb:us-west-2:123456789012:table/test_table", "us-west-2")
        self.validateGetRegion("arn:aws:acm:us-east-1:123456789012:certificate:abc-123", "us-east-1")

    def validateGetRegion(self, arn, expected_region):
        self.assertEqual(RegionalResourceArnParser.get_region(arn), expected_region)

    def validateGetAccountId(self, arn, expected_account_id):
        self.assertEqual(RegionalResourceArnParser.get_account_id(arn), expected_account_id)
