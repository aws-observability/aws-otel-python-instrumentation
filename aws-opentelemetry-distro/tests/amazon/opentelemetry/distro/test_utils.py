# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from unittest import TestCase
from unittest.mock import patch, MagicMock

from amazon.opentelemetry.distro._utils import get_aws_region, is_agent_observability_enabled


class TestUtils(TestCase):
    def setUp(self):
        # Store original environment
        self.original_env = os.environ.copy()
    
    def tearDown(self):
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)
    
    def test_is_agent_observability_enabled_true(self):
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"
        self.assertTrue(is_agent_observability_enabled())
        
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "True"
        self.assertTrue(is_agent_observability_enabled())
        
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "TRUE"
        self.assertTrue(is_agent_observability_enabled())
    
    def test_is_agent_observability_enabled_false(self):
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "false"
        self.assertFalse(is_agent_observability_enabled())
        
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "False"
        self.assertFalse(is_agent_observability_enabled())
        
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "anything_else"
        self.assertFalse(is_agent_observability_enabled())
        
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)
        self.assertFalse(is_agent_observability_enabled())
    
    def test_get_aws_region_from_aws_region_env(self):
        os.environ["AWS_REGION"] = "us-west-2"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"  # Should be ignored
        self.assertEqual(get_aws_region(), "us-west-2")
    
    def test_get_aws_region_from_aws_default_region_env(self):
        os.environ.pop("AWS_REGION", None)
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        self.assertEqual(get_aws_region(), "eu-west-1")
    
    def test_get_aws_region_from_boto3(self):
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("AWS_DEFAULT_REGION", None)
        
        # Create a mock boto3 module
        mock_boto3 = MagicMock()
        mock_session = MagicMock()
        mock_session.region_name = "ap-southeast-1"
        mock_boto3.Session.return_value = mock_session
        
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            self.assertEqual(get_aws_region(), "ap-southeast-1")
    
    def test_get_aws_region_boto3_no_region(self):
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("AWS_DEFAULT_REGION", None)
        
        # Create a mock boto3 module
        mock_boto3 = MagicMock()
        mock_session = MagicMock()
        mock_session.region_name = None
        mock_boto3.Session.return_value = mock_session
        
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            with self.assertLogs(level="WARNING") as cm:
                region = get_aws_region()
            
            self.assertEqual(region, "us-east-1")
            self.assertIn("AWS region not found", cm.output[0])
    
    def test_get_aws_region_boto3_import_error(self):
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("AWS_DEFAULT_REGION", None)
        
        # Mock the import to raise ImportError by removing boto3 from sys.modules
        with patch.dict("sys.modules", {"boto3": None}):
            with self.assertLogs(level="WARNING") as cm:
                region = get_aws_region()
            
            self.assertEqual(region, "us-east-1")
            self.assertIn("AWS region not found", cm.output[0])
    
    def test_get_aws_region_boto3_exception(self):
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("AWS_DEFAULT_REGION", None)
        
        # Create a mock boto3 module that raises exception
        mock_boto3 = MagicMock()
        mock_boto3.Session.side_effect = Exception("Some boto3 error")
        
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            with self.assertLogs(level="WARNING") as cm:
                region = get_aws_region()
            
            self.assertEqual(region, "us-east-1")
            self.assertIn("AWS region not found", cm.output[0])