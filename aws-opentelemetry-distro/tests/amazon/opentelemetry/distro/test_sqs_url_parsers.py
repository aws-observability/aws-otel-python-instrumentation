# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.sqs_url_parser import SqsUrlParser


class TestSqsUrlParser(TestCase):
    def test_sqs_client_span_basic_urls(self):
        self.validate_get_queue_name("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Name-5", "Q_Name-5")
        self.validate_get_queue_name("https://sqs.af-south-1.amazonaws.com/999999999999/-_ThisIsValid", "-_ThisIsValid")
        self.validate_get_queue_name("http://sqs.eu-west-3.amazonaws.com/000000000000/FirstQueue", "FirstQueue")
        self.validate_get_queue_name("sqs.sa-east-1.amazonaws.com/123456781234/SecondQueue", "SecondQueue")

    def test_sqs_client_span_legacy_format_urls(self):
        self.validate_get_queue_name("https://ap-northeast-2.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validate_get_queue_name("http://cn-northwest-1.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validate_get_queue_name("http://cn-north-1.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validate_get_queue_name(
            "ap-south-1.queue.amazonaws.com/123412341234/MyLongerQueueNameHere", "MyLongerQueueNameHere"
        )
        self.validate_get_queue_name("https://queue.amazonaws.com/123456789012/MyQueue", "MyQueue")

    def test_sqs_client_span_custom_urls(self):
        self.validate_get_queue_name("http://127.0.0.1:1212/123456789012/MyQueue", "MyQueue")
        self.validate_get_queue_name("https://127.0.0.1:1212/123412341234/RRR", "RRR")
        self.validate_get_queue_name("127.0.0.1:1212/123412341234/QQ", "QQ")
        self.validate_get_queue_name("https://amazon.com/123412341234/BB", "BB")

    def test_sqs_client_span_long_urls(self):
        queue_name = "a" * 80
        self.validate_get_queue_name("http://127.0.0.1:1212/123456789012/" + queue_name, queue_name)

        queue_name_too_long = "a" * 81
        self.validate_get_queue_name("http://127.0.0.1:1212/123456789012/" + queue_name_too_long, None)

    def test_client_span_sqs_invalid_or_empty_urls(self):
        self.validate_get_queue_name(None, None)
        self.validate_get_queue_name("", None)
        self.validate_get_queue_name(" ", None)
        self.validate_get_queue_name("/", None)
        self.validate_get_queue_name("//", None)
        self.validate_get_queue_name("///", None)
        self.validate_get_queue_name("//asdf", None)
        self.validate_get_queue_name("/123412341234/as?df", None)
        self.validate_get_queue_name("invalidUrl", None)
        self.validate_get_queue_name("https://www.amazon.com", None)
        self.validate_get_queue_name("https://sqs.us-east-1.amazonaws.com/123412341234/.", None)
        self.validate_get_queue_name("https://sqs.us-east-1.amazonaws.com/1234123412xx/Queue", None)
        self.validate_get_queue_name("https://sqs.us-east-1.amazonaws.com/A/A", None)
        self.validate_get_queue_name("https://sqs.us-east-1.amazonaws.com/123412341234/A/ThisShouldNotBeHere", None)

    def test_get_account_id_from_sqs_url(self):
        self.validate_get_account_id(None, None)
        self.validate_get_account_id("", None)
        self.validate_get_account_id(" ", None)
        self.validate_get_account_id("/", None)
        self.validate_get_account_id("//", None)
        self.validate_get_account_id("///", None)
        self.validate_get_account_id("//asdf", None)
        self.validate_get_account_id("/123412341234/as?df", None)
        self.validate_get_account_id("invalidUrl", None)
        self.validate_get_account_id("https://www.amazon.com", None)
        self.validate_get_account_id("https://sqs.us-east-1.amazonaws.com/12341234/Queue", "12341234")
        self.validate_get_account_id("https://sqs.us-east-1.amazonaws.com/1234123412xx/Queue", None)
        self.validate_get_account_id("https://sqs.us-east-1.amazonaws.com/1234123412xx", None)
        self.validate_get_account_id("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Namez-5", "123412341234")

    def test_get_region_from_sqs_url(self):
        self.validate_get_region(None, None)
        self.validate_get_region("", None)
        self.validate_get_region(" ", None)
        self.validate_get_region("/", None)
        self.validate_get_region("//", None)
        self.validate_get_region("///", None)
        self.validate_get_region("//asdf", None)
        self.validate_get_region("/123412341234/as?df", None)
        self.validate_get_region("invalidUrl", None)
        self.validate_get_region("https://www.amazon.com", None)
        self.validate_get_region("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Namez-5", "us-east-1")

    def validate_get_region(self, url, expected_region):
        self.assertEqual(SqsUrlParser.get_region(url), expected_region)

    def validate_get_account_id(self, url, expected_account_id):
        self.assertEqual(SqsUrlParser.get_account_id(url), expected_account_id)

    def validate_get_queue_name(self, url, expected_name):
        self.assertEqual(SqsUrlParser.get_queue_name(url), expected_name)
