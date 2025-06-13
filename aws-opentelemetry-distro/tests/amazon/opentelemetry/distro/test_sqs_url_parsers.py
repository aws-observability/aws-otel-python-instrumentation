# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.sqs_url_parser import SqsUrlParser


class TestSqsUrlParser(TestCase):
    def test_sqs_client_span_basic_urls(self):
        self.validateGetQueueName("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Name-5", "Q_Name-5")
        self.validateGetQueueName("https://sqs.af-south-1.amazonaws.com/999999999999/-_ThisIsValid", "-_ThisIsValid")
        self.validateGetQueueName("http://sqs.eu-west-3.amazonaws.com/000000000000/FirstQueue", "FirstQueue")
        self.validateGetQueueName("sqs.sa-east-1.amazonaws.com/123456781234/SecondQueue", "SecondQueue")

    def test_sqs_client_span_legacy_format_urls(self):
        self.validateGetQueueName("https://ap-northeast-2.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validateGetQueueName("http://cn-northwest-1.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validateGetQueueName("http://cn-north-1.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validateGetQueueName(
            "ap-south-1.queue.amazonaws.com/123412341234/MyLongerQueueNameHere", "MyLongerQueueNameHere"
        )
        self.validateGetQueueName("https://queue.amazonaws.com/123456789012/MyQueue", "MyQueue")

    def test_sqs_client_span_custom_urls(self):
        self.validateGetQueueName("http://127.0.0.1:1212/123456789012/MyQueue", "MyQueue")
        self.validateGetQueueName("https://127.0.0.1:1212/123412341234/RRR", "RRR")
        self.validateGetQueueName("127.0.0.1:1212/123412341234/QQ", "QQ")
        self.validateGetQueueName("https://amazon.com/123412341234/BB", "BB")

    def test_sqs_client_span_long_urls(self):
        queue_name = "a" * 80
        self.validateGetQueueName("http://127.0.0.1:1212/123456789012/" + queue_name, queue_name)

        queue_name_too_long = "a" * 81
        self.validateGetQueueName("http://127.0.0.1:1212/123456789012/" + queue_name_too_long, None)

    def test_client_span_sqs_invalid_or_empty_urls(self):
        self.validateGetQueueName(None, None)
        self.validateGetQueueName("", None)
        self.validateGetQueueName(" ", None)
        self.validateGetQueueName("/", None)
        self.validateGetQueueName("//", None)
        self.validateGetQueueName("///", None)
        self.validateGetQueueName("//asdf", None)
        self.validateGetQueueName("/123412341234/as?df", None)
        self.validateGetQueueName("invalidUrl", None)
        self.validateGetQueueName("https://www.amazon.com", None)
        self.validateGetQueueName("https://sqs.us-east-1.amazonaws.com/123412341234/.", None)
        self.validateGetQueueName("https://sqs.us-east-1.amazonaws.com/12/Queue", None)
        self.validateGetQueueName("https://sqs.us-east-1.amazonaws.com/A/A", None)
        self.validateGetQueueName("https://sqs.us-east-1.amazonaws.com/123412341234/A/ThisShouldNotBeHere", None)

    def test_get_account_id_from_sqs_url(self):
        self.validateGetAccountId(None, None)
        self.validateGetAccountId("", None)
        self.validateGetAccountId(" ", None)
        self.validateGetAccountId("/", None)
        self.validateGetAccountId("//", None)
        self.validateGetAccountId("///", None)
        self.validateGetAccountId("//asdf", None)
        self.validateGetAccountId("/123412341234/as?df", None)
        self.validateGetAccountId("invalidUrl", None)
        self.validateGetAccountId("https://www.amazon.com", None)
        self.validateGetAccountId("https://sqs.us-east-1.amazonaws.com/12341234/Queue", None)
        self.validateGetAccountId("https://sqs.us-east-1.amazonaws.com/1234123412xx/Queue", None)
        self.validateGetAccountId("https://sqs.us-east-1.amazonaws.com/1234123412xx", None)
        self.validateGetAccountId("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Namez-5", "123412341234")

    def test_get_region_from_sqs_url(self):
        self.validateGetRegion(None, None)
        self.validateGetRegion("", None)
        self.validateGetRegion(" ", None)
        self.validateGetRegion("/", None)
        self.validateGetRegion("//", None)
        self.validateGetRegion("///", None)
        self.validateGetRegion("//asdf", None)
        self.validateGetRegion("/123412341234/as?df", None)
        self.validateGetRegion("invalidUrl", None)
        self.validateGetRegion("https://www.amazon.com", None)
        self.validateGetRegion("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Namez-5", "us-east-1")

    def validateGetRegion(self, url, expected_region):
        self.assertEqual(SqsUrlParser.get_region(url), expected_region)

    def validateGetAccountId(self, url, expected_account_id):
        self.assertEqual(SqsUrlParser.get_account_id(url), expected_account_id)

    def validateGetQueueName(self, url, expected_name):
        self.assertEqual(SqsUrlParser.get_queue_name(url), expected_name)
