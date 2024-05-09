# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.sqs_url_parser import SqsUrlParser


class TestSqsUrlParser(TestCase):
    def test_sqs_client_span_basic_urls(self):
        self.validate("https://sqs.us-east-1.amazonaws.com/123412341234/Q_Name-5", "Q_Name-5")
        self.validate("https://sqs.af-south-1.amazonaws.com/999999999999/-_ThisIsValid", "-_ThisIsValid")
        self.validate("http://sqs.eu-west-3.amazonaws.com/000000000000/FirstQueue", "FirstQueue")
        self.validate("sqs.sa-east-1.amazonaws.com/123456781234/SecondQueue", "SecondQueue")

    def test_sqs_client_span_legacy_format_urls(self):
        self.validate("https://ap-northeast-2.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validate("http://cn-northwest-1.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validate("http://cn-north-1.queue.amazonaws.com/123456789012/MyQueue", "MyQueue")
        self.validate("ap-south-1.queue.amazonaws.com/123412341234/MyLongerQueueNameHere", "MyLongerQueueNameHere")
        self.validate("https://queue.amazonaws.com/123456789012/MyQueue", "MyQueue")

    def test_sqs_client_span_custom_urls(self):
        self.validate("http://127.0.0.1:1212/123456789012/MyQueue", "MyQueue")
        self.validate("https://127.0.0.1:1212/123412341234/RRR", "RRR")
        self.validate("127.0.0.1:1212/123412341234/QQ", "QQ")
        self.validate("https://amazon.com/123412341234/BB", "BB")

    def test_sqs_client_span_long_urls(self):
        queue_name = "a" * 80
        self.validate("http://127.0.0.1:1212/123456789012/" + queue_name, queue_name)

        queue_name_too_long = "a" * 81
        self.validate("http://127.0.0.1:1212/123456789012/" + queue_name_too_long, None)

    def test_client_span_sqs_invalid_or_empty_urls(self):
        self.validate(None, None)
        self.validate("", None)
        self.validate(" ", None)
        self.validate("/", None)
        self.validate("//", None)
        self.validate("///", None)
        self.validate("//asdf", None)
        self.validate("/123412341234/as?df", None)
        self.validate("invalidUrl", None)
        self.validate("https://www.amazon.com", None)
        self.validate("https://sqs.us-east-1.amazonaws.com/123412341234/.", None)
        self.validate("https://sqs.us-east-1.amazonaws.com/12/Queue", None)
        self.validate("https://sqs.us-east-1.amazonaws.com/A/A", None)
        self.validate("https://sqs.us-east-1.amazonaws.com/123412341234/A/ThisShouldNotBeHere", None)

    def validate(self, url, expected_name):
        self.assertEqual(SqsUrlParser.get_queue_name(url), expected_name)
