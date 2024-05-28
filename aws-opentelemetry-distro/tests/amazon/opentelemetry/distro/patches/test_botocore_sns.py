# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import botocore.session
from moto import mock_aws

from amazon.opentelemetry.distro.patches._instrumentation_patch import apply_instrumentation_patches
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.test.test_base import TestBase


class TestSnsExtension(TestBase):
    def setUp(self):
        super().setUp()
        BotocoreInstrumentor().instrument()
        # Apply patches
        apply_instrumentation_patches()

        session = botocore.session.get_session()
        session.set_credentials(access_key="access-key", secret_key="secret-key")
        self.client = session.create_client("sns", region_name="us-west-2")
        self.topic_name = "my-topic"

    def tearDown(self):
        super().tearDown()
        BotocoreInstrumentor().uninstrument()

    @mock_aws
    def test_create_and_delete_topic(self):
        self.memory_exporter.clear()
        response = self.client.create_topic(Name=self.topic_name)
        topic_arn = response["TopicArn"]
        self.client.delete_topic(TopicArn=topic_arn)
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(2, len(spans))
        span = spans[1]
        self.assertEqual(topic_arn, span.attributes["aws.sns.topic_arn"])
