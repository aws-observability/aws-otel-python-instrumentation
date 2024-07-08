# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

import gevent.monkey
import pkg_resources

from amazon.opentelemetry.distro.patches._instrumentation_patch import (
    AWS_GEVENT_PATCH_MODULES,
    apply_instrumentation_patches,
)
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.semconv.trace import SpanAttributes

_STREAM_NAME: str = "streamName"
_BUCKET_NAME: str = "bucketName"
_QUEUE_NAME: str = "queueName"
_QUEUE_URL: str = "queueUrl"

# Patch names
GET_DISTRIBUTION_PATCH: str = (
    "amazon.opentelemetry.distro.patches._instrumentation_patch.pkg_resources.get_distribution"
)


class TestInstrumentationPatch(TestCase):
    """
    This test class has exactly one test, test_instrumentation_patch. This is an anti-pattern, but the scenario is
    fairly unusual and we feel justifies the code smell. Essentially the _instrumentation_patch module monkey-patches
    upstream components, so once it's run, it's challenging to "undo" between tests. To work around this, we have a
    monolith test framework that tests two major categories of test scenarios:
    1. Patch behaviour
    2. Patch mechanism

    Patch behaviour tests validate upstream behaviour without patches, apply patches, and validate patched behaviour.
    Patch mechanism tests validate the logic that is used to actually apply patches, and can be run regardless of the
    pre- or post-patch behaviour.
    """

    method_patches: Dict[str, patch] = {}
    mock_metric_exporter_init: patch

    def test_instrumentation_patch(self):
        # Set up method patches used by all tests
        self.method_patches[GET_DISTRIBUTION_PATCH] = patch(GET_DISTRIBUTION_PATCH).start()

        # Run tests that validate patch behaviour before and after patching
        self._run_patch_behaviour_tests()
        # Run tests not specifically related to patch behaviour
        self._run_patch_mechanism_tests()

        # Clean up method patches
        for method_patch in self.method_patches.values():
            method_patch.stop()

    def _run_patch_behaviour_tests(self):
        # Test setup
        self.method_patches[GET_DISTRIBUTION_PATCH].return_value = "CorrectDistributionObject"
        # Test setup to not patch gevent
        os.environ[AWS_GEVENT_PATCH_MODULES] = "none"

        # Validate unpatched upstream behaviour - important to detect upstream changes that may break instrumentation
        self._test_unpatched_botocore_instrumentation()
        self._test_unpatched_gevent_instrumentation()

        # Apply patches
        apply_instrumentation_patches()

        # Validate patched upstream behaviour - important to detect downstream changes that may break instrumentation
        self._test_patched_botocore_instrumentation()
        self._test_unpatched_gevent_instrumentation()

        # Test setup to check whether only these two modules get patched by gevent monkey
        os.environ[AWS_GEVENT_PATCH_MODULES] = "os, ssl"

        # Apply patches
        apply_instrumentation_patches()

        # Validate that os and ssl gevent monkey patch modules were patched
        self._test_patched_gevent_os_ssl_instrumentation()

        # Set the value to 'all' so that all the remaining gevent monkey patch modules are patched
        os.environ[AWS_GEVENT_PATCH_MODULES] = "all"

        # Apply patches again.
        apply_instrumentation_patches()

        # Validate that remaining gevent monkey patch modules were patched
        self._test_patched_gevent_instrumentation()

        # Test teardown
        self._reset_mocks()

    def _run_patch_mechanism_tests(self):
        """
        Each test should be invoked, resetting mocks in between each test. E.g.:
            self.test_x()
            self.reset_mocks()
            self.test_y()
            self.reset_mocks()
            etc.
        """
        self._test_botocore_installed_flag()
        self._reset_mocks()

    def _test_unpatched_botocore_instrumentation(self):
        # Kinesis
        self.assertFalse("kinesis" in _KNOWN_EXTENSIONS, "Upstream has added a Kinesis extension")

        # S3
        self.assertFalse("s3" in _KNOWN_EXTENSIONS, "Upstream has added a S3 extension")

        # SQS
        self.assertTrue("sqs" in _KNOWN_EXTENSIONS, "Upstream has removed the SQS extension")
        attributes: Dict[str, str] = _do_extract_sqs_attributes()
        self.assertTrue("aws.queue_url" in attributes)
        self.assertFalse("aws.sqs.queue.url" in attributes)
        self.assertFalse("aws.sqs.queue.name" in attributes)

    def _test_unpatched_gevent_instrumentation(self):
        self.assertFalse(gevent.monkey.is_module_patched("os"), "gevent os module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("thread"), "gevent thread module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("time"), "gevent time module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("sys"), "gevent sys module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("socket"), "gevent socket module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("select"), "gevent select module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("ssl"), "gevent ssl module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("subprocess"), "gevent subprocess module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("builtins"), "gevent builtins module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("signal"), "gevent signal module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("queue"), "gevent queue module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("contextvars"), "gevent contextvars module has been patched")

    def _test_patched_botocore_instrumentation(self):
        # Kinesis
        self.assertTrue("kinesis" in _KNOWN_EXTENSIONS)
        kinesis_attributes: Dict[str, str] = _do_extract_kinesis_attributes()
        self.assertTrue("aws.kinesis.stream.name" in kinesis_attributes)
        self.assertEqual(kinesis_attributes["aws.kinesis.stream.name"], _STREAM_NAME)

        # S3
        self.assertTrue("s3" in _KNOWN_EXTENSIONS)
        s3_attributes: Dict[str, str] = _do_extract_s3_attributes()
        self.assertTrue(SpanAttributes.AWS_S3_BUCKET in s3_attributes)
        self.assertEqual(s3_attributes[SpanAttributes.AWS_S3_BUCKET], _BUCKET_NAME)

        # SQS
        self.assertTrue("sqs" in _KNOWN_EXTENSIONS)
        sqs_attributes: Dict[str, str] = _do_extract_sqs_attributes()
        self.assertTrue("aws.queue_url" in sqs_attributes)
        self.assertTrue("aws.sqs.queue.url" in sqs_attributes)
        self.assertEqual(sqs_attributes["aws.sqs.queue.url"], _QUEUE_URL)
        self.assertTrue("aws.sqs.queue.name" in sqs_attributes)
        self.assertEqual(sqs_attributes["aws.sqs.queue.name"], _QUEUE_NAME)

    def _test_patched_gevent_os_ssl_instrumentation(self):
        # Only ssl and os module should have been patched since the environment variable was set to 'os, ssl'
        self.assertTrue(gevent.monkey.is_module_patched("ssl"), "gevent ssl module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("os"), "gevent os module has not been patched")
        # Rest should still be unpatched
        self.assertFalse(gevent.monkey.is_module_patched("thread"), "gevent thread module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("time"), "gevent time module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("sys"), "gevent sys module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("socket"), "gevent socket module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("select"), "gevent select module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("subprocess"), "gevent subprocess module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("builtins"), "gevent builtins module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("signal"), "gevent signal module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("queue"), "gevent queue module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("contextvars"), "gevent contextvars module has been patched")

    def _test_patched_gevent_instrumentation(self):
        self.assertTrue(gevent.monkey.is_module_patched("os"), "gevent os module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("time"), "gevent time module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("socket"), "gevent socket module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("select"), "gevent select module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("ssl"), "gevent ssl module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("subprocess"), "gevent subprocess module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("signal"), "gevent signal module has not been patched")
        self.assertTrue(gevent.monkey.is_module_patched("queue"), "gevent queue module has not been patched")

        # Current version of gevent.monkey.patch_all() does not do anything to these modules despite being called
        self.assertFalse(gevent.monkey.is_module_patched("thread"), "gevent thread module has been patched")
        self.assertFalse(gevent.monkey.is_module_patched("sys"), "gevent sys module has  been patched")
        self.assertFalse(gevent.monkey.is_module_patched("builtins"), "gevent builtins module not been patched")
        self.assertFalse(gevent.monkey.is_module_patched("contextvars"), "gevent contextvars module has been patched")

    def _test_botocore_installed_flag(self):
        with patch(
            "amazon.opentelemetry.distro.patches._botocore_patches._apply_botocore_instrumentation_patches"
        ) as mock_apply_patches:
            get_distribution_patch: patch = self.method_patches[GET_DISTRIBUTION_PATCH]
            get_distribution_patch.side_effect = pkg_resources.DistributionNotFound
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            get_distribution_patch.side_effect = pkg_resources.VersionConflict("botocore==1.0.0", "botocore==0.0.1")
            apply_instrumentation_patches()
            mock_apply_patches.assert_not_called()

            get_distribution_patch.side_effect = None
            get_distribution_patch.return_value = "CorrectDistributionObject"
            apply_instrumentation_patches()
            mock_apply_patches.assert_called()

    def _reset_mocks(self):
        for method_patch in self.method_patches.values():
            method_patch.reset_mock()


def _do_extract_kinesis_attributes() -> Dict[str, str]:
    service_name: str = "kinesis"
    params: Dict[str, str] = {"StreamName": _STREAM_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_s3_attributes() -> Dict[str, str]:
    service_name: str = "s3"
    params: Dict[str, str] = {"Bucket": _BUCKET_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_sqs_attributes() -> Dict[str, str]:
    service_name: str = "sqs"
    params: Dict[str, str] = {"QueueUrl": _QUEUE_URL, "QueueName": _QUEUE_NAME}
    return _do_extract_attributes(service_name, params)


def _do_extract_attributes(service_name: str, params: Dict[str, str]) -> Dict[str, str]:
    mock_call_context: MagicMock = MagicMock()
    mock_call_context.params = params
    attributes: Dict[str, str] = {}
    sqs_extension = _KNOWN_EXTENSIONS[service_name]()(mock_call_context)
    sqs_extension.extract_attributes(attributes)
    return attributes
