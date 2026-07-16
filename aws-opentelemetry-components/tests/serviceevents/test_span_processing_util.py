# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the span-processing helpers moved into ServiceEvents.

This closure (``get_ingress_operation`` and friends) was extracted from
``amazon.opentelemetry.distro._aws_span_processing_util`` into
``amazon.opentelemetry.serviceevents.span_processing_util`` when ServiceEvents became its own
distribution. The distro still consumes these via a re-export, so the behaviour must not drift.
"""
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.serviceevents.span_processing_util import (
    _AWS_LAMBDA_FUNCTION_NAME,
    INTERNAL_OPERATION,
    UNKNOWN_OPERATION,
    _generate_ingress_operation,
    _get_http_method,
    _is_valid_operation,
    extract_api_path_value,
    get_ingress_operation,
    is_key_present,
    is_local_root,
    should_use_internal_operation,
)
from opentelemetry.sdk.trace import Span, SpanContext
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import Attributes

_DEFAULT_PATH_VALUE: str = "/"


class TestSpanProcessingUtil(TestCase):
    def setUp(self):
        self.attributes_mock: Attributes = MagicMock()
        self.span_data_mock: Span = MagicMock()
        self.span_context_mock: SpanContext = MagicMock()
        self.span_data_mock.get_span_context.return_value = self.span_context_mock
        self.span_data_mock.attributes = self.attributes_mock
        self.attributes_mock.get = MagicMock(return_value=None)

    def test_get_ingress_operation_valid_name(self):
        self.span_data_mock.name = "ValidName"
        self.span_data_mock.kind = SpanKind.SERVER
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), "ValidName")

    def test_get_ingress_operation_with_not_server(self):
        self.span_data_mock.name = "ValidName"
        self.span_data_mock.kind = SpanKind.CLIENT
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), INTERNAL_OPERATION)

    @patch.dict(os.environ, {_AWS_LAMBDA_FUNCTION_NAME: "MyLambda"})
    def test_get_ingress_operation_in_lambda(self):
        self.span_data_mock.name = "ValidName"
        self.span_data_mock.kind = SpanKind.SERVER
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), "MyLambda/FunctionHandler")

    def test_get_ingress_operation_http_method_name_and_no_fallback(self):
        self.span_data_mock.name = "GET"
        self.span_data_mock.kind = SpanKind.SERVER

        def side_effect(key):
            return "GET" if key == SpanAttributes.HTTP_METHOD else None

        self.attributes_mock.get.side_effect = side_effect
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), UNKNOWN_OPERATION)

    def test_get_ingress_operation_invalid_name_and_valid_target_and_method(self):
        self.span_data_mock.name = None
        self.span_data_mock.kind = SpanKind.SERVER

        def side_effect(key):
            if key == SpanAttributes.HTTP_TARGET:
                return "/"
            if key == SpanAttributes.HTTP_METHOD:
                return "GET"
            return None

        self.attributes_mock.get.side_effect = side_effect
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), "GET /")

    def test_get_ingress_operation_stable_url_path_and_method(self):
        self.span_data_mock.name = None
        self.span_data_mock.kind = SpanKind.SERVER

        def side_effect(key):
            if key == SpanAttributes.URL_PATH:
                return "/wp-admin"
            if key == SpanAttributes.HTTP_REQUEST_METHOD:
                return "GET"
            return None

        self.attributes_mock.get.side_effect = side_effect
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), "GET /wp-admin")

    def test_get_ingress_operation_stable_url_full(self):
        self.span_data_mock.name = None
        self.span_data_mock.kind = SpanKind.SERVER

        def side_effect(key):
            if key == SpanAttributes.URL_FULL:
                return "https://example.com/wp-admin/login?a=b"
            if key == SpanAttributes.HTTP_REQUEST_METHOD:
                return "POST"
            return None

        self.attributes_mock.get.side_effect = side_effect
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), "POST /wp-admin")

    def test_get_ingress_operation_legacy_target_preferred_over_stable_url_path(self):
        self.span_data_mock.name = None
        self.span_data_mock.kind = SpanKind.SERVER

        def side_effect(key):
            if key == SpanAttributes.HTTP_TARGET:
                return "/legacy"
            if key == SpanAttributes.URL_PATH:
                return "/stable"
            return None

        self.attributes_mock.get.side_effect = side_effect
        self.assertEqual(get_ingress_operation(self, self.span_data_mock), "/legacy")

    def test_is_key_present(self):
        def side_effect(key):
            return "target" if key == SpanAttributes.HTTP_TARGET else None

        self.attributes_mock.get.side_effect = side_effect
        self.assertTrue(is_key_present(self.span_data_mock, SpanAttributes.HTTP_TARGET))
        self.attributes_mock.get.side_effect = None
        self.attributes_mock.get.return_value = None
        self.assertFalse(is_key_present(self.span_data_mock, "missing"))

    def test_should_use_internal_operation_false_for_server(self):
        self.span_data_mock.kind = SpanKind.SERVER
        self.assertFalse(should_use_internal_operation(self.span_data_mock))

    def test_extract_api_path_value(self):
        self.assertEqual(extract_api_path_value(""), _DEFAULT_PATH_VALUE)
        self.assertEqual(extract_api_path_value(None), _DEFAULT_PATH_VALUE)
        self.assertEqual(extract_api_path_value("/payment/1234"), "/payment")
        self.assertEqual(extract_api_path_value("noslash"), _DEFAULT_PATH_VALUE)

    def test_is_local_root(self):
        self.span_data_mock.parent = None
        self.assertTrue(is_local_root(self.span_data_mock))

        parent = MagicMock()
        parent.is_valid = True
        parent.is_remote = False
        self.span_data_mock.parent = parent
        self.assertFalse(is_local_root(self.span_data_mock))

        parent.is_remote = True
        self.assertTrue(is_local_root(self.span_data_mock))

    def test_get_http_method(self):
        def side_effect(key):
            return "PUT" if key == SpanAttributes.HTTP_REQUEST_METHOD else None

        self.attributes_mock.get.side_effect = side_effect
        self.assertEqual(_get_http_method(self.span_data_mock), "PUT")

    def test_is_valid_operation(self):
        self.assertFalse(_is_valid_operation(self.span_data_mock, None))
        self.assertFalse(_is_valid_operation(self.span_data_mock, UNKNOWN_OPERATION))
        self.attributes_mock.get.return_value = None
        self.assertTrue(_is_valid_operation(self.span_data_mock, "SomeOperation"))

    def test_generate_ingress_operation_no_path(self):
        self.attributes_mock.get.return_value = None
        self.assertEqual(_generate_ingress_operation(self.span_data_mock), UNKNOWN_OPERATION)
