# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from mock_collector_client import ResourceScopeMetric, ResourceScopeSpan
from typing_extensions import override

from resource_attributes_test_base import ResourceAttributesTest
from amazon.utils.app_signals_constants import AWS_LOCAL_OPERATION, AWS_LOCAL_SERVICE, AWS_SPAN_KIND
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import ExponentialHistogramDataPoint, Metric
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from opentelemetry.semconv.trace import SpanAttributes
from requests import Response, request

class ServiceNameInResourceAttributesTest(ResourceAttributesTest):

    @override
    def get_application_extra_environment_variables(self) -> str:
        return {"DJANGO_SETTINGS_MODULE": "django_server.settings"}

    @override
    def get_application_otel_resource_attributes(self):
        pairlist = []
        for key, value in self._get_k8s_attributes().items():
            pairlist.append(key + "=" + value)
        return ','.join(pairlist)

    def test_service(self):
        self.do_misc_test_request("unknown_service")
