# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from resource_attributes_test_base import ResourceAttributesTest, _get_k8s_attributes
from typing_extensions import override


class ServiceNameInResourceAttributesTest(ResourceAttributesTest):

    @override
    # pylint: disable=no-self-use
    def get_application_extra_environment_variables(self) -> str:
        return {"DJANGO_SETTINGS_MODULE": "django_server.settings", "OTEL_SERVICE_NAME": "service-name-test"}

    @override
    # pylint: disable=no-self-use
    def get_application_otel_resource_attributes(self):
        pairlist = []
        for key, value in _get_k8s_attributes().items():
            pairlist.append(key + "=" + value)
        return ",".join(pairlist)

    def test_service(self):
        self.do_misc_test_request("service-name-test")