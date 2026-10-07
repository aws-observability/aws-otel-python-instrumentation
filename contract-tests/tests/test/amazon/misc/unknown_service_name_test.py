# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List

from resource_attributes_test_base import ResourceAttributesTest, _get_k8s_attributes
from typing_extensions import override


class UnknownServiceNameTest(ResourceAttributesTest):
    @override
    # pylint: disable=no-self-use
    def get_application_otel_resource_attributes(self) -> str:
        pairlist: List[str] = []
        for key, value in _get_k8s_attributes().items():
            pairlist.append(key + "=" + value)
        return ",".join(pairlist)

    def test_service(self) -> None:
        # Since OTel Python 1.45.0 the SDK appends the interpreter's executable name (the app runs as `python`).
        self.do_test_resource_attributes("unknown_service:python")
