# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List

from testcontainers.mysql import MySqlContainer
from typing_extensions import override

from amazon.base.contract_test_base import NETWORK_NAME
from amazon.base.database_contract_test_base import (
    DATABASE_HOST,
    DATABASE_NAME,
    DATABASE_PASSWORD,
    DATABASE_USER,
    SPAN_KIND_LOCAL_ROOT,
    DatabaseContractTestBase,
)
from amazon.utils.application_signals_constants import (
    AWS_LOCAL_OPERATION,
    AWS_LOCAL_SERVICE,
    AWS_REMOTE_OPERATION,
    AWS_REMOTE_RESOURCE_IDENTIFIER,
    AWS_REMOTE_RESOURCE_TYPE,
    AWS_REMOTE_SERVICE,
    AWS_SPAN_KIND,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue


class MysqlClientTest(DatabaseContractTestBase):
    @override
    @classmethod
    def set_up_dependency_container(cls) -> None:
        cls.container = (
            MySqlContainer(MYSQL_USER=DATABASE_USER, MYSQL_PASSWORD=DATABASE_PASSWORD, MYSQL_DATABASE=DATABASE_NAME)
            .with_kwargs(network=NETWORK_NAME)
            .with_name(DATABASE_HOST)
        )
        cls.container.start()

    @override
    @classmethod
    def tear_down_dependency_container(cls) -> None:
        cls.container.stop()

    @override
    @staticmethod
    def get_remote_service() -> str:
        return "mysql"

    @override
    @staticmethod
    def get_database_port() -> int:
        return 3306

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return "aws-application-signals-tests-mysqlclient-app"

    def test_select_succeeds(self) -> None:
        self.assert_select_succeeds()

    def test_drop_table_succeeds(self) -> None:
        self.assert_drop_table_succeeds()

    def test_create_database_succeeds(self) -> None:
        self.assert_create_database_succeeds()

    def test_fault(self) -> None:
        self.assert_fault()

    # This adapter is not currently fully supported by OTEL
    # GitHub issue: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/1319
    # TODO: Once the adapter is supported, we could remove _assert_aws_attributes and
    # _assert_semantic_conventions_attributes methods from this class.
    @override
    def _assert_aws_attributes(
        self, attributes_list: List[KeyValue], expected_span_kind: str = SPAN_KIND_LOCAL_ROOT, **kwargs
    ) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_SERVICE, self.get_application_otel_service_name())
        # InternalOperation as OTEL does not instrument the basic server we are using, so the client span is a local
        # root.
        self._assert_str_attribute(attributes_dict, AWS_LOCAL_OPERATION, "InternalOperation")
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_SERVICE, self.get_remote_service())
        self._assert_str_attribute(attributes_dict, AWS_REMOTE_OPERATION, kwargs.get("sql_command"))
        self.assertTrue(AWS_REMOTE_RESOURCE_TYPE not in attributes_dict)
        self.assertTrue(AWS_REMOTE_RESOURCE_IDENTIFIER not in attributes_dict)
        # See comment above AWS_LOCAL_OPERATION
        self._assert_str_attribute(attributes_dict, AWS_SPAN_KIND, expected_span_kind)

    @override
    def _assert_semantic_conventions_attributes(self, attributes_list: List[KeyValue], command: str) -> None:
        attributes_dict: Dict[str, AnyValue] = self._get_attributes_dict(attributes_list)
        self.assertTrue(attributes_dict.get("db.statement").string_value.startswith(command))
        self._assert_str_attribute(attributes_dict, "db.system", self.get_remote_service())
        self._assert_str_attribute(attributes_dict, "db.name", "")
        self.assertTrue("net.peer.name" not in attributes_dict)
        self._assert_int_attribute(attributes_dict, "net.peer.port", self.get_database_port())
        self.assertTrue("server.address" not in attributes_dict)
        self.assertTrue("server.port" not in attributes_dict)
        self.assertTrue("db.operation" not in attributes_dict)
