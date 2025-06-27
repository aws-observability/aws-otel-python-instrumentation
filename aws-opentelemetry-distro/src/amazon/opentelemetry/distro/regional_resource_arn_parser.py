# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from amazon.opentelemetry.distro._utils import is_account_id


class RegionalResourceArnParser:
    @staticmethod
    def get_account_id(arn: str) -> Optional[str]:
        if _is_arn(arn):
            return str(arn).split(":")[4]
        return None

    @staticmethod
    def get_region(arn: str) -> Optional[str]:
        if _is_arn(arn):
            return str(arn).split(":")[3]
        return None

    @staticmethod
    def extract_dynamodb_table_name_from_arn(arn: str) -> Optional[str]:
        resource_name = RegionalResourceArnParser.extract_resource_name_from_arn(arn)
        if resource_name:
            return resource_name.replace("table/", "")
        return None

    @staticmethod
    def extract_kinesis_stream_name_from_arn(arn: str) -> Optional[str]:
        resource_name = RegionalResourceArnParser.extract_resource_name_from_arn(arn)
        if resource_name:
            return resource_name.replace("stream/", "")
        return None

    @staticmethod
    def extract_resource_name_from_arn(arn: str) -> Optional[str]:
        # Extracts the name of the resource from an arn
        if _is_arn(arn):
            split = arn.split(":")
            return split[-1]
        return None


def _is_arn(arn: str) -> bool:
    # Check if arn follows the format:
    # arn:partition:service:region:account-id:resource-type/resource-id or
    # arn:partition:service:region:account-id:resource-type:resource-id
    if arn is None:
        return False

    if not str(arn).startswith("arn"):
        return False

    arn_parts = str(arn).split(":")
    return len(arn_parts) >= 6 and is_account_id(arn_parts[4])
