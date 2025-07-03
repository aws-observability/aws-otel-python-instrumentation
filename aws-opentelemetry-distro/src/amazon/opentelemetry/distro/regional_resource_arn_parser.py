# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from amazon.opentelemetry.distro._utils import is_account_id


class RegionalResourceArnParser:
    @staticmethod
    def get_account_id(arn: str) -> Optional[str]:
        parts = _get_arn_parts(arn)
        return parts[4] if parts else None

    @staticmethod
    def get_region(arn: str) -> Optional[str]:
        parts = _get_arn_parts(arn)
        return parts[3] if parts else None

    @staticmethod
    def extract_dynamodb_table_name_from_arn(arn: str) -> Optional[str]:
        parts = _get_arn_parts(arn)
        return parts[-1].replace("table/", "") if parts else None

    @staticmethod
    def extract_kinesis_stream_name_from_arn(arn: str) -> Optional[str]:
        parts = _get_arn_parts(arn)
        return parts[-1].replace("stream/", "") if parts else None

    @staticmethod
    def extract_resource_name_from_arn(arn: str) -> Optional[str]:
        parts = _get_arn_parts(arn)
        return parts[-1] if parts else None


def _get_arn_parts(arn: str) -> Optional[list]:
    if not arn or not arn.startswith("arn"):
        return None
    parts = arn.split(":")
    return parts if len(parts) >= 6 and is_account_id(parts[4]) else None
