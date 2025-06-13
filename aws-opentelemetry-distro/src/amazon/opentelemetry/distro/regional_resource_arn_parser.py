# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional


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


def _is_arn(arn: str) -> bool:
    # Check if arn follows the format:
    # arn:partition:service:region:account-id:resource-type/resource-id or
    # arn:partition:service:region:account-id:resource-type:resource-id
    if arn is None:
        return False

    if not str(arn).startswith("arn"):
        return False

    arn_parts = str(arn).split(":")
    return len(arn_parts) >= 6 and _is_account_id(arn_parts[4])


def _is_account_id(input: str) -> bool:
    if input is None or len(input) != 12:
        return False

    if not _check_digits(input):
        return False

    return True


def _check_digits(string: str) -> bool:
    return string.isdigit()
