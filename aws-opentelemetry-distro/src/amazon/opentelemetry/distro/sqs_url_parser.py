# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

_ARN_DELIMETER: str = ":"
_HTTP_SCHEMA: str = "http://"
_HTTPS_SCHEMA: str = "https://"


class SqsUrlParser:
    @staticmethod
    def get_sqs_remote_target(sqs_url: str) -> Optional[str]:
        sqs_url: str = _strip_schema_from_url(sqs_url)

        if not _is_sqs_url(sqs_url) and not _is_legacy_sqs_url(sqs_url) and not _is_custom_url(sqs_url):
            return None

        region: str = _get_region(sqs_url)
        account_id: str = _get_account_id(sqs_url)
        partition: str = _get_partition(sqs_url)
        queue_name: str = _get_queue_name(sqs_url)

        remote_target: List[Optional[str]] = []

        if all((region, account_id, partition, queue_name)):
            remote_target.append("arn")

        remote_target.extend(
            [
                _ARN_DELIMETER,
                _null_to_empty(partition),
                _ARN_DELIMETER,
                "sqs",
                _ARN_DELIMETER,
                _null_to_empty(region),
                _ARN_DELIMETER,
                _null_to_empty(account_id),
                _ARN_DELIMETER,
                queue_name,
            ]
        )

        return "".join(remote_target)


def _strip_schema_from_url(url: str) -> str:
    return url.replace(_HTTP_SCHEMA, "").replace(_HTTPS_SCHEMA, "")


def _get_region(sqs_url: str) -> Optional[str]:
    if sqs_url is None:
        return None

    if sqs_url.startswith("queue.amazonaws.com/"):
        return "us-east-1"

    if _is_sqs_url(sqs_url):
        return _get_region_from_sqs_url(sqs_url)

    if _is_legacy_sqs_url(sqs_url):
        return _get_region_from_legacy_sqs_url(sqs_url)

    return None


def _is_sqs_url(sqs_url: str) -> bool:
    split: List[Optional[str]] = sqs_url.split("/")
    return (
        len(split) == 3
        and split[0].startswith("sqs.")
        and split[0].endswith(".amazonaws.com")
        and _is_account_id(split[1])
        and _is_valid_queue_name(split[2])
    )


def _is_legacy_sqs_url(sqs_url: str) -> bool:
    split: List[Optional[str]] = sqs_url.split("/")
    return (
        len(split) == 3
        and split[0].endswith(".queue.amazonaws.com")
        and _is_account_id(split[1])
        and _is_valid_queue_name(split[2])
    )


def _is_custom_url(sqs_url: str) -> bool:
    split: List[Optional[str]] = sqs_url.split("/")
    return len(split) == 3 and _is_account_id(split[1]) and _is_valid_queue_name(split[2])


def _is_valid_queue_name(input_str: str) -> bool:
    if len(input_str) == 0 or len(input_str) > 80:
        return False

    for char in input_str:
        if char != "_" and char != "-" and not char.isalpha() and not char.isdigit():
            return False

    return True


def _is_account_id(input_str: str) -> bool:
    if len(input_str) != 12:
        return False

    try:
        int(input_str)
    except ValueError:
        return False

    return True


def _get_region_from_sqs_url(sqs_url: str) -> Optional[str]:
    split: List[Optional[str]] = sqs_url.split(".")
    return split[1] if len(split) >= 2 else None


def _get_region_from_legacy_sqs_url(sqs_url: str) -> Optional[str]:
    split: List[Optional[str]] = sqs_url.split(".")
    return split[0]


def _get_account_id(sqs_url: str) -> Optional[str]:
    if sqs_url is None:
        return None

    split: List[Optional[str]] = sqs_url.split("/")
    return split[1] if len(split) >= 2 else None


def _get_partition(sqs_url: str) -> Optional[str]:
    region: Optional[str] = _get_region(sqs_url)

    if region is None:
        return None

    if region.startswith("us-gov-"):
        return "aws-us-gov"

    if region.startswith("cn-"):
        return "aws-cn"

    return "aws"


def _get_queue_name(sqs_url: str) -> Optional[str]:
    split: List[Optional[str]] = sqs_url.split("/")
    return split[2] if len(split) >= 3 else None


def _null_to_empty(input_str: str) -> str:
    return input_str if input_str is not None else ""
