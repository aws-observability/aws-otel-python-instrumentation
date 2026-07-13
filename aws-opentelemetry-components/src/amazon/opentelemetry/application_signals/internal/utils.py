# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


def is_account_id(input_str: str) -> bool:
    return input_str is not None and input_str.isdigit()
