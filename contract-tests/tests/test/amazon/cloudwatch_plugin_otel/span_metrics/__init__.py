# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class InstrumentationMode(str, Enum):
    AUTO = "auto"
    MANUAL = "manual"
    MANUAL_GLOBAL = "manual-global-providers"

    __str__ = str.__str__
