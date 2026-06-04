# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Variable serialization utilities for debugger.

DEPRECATED: This module is superseded by _snapshot_serializer.py.
Kept for backward compatibility with existing tests.
"""

import logging
from typing import Any, Dict, List, Optional

from amazon.opentelemetry.distro.debugger._data_models import DEFAULT_MAX_STRING_LENGTH, CaptureConfig
from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer

logger = logging.getLogger(__name__)


class VariableSerializer:
    """Shared utility for safely serializing variables with limits and filtering.

    DEPRECATED: Use SnapshotSerializer instead for CapturedValue output.
    """

    @staticmethod
    def safe_json_serialize(
        obj: Any,
        max_length: int = DEFAULT_MAX_STRING_LENGTH,
        max_collection_depth: int = 3,
        max_width: int = 10,
        max_fields: int = 10,
        max_object_depth: int = 3,
    ) -> str:
        """
        Safely serialize object with depth and width limits.

        Returns a string representation (legacy format).
        """
        serializer = SnapshotSerializer(
            max_depth=max_object_depth,
            max_collection_size=max_width,
            max_string_length=max_length,
            max_fields=max_fields,
        )
        cv = serializer.serialize(obj)
        # Return a simple string representation for backward compat
        return str(cv.value) if cv.value is not None else str(cv.to_dict())

    @staticmethod
    def capture_variables_as_attributes(
        variables: Dict[str, Any],
        attribute_prefix: str,
        capture_config: CaptureConfig,
        variable_filter: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        Capture variables as attributes with limits and filtering.

        DEPRECATED: Use SnapshotSerializer.serialize_variables() instead.
        """
        attributes = {}
        captured_count = 0
        max_attrs = capture_config.max_fields_per_object

        if variable_filter:
            filtered_vars = {k: v for k, v in variables.items() if k in variable_filter}
        else:
            filtered_vars = variables

        serializer = SnapshotSerializer(
            max_depth=capture_config.max_object_depth,
            max_collection_size=capture_config.max_collection_width,
            max_string_length=capture_config.max_string_length,
            max_fields=capture_config.max_fields_per_object,
        )

        for var_name, var_value in filtered_vars.items():
            if captured_count >= max_attrs:
                break
            try:
                cv = serializer.serialize(var_value)
                attr_name = f"{attribute_prefix}{var_name}"
                attributes[attr_name] = str(cv.value) if cv.value is not None else str(cv.to_dict())
                captured_count += 1
            except Exception as exception:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to serialize variable %s: %s", var_name, exception)
                attr_name = f"{attribute_prefix}{var_name}"
                attributes[attr_name] = f"<serialization error: {type(exception).__name__}>"
                captured_count += 1

        return attributes
