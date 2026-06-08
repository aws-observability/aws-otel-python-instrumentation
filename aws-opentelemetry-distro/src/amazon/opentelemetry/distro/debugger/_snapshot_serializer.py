# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Variable serializer for Snapshot CapturedValue format.

Replaces the flat span-attribute serialization with the recursive
CapturedValue tree structure defined in the Snapshot v1 spec.
"""

import itertools
import logging
import time
from typing import Any, Dict, Set

from amazon.opentelemetry.distro.debugger._snapshot_models import CapturedValue

logger = logging.getLogger(__name__)

# Default capture limits
DEFAULT_MAX_DEPTH = 3
DEFAULT_MAX_COLLECTION_SIZE = 20
DEFAULT_MAX_STRING_LENGTH = 255
DEFAULT_MAX_FIELDS = 20
DEFAULT_SERIALIZATION_TIMEOUT_MS = 200


class SnapshotSerializer:
    """
    Serializes Python values into CapturedValue trees.

    Handles depth limits, collection size limits, string truncation,
    circular reference detection, and timeout.
    """

    def __init__(
        self,
        max_depth: int = DEFAULT_MAX_DEPTH,
        max_collection_size: int = DEFAULT_MAX_COLLECTION_SIZE,
        max_string_length: int = DEFAULT_MAX_STRING_LENGTH,
        max_fields: int = DEFAULT_MAX_FIELDS,
        timeout_ms: int = DEFAULT_SERIALIZATION_TIMEOUT_MS,
    ):
        self.max_depth = max_depth
        self.max_collection_size = max_collection_size
        self.max_string_length = max_string_length
        self.max_fields = max_fields
        self.timeout_ms = timeout_ms

    def serialize(self, value: Any) -> CapturedValue:
        """
        Serialize a Python value into a CapturedValue.

        Args:
            value: The Python value to serialize.

        Returns:
            CapturedValue representing the serialized value.
        """
        deadline = time.monotonic() + (self.timeout_ms / 1000.0)
        seen: Set[int] = set()
        try:
            return self._serialize_value(value, 0, deadline, seen)
        except Exception:  # pylint: disable=broad-exception-caught
            type_name = type(value).__name__
            return CapturedValue(type=type_name, not_captured_reason="timeout")

    def _serialize_value(  # pylint: disable=too-many-return-statements
        self,
        value: Any,
        depth: int,
        deadline: float,
        seen: Set[int],
    ) -> CapturedValue:
        """Recursively serialize a value with all safety checks."""
        # Timeout check
        if time.monotonic() > deadline:
            return CapturedValue(type=type(value).__name__, not_captured_reason="timeout")

        # None
        if value is None:
            return CapturedValue(type="NoneType", is_null=True)

        # Depth check (for compound types)
        type_name = type(value).__name__

        # Primitives (no depth limit)
        if isinstance(value, bool):
            return CapturedValue(type="bool", value=str(value).lower())

        if isinstance(value, int):
            return CapturedValue(type="int", value=str(value))

        if isinstance(value, float):
            return CapturedValue(type="float", value=str(value))

        if isinstance(value, str):
            return self._serialize_string(value)

        # Compound types need depth check
        if depth >= self.max_depth:
            return CapturedValue(type=type_name, not_captured_reason="depth")

        # Circular reference check for mutable compound types
        obj_id = id(value)
        if obj_id in seen:
            return CapturedValue(type=type_name, not_captured_reason="depth")
        seen.add(obj_id)

        try:
            if isinstance(value, dict):
                return self._serialize_dict(value, depth, deadline, seen)

            if isinstance(value, (list, tuple, set, frozenset)):
                return self._serialize_collection(value, depth, deadline, seen)

            # Object with attributes
            return self._serialize_object(value, depth, deadline, seen)
        finally:
            seen.discard(obj_id)

    def _serialize_string(self, value: str) -> CapturedValue:
        """Serialize a string with truncation."""
        if len(value) <= self.max_string_length:
            return CapturedValue(type="str", value=value)
        return CapturedValue(
            type="str",
            value=value[: self.max_string_length],
            truncated=True,
            size=len(value),
        )

    def _serialize_dict(self, value: dict, depth: int, deadline: float, seen: Set[int]) -> CapturedValue:
        """Serialize a dict as entries list."""
        original_size = len(value)
        entries = []
        for idx, (key, value_item) in enumerate(value.items()):
            if idx >= self.max_collection_size:
                break
            if time.monotonic() > deadline:
                return CapturedValue(type="dict", not_captured_reason="timeout")
            key_cv = self._serialize_value(key, depth + 1, deadline, seen)
            val_cv = self._serialize_value(value_item, depth + 1, deadline, seen)
            entries.append({"key": key_cv, "value": val_cv})

        cv = CapturedValue(type="dict", entries=entries)
        if original_size > self.max_collection_size:
            cv.truncated = True
            cv.size = original_size
        return cv

    def _serialize_collection(self, value: Any, depth: int, deadline: float, seen: Set[int]) -> CapturedValue:
        """Serialize list/tuple/set/frozenset as elements."""
        type_name = type(value).__name__
        original_size = len(value)
        elements = []
        for item in itertools.islice(value, self.max_collection_size):
            if time.monotonic() > deadline:
                return CapturedValue(type=type_name, not_captured_reason="timeout")
            elements.append(self._serialize_value(item, depth + 1, deadline, seen))

        cv = CapturedValue(type=type_name, elements=elements)
        if original_size > self.max_collection_size:
            cv.truncated = True
            cv.size = original_size
        return cv

    def _serialize_object(self, value: Any, depth: int, deadline: float, seen: Set[int]) -> CapturedValue:
        """Serialize an arbitrary object via its __dict__ or repr."""
        type_name = type(value).__qualname__
        module = getattr(type(value), "__module__", "")
        if module and module != "builtins":
            type_name = f"{module}.{type_name}"

        try:
            obj_dict = getattr(value, "__dict__", None)
        except Exception:  # pylint: disable=broad-exception-caught
            # Property descriptor on __dict__ threw — fall back to type name only
            return CapturedValue(type=type_name, not_captured_reason="fieldCount")

        if obj_dict is None:
            # No __dict__ — fall back to str representation
            try:
                repr_str = repr(value)
            except Exception:  # pylint: disable=broad-exception-caught
                repr_str = f"<{type_name}: repr failed>"
            return CapturedValue(type=type_name, value=self._truncate_str(repr_str))

        fields: Dict[str, CapturedValue] = {}
        field_count = 0
        try:
            dict_items = obj_dict.items()
        except Exception:  # pylint: disable=broad-exception-caught
            # Custom dict-like with broken items() — fall back to type only
            return CapturedValue(type=type_name, not_captured_reason="fieldCount")

        for attr_name, attr_value in dict_items:
            if field_count >= self.max_fields:
                break
            if time.monotonic() > deadline:
                return CapturedValue(type=type_name, not_captured_reason="timeout")
            fields[attr_name] = self._serialize_value(attr_value, depth + 1, deadline, seen)
            field_count += 1

        cv = CapturedValue(type=type_name, fields=fields)
        if len(obj_dict) > self.max_fields:
            cv.not_captured_reason = "fieldCount"
            cv.size = len(obj_dict)
        return cv

    def _truncate_str(self, s: str) -> str:
        if len(s) <= self.max_string_length:
            return s
        return s[: self.max_string_length]

    def serialize_variables(self, variables: Dict[str, Any]) -> Dict[str, CapturedValue]:
        """
        Serialize a dict of variable name -> value into CapturedValue map.

        Args:
            variables: Dict of variable names to their values.

        Returns:
            Dict mapping variable names to CapturedValue instances.
        """
        result: Dict[str, CapturedValue] = {}
        deadline = time.monotonic() + (self.timeout_ms / 1000.0)
        seen: Set[int] = set()
        for name, val in variables.items():
            if time.monotonic() > deadline:
                result[name] = CapturedValue(type=type(val).__name__, not_captured_reason="timeout")
                continue
            try:
                result[name] = self._serialize_value(val, 0, deadline, seen)
            except Exception:  # pylint: disable=broad-exception-caught
                result[name] = CapturedValue(type=type(val).__name__, not_captured_reason="timeout")
        return result
