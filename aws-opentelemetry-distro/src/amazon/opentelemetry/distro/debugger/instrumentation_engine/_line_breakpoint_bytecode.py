# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-Python-version bytecode templates for line-level breakpoint injection."""

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED

_HANDLER_NAME = "_breakpoint_handler"
_LOCALS_NAME = "_breakpoint_locals"

if IS_BYTECODE_INSTALLED:
    from bytecode import Instr
else:
    Instr = None  # type: ignore[misc, assignment]


def create_breakpoint_instructions_py311(function_key: str, line_number: int) -> list:
    """3.11: ``_breakpoint_handler(function_key, line_number, locals())``."""
    return [
        Instr("PUSH_NULL"),
        Instr("LOAD_GLOBAL", (False, _HANDLER_NAME)),
        Instr("LOAD_CONST", function_key),
        Instr("LOAD_CONST", line_number),
        Instr("LOAD_GLOBAL", (True, _LOCALS_NAME)),
        Instr("PRECALL", 0),
        Instr("CALL", 0),
        Instr("PRECALL", 3),
        Instr("CALL", 3),
        Instr("POP_TOP"),
    ]


def create_breakpoint_instructions_py39_py310(function_key: str, line_number: int) -> list:
    """3.9/3.10."""
    return [
        Instr("LOAD_GLOBAL", _HANDLER_NAME),
        Instr("LOAD_CONST", function_key),
        Instr("LOAD_CONST", line_number),
        Instr("LOAD_GLOBAL", _LOCALS_NAME),
        Instr("CALL_FUNCTION", 0),
        Instr("CALL_FUNCTION", 3),
        Instr("POP_TOP"),
    ]
