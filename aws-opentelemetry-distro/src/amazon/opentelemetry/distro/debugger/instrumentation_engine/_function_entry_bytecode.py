# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-Python-version bytecode templates for function-entry/exit injection."""

from typing import Any, Dict

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED

_LOCALS_NAME = "_breakpoint_locals"

if IS_BYTECODE_INSTALLED:
    from bytecode import Instr
else:
    Instr = None  # type: ignore[misc, assignment]


def create_function_entry_instructions_py311(
    entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str
) -> list:
    """3.11 entry: ``(start_ns, entry_ctx) = handler(entry, locals())``."""
    return [
        Instr("PUSH_NULL"),
        Instr("LOAD_CONST", handler),
        Instr("LOAD_CONST", entry),
        Instr("LOAD_GLOBAL", (True, _LOCALS_NAME)),
        Instr("PRECALL", 0),
        Instr("CALL", 0),
        Instr("PRECALL", 2),
        Instr("CALL", 2),
        Instr("UNPACK_SEQUENCE", 2),
        Instr("STORE_FAST", start_ns_local),
        Instr("STORE_FAST", entry_ctx_local),
    ]


def create_function_entry_instructions_py39_py310(
    entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str
) -> list:
    """3.9/3.10 entry."""
    return [
        Instr("LOAD_CONST", handler),
        Instr("LOAD_CONST", entry),
        Instr("LOAD_GLOBAL", _LOCALS_NAME),
        Instr("CALL_FUNCTION", 0),
        Instr("CALL_FUNCTION", 2),
        Instr("UNPACK_SEQUENCE", 2),
        Instr("STORE_FAST", start_ns_local),
        Instr("STORE_FAST", entry_ctx_local),
    ]


def create_function_exit_instructions_py311(  # pylint: disable=too-many-arguments
    entry: Dict[str, Any],
    retval_local: str,
    handler: Any,
    start_ns_local: str,
    entry_ctx_local: str,
) -> list:
    """3.11 exit: stash retval, call ``handler(entry, retval, start_ns, entry_ctx)``, restore retval."""
    return [
        Instr("STORE_FAST", retval_local),
        Instr("PUSH_NULL"),
        Instr("LOAD_CONST", handler),
        Instr("LOAD_CONST", entry),
        Instr("LOAD_FAST", retval_local),
        Instr("LOAD_FAST", start_ns_local),
        Instr("LOAD_FAST", entry_ctx_local),
        Instr("PRECALL", 4),
        Instr("CALL", 4),
        Instr("POP_TOP"),
        Instr("LOAD_FAST", retval_local),
    ]


def create_function_exit_instructions_py39_py310(  # pylint: disable=too-many-arguments
    entry: Dict[str, Any],
    retval_local: str,
    handler: Any,
    start_ns_local: str,
    entry_ctx_local: str,
) -> list:
    """3.9/3.10 exit."""
    return [
        Instr("STORE_FAST", retval_local),
        Instr("LOAD_CONST", handler),
        Instr("LOAD_CONST", entry),
        Instr("LOAD_FAST", retval_local),
        Instr("LOAD_FAST", start_ns_local),
        Instr("LOAD_FAST", entry_ctx_local),
        Instr("CALL_FUNCTION", 4),
        Instr("POP_TOP"),
        Instr("LOAD_FAST", retval_local),
    ]


def create_function_exception_instructions_py311(
    entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str
) -> list:
    """3.11 exception tail: ``handler(entry, start_ns, entry_ctx)`` then RERAISE."""
    return [
        Instr("PUSH_NULL"),
        Instr("LOAD_CONST", handler),
        Instr("LOAD_CONST", entry),
        Instr("LOAD_FAST", start_ns_local),
        Instr("LOAD_FAST", entry_ctx_local),
        Instr("PRECALL", 3),
        Instr("CALL", 3),
        Instr("POP_TOP"),
        Instr("RERAISE", 0),
    ]


def create_function_exception_instructions_py39_py310(
    entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str
) -> list:
    """3.9/3.10 exception tail."""
    return [
        Instr("LOAD_CONST", handler),
        Instr("LOAD_CONST", entry),
        Instr("LOAD_FAST", start_ns_local),
        Instr("LOAD_FAST", entry_ctx_local),
        Instr("CALL_FUNCTION", 3),
        Instr("POP_TOP"),
        Instr("RERAISE", 0),
    ]
