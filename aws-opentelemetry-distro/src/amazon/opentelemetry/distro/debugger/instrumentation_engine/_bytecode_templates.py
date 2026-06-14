# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Per-Python-version bytecode templates for instrumentation injection.

Two namespaces of static methods:

* ``FunctionEntryBytecode`` — entry / exit / exception-tail instruction lists
  used to wrap a function body for PROBE / function-level instrumentation.
* ``LineBreakpointBytecode`` — the call-handler instruction list inserted
  immediately before a target line for line-level breakpoints.

3.11 and 3.9/3.10 use materially different calling conventions
(``PRECALL``/``CALL`` vs ``CALL_FUNCTION``, tuple vs string ``LOAD_GLOBAL``
operand). Each class exposes a ``select_*`` dispatcher that picks the right
helper for the running interpreter so callers don't need a version ladder.
"""

import sys
from typing import Any, Callable, Dict, Optional

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED

_HANDLER_NAME = "_breakpoint_handler"
_LOCALS_NAME = "_breakpoint_locals"

if IS_BYTECODE_INSTALLED:
    from bytecode import Instr
else:
    Instr = None  # type: ignore[misc, assignment]


class FunctionEntryBytecode:
    """Static helpers for function-entry/exit/exception bytecode injection."""

    @staticmethod
    def entry_py311(entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str) -> list:
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

    @staticmethod
    def entry_py39_py310(entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str) -> list:
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

    @staticmethod
    def exit_py311(  # pylint: disable=too-many-arguments
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

    @staticmethod
    def exit_py39_py310(  # pylint: disable=too-many-arguments
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

    @staticmethod
    def exception_py311(entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str) -> list:
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

    @staticmethod
    def exception_py39_py310(entry: Dict[str, Any], handler: Any, start_ns_local: str, entry_ctx_local: str) -> list:
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

    @staticmethod
    def select_entry() -> Optional[Callable]:
        """Return the entry-template builder for the running interpreter."""
        if sys.version_info >= (3, 11):
            return FunctionEntryBytecode.entry_py311
        if sys.version_info >= (3, 9):
            return FunctionEntryBytecode.entry_py39_py310
        return None

    @staticmethod
    def select_exit() -> Optional[Callable]:
        """Return the exit-template builder for the running interpreter."""
        if sys.version_info >= (3, 11):
            return FunctionEntryBytecode.exit_py311
        if sys.version_info >= (3, 9):
            return FunctionEntryBytecode.exit_py39_py310
        return None

    @staticmethod
    def select_exception() -> Optional[Callable]:
        """Return the exception-tail builder for the running interpreter."""
        if sys.version_info >= (3, 11):
            return FunctionEntryBytecode.exception_py311
        if sys.version_info >= (3, 9):
            return FunctionEntryBytecode.exception_py39_py310
        return None


class LineBreakpointBytecode:
    """Static helpers for line-level breakpoint bytecode injection."""

    @staticmethod
    def breakpoint_py311(function_key: str, line_number: int) -> list:
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

    @staticmethod
    def breakpoint_py39_py310(function_key: str, line_number: int) -> list:
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

    @staticmethod
    def select() -> Optional[Callable]:
        """Return the breakpoint-template builder for the running interpreter."""
        if sys.version_info >= (3, 11):
            return LineBreakpointBytecode.breakpoint_py311
        if sys.version_info >= (3, 9):
            return LineBreakpointBytecode.breakpoint_py39_py310
        return None
