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

The pre-3.11 path additionally relies on a synthesized context-manager object
(``_FunctionEntryContextManager``) baked as a single ``LOAD_CONST`` and driven
by ``SETUP_WITH`` / ``WITH_EXCEPT_START`` / ``RERAISE``, mirroring the design
in ``ddtrace/internal/wrapping/context.py``. The 3.11+ disjoint-segment path
uses zero-cost exception tables and does NOT use the context manager.
"""

import sys
from contextvars import ContextVar
from typing import Any, Callable, Dict, Optional

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED

_HANDLER_NAME = "_breakpoint_handler"
_LOCALS_NAME = "_breakpoint_locals"

if IS_BYTECODE_INSTALLED:
    from bytecode import Instr
else:
    Instr = None  # type: ignore[misc, assignment]


class _FunctionEntryContextManager:
    """Context manager for pre-3.11 ``SETUP_WITH``-based function-entry wrapping.

    A single instance is baked as a ``LOAD_CONST`` operand so the rewritten
    bytecode looks roughly like::

        with cm:
            # original body, with each ``return value`` rewritten to:
            #   POP_BLOCK; cm.__return__(value); return value

    ``__enter__`` invokes the engine's entry handler and stashes
    ``(start_ns, entry_ctx)`` in a :class:`ContextVar` so ``__return__`` and
    ``__exit__`` can recover them. A ContextVar (not :class:`threading.local`)
    is used so recursion and async tasks each see their own frame — a fresh
    dict whose ``__otel_di_prev__`` slot points back to the previous frame.

    ``__return__`` is invoked explicitly by the bytecode-level return splice
    on the normal exit path. ``__exit__`` only runs on the exception path
    because ``POP_BLOCK`` removes the with-frame before ``RETURN_VALUE``.
    """

    _STACK_KEY = "__otel_di_prev__"

    def __init__(
        self,
        entry: Dict[str, Any],
        entry_handler: Callable,
        exit_handler: Callable,
        exception_handler: Callable,
    ) -> None:
        self._entry = entry
        self._entry_handler = entry_handler
        self._exit_handler = exit_handler
        self._exception_handler = exception_handler
        # Per-instance ContextVar so concurrent functions don't share state.
        # The id() suffix keeps the var name unique within the process.
        self._storage: ContextVar = ContextVar(
            "otel_di_cm_storage_" + str(id(entry)),
            default=None,
        )

    def __enter__(self) -> "_FunctionEntryContextManager":
        prev = self._storage.get()
        # Grab the user function's locals (not __enter__'s own frame).
        # SETUP_WITH calls __enter__() with no args; frame 0 is here,
        # frame 1 is the user function executing SETUP_WITH.
        try:
            user_locals = sys._getframe(1).f_locals  # pylint: disable=protected-access
        except (ValueError, AttributeError):
            user_locals = {}
        try:
            start_ns, entry_ctx = self._entry_handler(self._entry, user_locals)
        except Exception:  # pylint: disable=broad-exception-caught
            start_ns, entry_ctx = (0, None)
        self._storage.set(
            {
                self._STACK_KEY: prev,
                "start_ns": start_ns,
                "entry_ctx": entry_ctx,
            }
        )
        return self

    def _pop_frame(self) -> Dict[str, Any]:
        frame = self._storage.get() or {}
        self._storage.set(frame.get(self._STACK_KEY))
        return frame

    def __return__(self, value: Any) -> Any:
        """Called explicitly before each ``RETURN_VALUE`` in the wrapped body."""
        frame = self._pop_frame()
        try:
            self._exit_handler(
                self._entry,
                value,
                frame.get("start_ns", 0),
                frame.get("entry_ctx"),
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return value

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool:
        """Only runs on the exception path; returns ``False`` so we re-raise."""
        frame = self._pop_frame()
        if exc_val is None:
            return False
        try:
            self._exception_handler(
                self._entry,
                frame.get("start_ns", 0),
                frame.get("entry_ctx"),
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return False


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
        """3.9/3.10 exception tail (legacy; superseded by the SETUP_WITH path)."""
        return [
            Instr("LOAD_CONST", handler),
            Instr("LOAD_CONST", entry),
            Instr("LOAD_FAST", start_ns_local),
            Instr("LOAD_FAST", entry_ctx_local),
            Instr("CALL_FUNCTION", 3),
            Instr("POP_TOP"),
            Instr("RERAISE", 0),
        ]

    # ------------------------------------------------------------------
    # Pre-3.11 SETUP_WITH-based templates. These are the ones the engine
    # actually uses on 3.9/3.10. They mirror Datadog's CONTEXT_HEAD /
    # CONTEXT_RETURN / CONTEXT_FOOT in ddtrace/internal/wrapping/context.py.
    # The CM is baked as a single ``LOAD_CONST`` operand by the caller.
    # ------------------------------------------------------------------

    @staticmethod
    def cm_setup_py39_py310(cm: Any, except_label: Any) -> list:
        """SETUP_WITH head: push CM, install with-block targeting ``except_label``.

        Stack walk:
            LOAD_CONST cm        ; [cm]
            SETUP_WITH @except   ; [__exit__-bound, __enter__-result]
                                 ; (block-stack now has the with-frame; on
                                 ;  exception, jumps to ``except_label`` with
                                 ;  the standard 6/7-item exception layout)
            POP_TOP              ; [__exit__-bound]
                                 ; (we don't bind ``as`` so we discard the
                                 ;  __enter__ result)

        The single LOAD_CONST operand is the synthesized
        :class:`_FunctionEntryContextManager` instance.
        """
        return [
            Instr("LOAD_CONST", cm),
            Instr("SETUP_WITH", except_label),
            Instr("POP_TOP"),
        ]

    @staticmethod
    def cm_return_py39_py310(cm: Any) -> list:
        """Spliced before each ``RETURN_VALUE`` in the user body.

        Stack on entry: ``[retval]``. Net effect: ``cm.__return__(retval)`` is
        called, its result is discarded, and the original ``retval`` is left on
        TOS for the trailing ``RETURN_VALUE``. ``POP_BLOCK`` removes the
        with-frame from the block stack so ``__exit__`` is NOT auto-invoked on
        the normal-exit path.

        Mirrors Datadog CONTEXT_RETURN exactly:

            pop_block
            load_const     {context}
            load_method    $__return__
            rot_three
            rot_three
            call_method    1
            rot_two
            pop_top
        """
        return [
            Instr("POP_BLOCK"),
            Instr("LOAD_CONST", cm),
            Instr("LOAD_METHOD", "__return__"),
            Instr("ROT_THREE"),
            Instr("ROT_THREE"),
            Instr("CALL_METHOD", 1),
            Instr("ROT_TWO"),
            Instr("POP_TOP"),
        ]

    @staticmethod
    def cm_exception_py39() -> list:
        """3.9 exception tail: ``WITH_EXCEPT_START`` + bare ``RERAISE`` (no oparg).

        ``RERAISE`` on 3.9 takes NO oparg — the bytecode library raises if one
        is supplied. ``WITH_EXCEPT_START`` invokes ``cm.__exit__(typ, val, tb)``
        from the standard exception layout pushed by ``SETUP_WITH``; we always
        re-raise (we don't honour suppression by truthy ``__exit__``).
        """
        return [
            Instr("WITH_EXCEPT_START"),
            Instr("POP_TOP"),
            Instr("RERAISE"),
        ]

    @staticmethod
    def cm_exception_py310() -> list:
        """3.10 exception tail: ``WITH_EXCEPT_START`` + ``RERAISE 1``.

        ``RERAISE 1`` re-raises with the saved ``f_lasti`` so the traceback
        points at the failing user instruction rather than our injected tail.
        """
        return [
            Instr("WITH_EXCEPT_START"),
            Instr("POP_TOP"),
            Instr("RERAISE", 1),
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
        """Return the exception-tail builder for the running interpreter (3.11+ only)."""
        if sys.version_info >= (3, 11):
            return FunctionEntryBytecode.exception_py311
        if sys.version_info >= (3, 9):
            return FunctionEntryBytecode.exception_py39_py310
        return None

    @staticmethod
    def select_pre311() -> Optional[Dict[str, Callable]]:
        """Return the SETUP_WITH-based template trio for 3.9/3.10.

        Returns a dict with keys ``setup``, ``return_``, and ``exception``
        bound to the version-appropriate builders, or ``None`` on 3.11+ /
        unsupported versions. The caller wires these together with a
        :class:`_FunctionEntryContextManager` instance.
        """
        if sys.version_info >= (3, 11) or sys.version_info < (3, 9):
            return None
        if sys.version_info >= (3, 10):
            return {
                "setup": FunctionEntryBytecode.cm_setup_py39_py310,
                "return_": FunctionEntryBytecode.cm_return_py39_py310,
                "exception": FunctionEntryBytecode.cm_exception_py310,
            }
        return {
            "setup": FunctionEntryBytecode.cm_setup_py39_py310,
            "return_": FunctionEntryBytecode.cm_return_py39_py310,
            "exception": FunctionEntryBytecode.cm_exception_py39,
        }


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
