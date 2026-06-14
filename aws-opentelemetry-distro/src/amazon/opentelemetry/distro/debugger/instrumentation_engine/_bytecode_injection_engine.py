# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Bytecode injection engine for live debugger.

This module handles the injection of breakpoint calls into Python functions
using bytecode injection for Python 3.9-3.11.
"""

import inspect
import logging
import sys
import threading
import time
from dataclasses import dataclass
from types import CodeType, FunctionType
from typing import Any, Dict, List, Optional, Set

from amazon.opentelemetry.distro._utils import IS_BYTECODE_INSTALLED
from amazon.opentelemetry.distro.debugger._data_models import (
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_STRING_LENGTH,
    CaptureConfig,
)
from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    ThreadInfo,
    TraceContext,
)
from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer
from amazon.opentelemetry.distro.debugger._stack_utils import capture_stack_frames
from amazon.opentelemetry.distro.debugger.instrumentation_engine._function_entry_bytecode import (
    create_function_entry_instructions_py39_py310,
    create_function_entry_instructions_py311,
    create_function_exception_instructions_py39_py310,
    create_function_exception_instructions_py311,
    create_function_exit_instructions_py39_py310,
    create_function_exit_instructions_py311,
)
from amazon.opentelemetry.distro.debugger.instrumentation_engine._instrumentation_engine import InstrumentationEngine
from amazon.opentelemetry.distro.debugger.instrumentation_engine._line_breakpoint_bytecode import (
    create_breakpoint_instructions_py39_py310,
    create_breakpoint_instructions_py311,
)
from amazon.opentelemetry.distro.debugger.instrumentation_engine._undecorate import undecorated

logger = logging.getLogger(__name__)

# Global name for the breakpoint handler function injected into function globals
_HANDLER_NAME = "_breakpoint_handler"
# Global name for the locals() builtin injected into function globals
_LOCALS_NAME = "_breakpoint_locals"

# CPython code-object flag bits — generator/async-generator/iterable-coroutine
# exit via YIELD_VALUE, so we skip them to avoid corrupting ``.send()``.
_CO_GENERATOR = 0x0020
_CO_ITERABLE_COROUTINE = 0x0100
_CO_ASYNC_GENERATOR = 0x0200

_RETVAL_LOCAL_NAME = "__otel_di_retval__"
_START_NS_LOCAL_NAME = "__otel_di_start_ns__"
_ENTRY_CTX_LOCAL_NAME = "__otel_di_entry_ctx__"

# Import bytecode classes if available
if IS_BYTECODE_INSTALLED:
    from bytecode import Bytecode, Instr, Label, TryBegin, TryEnd
else:
    Bytecode = None  # type: ignore[misc, assignment]
    Instr = None  # type: ignore[misc, assignment]
    Label = None  # type: ignore[misc, assignment]
    TryBegin = None  # type: ignore[misc, assignment]
    TryEnd = None  # type: ignore[misc, assignment]


@dataclass
class InjectionState:
    """
    Tracks bytecode injection state for a function.

    Only stores what's needed for restoration and callback:
    - original_code: To restore the function's original bytecode
    - function_ref: To access the function for restoration and cleanup
    - function_key: For constructing breakpoint_key in callback
    """

    original_code: CodeType
    function_ref: Optional[FunctionType]
    function_key: str


class BytecodeInjectionEngine(InstrumentationEngine):
    """
    Bytecode injection engine for line-level debugging on Python 3.9-3.11.

    Safety guarantees:
    - Never crashes the application (comprehensive exception handling)
    - Stores original code for restoration
    - Thread-safe for concurrent operations
    - Gracefully degrades on errors
    """

    def __init__(self):
        """Initialize the bytecode injection engine."""
        super().__init__()
        self._lock = threading.RLock()
        self._injection_states: Dict[int, InjectionState] = {}
        self._initialized = False
        # Callback for hit count tracking
        self._hit_count_callback = None
        # Maps (function_key, line_number) to location_hash for span events
        self._location_hashes: Dict[tuple, str] = {}
        # Maps (function_key, line_number) to CaptureConfig for filtering captured data
        self._capture_configs: Dict[tuple, CaptureConfig] = {}

        # Function-entry state keyed by id(func). Bytecode rewrite mutates
        # __code__ in place, so every existing reference picks up the new
        # bytecode on next call without registry traversal.
        self._function_entries: Dict[int, Dict[str, Any]] = {}
        self._tls = threading.local()
        self._reentrancy_guard = threading.local()

        if not IS_BYTECODE_INSTALLED:
            logger.warning(
                "bytecode library not available. "
                "Debugger will not function on Python 3.9-3.11. "
                "Install with: pip install bytecode"
            )

    def initialize(self, hit_count_callback=None) -> None:
        """
        Initialize the bytecode injection engine.

        Args:
            hit_count_callback: Optional callback to be called when a breakpoint is hit
        """
        try:
            if not IS_BYTECODE_INSTALLED:
                logger.warning("Cannot initialize: bytecode library not available")
                return

            logger.debug(
                "BytecodeInjectionEngine initialized for Python %d.%d",
                sys.version_info.major,
                sys.version_info.minor,
            )
            self._initialized = True
            self._hit_count_callback = hit_count_callback
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to initialize BytecodeInjectionEngine: %s", exc, exc_info=True)

    @staticmethod
    def supports_runtime() -> bool:
        """Check if bytecode injection is supported (Python 3.9-3.11)."""
        return (3, 9) <= sys.version_info < (3, 12) and IS_BYTECODE_INSTALLED

    def enable_breakpoints_for_function(
        self,
        code: CodeType,
        func: FunctionType,
        line_numbers: Set[int],
        function_key: str,
        line_location_hashes: Optional[Dict[int, str]] = None,
        line_capture_configs: Optional[Dict[int, Any]] = None,
    ) -> None:
        """
        Enable multiple breakpoints for a function atomically.

        Injects all breakpoints into the function's bytecode in a single operation.

        Args:
            code: Code object of the function
            func: Function object (needed for bytecode injection)
            line_numbers: Set of line numbers to enable breakpoints on
            function_key: Function key (module.function) for constructing breakpoint_key
            line_location_hashes: Optional mapping of line_number -> location_hash for span events
        """
        if not self._initialized:
            logger.warning("Engine not initialized")
            return

        if not line_numbers:
            logger.debug("No line numbers provided, nothing to enable")
            return

        try:
            func_id = id(func)

            # Create new bytecode with injected breakpoints outside the lock since it doesn't modify shared state
            new_code, actually_injected = self._create_code_with_breakpoints(code, line_numbers, function_key)

            if new_code is None:
                logger.warning("Failed to inject breakpoints for %s", function_key)
                return

            with self._lock:
                if func_id not in self._injection_states:
                    self._injection_states[func_id] = InjectionState(
                        original_code=code, function_ref=func, function_key=function_key
                    )
                    logger.debug("%s entry added to _injection_states for %s", func_id, function_key)

                # Update function code directly
                func.__code__ = new_code
                # Ensure handler is in globals
                func.__globals__[_HANDLER_NAME] = self._breakpoint_handler
                func.__globals__[_LOCALS_NAME] = locals

                # Store location hashes and capture configs for each line
                if line_location_hashes:
                    for line_num, location_hash in line_location_hashes.items():
                        self._location_hashes[(function_key, line_num)] = location_hash
                if line_capture_configs:
                    for line_num, capture_config in line_capture_configs.items():
                        self._capture_configs[(function_key, line_num)] = capture_config

                logger.debug(
                    "Enabled %d breakpoints for %s at lines: %s",
                    len(actually_injected),
                    function_key,
                    actually_injected,
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error enabling breakpoints for %s: %s", function_key, exc, exc_info=True)

    def disable_breakpoints_for_function(self, code: CodeType, func: FunctionType) -> None:
        """
        Disable ALL breakpoints for a function and restore it to original state.

        This is an all-or-nothing operation that:
        - Removes ALL active breakpoints for the function
        - Restores the function to its original (uninstrumented) bytecode
        - Cleans up injected globals and engine-specific state

        Args:
            code: Code object of the function
            func: Function object (needed for bytecode restoration)
        """
        if not self._initialized:
            logger.warning("Engine not initialized")
            return

        try:
            func_id = id(func)

            with self._lock:
                if func_id not in self._injection_states:
                    logger.debug("No injection state for %s", code.co_name)
                    return

                function_key = self._injection_states[func_id].function_key

                state = self._injection_states[func_id]

                # Restore original code (removes all breakpoints)
                func.__code__ = state.original_code

                # Clean up injected globals, but only if no OTHER instrumented function
                # shares this module's globals dict. Two functions defined in the same
                # module share one __globals__; popping the handler while a sibling is
                # still instrumented would make the sibling's injected LOAD_GLOBAL raise
                # NameError inside user code. Compare by identity since dicts aren't hashable.
                target_globals_id = id(func.__globals__)
                shared_by_other = any(
                    other_id != func_id
                    and other_state.function_ref is not None
                    and id(other_state.function_ref.__globals__) == target_globals_id
                    for other_id, other_state in self._injection_states.items()
                )
                if not shared_by_other:
                    func.__globals__.pop(_HANDLER_NAME, None)
                    func.__globals__.pop(_LOCALS_NAME, None)

                # Clean up location hashes and capture configs for this function
                keys_to_remove = [key for key in self._location_hashes if key[0] == function_key]
                for key in keys_to_remove:
                    del self._location_hashes[key]
                config_keys_to_remove = [key for key in self._capture_configs if key[0] == function_key]
                for key in config_keys_to_remove:
                    del self._capture_configs[key]

                # Clean up state
                del self._injection_states[func_id]
                logger.debug("Disabled all breakpoints for %s", function_key)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error disabling breakpoints for %s: %s", code.co_name, exc, exc_info=True)

    # Function-entry instrumentation (PROBE / function-level BREAKPOINT).
    # Mirrors SysMonitoringEngine. Rewrites ``func.__code__`` so every
    # existing reference picks up the new bytecode on next invocation.

    def enable_function_entry(  # pylint: disable=too-many-arguments
        self,
        code: CodeType,
        func: FunctionType,
        function_key: str,
        module_name: str,
        qualified_name: str,
        capture_config: Optional[CaptureConfig] = None,
        location_hash: Optional[str] = None,
        instrumentation_type: Optional[str] = None,
    ) -> bool:
        """Inject function-entry/exit hooks. Returns True on success."""
        if not self._initialized:
            logger.warning("BytecodeInjectionEngine not initialized, cannot enable function entry")
            return False
        if not IS_BYTECODE_INSTALLED:
            logger.warning("bytecode library not available, cannot enable function entry for %s", function_key)
            return False

        try:
            target_func, target_code = self._resolve_target(code, func, qualified_name)
            if self._is_generator_code(target_code):
                logger.debug(
                    "Skipping function-entry injection for generator/async-generator %s",
                    function_key,
                )
                return False

            entry = {
                "func": target_func,
                "function_key": function_key,
                "module_name": module_name,
                "qualified_name": qualified_name,
                "capture_config": capture_config,
                "location_hash": location_hash,
                "instrumentation_type": instrumentation_type,
                "original_code": target_code,
            }

            new_code = self._create_code_with_function_entry_exit(target_code, entry)
            if new_code is None:
                logger.warning("Failed to inject function entry/exit bytecode for %s", function_key)
                return False

            self._install_rewrite(target_func, target_code, new_code, entry)
            return True

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to enable function entry for %s: %s", function_key, exc, exc_info=True)
            return False

    @staticmethod
    def _resolve_target(code: CodeType, func: FunctionType, qualified_name: str):
        target_path = getattr(code, "co_filename", None)
        target_func = undecorated(func, qualified_name.split(".")[-1], target_path)
        target_code = target_func.__code__ if hasattr(target_func, "__code__") else code
        return target_func, target_code

    @staticmethod
    def _is_generator_code(code: CodeType) -> bool:
        flags = _CO_GENERATOR | _CO_ASYNC_GENERATOR | _CO_ITERABLE_COROUTINE
        return bool(code.co_flags & flags)

    def _install_rewrite(
        self,
        target_func: FunctionType,
        target_code: CodeType,
        new_code: CodeType,
        entry: Dict[str, Any],
    ) -> None:
        with self._lock:
            func_id = id(target_func)
            self._function_entries[func_id] = entry
            target_func.__code__ = new_code
            target_func.__globals__[_LOCALS_NAME] = locals
            logger.debug(
                "Enabled function entry for %s (func_id=%s, code_id=%s, type=%s)",
                entry.get("function_key"),
                func_id,
                id(target_code),
                entry.get("instrumentation_type"),
            )

    def disable_function_entry(self, code: CodeType, func: Optional[FunctionType] = None) -> None:
        """Tear down function-entry hook and restore original bytecode."""
        if not self._initialized:
            return
        try:
            with self._lock:
                target_func_id = None
                if func is not None and id(func) in self._function_entries:
                    target_func_id = id(func)
                else:
                    # Fallback: scan by code identity (matches rewritten
                    # ``func.__code__`` or stored ``original_code``).
                    for func_id, entry in self._function_entries.items():
                        entry_func = entry.get("func")
                        if entry_func is None:
                            continue
                        if entry_func.__code__ is code or entry.get("original_code") is code:
                            target_func_id = func_id
                            break
                if target_func_id is None:
                    return
                entry = self._function_entries.pop(target_func_id)
                target_func = entry["func"]
                try:
                    target_func.__code__ = entry["original_code"]
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to restore __code__ for %s: %s",
                        entry.get("function_key"),
                        exc,
                    )
                logger.debug("Disabled function entry for %s", entry.get("function_key"))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Failed to disable function entry: %s", exc, exc_info=True)

    def _function_entry_handler(self, entry: Dict[str, Any], local_vars: dict) -> tuple:
        """Entry callback. Returns ``(start_ns, entry_context)``; ``(0, None)`` to skip."""
        try:
            if getattr(self._reentrancy_guard, "active", False):
                return (0, None)
            capture_config = entry.get("capture_config")
            entry_context = self._build_entry_context(local_vars, capture_config)
            return (time.time_ns(), entry_context)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Critical error in entry handler for %s: %s",
                entry.get("function_key"),
                exc,
                exc_info=True,
            )
            return (0, None)

    def _function_exit_handler(
        self,
        entry: Dict[str, Any],
        retval: Any,
        start_ns: int,
        entry_context: Optional[CapturedContext],
    ) -> None:
        """Exit callback before each RETURN_VALUE. ``start_ns == 0`` skips."""
        try:
            if getattr(self._reentrancy_guard, "active", False):
                return
            if start_ns == 0:
                return
            frame_info = {"start_ns": start_ns, "entry_context": entry_context}
            self._reentrancy_guard.active = True
            try:
                self._handle_function_entry(entry, frame_info, retval)
            finally:
                self._reentrancy_guard.active = False
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Critical error in exit handler for %s: %s",
                entry.get("function_key"),
                exc,
                exc_info=True,
            )

    def _function_exception_handler(
        self,
        entry: Dict[str, Any],
        start_ns: int,
        entry_context: Optional[CapturedContext],
    ) -> None:
        """Exception callback. Builds snapshot with retval=None; RERAISE follows."""
        try:
            if getattr(self._reentrancy_guard, "active", False):
                return
            if start_ns == 0:
                return
            frame_info = {"start_ns": start_ns, "entry_context": entry_context}
            self._reentrancy_guard.active = True
            try:
                self._handle_function_entry(entry, frame_info, None)
            finally:
                self._reentrancy_guard.active = False
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Critical error in exception handler for %s: %s",
                entry.get("function_key"),
                exc,
                exc_info=True,
            )

    @staticmethod
    def _build_entry_context(local_vars: dict, capture_config: Optional[CaptureConfig]) -> Optional[CapturedContext]:
        """Filter and serialize captured arguments per ``capture_config``."""
        if capture_config is None or capture_config.capture_arguments is None:
            return None
        try:
            args_dict = local_vars
            if capture_config.capture_arguments:
                args_dict = {k: v for k, v in args_dict.items() if k in capture_config.capture_arguments}
            if not args_dict:
                return None
            serializer = SnapshotSerializer(
                max_fields=capture_config.max_fields_per_object or DEFAULT_MAX_FIELDS_PER_OBJECT,
                max_string_length=capture_config.max_string_length or DEFAULT_MAX_STRING_LENGTH,
                max_depth=capture_config.max_object_depth or 3,
                max_collection_size=capture_config.max_collection_width or 10,
            )
            return CapturedContext(arguments=serializer.serialize_variables(args_dict))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to build entry context: %s", exc)
            return None

    def _handle_function_entry(
        self,
        entry: Dict[str, Any],
        frame_info: Dict[str, Any],
        retval: Any,
    ) -> None:
        """Build and emit the function-entry snapshot via the shared factory."""
        try:
            function_key = entry["function_key"]

            if self._hit_count_callback is not None:
                if not self._hit_count_callback(f"{function_key}:0"):
                    return

            # pylint: disable=import-outside-toplevel
            from amazon.opentelemetry.distro.debugger._snapshot_factory import (
                build_function_entry_snapshot,
                emit_snapshot,
            )

            snapshot = build_function_entry_snapshot(
                entry=entry,
                frame_info=frame_info,
                retval=retval,
            )
            emit_snapshot(snapshot)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error handling function entry for %s: %s", entry.get("function_key"), exc, exc_info=True)

    def _create_code_with_function_entry_exit(self, code: CodeType, entry: Dict[str, Any]) -> Optional[CodeType]:
        """Rewrite ``code`` to call entry/exit/exception handlers around the body."""
        try:
            slots = self._allocate_local_slots(code)
            instrs = self._build_injection_instrs(entry, slots)
            if instrs is None:
                return None
            bc = Bytecode.from_code(code)
            new_bc = bc.copy()
            new_bc.clear()
            new_bc.extend(self._weave_into_body(bc, instrs))
            return new_bc.to_code()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error injecting entry/exit bytecode for %s: %s", code.co_name, exc, exc_info=True)
            return None

    @dataclass
    class _LocalSlots:
        retval: str
        start_ns: str
        entry_ctx: str

    @dataclass
    class _InjectionInstrs:
        entry: list
        exit: list
        exception_tail: list
        except_label: Any

    def _allocate_local_slots(self, code: CodeType) -> "BytecodeInjectionEngine._LocalSlots":
        """Pick three non-colliding local names so recursion stays correct."""
        retval = self._unique_local(code, _RETVAL_LOCAL_NAME)
        start_ns = self._unique_local(code, _START_NS_LOCAL_NAME, taken={retval})
        entry_ctx = self._unique_local(
            code, _ENTRY_CTX_LOCAL_NAME, taken={retval, start_ns}
        )
        return BytecodeInjectionEngine._LocalSlots(retval=retval, start_ns=start_ns, entry_ctx=entry_ctx)

    def _build_injection_instrs(
        self,
        entry: Dict[str, Any],
        slots: "BytecodeInjectionEngine._LocalSlots",
    ) -> "Optional[BytecodeInjectionEngine._InjectionInstrs]":
        entry_instrs = self._create_function_entry_instructions(entry, slots.start_ns, slots.entry_ctx)
        exit_instrs = self._create_function_exit_instructions(
            entry, slots.retval, slots.start_ns, slots.entry_ctx
        )
        exception_tail = self._create_function_exception_instructions(
            entry, slots.start_ns, slots.entry_ctx
        )
        if entry_instrs is None or exit_instrs is None or exception_tail is None:
            return None
        return BytecodeInjectionEngine._InjectionInstrs(
            entry=entry_instrs,
            exit=exit_instrs,
            exception_tail=exception_tail,
            except_label=Label(),
        )

    @staticmethod
    def _weave_into_body(
        bc: "Bytecode",
        instrs: "BytecodeInjectionEngine._InjectionInstrs",
    ) -> list:
        """Splice entry/exit/exception around the user body.

        3.11+: insert entry AFTER the leading RESUME. 3.9/3.10: prepend
        and rely on SETUP_FINALLY for exception routing.

        On 3.11+, CPython's zero-cost exception model forbids nesting
        TryBegin pseudo-instructions: the bytecode library raises
        "TryBegin pseudo instructions cannot be nested" if our outer
        region encloses any user-owned try/except. Mirroring Datadog's
        ddtrace.internal.wrapping.context approach, we instead emit a
        chain of *disjoint* TryBegin/TryEnd segments that all target
        the same except_label — pausing our region across user-owned
        try blocks and re-opening it afterwards.
        """
        new_instrs: List[Any] = []
        entry_inserted = False

        for instr in bc:
            if (
                not entry_inserted
                and sys.version_info >= (3, 11)
                and isinstance(instr, Instr)
                and instr.name == "RESUME"
            ):
                new_instrs.append(instr)
                new_instrs.extend(instrs.entry)
                entry_inserted = True
                continue
            # Prefix each RETURN_VALUE with the exit splice; don't close
            # the wrapper's try region here. The disjoint-segment walk
            # below handles try-region bookkeeping.
            if isinstance(instr, Instr) and instr.name == "RETURN_VALUE":
                new_instrs.extend(instrs.exit)
                new_instrs.append(instr)
                continue
            new_instrs.append(instr)

        if not entry_inserted:
            # 3.9/3.10: prepend entry. Pre-3.11 has no exception-table
            # markers, so simple wrap-and-close logic is sufficient.
            new_instrs = list(instrs.entry) + [
                TryBegin(instrs.except_label, push_lasti=False)
            ] + new_instrs
            new_instrs.append(TryEnd(new_instrs[len(instrs.entry)]))
            new_instrs.append(instrs.except_label)
            new_instrs.extend(instrs.exception_tail)
            return new_instrs

        # 3.11+: walk the spliced body and emit disjoint try segments
        # that step around any user-owned TryBegin/TryEnd pairs.
        first_try_begin = TryBegin(instrs.except_label, push_lasti=False)
        last_try_begin = first_try_begin

        i = 0
        while i < len(new_instrs):
            item = new_instrs[i]
            if isinstance(item, TryBegin) and last_try_begin is not None:
                # User region opens — close ours just before it.
                new_instrs.insert(i, TryEnd(last_try_begin))
                last_try_begin = None
                i += 2  # skip the TryEnd we inserted and the user's TryBegin
                continue
            if isinstance(item, TryEnd) and last_try_begin is None:
                # User region closes — re-open ours immediately after,
                # but only if there's another real instruction following
                # (otherwise the segment would be empty).
                j = i + 1
                while j < len(new_instrs) and not isinstance(new_instrs[j], TryBegin):
                    if isinstance(new_instrs[j], Instr):
                        last_try_begin = TryBegin(instrs.except_label, push_lasti=False)
                        new_instrs.insert(i + 1, last_try_begin)
                        i += 1  # account for inserted TryBegin
                        break
                    j += 1
            i += 1

        # Open the leading wrapper region right after the entry splice
        # (entry block sits at indices 1..len(entry) just past RESUME).
        # Inserting at index 0 is also valid; placing it after entry
        # keeps entry-handler exceptions from being re-routed through
        # the exception_tail.
        insert_at = 1 + len(instrs.entry)  # past RESUME + entry
        new_instrs.insert(insert_at, first_try_begin)

        if last_try_begin is not None:
            new_instrs.append(TryEnd(last_try_begin))

        new_instrs.append(instrs.except_label)
        new_instrs.extend(instrs.exception_tail)
        return new_instrs

    @staticmethod
    def _unique_local(code: CodeType, base: str, taken: Optional[Set[str]] = None) -> str:
        existing = set(code.co_varnames)
        if taken:
            existing = existing | taken
        name = base
        while name in existing:
            name = "_" + name
        return name

    def _create_function_entry_instructions(
        self, entry: Dict[str, Any], start_ns_local: str, entry_ctx_local: str
    ) -> Optional[list]:
        builder = (
            create_function_entry_instructions_py311
            if sys.version_info >= (3, 11)
            else create_function_entry_instructions_py39_py310
            if sys.version_info >= (3, 9)
            else None
        )
        if builder is None:
            logger.error("Unsupported Python version for entry injection: %s", sys.version_info)
            return None
        try:
            return builder(entry, self._function_entry_handler, start_ns_local, entry_ctx_local)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating entry instructions: %s", exc, exc_info=True)
            return None

    def _create_function_exception_instructions(
        self, entry: Dict[str, Any], start_ns_local: str, entry_ctx_local: str
    ) -> Optional[list]:
        builder = (
            create_function_exception_instructions_py311
            if sys.version_info >= (3, 11)
            else create_function_exception_instructions_py39_py310
            if sys.version_info >= (3, 9)
            else None
        )
        if builder is None:
            logger.error("Unsupported Python version for exception injection: %s", sys.version_info)
            return None
        try:
            return builder(entry, self._function_exception_handler, start_ns_local, entry_ctx_local)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating exception instructions: %s", exc, exc_info=True)
            return None

    def _create_function_exit_instructions(
        self,
        entry: Dict[str, Any],
        retval_local: str,
        start_ns_local: str,
        entry_ctx_local: str,
    ) -> Optional[list]:
        builder = (
            create_function_exit_instructions_py311
            if sys.version_info >= (3, 11)
            else create_function_exit_instructions_py39_py310
            if sys.version_info >= (3, 9)
            else None
        )
        if builder is None:
            logger.error("Unsupported Python version for exit injection: %s", sys.version_info)
            return None
        try:
            return builder(
                entry, retval_local, self._function_exit_handler, start_ns_local, entry_ctx_local
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating exit instructions: %s", exc, exc_info=True)
            return None

    @staticmethod
    def _get_service_name():
        try:
            from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel
            from opentelemetry.sdk.trace import TracerProvider  # pylint: disable=import-outside-toplevel

            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                return provider.resource.attributes.get("service.name")
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    @staticmethod
    def _get_environment():
        try:
            from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel
            from opentelemetry.sdk.trace import TracerProvider  # pylint: disable=import-outside-toplevel

            provider = otel_trace.get_tracer_provider()
            if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
                return provider.resource.attributes.get("deployment.environment.name")
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    def _breakpoint_handler(  # pylint: disable=too-many-locals
        self, function_key: str, line_number: int, local_vars: dict
    ) -> None:
        """
        Handler called from injected bytecode when breakpoint is hit.

        Produces a line-level Snapshot instead of a span event.

        Args:
            function_key: Fully qualified function name (e.g., "mymodule.MyClass.method")
            line_number: Line number of breakpoint
            local_vars: Local variables captured via locals() in injected bytecode
        """
        try:
            # Check rate limit FIRST — skip all expensive work if rate-limited
            if self._hit_count_callback:
                breakpoint_key = f"{function_key}:{line_number}"
                if not self._hit_count_callback(breakpoint_key):
                    return

            # Look up capture config for this breakpoint
            capture_config = self._capture_configs.get((function_key, line_number))

            # Apply capture_locals filtering:
            # None = field absent (do not capture locals)
            # [] = capture all locals
            # ["a", "b"] = capture only those
            capture_locals_list = capture_config.capture_locals if capture_config else None

            # Use per-breakpoint limits if available, otherwise defaults
            serializer = SnapshotSerializer(
                max_fields=capture_config.max_fields_per_object if capture_config else DEFAULT_MAX_FIELDS_PER_OBJECT,
                max_string_length=capture_config.max_string_length if capture_config else DEFAULT_MAX_STRING_LENGTH,
                max_depth=capture_config.max_object_depth if capture_config else 3,
                max_collection_size=capture_config.max_collection_width if capture_config else 10,
            )

            # Serialize local variables into CapturedValue map
            locals_captured = {}
            if capture_locals_list is not None and local_vars:
                try:
                    # Filter out functions, modules, classes, etc.
                    filtered = {
                        k: v
                        for k, v in local_vars.items()
                        if not (
                            inspect.isfunction(v)
                            or inspect.ismodule(v)
                            or inspect.isclass(v)
                            or inspect.ismethod(v)
                            or inspect.isbuiltin(v)
                        )
                    }
                    # Filter by capture_locals list if specific names given
                    if len(capture_locals_list) > 0:
                        filtered = {k: v for k, v in filtered.items() if k in capture_locals_list}
                    locals_captured = serializer.serialize_variables(filtered)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.debug("Failed to capture variables at %s:%s: %s", function_key, line_number, exc)

            # Build line-level captures
            line_context = CapturedContext(locals=locals_captured if locals_captured else None)
            captures = Captures(lines={line_number: line_context})

            # Build instrumentation details
            location_hash = self._location_hashes.get((function_key, line_number), "")
            method_name = function_key.split(".")[-1]
            module_parts = function_key.split(".")
            code_unit = ".".join(module_parts[:-1]) if len(module_parts) > 1 else function_key
            class_name = code_unit  # For Python, className = module path

            instrumentation = InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit=code_unit,
                    class_name=class_name,
                    method_name=method_name,
                    line_number=line_number,
                    language="python",
                ),
            )

            # Read current OTel trace context
            trace_ctx = None
            try:
                from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel

                span = otel_trace.get_current_span()
                if span and span.get_span_context().is_valid:
                    ctx = span.get_span_context()
                    trace_ctx = TraceContext(
                        trace_id=format(ctx.trace_id, "032x"),
                        span_id=format(ctx.span_id, "016x"),
                    )
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            # Thread info
            current_thread = threading.current_thread()
            thread_info = ThreadInfo(id=threading.get_ident(), name=current_thread.name)

            # Stack trace (only if configured — stack walk is expensive)
            stack = None
            if capture_config and capture_config.capture_stack_trace:
                stack = capture_stack_frames(capture_config.max_stack_frames)

            snapshot = Snapshot(
                timestamp=int(time.time() * 1000),
                location_hash=location_hash or None,
                service=self._get_service_name(),
                environment=self._get_environment(),
                instrumentation=instrumentation,
                trace=trace_ctx,
                thread=thread_info,
                stack=stack,
                captures=captures,
                instrumentation_type="BREAKPOINT",  # Line-level is always BREAKPOINT
            )

            # Emit snapshot (rate limit already checked at top of handler)
            try:
                # pylint: disable=import-outside-toplevel
                from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter

                emitter = get_snapshot_emitter()
                if emitter:
                    emitter.emit_snapshot(snapshot)
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            logger.debug("Created line snapshot for breakpoint at %s:%s", function_key, line_number)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error in breakpoint handler at %s:%s: %s", function_key, line_number, exc, exc_info=True)

    def _create_code_with_breakpoints(
        self, code: CodeType, line_numbers: Set[int], function_key: str
    ) -> tuple[Optional[CodeType], Set[int]]:
        """
        Create a new code object with breakpoint handler calls inserted at specified lines.
        Breakpoints are inserted BEFORE the target line executes.

        Note: This function does not modify the original code object.

        Args:
            code: Original code object
            line_numbers: Set of line numbers to insert breakpoints at
            function_key: Fully qualified function name (e.g., "mymodule.MyClass.method")

        Returns:
            Tuple of (new_code, injected_lines):
            - new_code: New code object with breakpoints, or None on failure
            - injected_lines: Set of lines where breakpoints were successfully inserted
        """
        try:
            bc = Bytecode.from_code(code)
            new_instructions = []
            injected_lines = set()

            for instr in bc:
                # Check if we should inject BEFORE this instruction
                if hasattr(instr, "lineno") and instr.lineno in line_numbers and instr.lineno not in injected_lines:

                    # Generate and inject breakpoint instructions BEFORE the line
                    bp_instructions = self._create_breakpoint_instructions(function_key, instr.lineno)

                    if bp_instructions:
                        new_instructions.extend(bp_instructions)
                        injected_lines.add(instr.lineno)
                        logger.debug("Injected breakpoint BEFORE line %s in %s", instr.lineno, code.co_name)

                # Add original instruction AFTER breakpoint
                new_instructions.append(instr)

            # Report missing lines
            missing_lines = line_numbers - injected_lines
            if missing_lines:
                logger.warning(
                    "Could not inject breakpoints at lines %s in %s. Lines may not have executable code.",
                    sorted(missing_lines),
                    code.co_name,
                )

            if not injected_lines:
                logger.warning("No breakpoints were injected in %s", code.co_name)
                return None, set()

            # Create new bytecode and convert to code object
            new_bc = bc.copy()
            new_bc.clear()
            new_bc.extend(new_instructions)

            return new_bc.to_code(), injected_lines

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error injecting bytecode for %s: %s", code.co_name, exc, exc_info=True)
            return None, set()

    @staticmethod
    def _create_breakpoint_instructions(function_key: str, line_number: int) -> Optional[list]:
        """
        Create version-specific breakpoint instructions.

        Args:
            function_key: Fully qualified function name (e.g., "mymodule.MyClass.method")
            line_number: Line number of the breakpoint

        Returns:
            List of bytecode.Instr objects, or None on error
        """
        builder = (
            create_breakpoint_instructions_py311
            if sys.version_info >= (3, 11)
            else create_breakpoint_instructions_py39_py310
            if sys.version_info >= (3, 9)
            else None
        )
        if builder is None:
            logger.error("Unsupported Python version: %s", sys.version_info)
            return None
        try:
            return builder(function_key, line_number)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error creating breakpoint instructions: %s", exc, exc_info=True)
            return None

    def cleanup(self) -> None:
        """
        Clean up all bytecode modifications and restore original code.

        Restores all modified functions to their original state and cleans up
        injected globals. Never raises exceptions.

        This method iterates through all injection states and calls the same
        restoration logic used by disable_breakpoints_for_function.
        """
        try:
            if not self._initialized:
                logger.debug("Engine not initialized, nothing to clean up")
                return

            with self._lock:
                # Restore all modified functions
                restored_count = 0
                failed_count = 0

                for _, state in list(self._injection_states.items()):
                    try:
                        if state.function_ref and state.original_code:
                            # Restore original code (same logic as disable_breakpoints_for_function)
                            state.function_ref.__code__ = state.original_code

                            # Clean up injected globals
                            state.function_ref.__globals__.pop(_HANDLER_NAME, None)
                            state.function_ref.__globals__.pop(_LOCALS_NAME, None)

                            restored_count += 1
                            logger.debug("Restored function %s", state.original_code.co_name)

                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        failed_count += 1
                        logger.warning("Failed to restore function during cleanup: %s", exc, exc_info=True)
                        # Continue cleanup even if one function fails
                        continue

                # Clear all state
                self._injection_states.clear()

                logger.debug(
                    "BytecodeInjectionEngine cleaned up: %d functions restored, %d failed", restored_count, failed_count
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error during cleanup: %s", exc, exc_info=True)
        finally:
            with self._lock:
                self._initialized = False
                self._hit_count_callback = None
