# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Resolve a function reference past common decorator wrappings.

When a user writes ``@functools.wraps`` decorators (Django ``@login_required``,
``@cache_page``, custom auth wrappers), the symbol they imported is the
*wrapper* — its ``__code__`` is the auth/cache check, not the user code the
PROBE was meant to capture. This module BFS-walks the wrappings to find the
underlying user function so the engine instruments the right layer.

Disambiguation: matching on ``co_name`` alone can pick a sibling helper of
the same name that happens to live in the same closure. We additionally
require ``co_filename`` to resolve to the same file path as the registration,
when the caller can supply one. Filename-only disambiguation is a heuristic
(file paths shift across deploys), so the path argument is optional.

The traversal covers:

* ``__wrapped__`` — set by ``functools.wraps``
* ``functools.partial`` — ``.func`` (callable), ``.args``, ``.keywords.values()``
* closure cells — ``__closure__[*].cell_contents``
* attribute slots — ``__dict__``, ``__slots__`` (read defensively)
* ``dir()`` last-resort sweep, callable-only (filtered to bound the cost)

A short-circuit honours an ``__otel_di_unwrapped__`` attribute: any wrapper
that wants to redirect to a specific function can set this attribute and
the BFS will stop there.
"""

import functools
import os
from collections import deque
from pathlib import Path
from types import CodeType, FunctionType
from typing import Any, Optional

_UNWRAP_MARKER = "__otel_di_unwrapped__"


def _resolved(path: Optional[str]) -> Optional[Path]:
    """Best-effort canonicalise a filesystem path. Returns ``None`` on error."""
    if not path:
        return None
    try:
        return Path(os.path.realpath(path)).resolve()
    except (OSError, ValueError):
        return None


def _matches(code: CodeType, name: str, target: Optional[Path]) -> bool:
    """``co_name`` must match; ``co_filename`` must match if a target was given."""
    if code.co_name != name:
        return False
    if target is None:
        return True
    candidate = _resolved(code.co_filename)
    return candidate is not None and candidate == target


# pylint: disable=too-many-branches,too-many-statements
# pylint: disable=too-many-branches,too-many-statements,too-many-locals
def undecorated(func: Any, name: str, path: Optional[str] = None) -> Any:
    """
    Return the innermost function whose ``co_name`` matches ``name`` (and
    ``co_filename`` matches ``path`` when provided).

    Falls back to ``func`` if no inner match is found — callers should still
    treat the result as the best available target.

    Args:
        func: Starting object. May be a ``FunctionType``, ``functools.partial``,
            or any callable; non-traversable inputs are returned unchanged.
        name: Expected ``co_name`` of the underlying user function.
        path: Optional filesystem path of the file where the user function is
            defined. When supplied, the BFS uses ``co_filename`` to
            disambiguate same-named helpers across files.

    Returns:
        The matched function, or ``func`` if no match was found.
    """
    if not isinstance(func, (FunctionType, functools.partial)):
        return func

    # Already at the right place? Common case: undecorated function.
    if isinstance(func, FunctionType) and _matches(func.__code__, name, _resolved(path)):
        return func

    target = _resolved(path)
    seen: set = {id(func)}
    queue: deque = deque([func])

    while queue:
        obj = queue.popleft()

        # Honour the explicit short-circuit marker.
        marker = getattr(obj, _UNWRAP_MARKER, None)
        if isinstance(marker, FunctionType):
            return marker

        # Direct match — we're done.
        if isinstance(obj, FunctionType) and _matches(obj.__code__, name, target):
            return obj

        # Expansion frontier. Order matters only weakly — we BFS to find the
        # match as fast as possible, but every reachable function gets
        # enqueued either way.

        # functools.wraps sets __wrapped__.
        wrapped = getattr(obj, "__wrapped__", None)
        if wrapped is not None and id(wrapped) not in seen:
            seen.add(id(wrapped))
            queue.append(wrapped)

        # functools.partial wraps in .func; partial.args / .keywords may also
        # contain callables (rare but real — see Django middleware factories).
        if isinstance(obj, functools.partial):
            for piece in (obj.func, *obj.args, *obj.keywords.values()):
                if id(piece) not in seen and callable(piece):
                    seen.add(id(piece))
                    queue.append(piece)

        # Closure cells — captures the inner user function for plain decorators
        # that don't use functools.wraps.
        if isinstance(obj, FunctionType):
            for cell in obj.__closure__ or ():
                try:
                    cell_obj = cell.cell_contents
                except ValueError:
                    continue
                if id(cell_obj) not in seen and callable(cell_obj):
                    seen.add(id(cell_obj))
                    queue.append(cell_obj)

        # __dict__ values — wrappers sometimes stash inner refs as attributes.
        attr_dict = getattr(obj, "__dict__", None)
        if isinstance(attr_dict, dict):
            for value in attr_dict.values():
                if id(value) not in seen and callable(value):
                    seen.add(id(value))
                    queue.append(value)

        # __slots__ accessors — same idea, defensively.
        for slot in getattr(obj, "__slots__", ()) or ():
            try:
                slot_value = getattr(obj, slot)
            except AttributeError:
                continue
            if id(slot_value) not in seen and callable(slot_value):
                seen.add(id(slot_value))
                queue.append(slot_value)

        # Last-resort dir() sweep, filtered to non-dunder callables only.
        # This is bounded by the object's attribute count (typically O(10)).
        try:
            attrs = dir(obj)
        except Exception:  # noqa: BLE001  # pylint: disable=broad-exception-caught
            attrs = ()
        for attr in attrs:
            if attr.startswith("__") and attr.endswith("__"):
                continue
            try:
                value = getattr(obj, attr)
            except Exception:  # noqa: BLE001  # pylint: disable=broad-exception-caught
                continue
            if callable(value) and id(value) not in seen:
                seen.add(id(value))
                queue.append(value)

    # No inner match — return the original. Caller still gets a usable
    # FunctionType (just one layer up from where they wanted to be).
    return func
