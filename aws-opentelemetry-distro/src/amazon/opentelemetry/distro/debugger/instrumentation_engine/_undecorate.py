# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Resolve a function reference past common decorator wrappings.

When a user writes ``@functools.wraps`` decorators (Django ``@login_required``,
``@cache_page``, custom auth wrappers), the symbol the manager's
``getattr(module, name)`` returns is the *wrapper* — its ``__code__`` is the
auth/cache check, not the user code the PROBE was meant to capture. This
module BFS-walks the wrappings to find the underlying user function so the
engine instruments the right layer.

Disambiguation: matching on ``co_name`` alone can pick a sibling helper of
the same name that happens to live in the same closure. We additionally
require ``co_filename`` to resolve to the same file path as the registration,
when the caller can supply one. Filename-only disambiguation is a heuristic
(file paths shift across deploys), so the path argument is optional.

The traversal covers:

* ``__wrapped__`` — set by ``functools.wraps``
* ``functools.partial`` — ``.func`` (callable), ``.args``, ``.keywords.values()``
* closure cells — ``__closure__[*].cell_contents``

A short-circuit honours an ``__otel_di_unwrapped__`` attribute: any wrapper
that wants to redirect to a specific function can set this attribute and the
BFS will stop there.
"""

import functools
import os
from collections import deque
from pathlib import Path
from types import CodeType, FunctionType
from typing import Any, Optional

_UNWRAP_MARKER = "__otel_di_unwrapped__"


def _resolved(path: Optional[str]) -> Optional[Path]:
    """Best-effort canonicalise a filesystem path. Returns None on error."""
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


# pylint: disable=too-many-locals,too-many-branches
def undecorated(func: Any, name: str, path: Optional[str] = None) -> Any:
    """Return the innermost function whose ``co_name`` matches ``name`` (and
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
        The deepest matching function, or ``func`` if no match is found.
    """
    target = _resolved(path)
    seen = set()
    queue = deque([func])

    # If the user-supplied func itself already matches, prefer the deepest
    # match still — keep it as the fallback.
    best_match = func if isinstance(func, FunctionType) and _matches(func.__code__, name, target) else None

    while queue:
        candidate = queue.popleft()
        cid = id(candidate)
        if cid in seen:
            continue
        seen.add(cid)

        # Explicit redirect attribute beats everything else.
        marker = getattr(candidate, _UNWRAP_MARKER, None)
        if marker is not None:
            queue.append(marker)
            continue

        # If this is a Python function and matches, keep it (deeper match wins).
        if isinstance(candidate, FunctionType):
            if _matches(candidate.__code__, name, target):
                best_match = candidate
            # Walk closure cells to find inner functions.
            closure = candidate.__closure__
            if closure is not None:
                for cell in closure:
                    try:
                        contents = cell.cell_contents
                    except ValueError:
                        # Empty closure cell.
                        continue
                    if callable(contents) or isinstance(contents, functools.partial):
                        queue.append(contents)

        # functools.wraps sets __wrapped__ on any wrapper.
        wrapped = getattr(candidate, "__wrapped__", None)
        if wrapped is not None:
            queue.append(wrapped)

        # functools.partial: walk the inner callable + bound args/kwargs.
        if isinstance(candidate, functools.partial):
            queue.append(candidate.func)
            for arg in candidate.args:
                if callable(arg) or isinstance(arg, functools.partial):
                    queue.append(arg)
            for value in candidate.keywords.values():
                if callable(value) or isinstance(value, functools.partial):
                    queue.append(value)

    return best_match if best_match is not None else func
