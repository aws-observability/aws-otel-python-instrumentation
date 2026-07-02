# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Crash-safety helpers for ServiceEvents code that runs on the customer's
request thread.

Invariant: ServiceEvents telemetry must NEVER propagate an exception into the
customer application's control flow. A telemetry defect may lose its own data,
but it must not crash, fail, or alter the result of a customer request.

``never_raises`` wraps a synchronous request-thread hook so that any exception
raised inside it is swallowed and a safe value is returned to the caller. The
failure is intentionally silent — on telemetry failure we lose telemetry, not
the request. Apply it uniformly to every hook that runs on a customer-observable
path so a future hook cannot reintroduce the gap.
"""

import functools
from typing import Any, Callable, TypeVar

_F = TypeVar("_F", bound=Callable[..., Any])


def never_raises(return_value: Any = None) -> Callable[[_F], _F]:
    """Decorate a sync request-thread hook so it can never raise into customer code.

    On any ``Exception`` the wrapped call returns ``return_value`` instead of
    propagating. ``BaseException`` (KeyboardInterrupt, SystemExit) is left to
    propagate so process-level signals still work.

    Args:
        return_value: Value to return when the wrapped hook raises. Defaults to
            None. Use a passthrough (e.g. the response object) for hooks whose
            return value matters to the framework — but when the safe return
            value is computed inside the body, prefer an inline try/except and
            keep the ``return`` outside it.
    """

    def deco(fn: _F) -> _F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            # This decorator exists precisely to swallow everything; telemetry must never crash the host app.
            except Exception:  # pylint: disable=broad-exception-caught
                return return_value

        return wrapper  # type: ignore[return-value]

    return deco
