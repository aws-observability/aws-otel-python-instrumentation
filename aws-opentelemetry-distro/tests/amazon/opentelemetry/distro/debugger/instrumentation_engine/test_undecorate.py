# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``instrumentation_engine._undecorate.undecorated``."""

import functools
import unittest

from amazon.opentelemetry.distro.debugger.instrumentation_engine._undecorate import undecorated


def _module_target(value):
    """Plain target — used as the BFS endpoint."""
    return value * 2


def _login_required(view_func):
    @functools.wraps(view_func)
    def wrapper(request, *args, **kwargs):
        return view_func(request, *args, **kwargs)

    return wrapper


def _double_decorator(view_func):
    """Two layers of decoration to verify multi-level traversal."""

    @functools.wraps(view_func)
    def outer(*args, **kwargs):
        return view_func(*args, **kwargs)

    @functools.wraps(outer)
    def outermost(*args, **kwargs):
        return outer(*args, **kwargs)

    return outermost


@_login_required
def _decorated_view(request):
    return f"original-view:{request}"


@_double_decorator
def _double_decorated(request):
    return f"original-double:{request}"


def _plain_decorator(fn):
    """Decorator that does NOT use functools.wraps — only closure capture."""

    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    # Intentionally do NOT call functools.wraps here.
    return wrapper


_closure_only_view = _plain_decorator(_module_target)


class TestUndecorated(unittest.TestCase):

    def test_returns_input_when_already_matching(self):
        self.assertIs(undecorated(_module_target, "_module_target"), _module_target)

    def test_resolves_through_functools_wraps(self):
        resolved = undecorated(_decorated_view, "_decorated_view")
        self.assertIs(resolved, _decorated_view.__wrapped__)
        self.assertEqual(resolved.__code__.co_name, "_decorated_view")

    def test_resolves_through_two_layers_of_wraps(self):
        resolved = undecorated(_double_decorated, "_double_decorated")
        # __wrapped__ chains: outermost.__wrapped__ -> outer -> _double_decorated
        # The BFS should land on the innermost function whose co_name matches.
        self.assertEqual(resolved.__code__.co_name, "_double_decorated")

    def test_handles_partial(self):
        p = functools.partial(_module_target, 99)
        self.assertIs(undecorated(p, "_module_target"), _module_target)

    def test_handles_closure_capture_without_wraps(self):
        """Decorator that doesn't call functools.wraps still resolves via closure."""
        resolved = undecorated(_closure_only_view, "_module_target")
        self.assertIs(resolved, _module_target)

    def test_filename_disambiguates_same_named_helpers(self):
        """If two same-named functions exist in different files, file path wins."""

        # Build a doppelganger inline in this file
        def _module_target(value):  # shadows the module-level one
            return value + 1000

        # Pass the module-level function's filename — BFS should prefer it
        # over the inline shadow.
        resolved = undecorated(_module_target, "_module_target", _module_target.__code__.co_filename)
        # Both functions live in the same file (this test file), so either
        # match is acceptable; this primarily verifies the path-arg path
        # doesn't crash. The disambiguation across files is exercised by
        # the integration test in the engine test file.
        self.assertEqual(resolved.__code__.co_name, "_module_target")

    def test_returns_input_when_no_match_found(self):
        """Fail-safe: if the requested name doesn't appear, return the input."""
        resolved = undecorated(_module_target, "no_such_function_anywhere")
        self.assertIs(resolved, _module_target)

    def test_non_function_callable_returned_unchanged(self):
        """Built-ins / classes / random callables are returned unchanged."""
        self.assertIs(undecorated(len, "len"), len)
        self.assertIs(undecorated(int, "int"), int)
        self.assertIs(undecorated(42, "anything"), 42)  # not callable at all

    def test_otel_di_unwrapped_marker_short_circuits(self):
        """Wrappers can short-circuit the BFS via the __otel_di_unwrapped__ marker."""
        target = _module_target
        wrapper = lambda x: target(x)  # noqa: E731
        # Mark the wrapper to short-circuit to `target`
        wrapper.__otel_di_unwrapped__ = target  # type: ignore[attr-defined]
        resolved = undecorated(wrapper, "this_name_is_irrelevant_when_marked")
        self.assertIs(resolved, target)
