# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests: get_code must not clobber a module-level docstring.

The AST transform injects a `PythonServiceEventsMonitor` import at the top of every
instrumented module. A module's __doc__ is populated only when the FIRST statement in
the body is a string-constant expression, so inserting that import at index 0 would push
the docstring out of first position and silently make __doc__ None (breaking
pydoc/help()/doctest/__doc__-based tooling). These tests load a transformed module and
assert the docstring is preserved, while the import is still injected and functional.
"""

import ast
import os
import tempfile
from unittest import TestCase

from amazon.opentelemetry.serviceevents.ast_transformation import (
    ServiceEventsSourceLoader,
    clear_function_registry,
)

MODULE_DOCSTRING = "Top-level module docstring that must survive instrumentation."


class TestGetCodePreservesModuleDocstring(TestCase):
    """get_code injects the monitor import without destroying the module docstring."""

    def setUp(self):
        clear_function_registry()
        self._tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        clear_function_registry()

    def _write(self, name, source):
        path = os.path.join(self._tmpdir, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(source)
        return path

    def _load(self, name, source):
        """Transform + compile + exec the source, returning the populated namespace."""
        path = self._write(f"{name}.py", source)
        code = ServiceEventsSourceLoader(name, path).get_code(name)
        self.assertIsNotNone(code)
        # The injected monitor import must be present in the compiled code's names.
        self.assertIn("PythonServiceEventsMonitor", code.co_names)
        namespace = {}
        # exec runs the (real) injected `from ...python_monitor import PythonServiceEventsMonitor`,
        # so a green exec proves the import is functional, not just syntactically present.
        exec(code, namespace)  # pylint: disable=exec-used
        return namespace

    def test_module_docstring_preserved_with_leading_docstring(self):
        """A module whose first statement is a docstring keeps __doc__ after transform."""
        source = f'"""{MODULE_DOCSTRING}"""\n' "\n" "def foo():\n" '    """func doc"""\n' "    return 1\n"
        namespace = self._load("with_docstring", source)
        # The bug: __doc__ would be None here. The fix preserves the original docstring.
        self.assertEqual(namespace.get("__doc__"), MODULE_DOCSTRING)
        # The injected import is functional and bound in the module namespace.
        self.assertIn("PythonServiceEventsMonitor", namespace)
        # The module's own code still works.
        self.assertEqual(namespace["foo"](), 1)

    def test_no_docstring_still_injects_import_at_index_zero(self):
        """A module with no docstring: import is still injected; __doc__ stays None."""
        source = "def bar():\n    return 2\n"
        namespace = self._load("no_docstring", source)
        # No docstring originally -> __doc__ is None (unchanged by the fix).
        self.assertIsNone(namespace.get("__doc__"))
        self.assertIn("PythonServiceEventsMonitor", namespace)
        self.assertEqual(namespace["bar"](), 2)

    def test_leading_string_after_real_docstring_is_not_confused(self):
        """Only the genuine first-statement docstring counts; later strings are unaffected."""
        source = f'"""{MODULE_DOCSTRING}"""\n' "\n" "x = 1\n" '"a non-docstring string expression"\n'
        namespace = self._load("doc_then_string", source)
        self.assertEqual(namespace.get("__doc__"), MODULE_DOCSTRING)
        self.assertEqual(namespace["x"], 1)
        self.assertIn("PythonServiceEventsMonitor", namespace)

    def test_docstring_then_future_import_keeps_docstring_and_instruments(self):
        """A module with a docstring followed by ``from __future__`` stays valid and instrumented.

        End-to-end through get_code, ``from __future__`` is folded into compiler flags by the
        transformer (visit_ImportFrom), not kept as a body statement, so this asserts the
        observable contract: the docstring survives and the monitor import is injected. The
        placement of the injected import relative to a *retained* ``__future__`` node is covered
        directly in TestPreambleInsertIndex below.
        """
        source = (
            f'"""{MODULE_DOCSTRING}"""\n' "from __future__ import annotations\n" "\n" "def qux():\n" "    return 4\n"
        )
        namespace = self._load("doc_then_future", source)
        self.assertEqual(namespace.get("__doc__"), MODULE_DOCSTRING)
        self.assertIn("PythonServiceEventsMonitor", namespace)
        self.assertEqual(namespace["qux"](), 4)


class TestPreambleInsertIndex(TestCase):
    """Direct unit tests for _preamble_insert_index on raw (un-transformed) AST bodies.

    These exercise the helper on bodies that still contain ``from __future__`` nodes — which
    the real get_code pipeline strips into compiler flags before insertion — so they actually
    cover the ``__future__``-skipping branch that the end-to-end get_code tests cannot reach.
    """

    @staticmethod
    def _index(source):
        return ServiceEventsSourceLoader._preamble_insert_index(ast.parse(source).body)

    def test_empty_body(self):
        self.assertEqual(ServiceEventsSourceLoader._preamble_insert_index([]), 0)

    def test_plain_module_inserts_at_zero(self):
        self.assertEqual(self._index("def f():\n    return 1\n"), 0)

    def test_leading_docstring_inserts_after_it(self):
        self.assertEqual(self._index('"""doc"""\ndef f():\n    return 1\n'), 1)

    def test_skips_single_future_import(self):
        self.assertEqual(self._index("from __future__ import annotations\ndef f():\n    return 1\n"), 1)

    def test_skips_docstring_then_future_import(self):
        self.assertEqual(self._index('"""doc"""\nfrom __future__ import annotations\ndef f():\n    return 1\n'), 2)

    def test_skips_multiple_future_imports(self):
        source = "from __future__ import annotations\nfrom __future__ import division\ndef f():\n    return 1\n"
        self.assertEqual(self._index(source), 2)

    def test_non_future_importfrom_is_not_skipped(self):
        """A regular ``from x import y`` is not preamble; the import goes before it."""
        self.assertEqual(self._index("from os import path\ndef f():\n    return 1\n"), 0)
