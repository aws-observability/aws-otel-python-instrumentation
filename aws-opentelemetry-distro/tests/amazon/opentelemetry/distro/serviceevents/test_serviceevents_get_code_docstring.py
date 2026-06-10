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

import os
import tempfile
from unittest import TestCase

from amazon.opentelemetry.distro.serviceevents.ast_transformation import (
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
