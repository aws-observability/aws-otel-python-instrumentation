# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import ast
import os
import sys
import tempfile
from importlib.machinery import ModuleSpec, SourceFileLoader
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.serviceevents.ast_transformation import (
    ServiceEventsASTTransformer,
    ServiceEventsMetaPathFinder,
    ServiceEventsSourceLoader,
    _file_path_to_module_path,
    build_function_name,
    clear_function_registry,
    get_deployment_event_telemetry,
    get_function_info,
    get_function_info_unlocked,
    get_function_registry,
    get_registry_size,
    install_ast_hooks,
    uninstall_ast_hooks,
)


class TestBuildFunctionName(TestCase):
    """Test composite function name generation."""

    def test_build_function_name_deterministic(self):
        """Test that function name is deterministic for same inputs."""
        name1 = build_function_name("my_func", "/path/to/file.py", 10)
        name2 = build_function_name("my_func", "/path/to/file.py", 10)

        self.assertEqual(name1, name2)

    def test_build_function_name_different_names(self):
        """Test that different function names produce different identifiers."""
        name1 = build_function_name("func1", "/path/to/file.py", 10)
        name2 = build_function_name("func2", "/path/to/file.py", 10)

        self.assertNotEqual(name1, name2)

    def test_build_function_name_different_paths(self):
        """Test that different file paths produce different identifiers."""
        name1 = build_function_name("my_func", "/path/to/file1.py", 10)
        name2 = build_function_name("my_func", "/path/to/file2.py", 10)

        self.assertNotEqual(name1, name2)

    def test_build_function_name_same_for_different_line_numbers(self):
        """Test that different line numbers produce the same name (line stored in registry, not in name)."""
        name1 = build_function_name("my_func", "/path/to/file.py", 10)
        name2 = build_function_name("my_func", "/path/to/file.py", 20)

        self.assertEqual(name1, name2)

    def test_build_function_name_format(self):
        """Test the composite name format."""
        name = build_function_name("my_func", "myapp/server.py", 42)
        self.assertEqual(name, "myapp/server.my_func")

    def test_build_function_name_init_py(self):
        """Test that __init__.py uses parent directory as module stem."""
        name = build_function_name("setup", "myapp/utils/__init__.py", 5)
        self.assertEqual(name, "myapp/utils.setup")

    def test_build_function_name_nested_path(self):
        """Test composite name with deeply nested relative path."""
        name = build_function_name("_process", "indico/modules/attachments/controllers/display/base.py", 36)
        self.assertEqual(name, "indico/modules/attachments/controllers/display/base._process")


class TestFilePathToModulePath(TestCase):
    """Test file path to module path conversion."""

    def test_simple_py_file(self):
        self.assertEqual(_file_path_to_module_path("myapp/server.py"), "myapp/server")

    def test_init_py(self):
        self.assertEqual(_file_path_to_module_path("myapp/utils/__init__.py"), "myapp/utils")

    def test_absolute_path(self):
        self.assertEqual(_file_path_to_module_path("/abs/path/to/module.py"), "/abs/path/to/module")

    def test_no_py_extension(self):
        self.assertEqual(_file_path_to_module_path("myapp/server"), "myapp/server")


class TestToRelativePath(TestCase):
    """Test relative path derivation from module names."""

    def test_module_file(self):
        """Test standard module.py path."""
        result = ServiceEventsSourceLoader._to_relative_path(
            "indico.modules.attachments.controllers.display.base",
            "/opt/indico/indico/modules/attachments/controllers/display/base.py",
        )
        self.assertEqual(result, "indico/modules/attachments/controllers/display/base.py")

    def test_package_init(self):
        """Test package __init__.py path."""
        result = ServiceEventsSourceLoader._to_relative_path(
            "indico.modules.attachments",
            "/opt/indico/indico/modules/attachments/__init__.py",
        )
        self.assertEqual(result, "indico/modules/attachments/__init__.py")

    def test_top_level_module(self):
        """Test top-level module path."""
        result = ServiceEventsSourceLoader._to_relative_path(
            "myapp.server",
            "/home/user/projects/myapp/server.py",
        )
        self.assertEqual(result, "myapp/server.py")

    def test_fallback_when_no_match(self):
        """Test fallback to absolute path when suffix doesn't match."""
        result = ServiceEventsSourceLoader._to_relative_path(
            "some.module",
            "/weird/path/not_matching.py",
        )
        self.assertEqual(result, "/weird/path/not_matching.py")


class TestServiceEventsASTTransformer(TestCase):
    """Test the AST transformation logic."""

    def test_transform_simple_function(self):
        """Test transformation of a simple function."""
        source = """
def my_function():
    return 42
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformed = transformer.visit(tree)

        # Function should still exist
        self.assertEqual(len(transformed.body), 1)
        self.assertIsInstance(transformed.body[0], ast.FunctionDef)

        # Function body should contain a with statement
        func_def = transformed.body[0]
        self.assertEqual(len(func_def.body), 1)
        self.assertIsInstance(func_def.body[0], ast.With)

        # The with statement should call PythonServiceEventsMonitor
        with_stmt = func_def.body[0]
        self.assertIsInstance(with_stmt.items[0].context_expr, ast.Call)
        self.assertEqual(with_stmt.items[0].context_expr.func.id, "PythonServiceEventsMonitor")

    def test_transform_function_with_docstring(self):
        """Test that docstrings are preserved."""
        source = '''
def documented_function():
    """This is a docstring."""
    return 42
'''

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformed = transformer.visit(tree)

        func_def = transformed.body[0]

        # Should have docstring + with statement
        self.assertEqual(len(func_def.body), 2)
        self.assertIsInstance(func_def.body[0], ast.Expr)

        # Verify docstring is preserved
        if sys.version_info >= (3, 8):
            self.assertIsInstance(func_def.body[0].value, ast.Constant)
            self.assertEqual(func_def.body[0].value.value, "This is a docstring.")
        else:
            self.assertIsInstance(func_def.body[0].value, ast.Str)

    def test_transform_async_function(self):
        """Test transformation of async functions and is_async registry flag."""
        clear_function_registry()
        source = """
async def async_function():
    return await something()
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/async_file.py")
        transformed = transformer.visit(tree)

        # Should still be async function
        self.assertIsInstance(transformed.body[0], ast.AsyncFunctionDef)

        # Body should contain with statement
        func_def = transformed.body[0]
        self.assertIsInstance(func_def.body[0], ast.With)

        # Verify is_async=True in registry
        registry = get_function_registry()
        async_entries = [v for v in registry.values() if v["name"] == "async_function"]
        self.assertEqual(len(async_entries), 1)
        self.assertTrue(async_entries[0]["is_async"])

    def test_sync_function_registry_is_async_false(self):
        """Test that sync functions get is_async=False in the registry."""
        clear_function_registry()
        source = """
def sync_function():
    return 42
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/sync_file.py")
        transformer.visit(tree)

        registry = get_function_registry()
        sync_entries = [v for v in registry.values() if v["name"] == "sync_function"]
        self.assertEqual(len(sync_entries), 1)
        self.assertFalse(sync_entries[0]["is_async"])

    def test_transform_empty_function(self):
        """Test transformation of empty function (pass statement)."""
        source = """
def empty_function():
    pass
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformed = transformer.visit(tree)

        func_def = transformed.body[0]
        with_stmt = func_def.body[0]

        # With statement should contain pass
        self.assertEqual(len(with_stmt.body), 1)
        self.assertIsInstance(with_stmt.body[0], ast.Pass)

    def test_transform_nested_functions(self):
        """Test transformation of nested function definitions."""
        source = """
def outer():
    def inner():
        return 42
    return inner()
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformer.visit(tree)

        # Both functions should be instrumented
        self.assertEqual(transformer.instrumented_functions, 2)

    def test_function_names_differ_by_name_not_line(self):
        """Test that different function names produce different composite names."""
        source = """
def func1():
    pass

def func2():
    pass
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformed = transformer.visit(tree)

        # Extract the function names from the transformed AST
        func1_with = transformed.body[0].body[0]
        func2_with = transformed.body[1].body[0]

        func1_name = func1_with.items[0].context_expr.args[0].value
        func2_name = func2_with.items[0].context_expr.args[0].value

        # Names should be different (by function name, not line number)
        self.assertNotEqual(func1_name, func2_name)
        self.assertNotIn("@line:", func1_name)
        self.assertIn("func1", func1_name)
        self.assertIn("func2", func2_name)

    def test_transform_future_imports(self):
        """Test handling of __future__ imports."""
        source = """
from __future__ import annotations

def my_function():
    pass
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformed = transformer.visit(tree)

        # __future__ import should be removed from AST
        # (it's processed for compiler flags but not kept)
        import_nodes = [node for node in ast.walk(transformed) if isinstance(node, ast.ImportFrom)]
        future_imports = [node for node in import_nodes if node.module == "__future__"]

        self.assertEqual(len(future_imports), 0)

    def test_transform_stores_relative_file_path_in_registry(self):
        """Test that the registry stores relative paths when using ServiceEventsSourceLoader."""
        clear_function_registry()
        source = """
def my_function():
    return 42
"""

        tree = ast.parse(source)
        # Simulate an absolute runtime path with a relative path derived from module name
        relative_path = ServiceEventsSourceLoader._to_relative_path(
            "indico.modules.foo.bar", "/opt/indico/indico/modules/foo/bar.py"
        )
        transformer = ServiceEventsASTTransformer(relative_path)
        transformer.visit(tree)

        registry = get_function_registry()
        entries = [v for v in registry.values() if v["name"] == "my_function"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["file_path"], "indico/modules/foo/bar.py")

    def test_instrumented_functions_counter(self):
        """Test that instrumented_functions counter is accurate."""
        source = """
def func1():
    pass

def func2():
    pass

def func3():
    pass
"""

        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/file.py")
        transformer.visit(tree)

        self.assertEqual(transformer.instrumented_functions, 3)


class TestGeneratorSkipping(TestCase):
    """Generators/async-generators must not be wrapped in a monitor `with` block."""

    @staticmethod
    def _transform(source):
        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/gen.py")
        transformed = transformer.visit(tree)
        return transformed, transformer

    def test_yield_generator_not_wrapped(self):
        """A function containing `yield` is left un-instrumented."""
        source = """
def gen():
    yield 1
    yield 2
"""
        transformed, transformer = self._transform(source)
        func_def = transformed.body[0]
        # Body unchanged: no injected `with` statement.
        self.assertNotIsInstance(func_def.body[0], ast.With)
        self.assertEqual(transformer.instrumented_functions, 0)

    def test_yield_from_generator_not_wrapped(self):
        """`yield from` also marks a generator."""
        source = """
def gen():
    yield from range(3)
"""
        transformed, transformer = self._transform(source)
        self.assertNotIsInstance(transformed.body[0].body[0], ast.With)
        self.assertEqual(transformer.instrumented_functions, 0)

    def test_async_generator_not_wrapped(self):
        """An async function with `yield` is an async generator — not wrapped."""
        source = """
async def agen():
    async for x in source():
        yield x
"""
        transformed, transformer = self._transform(source)
        self.assertIsInstance(transformed.body[0], ast.AsyncFunctionDef)
        # No `with` injected at the top of the body.
        self.assertFalse(any(isinstance(stmt, ast.With) for stmt in transformed.body[0].body))
        self.assertEqual(transformer.instrumented_functions, 0)

    def test_function_with_nested_generator_is_still_wrapped(self):
        """A `yield` in a NESTED function belongs to that inner scope; the outer
        non-generator function is still instrumented (and the inner generator is not)."""
        source = """
def outer():
    def inner():
        yield 1
    return inner
"""
        transformed, transformer = self._transform(source)
        outer = transformed.body[0]
        # Outer is a normal function → wrapped.
        self.assertIsInstance(outer.body[0], ast.With)
        # Inner generator (now nested under the with) is NOT wrapped.
        inner = outer.body[0].body[0]
        self.assertIsInstance(inner, ast.FunctionDef)
        self.assertEqual(inner.name, "inner")
        self.assertNotIsInstance(inner.body[0], ast.With)
        # Only the outer function counted as instrumented.
        self.assertEqual(transformer.instrumented_functions, 1)

    def test_function_with_yield_in_lambda_is_not_treated_as_generator(self):
        """A generator expression / lambda nested in the body does not make the
        enclosing function a generator (genexps open their own scope)."""
        source = """
def normal():
    data = (x for x in range(3))
    return list(data)
"""
        transformed, transformer = self._transform(source)
        # genexp has its own scope; `normal` is an ordinary function → wrapped.
        self.assertIsInstance(transformed.body[0].body[0], ast.With)
        self.assertEqual(transformer.instrumented_functions, 1)


class TestScopePrecedence(TestCase):
    """Exercise the scope rule in `ServiceEventsMetaPathFinder.should_instrument_module`.

    There is no implicit default scope: PACKAGES_INCLUDE is the only opt-in and
    PACKAGES_EXCLUDE the only way to subtract. Decision (highest priority first):
      0. Matches SDK_SELF_EXCLUDE (non-configurable) → drop
      1. PACKAGES_INCLUDE empty → drop
      2. Matches PACKAGES_EXCLUDE → drop
      3. Matches PACKAGES_INCLUDE → instrument
      4. Otherwise → drop

    Each test builds a finder with a known include/exclude config and a minimal
    ModuleSpec with a source origin. These rules are only reachable when
    FUNCTION_INSTRUMENT_ENABLED=true (the finder isn't installed otherwise) — the
    finder itself doesn't re-check that flag, so these tests assume it.
    """

    def _spec(self, origin: str = "/app/app.py"):
        from types import SimpleNamespace

        return SimpleNamespace(origin=origin)

    def _finder(self, packages_include=None, packages_exclude=None):
        from amazon.opentelemetry.serviceevents.ast_transformation import ServiceEventsMetaPathFinder

        return ServiceEventsMetaPathFinder(set(packages_include or []), packages_exclude or [])

    # --- Baseline rules 0–4 ---

    def test_empty_include_drops_all(self):
        """Rule 1: empty PACKAGES_INCLUDE → instrument nothing (no implicit default scope)."""
        finder = self._finder()
        self.assertFalse(finder.should_instrument_module("myapp.handler", self._spec()))

    def test_include_match_instruments(self):
        """Rule 3: include match → instrument."""
        finder = self._finder(packages_include=["myapp.*"])
        self.assertTrue(finder.should_instrument_module("myapp.foo", self._spec()))

    def test_include_no_match_drops(self):
        """Rule 4: PACKAGES_INCLUDE set but class doesn't match → drop."""
        finder = self._finder(packages_include=["myapp.*"])
        self.assertFalse(finder.should_instrument_module("other.bar", self._spec()))

    def test_exclude_beats_include(self):
        """Rule 2 wins over rule 3 — PACKAGES_EXCLUDE always wins."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["myapp.internal.*"])
        self.assertFalse(finder.should_instrument_module("myapp.internal.foo", self._spec()))

    def test_self_exclude_holds_under_wildcard_include(self):
        """Rule 0: SDK_SELF_EXCLUDE is non-configurable, even under a wildcard include.

        Uses ``*.*`` (a fnmatch wildcard that survives the validator — bare ``*`` is
        stripped in the config layer, ``*.*`` is not) to prove the self-exclude gate
        runs before include matching.
        """
        finder = self._finder(packages_include=["*.*"])
        self.assertFalse(finder.should_instrument_module("opentelemetry.sdk.trace", self._spec()))
        self.assertFalse(finder.should_instrument_module("amazon.opentelemetry.distro.foo", self._spec()))

    # --- INCLUDE coverage gaps ---

    def test_include_multi_pattern_union(self):
        """Any pattern matching = include."""
        finder = self._finder(packages_include=["myapp.*", "otherapp.*"])
        self.assertTrue(finder.should_instrument_module("otherapp.foo", self._spec()))

    def test_include_with_unrelated_exclude_still_instruments(self):
        """A non-empty exclude that doesn't match must not poison the include."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["otherapp.*"])
        self.assertTrue(finder.should_instrument_module("myapp.foo", self._spec()))

    def test_include_glob_depth_pinning(self):
        """Python fnmatch: ``myapp.*`` matches multi-segment names (``.`` is an ordinary char)."""
        finder = self._finder(packages_include=["myapp.*"])
        self.assertTrue(finder.should_instrument_module("myapp.sub.bar", self._spec()))

    # --- EXCLUDE coverage gaps ---

    def test_empty_include_with_nonempty_exclude_still_drops(self):
        """Rule 1 fires before rule 2 — EXCLUDE alone never opens the gate."""
        finder = self._finder(packages_exclude=["myapp.*"])
        self.assertFalse(finder.should_instrument_module("other.bar", self._spec()))

    def test_exclude_multi_pattern_union(self):
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["myapp.internal.*", "myapp.legacy.*"])
        self.assertFalse(finder.should_instrument_module("myapp.legacy.bar", self._spec()))

    def test_exclude_multi_pattern_unmatched_still_includes(self):
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["myapp.internal.*", "myapp.legacy.*"])
        self.assertTrue(finder.should_instrument_module("myapp.public.baz", self._spec()))

    def test_exclude_no_match_falls_through_to_include(self):
        """A non-empty exclude that doesn't match must not drop the class."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["otherapp.*"])
        self.assertTrue(finder.should_instrument_module("myapp.foo", self._spec()))

    def test_exclude_wins_when_collides_with_include(self):
        """Identical include/exclude patterns → exclude wins (rule 2 beats rule 3)."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["myapp.*"])
        self.assertFalse(finder.should_instrument_module("myapp.foo", self._spec()))

    def test_self_exclude_wins_over_redundant_exclude(self):
        """Rule 0 fires regardless of EXCLUDE — a customer can't 'cover' the SDK via EXCLUDE."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["opentelemetry.*"])
        self.assertFalse(finder.should_instrument_module("opentelemetry.sdk.trace", self._spec()))

    def test_exclude_glob_depth_pinning(self):
        """Python fnmatch: ``myapp.secret.*`` catches multi-segment names below it."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["myapp.secret.*"])
        self.assertFalse(finder.should_instrument_module("myapp.secret.sub.leak", self._spec()))

    # --- Validator/lifecycle gaps ---

    def test_double_star_passes_through_python(self):
        """Python validator strips only exact ``*``, not ``**``; ``**`` reaches fnmatch.

        ``fnmatch('myapp.foo', '**')`` is True (``*`` matches everything including dots),
        so a ``**`` include behaves as match-all here — pins the JS-vs-Python asymmetry.
        """
        finder = self._finder(packages_include=["**"])
        self.assertTrue(finder.should_instrument_module("myapp.foo", self._spec()))
        # ...but rule 0 still wins over the wildcard.
        self.assertFalse(finder.should_instrument_module("opentelemetry.sdk.trace", self._spec()))

    def test_structural_gate_drops_specless_module(self):
        """No spec / no origin / built-in / frozen → drop before any rule runs."""
        from types import SimpleNamespace

        finder = self._finder(packages_include=["*.*"])
        self.assertFalse(finder.should_instrument_module("myapp.foo", None))
        self.assertFalse(finder.should_instrument_module("myapp.foo", SimpleNamespace(origin=None)))
        self.assertFalse(finder.should_instrument_module("sys", SimpleNamespace(origin="built-in")))


class TestFindSpecCrashSafety(TestCase):
    """find_spec runs inside the customer's import machinery; a failure deciding
    whether to instrument a module must never break the customer's import."""

    def _finder(self, packages_include=None, packages_exclude=None):
        from amazon.opentelemetry.serviceevents.ast_transformation import ServiceEventsMetaPathFinder

        return ServiceEventsMetaPathFinder(set(packages_include or ["myapp.*"]), packages_exclude or [])

    def test_find_spec_returns_none_when_should_instrument_raises(self):
        """If should_instrument_module raises, find_spec returns None (defers to default import)."""
        from unittest.mock import patch

        finder = self._finder()
        with patch.object(finder, "should_instrument_module", side_effect=RuntimeError("boom")):
            # Must not raise — returns None so the default machinery loads the module uninstrumented.
            self.assertIsNone(finder.find_spec("myapp.foo", None, None))
        # The recursion-guard set must be cleaned up even on failure.
        self.assertNotIn("myapp.foo", finder._currently_loading)

    def test_find_spec_returns_none_when_find_spec_raises(self):
        """If importlib.util.find_spec raises, our find_spec swallows it and returns None."""
        from unittest.mock import patch

        finder = self._finder()
        with patch("importlib.util.find_spec", side_effect=RuntimeError("import machinery boom")):
            self.assertIsNone(finder.find_spec("myapp.foo", None, None))
        self.assertNotIn("myapp.foo", finder._currently_loading)


class TestRegistryAccessors(TestCase):
    """Cover the registry read accessors that aren't exercised elsewhere."""

    def setUp(self):
        clear_function_registry()

    def tearDown(self):
        clear_function_registry()

    def test_get_function_info_returns_metadata(self):
        """get_function_info returns the stored metadata dict for a known function."""
        name = build_function_name("widget", "pkg/mod.py", 7, is_async=False)
        info = get_function_info(name)
        self.assertIsNotNone(info)
        self.assertEqual(info["name"], "widget")
        self.assertEqual(info["file_path"], "pkg/mod.py")
        self.assertEqual(info["line"], 7)

    def test_get_function_info_missing_returns_none(self):
        """get_function_info returns None for an unknown function name."""
        self.assertIsNone(get_function_info("does/not.exist"))

    def test_get_function_info_unlocked_matches_locked(self):
        """The lock-free read returns the same metadata as the locked read."""
        name = build_function_name("gizmo", "pkg/other.py", 3)
        self.assertEqual(get_function_info_unlocked(name), get_function_info(name))
        self.assertIsNone(get_function_info_unlocked("unknown/name"))

    def test_get_registry_size_tracks_entries(self):
        """get_registry_size reflects the number of registered functions."""
        self.assertEqual(get_registry_size(), 0)
        build_function_name("a", "pkg/m.py", 1)
        build_function_name("b", "pkg/m.py", 2)
        self.assertEqual(get_registry_size(), 2)


class TestDeploymentEventTelemetry(TestCase):
    """Cover the get_deployment_event_telemetry convenience wrapper."""

    def test_returns_deployment_event_dict(self):
        """The wrapper returns a DeploymentEvent telemetry dict with the supplied metadata."""
        result = get_deployment_event_telemetry(
            service_name="my-service",
            environment="test",
            sdk_version="9.9.9",
            pid=4242,
        )
        self.assertIsInstance(result, dict)
        self.assertEqual(result["telemetry_type"], "DeploymentEvent")
        self.assertEqual(result["service_name"], "my-service")
        self.assertEqual(result["environment"], "test")
        self.assertEqual(result["sdk_version"], "9.9.9")
        self.assertEqual(result["pid"], 4242)


class TestTransformerEdgeCases(TestCase):
    """Cover branches in the transformer not hit by the main transform tests."""

    def setUp(self):
        clear_function_registry()

    def tearDown(self):
        clear_function_registry()

    def test_get_and_remove_docstring_empty_body_returns_none(self):
        """get_and_remove_docstring returns None when the function body is empty."""
        node = ast.FunctionDef(
            name="empty",
            args=ast.arguments(
                posonlyargs=[], args=[], vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]
            ),
            body=[],
            decorator_list=[],
            returns=None,
            type_comment=None,
        )
        self.assertIsNone(ServiceEventsASTTransformer.get_and_remove_docstring(node))

    def test_get_with_location_from_empty_body_uses_node_location(self):
        """With an empty body, the with-location falls back to the node's own position."""
        node = ast.FunctionDef(
            name="empty",
            args=ast.arguments(
                posonlyargs=[], args=[], vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]
            ),
            body=[],
            decorator_list=[],
            returns=None,
            type_comment=None,
            lineno=11,
            col_offset=4,
        )
        loc = ServiceEventsASTTransformer.get_with_location_from_node(node)
        self.assertEqual(loc["lineno"], 11)
        self.assertEqual(loc["col_offset"], 4)

    def test_docstring_only_function_gets_pass_body(self):
        """A function whose only statement is a docstring keeps the docstring and gets a `pass` body.

        Exercises the empty-body branch: after the docstring is removed, the
        `with` wraps an injected `ast.Pass` and the location falls back to the node.
        """
        source = '''
def doc_only():
    """just a docstring"""
'''
        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/doc_only.py")
        transformed = transformer.visit(tree)

        func_def = transformed.body[0]
        # Docstring preserved as first statement, with-statement second.
        self.assertEqual(len(func_def.body), 2)
        self.assertIsInstance(func_def.body[0], ast.Expr)
        with_stmt = func_def.body[1]
        self.assertIsInstance(with_stmt, ast.With)
        # Empty body was replaced by a single `pass`.
        self.assertEqual(len(with_stmt.body), 1)
        self.assertIsInstance(with_stmt.body[0], ast.Pass)
        self.assertEqual(transformer.instrumented_functions, 1)

    def test_non_future_import_from_is_preserved(self):
        """A non-__future__ ImportFrom is recursed into and kept in the tree."""
        source = """
from os import path
"""
        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/imp.py")
        transformed = transformer.visit(tree)

        import_nodes = [node for node in transformed.body if isinstance(node, ast.ImportFrom)]
        self.assertEqual(len(import_nodes), 1)
        self.assertEqual(import_nodes[0].module, "os")
        # No compiler flags set for a non-__future__ import.
        self.assertEqual(transformer.compiler_flags, 0)

    def test_future_import_sets_compiler_flags(self):
        """A __future__ import sets the transformer's compiler_flags and is dropped."""
        source = """
from __future__ import annotations
"""
        tree = ast.parse(source)
        transformer = ServiceEventsASTTransformer("/test/fut.py")
        transformer.visit(tree)
        self.assertNotEqual(transformer.compiler_flags, 0)


class TestSourceLoaderGetCode(TestCase):
    """Cover ServiceEventsSourceLoader.get_code transformation and fallbacks."""

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

    def test_get_code_transforms_and_compiles(self):
        """A valid source file is parsed, transformed and compiled to a code object."""
        path = self._write("good.py", "def foo():\n    return 1\n")
        loader = ServiceEventsSourceLoader("good", path)
        code = loader.get_code("good")
        self.assertIsNotNone(code)
        # The injected monitor import surfaces in the compiled code's constants/names.
        self.assertIn("PythonServiceEventsMonitor", code.co_names)
        # The function was registered during transformation.
        registry = get_function_registry()
        self.assertTrue(any(entry["name"] == "foo" for entry in registry.values()))

    def test_get_code_syntax_error_falls_back_to_super(self):
        """On a SyntaxError during parse, get_code defers to the default loader."""
        path = self._write("ok.py", "def foo():\n    return 1\n")
        loader = ServiceEventsSourceLoader("ok", path)
        sentinel = object()
        with patch.object(SourceFileLoader, "get_code", return_value=sentinel):
            with patch(
                "amazon.opentelemetry.serviceevents.ast_transformation.ast.parse",
                side_effect=SyntaxError("bad"),
            ):
                self.assertIs(loader.get_code("ok"), sentinel)

    def test_get_code_unexpected_error_falls_back_to_super(self):
        """Any non-syntax transformation failure also defers to the default loader."""
        path = self._write("ok2.py", "def foo():\n    return 1\n")
        loader = ServiceEventsSourceLoader("ok2", path)
        sentinel = object()
        with patch.object(SourceFileLoader, "get_code", return_value=sentinel):
            with patch(
                "amazon.opentelemetry.serviceevents.ast_transformation.ast.parse",
                side_effect=RuntimeError("boom"),
            ):
                self.assertIs(loader.get_code("ok2"), sentinel)


class TestNameCouldMatch(TestCase):
    """Cover the cheap name-only pre-filter _name_could_match."""

    def _finder(self, packages_include=None, packages_exclude=None):
        return ServiceEventsMetaPathFinder(set(packages_include or []), packages_exclude or [])

    def test_empty_include_returns_false(self):
        """Rule 1: empty include → never a candidate."""
        self.assertFalse(self._finder()._name_could_match("myapp.foo"))

    def test_self_exclude_returns_false(self):
        """Rule 0: SDK self-exclusion drops candidates by name alone."""
        finder = self._finder(packages_include=["*.*"])
        self.assertFalse(finder._name_could_match("opentelemetry.sdk.trace"))
        self.assertFalse(finder._name_could_match("amazon.opentelemetry.distro.foo"))

    def test_include_match_returns_true(self):
        """Rule 3 (name part): a matching include pattern makes it a candidate."""
        finder = self._finder(packages_include=["myapp.*"])
        self.assertTrue(finder._name_could_match("myapp.foo"))

    def test_no_include_match_returns_false(self):
        """No include pattern matches the name → not a candidate."""
        finder = self._finder(packages_include=["myapp.*"])
        self.assertFalse(finder._name_could_match("other.bar"))


class TestFindSpecMainPath(TestCase):
    """Cover the happy-path and structural-skip branches of find_spec."""

    def _finder(self, packages_include=None, packages_exclude=None):
        return ServiceEventsMetaPathFinder(set(packages_include or ["myapp.*"]), packages_exclude or [])

    def test_find_spec_replaces_loader_for_py_module(self):
        """A matching .py module gets its loader swapped for ServiceEventsSourceLoader."""
        finder = self._finder()
        fake_spec = ModuleSpec("myapp.foo", loader=None, origin="/app/myapp/foo.py")
        with patch("importlib.util.find_spec", return_value=fake_spec):
            result = finder.find_spec("myapp.foo", None, None)
        self.assertIs(result, fake_spec)
        self.assertIsInstance(result.loader, ServiceEventsSourceLoader)
        self.assertNotIn("myapp.foo", finder._currently_loading)

    def test_find_spec_returns_none_when_spec_is_none(self):
        """If the default machinery yields no spec, find_spec returns None."""
        finder = self._finder()
        with patch("importlib.util.find_spec", return_value=None):
            self.assertIsNone(finder.find_spec("myapp.missing", None, None))
        self.assertNotIn("myapp.missing", finder._currently_loading)

    def test_find_spec_skips_non_python_origin(self):
        """A non-.py origin (e.g. extension module) is left to the default loader."""
        finder = self._finder()
        fake_spec = ModuleSpec("myapp.ext", loader=None, origin="/app/myapp/ext.so")
        with patch("importlib.util.find_spec", return_value=fake_spec):
            self.assertIsNone(finder.find_spec("myapp.ext", None, None))

    def test_find_spec_skips_module_excluded_by_should_instrument(self):
        """A name candidate that should_instrument_module rejects returns None."""
        finder = self._finder(packages_include=["myapp.*"], packages_exclude=["myapp.foo"])
        fake_spec = ModuleSpec("myapp.foo", loader=None, origin="/app/myapp/foo.py")
        with patch("importlib.util.find_spec", return_value=fake_spec):
            self.assertIsNone(finder.find_spec("myapp.foo", None, None))

    def test_find_spec_recursion_guard_returns_none(self):
        """A module already mid-load short-circuits to None (recursion guard)."""
        finder = self._finder()
        finder._currently_loading.add("myapp.loop")
        try:
            self.assertIsNone(finder.find_spec("myapp.loop", None, None))
        finally:
            finder._currently_loading.discard("myapp.loop")

    def test_find_spec_name_prefilter_skips_unmatched(self):
        """A name that can't match the include is dropped before spec resolution."""
        finder = self._finder()
        with patch("importlib.util.find_spec", side_effect=AssertionError("should not resolve")):
            self.assertIsNone(finder.find_spec("stdlib.thing", None, None))


class TestInstallUninstallHooks(TestCase):
    """Cover install_ast_hooks / uninstall_ast_hooks, restoring sys.meta_path."""

    def setUp(self):
        self._saved_meta_path = list(sys.meta_path)

    def tearDown(self):
        sys.meta_path[:] = self._saved_meta_path

    def test_install_inserts_finder_at_front(self):
        """install_ast_hooks inserts a finder at position 0 with the given config."""
        install_ast_hooks({"myapp.*"}, ["myapp.internal.*"])
        finder = sys.meta_path[0]
        self.assertIsInstance(finder, ServiceEventsMetaPathFinder)
        self.assertEqual(finder.packages_include, {"myapp.*"})
        self.assertEqual(finder.packages_exclude, ["myapp.internal.*"])

    def test_install_defaults_to_empty_config(self):
        """With no args, the installed finder gets an empty include set and exclude list."""
        install_ast_hooks()
        finder = sys.meta_path[0]
        self.assertIsInstance(finder, ServiceEventsMetaPathFinder)
        self.assertEqual(finder.packages_include, set())
        self.assertEqual(finder.packages_exclude, [])

    def test_uninstall_removes_all_finders(self):
        """uninstall_ast_hooks removes every ServiceEventsMetaPathFinder from sys.meta_path."""
        install_ast_hooks({"a.*"})
        install_ast_hooks({"b.*"})
        self.assertTrue(any(isinstance(f, ServiceEventsMetaPathFinder) for f in sys.meta_path))
        uninstall_ast_hooks()
        self.assertFalse(any(isinstance(f, ServiceEventsMetaPathFinder) for f in sys.meta_path))

    def test_uninstall_is_idempotent(self):
        """Calling uninstall when no finder is installed is a no-op."""
        uninstall_ast_hooks()
        uninstall_ast_hooks()
        self.assertFalse(any(isinstance(f, ServiceEventsMetaPathFinder) for f in sys.meta_path))
