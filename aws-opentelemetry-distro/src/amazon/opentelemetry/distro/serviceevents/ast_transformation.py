# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
AST transformation for automatic function instrumentation.

This module implements Python's import hook mechanism to automatically wrap
all functions with ServiceEvents monitoring context managers during module loading.
"""

import ast
import fnmatch
import importlib
import importlib.abc
import importlib.util
import logging
import os
import sys
import threading
import types
from importlib.machinery import ModuleSpec, SourceFileLoader
from typing import Any, Dict, List, Optional, Sequence, Set, TypeVar, Union

logger = logging.getLogger(__name__)

# =============================================================================
# Function Registry - Thread-safe mapping of function_name -> function metadata
# =============================================================================

# Global registry storing composite function_name -> {name, file_path, line} mappings
_function_registry: Dict[str, Dict[str, Any]] = {}
_registry_lock = threading.Lock()

FunctionDef = TypeVar("FunctionDef", ast.FunctionDef, ast.AsyncFunctionDef)


def _file_path_to_module_path(file_path: str) -> str:
    """Convert a file path to a module-style path for function naming.

    Strips the .py extension and handles __init__.py by using the parent directory.

    Examples:
        'indico/modules/foo/bar.py' → 'indico/modules/foo/bar'
        'indico/modules/attachments/__init__.py' → 'indico/modules/attachments'
        '/absolute/path/to/server.py' → '/absolute/path/to/server'
    """
    # Normalize path separators to forward slashes
    path = file_path.replace(os.sep, "/")

    # Strip .py extension
    if path.endswith(".py"):
        path = path[:-3]

    # Handle __init__ files: use parent directory
    if path.endswith("/__init__"):
        path = path[:-9]

    return path


def build_function_name(function_name: str, file_path: str, lineno: int, is_async: bool = False) -> str:
    """
    Build a composite function identifier for ServiceEvents instrumentation.

    Creates a human-readable identifier from the module path and function name.
    Also registers the function in the global registry for later export.

    Format: "relative/path/to/module.function_name"

    The file_path is typically already a relative path when coming through
    the ServiceEventsSourceLoader import hooks (via _to_relative_path).

    Args:
        function_name: Name of the function
        file_path: Path to source file (relative when coming through import hooks)
        lineno: Line number where function is defined (stored in registry, not in name)
        is_async: Whether the function is an async function definition

    Returns:
        Composite function name (e.g., "myapp/server.my_func")

    Example:
        >>> build_function_name("my_func", "myapp/server.py", 42)
        "myapp/server.my_func"
    """
    module_path = _file_path_to_module_path(file_path)
    composite_name = f"{module_path}.{function_name}"

    # Register the function in the global registry (thread-safe)
    with _registry_lock:
        _function_registry[composite_name] = {
            "function_name": composite_name,
            "name": function_name,
            "file_path": file_path,
            "line": lineno,
            "is_async": is_async,
        }

    return composite_name


def get_function_registry() -> Dict[str, Dict[str, Any]]:
    """
    Get a copy of the current function registry.

    Returns:
        Dictionary mapping composite function_name to function metadata.
    """
    with _registry_lock:
        return dict(_function_registry)


def get_function_info(function_name: str) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a specific function.

    Args:
        function_name: The composite function name (e.g., "myapp/server.my_func").

    Returns:
        Function metadata dict or None if not found.
    """
    with _registry_lock:
        return _function_registry.get(function_name)


def get_function_info_unlocked(function_name: str) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a specific function WITHOUT acquiring _registry_lock.

    This is a best-effort, lock-free read intended for the hot path where
    lock contention is unacceptable. It is safe because:

    - CPython protects internal dict structures with per-object locks on both
      GIL and free-threaded (PEP 703) builds, so dict.get() observes a
      consistent state even under concurrent writes.
    - _function_registry entries are immutable once written (during module import).
    - The worst case is a momentary None for a function whose import hasn't
      completed yet — callers must tolerate a None return gracefully.

    Use this in performance-critical paths (e.g., __enter__/__exit__ of the
    monitor context manager). Use get_function_info() when correctness under
    concurrent writes matters (e.g., during module loading or in tests).

    Args:
        function_name: The composite function name (e.g., "myapp/server.my_func").

    Returns:
        Function metadata dict or None if not found (best-effort).
    """
    return _function_registry.get(function_name)


def get_registry_size() -> int:
    """Get the number of functions in the registry."""
    with _registry_lock:
        return len(_function_registry)


def get_deployment_event_telemetry(
    service_name: str = "unknown-service",
    environment: Optional[str] = None,
    sdk_version: str = "0.14.2",
    pid: Optional[int] = None,
    resource_attributes=None,
) -> Dict[str, Any]:
    """
    Get a deployment event as a telemetry event dict.

    Returns a dict with telemetry_type="DeploymentEvent" containing
    deployment metadata (git commit, CI/CD info).

    Args:
        service_name: Service name for the telemetry metadata.
        environment: Environment name for the telemetry metadata.
        sdk_version: SDK version string.
        pid: Process ID (defaults to current process).
        resource_attributes: Optional ResourceAttributes from OTel Resource detectors.

    Returns:
        Dict containing the deployment event telemetry.
    """
    # Lazy import to avoid a circular dependency with the models module.
    # pylint: disable-next=import-outside-toplevel
    from amazon.opentelemetry.distro.serviceevents.models import DeploymentEventTelemetry

    telemetry = DeploymentEventTelemetry.create(
        service_name=service_name,
        environment=environment,
        sdk_version=sdk_version,
        pid=pid,
        include_deployment_context=True,
        resource_attributes=resource_attributes,
    )

    return telemetry.to_dict()


def clear_function_registry():
    """Clear the function registry (mainly for testing)."""
    with _registry_lock:
        _function_registry.clear()


class ServiceEventsASTTransformer(ast.NodeTransformer):
    """
    AST transformer that wraps function bodies with PythonServiceEventsMonitor context manager.

    Transforms:
        def my_function(args):
            # body

    Into:
        def my_function(args):
            with PythonServiceEventsMonitor("module/path.my_function"):
                # body
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.compiler_flags = 0
        self.instrumented_functions = 0

    @staticmethod
    def get_and_remove_docstring(
        node: Union[ast.FunctionDef, ast.AsyncFunctionDef],
    ) -> Optional[ast.stmt]:
        """If the first expression in the function is a docstring, remove and return it."""
        # Mirrors an ast type name (Constant/Str); kept CamelCase to match the ast API.
        AstStrType = ast.Constant if sys.version_info >= (3, 8) else ast.Str  # pylint: disable=invalid-name

        if not node.body:
            return None

        if (
            isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, AstStrType)
            and (
                isinstance(node.body[0].value.value, str)  # type: ignore[attr-defined]
                if sys.version_info >= (3, 8)
                else isinstance(node.body[0].value.s, str)  # type: ignore[attr-defined]
            )
        ):
            return node.body.pop(0)
        return None

    @staticmethod
    def get_with_location_from_node(node: FunctionDef) -> dict:
        """Extract location info from AST node for the with statement."""
        if len(node.body) == 0:
            return {
                "lineno": node.lineno,
                "col_offset": node.col_offset,
                "end_lineno": getattr(node, "end_lineno", node.lineno),
                "end_col_offset": getattr(node, "end_col_offset", node.col_offset),
            }

        return {
            "lineno": node.body[0].lineno,
            "col_offset": node.body[0].col_offset,
            "end_lineno": getattr(node.body[0], "end_lineno", node.body[0].lineno),
            "end_col_offset": getattr(node.body[0], "end_col_offset", node.body[0].col_offset),
        }

    def get_with_stmt(self, function_name: str, node: FunctionDef) -> ast.With:
        """Create the 'with PythonServiceEventsMonitor(function_name):' statement."""
        locations = self.get_with_location_from_node(node)

        args = [ast.Constant(value=function_name, kind=None, **locations)]  # type: List[ast.expr]
        return ast.With(
            items=[
                ast.withitem(
                    context_expr=ast.Call(
                        func=ast.Name(id="PythonServiceEventsMonitor", ctx=ast.Load(), **locations),
                        args=args,
                        keywords=[],
                        **locations,
                    ),
                )
            ],
            body=[],
            type_comment=None,
            **locations,
        )

    @staticmethod
    def _is_generator(node: FunctionDef) -> bool:
        """Return True if this function is a (sync or async) generator.

        A function is a generator when its own body contains a `yield` or `yield from`.
        We must NOT descend into nested function/lambda bodies: a `yield` inside a nested
        function makes *that* inner function a generator, not this one. So this walks the
        function's own scope only, treating nested def/async-def/lambda as opaque.
        """
        # Seed the stack with the function's direct children, skipping the node itself
        # (whose own `yield`, if any, cannot exist at the def level).
        stack: List[ast.AST] = list(ast.iter_child_nodes(node))
        while stack:
            child = stack.pop()
            if isinstance(child, (ast.Yield, ast.YieldFrom)):
                return True
            # A nested function/lambda opens a new scope; any yield inside it belongs to
            # that inner scope, so do not descend.
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                continue
            stack.extend(ast.iter_child_nodes(child))
        return False

    # Name mirrors the ast.NodeTransformer visitor naming convention (visit_FunctionDef).
    def _visit_generic_FunctionDef(self, node: FunctionDef) -> FunctionDef:  # pylint: disable=invalid-name
        """Generic function transformation for both sync and async functions."""
        # Skip generators / async generators. Wrapping a generator body in a `with` would
        # bind the monitor's lifetime to the generator object: __enter__ runs on first
        # advance and __exit__ only at exhaustion or GC, so the recorded "duration" would
        # span the consumer's iteration (not the function's work) and the call-stack push
        # would leak across yield boundaries, corrupting caller attribution. Leave them
        # un-instrumented and recurse so nested non-generator functions still get wrapped.
        if self._is_generator(node):
            self.generic_visit(node)
            return node

        self.instrumented_functions += 1

        # Build composite function name
        is_async = isinstance(node, ast.AsyncFunctionDef)
        function_name = build_function_name(
            function_name=node.name,
            file_path=self.file_path,
            lineno=node.lineno,
            is_async=is_async,
        )

        # Extract and preserve docstring
        docstring = self.get_and_remove_docstring(node)

        # Create with statement
        with_stmt = self.get_with_stmt(function_name, node)
        with_stmt.body = node.body

        # Handle empty function bodies
        if not with_stmt.body:
            with_stmt.body = [ast.Pass(**self.get_with_location_from_node(node))]

        # Reconstruct function body with docstring (if any) and with statement
        if docstring is not None:
            node.body = [docstring, with_stmt]
        else:
            node.body = [with_stmt]

        # Continue visiting nested nodes
        self.generic_visit(node)
        return node

    # Required visitor name from the ast.NodeTransformer visitor API.
    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:  # pylint: disable=invalid-name
        """Transform synchronous function definitions."""
        return self._visit_generic_FunctionDef(node)

    # Required visitor name from the ast.NodeTransformer visitor API.
    # pylint: disable-next=invalid-name
    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AsyncFunctionDef:
        """Transform asynchronous function definitions."""
        return self._visit_generic_FunctionDef(node)

    # Required visitor name from the ast.NodeTransformer visitor API.
    # pylint: disable-next=invalid-name
    def visit_ImportFrom(self, node: ast.ImportFrom) -> Optional[ast.ImportFrom]:
        """
        Handle __future__ imports to set compiler flags.
        When passing an AST to compile(), __future__ imports need special handling.
        """
        if node.module == "__future__":
            # Lazy import: __future__ is only needed to resolve compiler flags here.
            import __future__  # pylint: disable=import-outside-toplevel

            for name in node.names:
                feature = getattr(__future__, name.name)
                self.compiler_flags |= feature.compiler_flag
            return None

        self.generic_visit(node)
        return node


class ServiceEventsSourceLoader(SourceFileLoader):
    """Custom source loader that applies AST transformation before loading modules."""

    @staticmethod
    def _to_relative_path(fullname: str, source_path: str) -> str:
        """Derive a relative file path from the module's fully qualified name.

        Uses the module name to strip the installation prefix from the absolute
        path, producing a path like ``indico/modules/foo/bar.py`` that can be
        matched against local source trees regardless of where the package is
        installed.
        """
        # module.py pattern (e.g. "indico/modules/foo/bar.py")
        module_suffix = fullname.replace(".", os.sep) + ".py"
        if source_path.endswith(os.sep + module_suffix):
            return module_suffix

        # package/__init__.py pattern
        package_suffix = fullname.replace(".", os.sep) + os.sep + "__init__.py"
        if source_path.endswith(os.sep + package_suffix):
            return package_suffix

        # Fallback: return the original path unchanged
        return source_path

    def get_code(self, fullname: str) -> Optional[types.CodeType]:
        """Load and transform the module's source code."""
        source_path = self.get_filename(fullname)

        try:
            # Read source file
            with open(source_path, "rb") as source_file:
                source_bytes = source_file.read()

            # Parse source to AST
            try:
                tree = ast.parse(source_bytes, filename=source_path)
            except SyntaxError:
                # If AST parsing fails, fall back to default behavior
                return super().get_code(fullname)

            # Use relative path for telemetry so paths match local source trees
            relative_path = self._to_relative_path(fullname, source_path)

            # Transform AST
            transformer = ServiceEventsASTTransformer(relative_path)
            transformed_tree = transformer.visit(tree)

            # Fix missing locations in AST nodes
            ast.fix_missing_locations(transformed_tree)

            # Inject PythonServiceEventsMonitor import at the top of the module
            import_node = ast.ImportFrom(
                module="amazon.opentelemetry.distro.serviceevents.python_monitor",
                names=[ast.alias(name="PythonServiceEventsMonitor", asname=None)],
                level=0,
            )
            ast.fix_missing_locations(import_node)
            transformed_tree.body.insert(0, import_node)

            # Compile transformed AST to code object
            code = compile(
                transformed_tree,
                source_path,
                "exec",
                flags=transformer.compiler_flags,
                dont_inherit=True,
            )

            return code

        # Crash-safety: any transformation failure must fall back to normal loading.
        except Exception:  # pylint: disable=broad-exception-caught
            # On any error, fall back to default behavior
            return super().get_code(fullname)


# SDK self-exclusion (SDK_SELF_EXCLUDE) — the non-configurable safety boundary.
# These prefixes cover the entire ADOT distro and OpenTelemetry itself; a customer
# cannot opt them back in via PACKAGES_INCLUDE. Instrumenting them would cause import
# cycles (the distro's own modules) or infinite recursion in the tracing pipeline
# (every signal emit re-enters instrumentation). Matched by module `fullname`
# (exact-or-dotted-prefix), so it catches OTel/distro modules even when installed in
# site-packages — making a path-based gate unnecessary.
#
#   - "amazon.opentelemetry" — the entire ADOT distro (it installs under the single
#     `amazon` src root, so every distro submodule is caught by this one prefix).
#   - "opentelemetry" — OTel API/SDK + contrib, and the lambda-layer's vendored
#     `opentelemetry/` tree (caught regardless of install path).
SDK_SELF_EXCLUDE: List[str] = [
    "amazon.opentelemetry",
    "opentelemetry",
]


class ServiceEventsMetaPathFinder(importlib.abc.MetaPathFinder):
    """
    Meta path finder that intercepts module imports and applies AST transformation.

    This is installed in sys.meta_path to automatically instrument all imported modules.
    """

    def __init__(self, packages_include: Set[str], packages_exclude: List[str]):
        self.packages_include = packages_include
        self.packages_exclude = packages_exclude
        self._currently_loading: Set[str] = set()

    # pylint: disable-next=too-many-return-statements
    def should_instrument_module(self, fullname: str, spec: Optional[ModuleSpec]) -> bool:
        """Determine if a module should be instrumented.

        There is no implicit default scope: PACKAGES_INCLUDE is the only way to opt in
        and PACKAGES_EXCLUDE is the only way to subtract. The decision (highest priority
        first):

          0. Matches SDK_SELF_EXCLUDE (non-configurable) → drop
          1. PACKAGES_INCLUDE is empty → drop (no implicit default scope)
          2. Matches PACKAGES_EXCLUDE → drop
          3. Matches PACKAGES_INCLUDE → instrument
          4. Otherwise → drop

        Plus the structural gates that precede rule 0: no spec / no origin / built-in /
        frozen → drop (those modules have no source file to transform).
        """
        # Structural gates: nothing to transform.
        if spec is None or spec.origin is None:
            return False
        if spec.origin in ("built-in", "frozen"):
            return False

        # Rule 0: SDK self-exclusion (non-configurable absolute gate, before rule 1).
        for prefix in SDK_SELF_EXCLUDE:
            if fullname == prefix or fullname.startswith(f"{prefix}."):
                return False

        # Rule 1: no implicit default scope — empty include means instrument nothing.
        if not self.packages_include:
            return False

        # Rule 2: exclude always wins over include.
        for pattern in self.packages_exclude:
            if fnmatch.fnmatch(fullname, pattern):
                return False

        # Rule 3: include match → instrument.
        for pattern in self.packages_include:
            if fnmatch.fnmatch(fullname, pattern) or fullname == pattern or fullname.startswith(f"{pattern}."):
                return True

        # Rule 4: no include match → drop.
        return False

    def _name_could_match(self, fullname: str) -> bool:
        """Cheap name-only pre-filter applied before the expensive spec resolution.

        find_spec sits at the front of sys.meta_path and is consulted for EVERY import in
        the process — overwhelmingly stdlib/OTel/third-party modules that can never match
        the allowlist. Those decisions depend only on the module name, so resolve them here
        (string checks) and skip the costly importlib.util.find_spec call for them. This is
        a strict subset of should_instrument_module's name-based rules (0, 1, 3); the full
        check still runs afterward on survivors with the resolved spec for the structural
        and exclude rules, so the decision is unchanged — only cheaper on the common path.
        """
        # Rule 1: empty include → instrument nothing (no need to resolve anything).
        if not self.packages_include:
            return False
        # Rule 0: SDK self-exclusion — never instrument the distro/OTel.
        for prefix in SDK_SELF_EXCLUDE:
            if fullname == prefix or fullname.startswith(f"{prefix}."):
                return False
        # Rule 3 (name part): must match at least one include pattern to be a candidate.
        for pattern in self.packages_include:
            if fnmatch.fnmatch(fullname, pattern) or fullname == pattern or fullname.startswith(f"{pattern}."):
                return True
        return False

    # pylint: disable-next=too-many-return-statements
    def find_spec(
        self,
        fullname: str,
        _path: Optional[Sequence[str]],
        _target: Optional[types.ModuleType] = None,
    ) -> Optional[ModuleSpec]:
        """Find and potentially modify the module spec to use our custom loader."""
        # Prevent infinite recursion
        if fullname in self._currently_loading:
            return None

        # Cheap name-only gate before the expensive spec resolution: the vast majority of
        # imports can't match the allowlist, and that's decidable from the name alone.
        if not self._name_could_match(fullname):
            return None

        self._currently_loading.add(fullname)
        try:
            # Get the original spec using the default import machinery
            spec = importlib.util.find_spec(fullname)

            if spec is None:
                return None

            # Check if we should instrument this module
            if not self.should_instrument_module(fullname, spec):
                return None

            # Only instrument source files (.py)
            if not (spec.origin and spec.origin.endswith(".py")):
                return None

            # Replace loader with our custom loader
            spec.loader = ServiceEventsSourceLoader(fullname, spec.origin)

            return spec

        # Crash-safety: a failure deciding whether/how to instrument a module
        # must never break the customer's import. Returning None defers to the
        # default import machinery, leaving the module uninstrumented.
        except Exception:  # pylint: disable=broad-exception-caught
            return None
        finally:
            self._currently_loading.discard(fullname)


def install_ast_hooks(packages_include: Optional[Set[str]] = None, packages_exclude: Optional[List[str]] = None):
    """
    Install AST transformation hooks into sys.meta_path.

    Args:
        packages_include: Set of module patterns to instrument (empty = instrument nothing)
        packages_exclude: List of fnmatch patterns for modules to exclude (always wins)
    """
    if packages_include is None:
        packages_include = set()

    if packages_exclude is None:
        packages_exclude = []

    finder = ServiceEventsMetaPathFinder(packages_include, packages_exclude)

    # Insert at the beginning of sys.meta_path to have priority
    sys.meta_path.insert(0, finder)


def uninstall_ast_hooks():
    """Remove ServiceEvents AST hooks from sys.meta_path."""
    sys.meta_path = [finder for finder in sys.meta_path if not isinstance(finder, ServiceEventsMetaPathFinder)]
