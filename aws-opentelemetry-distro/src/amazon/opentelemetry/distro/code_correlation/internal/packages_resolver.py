# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Package discovery and classification module for OpenTelemetry code correlation.

This module provides utilities to:
- Classify Python code as standard library, third-party, or user code
- Map file paths to their corresponding Python packages
- Cache package metadata for performance optimization
- Support code correlation features in AWS OpenTelemetry distribution
"""

import logging
import sys
import sysconfig
import threading
from functools import lru_cache, wraps
from inspect import FullArgSpec, getfullargspec, isgeneratorfunction
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Union

from ..config import AwsCodeAttributesConfig

# Module-level constants
_logger = logging.getLogger(__name__)

# Configuration
_code_attributes_config = AwsCodeAttributesConfig.from_env()

# Global caching variables
_sys_path_hash: Optional[int] = None
_resolved_sys_path: List[Path] = []
_sys_path_lock = threading.Lock()

# Standard library paths (computed once at module load)
_STDLIB_PATH = Path(sysconfig.get_path("stdlib")).resolve()
_PLATSTDLIB_PATH = Path(sysconfig.get_path("platstdlib")).resolve()
_PURELIB_PATH = Path(sysconfig.get_path("purelib")).resolve()
_PLATLIB_PATH = Path(sysconfig.get_path("platlib")).resolve()


class Distribution(NamedTuple):
    """Represents a Python distribution with name and version."""

    name: str
    version: str


def _validate_void_function(func: Callable, argspec: FullArgSpec) -> bool:
    """Check if a function has no arguments or special characteristics."""
    return not (
        argspec.args
        or argspec.varargs
        or argspec.varkw
        or argspec.defaults
        or argspec.kwonlyargs
        or argspec.kwonlydefaults
        or isgeneratorfunction(func)
    )


def execute_once(func: Callable) -> Callable:
    """
    Decorator that ensures a function is executed only once.

    Args:
        func: Function to be decorated (must have no arguments)

    Returns:
        Wrapped function that caches its result

    Raises:
        ValueError: If the function has arguments
    """
    argspec = getfullargspec(func)
    if not _validate_void_function(func, argspec):
        raise ValueError("The execute_once decorator can only be applied to functions with no arguments")

    @wraps(func)
    def wrapper() -> Any:
        try:
            result, exception = func.__execute_once_result__  # type: ignore[attr-defined]
        except AttributeError:
            try:
                result = func()
                exception = None
            except Exception as error:  # pylint: disable=broad-exception-caught
                result = None
                exception = error
            func.__execute_once_result__ = result, exception  # type: ignore[attr-defined]

        if exception is not None:
            raise exception

        return result

    return wrapper


def _determine_effective_root(relative_path: Path, parent_path: Path) -> str:
    """
    Determine the effective root module for a given path.

    Args:
        relative_path: Path relative to the parent
        parent_path: Parent directory path

    Returns:
        Root module name
    """
    base_name = relative_path.parts[0]
    root_dir = parent_path / base_name

    if root_dir.is_dir() and (root_dir / "__init__.py").exists():
        return base_name
    return str(Path(*relative_path.parts[:2]))


def _resolve_system_paths() -> List[Path]:
    """
    Resolve and cache system paths from sys.path.

    Uses double-checked locking for thread safety while maintaining performance.

    Returns:
        List of resolved Path objects from sys.path
    """
    global _sys_path_hash, _resolved_sys_path  # pylint: disable=global-statement

    current_hash = hash(tuple(sys.path))

    # Fast path: check without lock (common case when no update needed)
    if current_hash == _sys_path_hash:
        return _resolved_sys_path

    # Slow path: acquire lock and double-check
    with _sys_path_lock:
        # Double-check inside lock in case another thread already updated
        if current_hash != _sys_path_hash:
            _sys_path_hash = current_hash
            _resolved_sys_path = [Path(path).resolve() for path in sys.path]

    return _resolved_sys_path


@lru_cache(maxsize=256)
def _extract_root_module_name(file_path: Path) -> str:
    """
    Extract the root module name from a file path.

    Args:
        file_path: Path to the Python file

    Returns:
        Root module name

    Raises:
        ValueError: If root module cannot be determined
    """
    # Try standard library paths first (most common case)
    for parent_path in (_PURELIB_PATH, _PLATLIB_PATH):
        try:
            relative_path = file_path.resolve().relative_to(parent_path)
            return _determine_effective_root(relative_path, parent_path)
        except ValueError:
            continue

    # Try sys.path resolution with shortest relative path priority
    shortest_relative = None
    best_parent = None

    for parent_path in _resolve_system_paths():
        try:
            relative_path = file_path.relative_to(parent_path)
            if shortest_relative is None or len(relative_path.parents) < len(shortest_relative.parents):
                shortest_relative = relative_path
                best_parent = parent_path
        except ValueError:
            continue

    if shortest_relative is not None and best_parent is not None:
        try:
            return _determine_effective_root(shortest_relative, best_parent)
        except IndexError:
            pass

    raise ValueError(f"Could not determine root module for path: {file_path}")


@execute_once
def _build_package_mapping() -> Optional[Dict[str, Distribution]]:
    """
    Build mapping from root modules to their distributions.

    Returns:
        Dictionary mapping module names to Distribution objects, or None if failed
    """
    try:
        import importlib.metadata as importlib_metadata  # pylint: disable=import-outside-toplevel

        # Cache for namespace package detection
        namespace_cache: Dict[str, bool] = {}

        def is_namespace_package(package_file: importlib_metadata.PackagePath) -> bool:
            """Check if a package file belongs to a namespace package."""
            root = package_file.parts[0]

            if root in namespace_cache:
                return namespace_cache[root]

            if len(package_file.parts) < 2:
                namespace_cache[root] = False
                return False

            located_file = package_file.locate()
            if located_file is None:
                namespace_cache[root] = False
                return False

            parent_dir = Path(located_file).parents[len(package_file.parts) - 2]
            is_namespace = parent_dir.is_dir() and not (parent_dir / "__init__.py").exists()

            namespace_cache[root] = is_namespace
            return is_namespace

        package_mapping = {}

        for distribution in importlib_metadata.distributions():
            files = distribution.files
            if not files:
                continue

            metadata = distribution.metadata
            dist_info = Distribution(name=metadata["name"], version=metadata["version"])

            for file_path in files:
                root_module = file_path.parts[0]

                # Skip metadata directories
                if root_module.endswith((".dist-info", ".egg-info")) or root_module == "..":
                    continue

                # Handle namespace packages
                if is_namespace_package(file_path):
                    root_module = "/".join(file_path.parts[:2])

                # Only add if not already present (first distribution wins)
                if root_module not in package_mapping:
                    package_mapping[root_module] = dist_info

        return package_mapping

    except Exception:  # pylint: disable=broad-exception-caught
        _logger.warning(
            "Failed to build package mapping. Please report this issue to "
            "https://github.com/aws/aws-otel-python-instrumentation/issues",
            exc_info=True,
        )
        return None


@execute_once
def _load_third_party_packages() -> Set[str]:
    """
    Load the set of third-party package names from configuration.

    Returns:
        Set of third-party package names
    """
    try:
        from importlib.resources import read_text  # pylint: disable=import-outside-toplevel

        # Load package list from text file
        content = read_text("amazon.opentelemetry.distro.code_correlation.internal", "3rd.txt")
        package_names = set(content.splitlines())

        # Apply configuration overrides
        configured_packages = (package_names | set(_code_attributes_config.include)) - set(
            _code_attributes_config.exclude
        )

        return configured_packages

    except Exception:  # pylint: disable=broad-exception-caught
        _logger.warning("Failed to load third-party packages configuration", exc_info=True)
        return set()


@lru_cache(maxsize=20000)
def resolve_package_from_filename(filename: Union[str, Path]) -> Optional[Distribution]:
    """
    Resolve a Python distribution from a file path.

    Args:
        filename: Path to the Python file (string or Path object)

    Returns:
        Distribution object if found, None otherwise
    """
    package_mapping = _build_package_mapping()
    if package_mapping is None:
        return None

    try:
        file_path = Path(filename) if isinstance(filename, str) else filename
        root_module = _extract_root_module_name(file_path)

        # Try exact module match first
        if root_module in package_mapping:
            _logger.debug("Found distribution by exact match: %s", root_module)
            return package_mapping[root_module]

        # Try distribution name match as fallback
        for distribution in package_mapping.values():
            if distribution.name == root_module:
                _logger.debug("Found distribution by name match: %s", distribution.name)
                return distribution

        _logger.debug("No distribution found for module: %s", root_module)
        return None

    except (ValueError, OSError) as error:
        _logger.debug("Error resolving package for %s: %s", filename, error)
        return None


@lru_cache(maxsize=256)
def is_standard_library(file_path: Path) -> bool:
    """
    Check if a file path belongs to the Python standard library.

    Args:
        file_path: Path to check

    Returns:
        True if the path is in the standard library, False otherwise
    """
    resolved_path = file_path
    if not resolved_path.is_absolute() or resolved_path.is_symlink():
        resolved_path = resolved_path.resolve()

    # Check if in standard library paths but not in site-packages
    is_in_stdlib = resolved_path.is_relative_to(_STDLIB_PATH) or resolved_path.is_relative_to(_PLATSTDLIB_PATH)

    is_in_site_packages = resolved_path.is_relative_to(_PURELIB_PATH) or resolved_path.is_relative_to(_PLATLIB_PATH)

    return is_in_stdlib and not is_in_site_packages


@lru_cache(maxsize=256)
def is_third_party_package(file_path: Path) -> bool:
    """
    Check if a file path belongs to a third-party package.

    Args:
        file_path: Path to check

    Returns:
        True if the path belongs to a third-party package, False otherwise
    """
    distribution = resolve_package_from_filename(file_path)
    if distribution is None:
        return False

    third_party_packages = _load_third_party_packages()
    return distribution.name in third_party_packages


@lru_cache(maxsize=1024)
def is_user_code(file_path: str) -> bool:
    """
    Check if a file path represents user code (not stdlib or third-party).

    Args:
        file_path: Path to check as string

    Returns:
        True if the path represents user code, False otherwise
    """
    path_obj = Path(file_path)
    return not (is_standard_library(path_obj) or is_third_party_package(path_obj))
