# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=too-many-lines
# pylint: disable=broad-exception-caught

"""
Function wrapper for runtime function modification with comprehensive error handling.

This module provides functionality to dynamically modify function behavior at runtime
by replacing function objects with OpenTelemetry-instrumented wrappers.

Key principles:
- Never raise exceptions that could break user applications
- Graceful degradation on errors
- Comprehensive logging for debugging
"""

import functools
import importlib
import inspect
import logging
import os
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, Tuple, Type, Union

from amazon.opentelemetry.distro.debugger._data_models import (
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_STRING_LENGTH,
    CaptureConfig,
)
from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    CapturedThrowable,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    StackFrame,
    ThreadInfo,
    TraceContext,
)
from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer
from amazon.opentelemetry.distro.debugger._stack_utils import capture_stack_frames, is_internal_frame

logger = logging.getLogger(__name__)


class MethodType(Enum):
    """Enumeration of different method types that can be instrumented."""

    STATIC = "static"
    CLASS = "class"
    INSTANCE = "instance"


@dataclass
class MethodInfo:
    """Information about a discovered class method for instrumentation."""

    method: Callable
    class_obj: Type
    class_name: str
    method_name: str
    method_type: MethodType
    module_name: str

    @property
    def full_name(self) -> str:
        """Get the full method specification (module.ClassName.method_name)."""
        return f"{self.module_name}.{self.class_name}.{self.method_name}"


# Global snapshot emitter instance (set by InstrumentationManager)
_snapshot_emitter = None


def get_snapshot_emitter():
    """Get the global snapshot OTLP emitter instance."""
    return _snapshot_emitter


def set_snapshot_emitter(emitter):
    """Set the global snapshot OTLP emitter instance."""
    global _snapshot_emitter  # pylint: disable=global-statement
    if _snapshot_emitter is not None and _snapshot_emitter is not emitter:
        try:
            _snapshot_emitter.shutdown()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("Error shutting down previous emitter", exc_info=True)
    _snapshot_emitter = emitter


class _FlaskPatcher:
    """Patches Flask app.view_functions entries that reference the original function."""

    name = "flask"

    def patch(self, module, original_func: Callable, new_func: Callable) -> None:
        FunctionWrapper._patch_flask_view_functions(module, original_func, new_func)


class _DjangoPatcher:
    """Patches Django URLPattern.callback entries reachable from the resolver tree."""

    name = "django"

    def patch(self, module, original_func: Callable, new_func: Callable) -> None:
        FunctionWrapper._patch_django_url_patterns(module, original_func, new_func)


# Registry the dispatcher iterates. Adding a new framework: write a small
# class with ``name`` and ``patch(module, original, new)``, and append an
# instance here.
_FRAMEWORK_PATCHERS = (_FlaskPatcher(), _DjangoPatcher())


class FunctionWrapper:
    """
    Handles runtime modification of function objects with comprehensive error handling.

    Produces Snapshot objects (JSON) instead of OTel Spans.

    CRITICAL: All methods must handle errors gracefully and never crash user applications.
    """

    def __init__(self):
        """Initialize the FunctionWrapper."""
        logger.debug("FunctionWrapper initialized")

    @staticmethod
    def _build_serializer(capture_config: Optional[CaptureConfig]) -> SnapshotSerializer:
        return SnapshotSerializer(
            max_fields=capture_config.max_fields_per_object if capture_config else DEFAULT_MAX_FIELDS_PER_OBJECT,
            max_string_length=capture_config.max_string_length if capture_config else DEFAULT_MAX_STRING_LENGTH,
            max_depth=capture_config.max_object_depth if capture_config else 3,
            max_collection_size=capture_config.max_collection_width if capture_config else 10,
        )

    def instrument_function(  # pylint: disable=too-many-arguments
        self,
        module_name: str,
        function_name: str,
        capture_config: Optional[CaptureConfig] = None,
        location_hash: Optional[str] = None,
        manager=None,
    ) -> Tuple[Callable, Callable]:
        """
        Instrument a function by replacing it with a snapshot-producing wrapper.

        Supports both module-level functions and class methods.

        CRITICAL: This method can raise exceptions (caught by InstrumentationManager).

        Args:
            module_name: Name of the module containing the function
            function_name: Name of the function to instrument (may include class path)
            capture_config: Optional configuration for parameter and return value capture
            location_hash: Optional LocationHash to attach to the snapshot
            manager: Optional InstrumentationManager for hit count checking

        Returns:
            Tuple of (original_function, instrumented_function)

        Raises:
            ImportError: If the module cannot be imported
            AttributeError: If the function doesn't exist in the module
            RuntimeError: If instrumentation fails for any other reason
        """
        try:
            # Discover the target function (returns Callable or MethodInfo)
            discovered = FunctionWrapper._discover_function(module_name, function_name)

            # Extract the actual callable
            method_type: Optional[MethodType] = None
            if isinstance(discovered, MethodInfo):
                # Class method - use the method from MethodInfo
                original_func = discovered.method
                method_type = discovered.method_type
                logger.debug(
                    "Instrumenting %s method: %s",
                    discovered.method_type.value,
                    discovered.full_name,
                )
            else:
                # Regular function
                original_func = discovered

            # Create the instrumented wrapper
            instrumented_func = self._create_wrapper(original_func, capture_config, module_name, location_hash, manager)

            # Re-apply the descriptor type so instance access binds correctly.
            if method_type == MethodType.STATIC:
                installed: Any = staticmethod(instrumented_func)
                original_descriptor: Any = staticmethod(original_func)
            elif method_type == MethodType.CLASS:
                installed = classmethod(instrumented_func)
                original_descriptor = classmethod(original_func)
            else:
                installed = instrumented_func
                original_descriptor = original_func

            # Replace the function in the module
            FunctionWrapper._replace_function_in_module(module_name, function_name, installed)

            logger.debug("Successfully instrumented function: %s.%s", module_name, function_name)
            return original_descriptor, instrumented_func

        except (ImportError, AttributeError):
            # Re-raise known exceptions for manager to handle
            raise
        except Exception as exception:
            # Wrap unexpected exceptions
            error_msg = f"Failed to instrument function " f"{module_name}.{function_name}: {exception}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from exception

    @staticmethod
    def restore_function(module_name: str, function_name: str, original_func: Callable) -> bool:
        """
        Restore a function or class method to its original implementation.

        Supports both module-level functions and class methods.
        Also restores framework-level references (e.g., Flask view_functions).

        CRITICAL: Never raises exceptions.

        Args:
            module_name: Name of the module containing the function
            function_name: Name of the function to restore (may include class path)
            original_func: Original function implementation to restore

        Returns:
            True if restoration was successful, False otherwise
        """
        try:
            module = FunctionWrapper._resolve_module(module_name)

            # Check if it's a class method (contains '.')
            if "." in function_name:
                # Class method restoration
                parts = function_name.split(".")
                method_name = parts[-1]
                class_path = parts[:-1]

                # Navigate to the class
                current_obj = module
                for class_name in class_path:
                    current_obj = getattr(current_obj, class_name)

                # Restore the method in the class
                setattr(current_obj, method_name, original_func)
                logger.debug("Successfully restored class method: %s.%s", module_name, function_name)
            else:
                # Get the current (wrapped) function before restoring
                wrapped_func = getattr(module, function_name, None)

                # Module-level function restoration
                setattr(module, function_name, original_func)
                logger.debug("Successfully restored function: %s.%s", module_name, function_name)

                # Restore framework-level references back to the original function
                if wrapped_func is not None:
                    FunctionWrapper._patch_framework_references(module, wrapped_func, original_func)

            return True
        except Exception as exception:
            logger.error(
                "Failed to restore function %s.%s: %s",
                module_name,
                function_name,
                exception,
                exc_info=True,
            )
            return False

    @staticmethod
    def _discover_function(module_name: str, function_name: str) -> Union[Callable, MethodInfo]:
        """
        Discover and return a function or class method from a specified module.

        Supports both module-level functions and class methods:
        - Module function: 'my_function'
        - Class method: 'MyClass.my_method'
        - Nested class: 'OuterClass.InnerClass.method'

        Args:
            module_name: Name of the module containing the function
            function_name: Name of the function to discover (may include class path)

        Returns:
            The function object (for module functions) or MethodInfo (for class methods)

        Raises:
            ImportError: If the module cannot be imported
            AttributeError: If the function/method doesn't exist
        """
        try:
            module = FunctionWrapper._resolve_module(module_name)
            logger.debug("Successfully imported module: %s", module_name)
        except ImportError as exception:
            logger.error("Failed to import module '%s': %s", module_name, exception)
            raise

        # Check if it's a class method (contains '.')
        if "." in function_name:
            return FunctionWrapper._discover_class_method(module, module_name, function_name)

        # Regular module-level function
        try:
            function = getattr(module, function_name)
            if not callable(function):
                raise AttributeError(f"'{function_name}' is not callable")
            logger.debug("Successfully discovered function: %s.%s", module_name, function_name)
            return function
        except AttributeError as exception:
            logger.error(
                "Function '%s' not found in module '%s': %s",
                function_name,
                module_name,
                exception,
            )
            raise

    @staticmethod
    def _discover_class_method(module, module_name: str, function_name: str) -> MethodInfo:
        """
        Discover a class method from a module.

        Handles nested classes and inheritance through MRO.

        Args:
            module: Module object
            module_name: Module name for MethodInfo
            function_name: Class method path (e.g., 'MyClass.method' or 'Outer.Inner.method')

        Returns:
            MethodInfo with method details

        Raises:
            AttributeError: If class or method not found
        """
        try:
            # Parse: 'ClassName.method_name' or 'Outer.Inner.method_name'
            parts = function_name.split(".")
            method_name = parts[-1]
            class_path = parts[:-1]  # ['ClassName'] or ['Outer', 'Inner']

            # Navigate to the class (handle nested classes)
            current_obj = module
            for class_name in class_path:
                current_obj = getattr(current_obj, class_name)
                if not inspect.isclass(current_obj):
                    raise AttributeError(f"'{class_name}' is not a class")

            class_obj = current_obj

            # Get the method
            if not hasattr(class_obj, method_name):
                raise AttributeError(f"Method '{method_name}' not found in " f"class '{class_obj.__name__}'")

            method = getattr(class_obj, method_name)
            if not callable(method):
                raise AttributeError(f"'{method_name}' is not callable")

            # Find which class in MRO actually defines this method
            defining_class = FunctionWrapper._find_defining_class(class_obj, method_name)

            if defining_class is not class_obj:
                raise AttributeError(
                    f"Method '{method_name}' not found as a declared method on class "
                    f"'{class_obj.__name__}' (inherited from '{defining_class.__name__}')"
                )

            # Detect method type (static, class, instance)
            method_type = FunctionWrapper._detect_method_type(defining_class, method_name)

            if method_type == MethodType.CLASS:
                method = getattr(method, "__func__", method)

            logger.debug(
                "Successfully discovered %s method: %s.%s",
                method_type.value,
                module_name,
                function_name,
            )

            return MethodInfo(
                method=method,
                class_obj=defining_class,
                class_name=defining_class.__name__,
                method_name=method_name,
                method_type=method_type,
                module_name=module_name,
            )

        except AttributeError as exception:
            logger.error("Failed to discover class method '%s': %s", function_name, exception)
            raise

    @staticmethod
    def _find_defining_class(class_obj: Type, method_name: str) -> Type:
        """
        Find the class in MRO that actually defines a method.

        Args:
            class_obj: Class to start searching from
            method_name: Name of the method

        Returns:
            Class that defines the method
        """
        for cls in class_obj.__mro__:
            if method_name in cls.__dict__:
                logger.debug("Method '%s' defined in class '%s'", method_name, cls.__name__)
                return cls

        # Fallback to original class if not found in __dict__ (shouldn't happen)
        return class_obj

    @staticmethod
    def _detect_method_type(class_obj: Type, method_name: str) -> MethodType:
        """
        Detect if a method is static, class, or instance method.

        Args:
            class_obj: Class containing the method
            method_name: Name of the method

        Returns:
            MethodType enum value
        """
        # Get the raw method descriptor from class __dict__
        raw_method = class_obj.__dict__.get(method_name)

        if raw_method is None:
            # Method might be inherited, check MRO
            for cls in class_obj.__mro__:
                if method_name in cls.__dict__:
                    raw_method = cls.__dict__[method_name]
                    break

        if raw_method is None:
            logger.warning("Could not find method '%s' in __dict__, assuming instance", method_name)
            return MethodType.INSTANCE

        # Check descriptor type
        if isinstance(raw_method, staticmethod):
            return MethodType.STATIC
        if isinstance(raw_method, classmethod):
            return MethodType.CLASS
        return MethodType.INSTANCE

    def _create_wrapper(  # pylint: disable=too-many-arguments
        self,
        original_func: Callable,
        capture_config: Optional[CaptureConfig],
        module_name: str,
        location_hash: Optional[str] = None,
        manager=None,
    ) -> Callable:
        """
        Create instrumented wrapper that handles both sync and async functions.

        Args:
            original_func: The original function to wrap
            capture_config: Optional configuration for data capture
            module_name: Module name for snapshot metadata
            location_hash: Optional location hash for instrumentation ID
            manager: Optional InstrumentationManager for hit count checking

        Returns:
            Instrumented wrapper function
        """
        if inspect.iscoroutinefunction(original_func):
            return self._create_async_wrapper(original_func, capture_config, module_name, location_hash, manager)
        return self._create_sync_wrapper(original_func, capture_config, module_name, location_hash, manager)

    def _create_sync_wrapper(  # pylint: disable=too-many-arguments,too-many-statements
        self,
        original_func: Callable,
        capture_config: Optional[CaptureConfig],
        module_name: str,
        location_hash: Optional[str] = None,
        manager=None,
    ) -> Callable:
        """Create synchronous wrapper that produces Snapshots instead of Spans."""
        wrapper_self = self

        def sync_wrapper(*args, **kwargs):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
            """
            CRITICAL: This wrapper must never crash user's function.
            All instrumentation errors are caught and logged.
            """
            # Check if all temporary breakpoints are disabled
            line0_breakpoint_key = None
            instr_type = None
            qualified_name = FunctionWrapper._get_qualified_name(original_func)
            # Defensive local snapshot of manager for clarity. The actual safety
            # against shutdown races is provided by the try/except below.
            mgr = manager
            if mgr:
                func_key = f"{module_name}.{qualified_name}"
                try:
                    with mgr._lock:  # pylint: disable=protected-access
                        bp_set = mgr._active_functions.get(func_key)
                        if bp_set:
                            if bp_set.states:
                                has_permanent = 0 in bp_set.breakpoints
                                if not has_permanent and all(s.is_disabled for s in bp_set.states.values()):
                                    return original_func(*args, **kwargs)
                            if 0 in bp_set.breakpoints:
                                line0_breakpoint_key = f"{func_key}:0"
                                instr_type = bp_set.breakpoints[0].instrumentation_type
                except Exception:
                    pass

            # Record entry time
            start_ns = time.time_ns()

            # Increment hit count and check rate limit
            # Only check rate limit for function-level (line0) breakpoints here.
            # Line-level breakpoints have their own rate limiting in the engine.
            capture_allowed = True
            if line0_breakpoint_key and mgr:
                try:
                    capture_allowed = mgr.increment_hit_count(line0_breakpoint_key)
                except Exception:
                    pass

            # If function-level breakpoint is rate-limited or disabled, skip capture
            # but still call original function (line-level engine may still fire)
            has_function_level_bp = line0_breakpoint_key is not None

            if has_function_level_bp and not capture_allowed:
                return original_func(*args, **kwargs)

            # Capture entry context (arguments) — only for function-level breakpoints
            entry_context = None
            if has_function_level_bp and capture_config and capture_config.capture_arguments is not None:
                try:
                    entry_context = wrapper_self._capture_entry_context(original_func, args, kwargs, capture_config)
                except Exception as exc:
                    logger.warning("Failed to capture entry context: %s", exc)

            # Call original function
            result = None
            thrown = None
            thrown_caller_stack = None
            try:
                result = original_func(*args, **kwargs)
            except Exception as exception:
                thrown = exception
                try:
                    thrown_caller_stack = traceback.extract_stack()
                except Exception:
                    pass
                raise
            finally:
                # Build and emit snapshot — only for function-level breakpoints
                if has_function_level_bp:
                    try:
                        duration_ns = time.time_ns() - start_ns
                        return_context = None
                        if capture_config and (capture_config.capture_return or thrown):
                            return_context = wrapper_self._capture_return_context(
                                result, thrown, capture_config, thrown_caller_stack
                            )

                        snapshot = wrapper_self._build_snapshot(
                            module_name=module_name,
                            qualified_name=qualified_name,
                            original_func=original_func,
                            location_hash=location_hash,
                            duration_ns=duration_ns,
                            entry_context=entry_context,
                            return_context=return_context,
                            capture_config=capture_config,
                            instrumentation_type=instr_type,
                        )
                        wrapper_self._emit_snapshot(snapshot)
                    except Exception as exc:
                        logger.warning("Failed to build/emit snapshot: %s", exc)

            return result

        # Apply functools.wraps safely — exotic descriptors on original_func could throw
        try:
            functools.update_wrapper(sync_wrapper, original_func)
        except Exception:
            sync_wrapper.__name__ = getattr(original_func, "__name__", "wrapped")
            sync_wrapper.__wrapped__ = original_func

        return sync_wrapper

    def _create_async_wrapper(  # pylint: disable=too-many-arguments,too-many-statements
        self,
        original_func: Callable,
        capture_config: Optional[CaptureConfig],
        module_name: str,
        location_hash: Optional[str] = None,
        manager=None,
    ) -> Callable:
        """Create asynchronous wrapper that produces Snapshots instead of Spans."""
        wrapper_self = self

        async def async_wrapper(
            *args, **kwargs
        ):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
            """
            CRITICAL: This wrapper must never crash user's async function.
            All instrumentation errors are caught and logged.
            """
            # Check if all temporary breakpoints are disabled
            line0_breakpoint_key = None
            instr_type = None
            qualified_name = FunctionWrapper._get_qualified_name(original_func)
            # Defensive local snapshot of manager for clarity. The actual safety
            # against shutdown races is provided by the try/except below.
            mgr = manager
            if mgr:
                func_key = f"{module_name}.{qualified_name}"
                try:
                    with mgr._lock:  # pylint: disable=protected-access
                        bp_set = mgr._active_functions.get(func_key)
                        if bp_set:
                            if bp_set.states:
                                has_permanent = 0 in bp_set.breakpoints
                                if not has_permanent and all(s.is_disabled for s in bp_set.states.values()):
                                    return await original_func(*args, **kwargs)
                            if 0 in bp_set.breakpoints:
                                line0_breakpoint_key = f"{func_key}:0"
                                instr_type = bp_set.breakpoints[0].instrumentation_type
                except Exception:
                    pass

            start_ns = time.time_ns()

            # Increment hit count and check rate limit
            # Only check rate limit for function-level (line0) breakpoints here.
            capture_allowed = True
            if line0_breakpoint_key and mgr:
                try:
                    capture_allowed = mgr.increment_hit_count(line0_breakpoint_key)
                except Exception:
                    pass

            has_function_level_bp = line0_breakpoint_key is not None

            if has_function_level_bp and not capture_allowed:
                return await original_func(*args, **kwargs)

            # Capture entry context (arguments) — only for function-level breakpoints
            entry_context = None
            if has_function_level_bp and capture_config and capture_config.capture_arguments is not None:
                try:
                    entry_context = wrapper_self._capture_entry_context(original_func, args, kwargs, capture_config)
                except Exception as exc:
                    logger.warning("Failed to capture entry context: %s", exc)

            # Call original async function
            result = None
            thrown = None
            thrown_caller_stack = None
            try:
                result = await original_func(*args, **kwargs)
            except Exception as exception:
                thrown = exception
                try:
                    thrown_caller_stack = traceback.extract_stack()
                except Exception:
                    pass
                raise
            finally:
                if has_function_level_bp:
                    try:
                        duration_ns = time.time_ns() - start_ns
                        return_context = None
                        if capture_config and (capture_config.capture_return or thrown):
                            return_context = wrapper_self._capture_return_context(
                                result, thrown, capture_config, thrown_caller_stack
                            )

                        snapshot = wrapper_self._build_snapshot(
                            module_name=module_name,
                            qualified_name=qualified_name,
                            original_func=original_func,
                            location_hash=location_hash,
                            duration_ns=duration_ns,
                            entry_context=entry_context,
                            return_context=return_context,
                            capture_config=capture_config,
                            instrumentation_type=instr_type,
                        )
                        wrapper_self._emit_snapshot(snapshot)
                    except Exception as exc:
                        logger.warning("Failed to build/emit snapshot: %s", exc)

            return result

        # Apply functools.wraps safely — exotic descriptors on original_func could throw
        try:
            functools.update_wrapper(async_wrapper, original_func)
        except Exception:
            async_wrapper.__name__ = getattr(original_func, "__name__", "wrapped")
            async_wrapper.__wrapped__ = original_func

        return async_wrapper

    @staticmethod
    def _resolve_module(module_name: str):
        """
        Resolve a module by name, handling the __main__ module edge case.

        When a script is run directly (e.g., ``python3 demo_app.py``), it is
        loaded as ``__main__``.  ``importlib.import_module("demo_app")`` would
        create a *second*, independent copy of the module, so any monkey-
        patching on that copy would be invisible to the running application.

        This helper checks whether ``__main__`` corresponds to *module_name*
        and, if so, returns the ``__main__`` module object instead.
        """
        main_module = sys.modules.get("__main__")
        if main_module is not None:
            # Match by __spec__.name (most reliable)
            spec = getattr(main_module, "__spec__", None)
            if spec and spec.name == module_name:
                return main_module
            # Fallback: match by filename stem
            main_file = getattr(main_module, "__file__", None)
            if main_file:
                stem = os.path.splitext(os.path.basename(main_file))[0]
                if stem == module_name:
                    return main_module
        return importlib.import_module(module_name)

    @staticmethod
    def _replace_function_in_module(module_name: str, function_name: str, new_func: Callable) -> None:
        """
        Replace a function or class method in a module's namespace.

        Supports both module-level functions and class methods.
        Also patches framework-level references (e.g., Flask view_functions)
        that hold direct references to the original function object.

        Args:
            module_name: Name of the module containing the function
            function_name: Name of the function to replace (may include class path)
            new_func: New function to replace the original with

        Raises:
            ImportError: If the module cannot be imported
            AttributeError: If the function doesn't exist
        """
        try:
            module = FunctionWrapper._resolve_module(module_name)

            # Check if it's a class method (contains '.')
            if "." in function_name:
                # Class method replacement
                parts = function_name.split(".")
                method_name = parts[-1]
                class_path = parts[:-1]

                # Navigate to the class
                current_obj = module
                for class_name in class_path:
                    current_obj = getattr(current_obj, class_name)

                # Verify method exists
                if not hasattr(current_obj, method_name):
                    raise AttributeError(f"Method '{method_name}' not found in " f"class '{current_obj.__name__}'")

                # Replace the method in the class
                setattr(current_obj, method_name, new_func)
                logger.debug("Successfully replaced class method %s.%s", module_name, function_name)
            else:
                # Get the original function before replacing (for framework patching)
                original_func = getattr(module, function_name, None)

                # Module-level function replacement
                if not hasattr(module, function_name):
                    raise AttributeError(f"Function '{function_name}' not found in " f"module '{module_name}'")

                setattr(module, function_name, new_func)
                logger.debug("Successfully replaced function %s.%s", module_name, function_name)

                # Patch framework-level references that hold direct function references.
                # Frameworks like Flask register route handlers at import time via decorators
                # (e.g., @app.route). These store a direct reference to the original function
                # object in internal data structures (e.g., Flask's app.view_functions dict).
                # Simply replacing the module-level name via setattr does NOT update these
                # internal references, so the framework continues calling the original
                # unwrapped function, bypassing DI instrumentation entirely.
                if original_func is not None:
                    FunctionWrapper._patch_framework_references(module, original_func, new_func)

        except ImportError as exception:
            logger.error("Failed to import module '%s' for replacement: %s", module_name, exception)
            raise
        except AttributeError as exception:
            logger.error("Cannot replace function '%s' in module '%s': %s", function_name, module_name, exception)
            raise

    @staticmethod
    def _patch_framework_references(module, original_func: Callable, new_func: Callable) -> None:
        """
        Patch framework-level references that hold direct pointers to the original function.

        When frameworks like Flask use decorators (e.g., @app.route) at import time,
        they store direct references to the function object. Replacing the module-level
        name via setattr does not update these internal references. This method scans
        for known framework patterns and patches them.

        Currently supports:
        - Flask: patches app.view_functions entries that reference the original function
        - Django: patches URLPattern.callback entries (incl. include()-nested resolvers)
          that reference the original function

        Args:
            module: The module object containing the function
            original_func: The original function that was replaced
            new_func: The new wrapper function
        """
        for patcher in _FRAMEWORK_PATCHERS:
            try:
                patcher.patch(module, original_func, new_func)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                # Never let framework patching failures break instrumentation.
                logger.debug("%s reference patching encountered an error: %s", patcher.name, exc)

    @staticmethod
    def _patch_flask_view_functions(module, original_func: Callable, new_func: Callable) -> None:
        """
        Patch Flask app.view_functions entries that reference the original function.

        Flask's @app.route() decorator stores the view function in app.view_functions
        at import time. When DI replaces the function on the module, Flask still calls
        the original. This method finds and patches those references.

        Args:
            module: The module object that may contain Flask app instances
            original_func: The original function to find in view_functions
            new_func: The new wrapper function to replace it with
        """
        try:
            try:
                from flask import Flask  # pylint: disable=import-outside-toplevel
            except ImportError:
                return  # Flask not installed, nothing to patch

            func_name = getattr(original_func, "__name__", "unknown")
            func_module = getattr(original_func, "__module__", None)

            # Scan module attributes for Flask app instances
            for attr_name in dir(module):
                try:
                    attr = getattr(module, attr_name, None)
                    if attr is None or not isinstance(attr, Flask):
                        continue
                    FunctionWrapper._patch_single_flask_app(
                        attr, attr_name, original_func, new_func, func_name, func_module
                    )
                except Exception as exc:
                    logger.debug("Error checking module attribute '%s' for Flask app: %s", attr_name, exc)
                    continue

        except Exception as exc:
            logger.debug("Error patching Flask view_functions: %s", exc)

    @staticmethod
    def _patch_single_flask_app(flask_app, app_name, original_func, new_func, func_name, func_module=None):
        """Patch view_functions in a single Flask app instance."""
        view_functions = getattr(flask_app, "view_functions", None)
        if not view_functions or not isinstance(view_functions, dict):
            logger.debug("Flask app '%s' has no view_functions dict", app_name)
            return

        patched_count = 0
        for endpoint, view_func in view_functions.items():
            if view_func is original_func:
                view_functions[endpoint] = new_func
                patched_count += 1
                logger.debug("Patched Flask view_functions[%s] to use DI wrapper", endpoint)

        if patched_count > 0:
            logger.debug(
                "Patched %d Flask route(s) for function '%s' on app '%s'",
                patched_count,
                func_name,
                app_name,
            )
            return

        # Identity check failed — try name+module-based matching. functools.wraps
        # (used by OTel's Flask instrumentation) preserves __module__, so requiring
        # both name and module avoids accidentally patching a same-named view from
        # a different module. If the original has no __module__, fall back to
        # name-only matching.
        def _matches(vf):
            if getattr(vf, "__name__", None) != func_name:
                return False
            if func_module is None:
                return True
            return getattr(vf, "__module__", None) == func_module

        matching_endpoints = [ep for ep, vf in view_functions.items() if _matches(vf)]
        if matching_endpoints:
            logger.debug(
                "Flask app '%s' has endpoint(s) %s with name '%s' (module '%s') but identity "
                "mismatch (original_func id=%d, view_func id=%d). Patching by name+module.",
                app_name,
                matching_endpoints,
                func_name,
                func_module,
                id(original_func),
                id(view_functions[matching_endpoints[0]]),
            )
            for ep in matching_endpoints:
                view_functions[ep] = new_func
                logger.debug("Patched Flask view_functions[%s] by name+module match", ep)

    @staticmethod
    def _patch_django_url_patterns(  # pylint: disable=too-many-branches
        module, original_func: Callable, new_func: Callable
    ) -> None:
        """
        Patch Django URLPattern.callback entries that reference the original function.

        Django's ``path('foo/', views.foo)`` stores ``views.foo`` directly on
        ``URLPattern.callback`` at module-import time. When DI replaces
        ``views.foo`` on its module, the URL resolver tree still holds the
        pre-wrap reference, so requests bypass the DI wrapper. This method
        finds and patches those references.

        Discovery walks Django's authoritative resolver via ``get_resolver(None)``
        (the same root resolver Django uses at request time). It also scans the
        passed-in module for top-level ``urlpatterns`` / ``URLPattern`` /
        ``URLResolver`` attributes as a defensive belt-and-suspenders pass for
        unconfigured / test-only contexts.

        Args:
            module: The module object that may contain Django URL config
            original_func: The original function to find on URLPattern.callback
            new_func: The new wrapper function to replace it with
        """
        try:
            try:
                # pylint: disable=import-outside-toplevel
                from django.urls import URLPattern, URLResolver, get_resolver
            except ImportError:
                return  # Django not installed, nothing to patch

            func_name = getattr(original_func, "__name__", "unknown")
            func_module = getattr(original_func, "__module__", None)
            # Track resolver and pattern ids separately. The same URLPattern
            # can be reached from both the get_resolver(None) walk and the
            # module-scan fallback; pattern dedup avoids a no-op double write.
            visited: set = set()
            visited_patterns: set = set()

            # Authoritative source: Django's root resolver (the same instance the
            # framework dispatches through at request time).
            try:
                root_resolver = get_resolver(None)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.debug("Django get_resolver(None) failed (likely unconfigured): %s", exc)
                root_resolver = None
            if root_resolver is not None:
                FunctionWrapper._patch_single_resolver(
                    root_resolver,
                    "<root>",
                    original_func,
                    new_func,
                    func_name,
                    func_module,
                    URLPattern,
                    URLResolver,
                    visited,
                    visited_patterns,
                )

            # Belt-and-suspenders: scan the passed-in module for top-level
            # patterns/resolvers (covers configured-but-not-yet-resolved cases).
            for attr_name in dir(module):
                try:
                    attr = getattr(module, attr_name, None)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.debug("Error reading module attribute '%s' for Django scan: %s", attr_name, exc)
                    continue
                try:
                    if isinstance(attr, URLPattern):
                        FunctionWrapper._maybe_patch_pattern(
                            attr, original_func, new_func, func_name, func_module, visited_patterns
                        )
                    elif isinstance(attr, URLResolver):
                        FunctionWrapper._patch_single_resolver(
                            attr,
                            attr_name,
                            original_func,
                            new_func,
                            func_name,
                            func_module,
                            URLPattern,
                            URLResolver,
                            visited,
                            visited_patterns,
                        )
                    elif isinstance(attr, (list, tuple)) and attr_name == "urlpatterns":
                        for item in attr:
                            if isinstance(item, URLPattern):
                                FunctionWrapper._maybe_patch_pattern(
                                    item, original_func, new_func, func_name, func_module, visited_patterns
                                )
                            elif isinstance(item, URLResolver):
                                FunctionWrapper._patch_single_resolver(
                                    item,
                                    attr_name,
                                    original_func,
                                    new_func,
                                    func_name,
                                    func_module,
                                    URLPattern,
                                    URLResolver,
                                    visited,
                                    visited_patterns,
                                )
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.debug("Error checking module attribute '%s' for Django patterns: %s", attr_name, exc)
                    continue

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Error patching Django URLPattern.callback: %s", exc)

    @staticmethod
    def _patch_single_resolver(  # pylint: disable=too-many-arguments
        resolver,
        resolver_label: str,
        original_func: Callable,
        new_func: Callable,
        func_name: str,
        func_module,
        url_pattern_cls,
        url_resolver_cls,
        visited: set,
        visited_patterns: set,
    ) -> None:
        """Recursively walk a URLResolver, patching matching URLPattern.callback
        entries and descending through ``include()``-nested children. ``visited``
        protects against resolver cycles (``id(resolver)``); ``visited_patterns``
        prevents double-patching the same URLPattern when reachable from both
        the get_resolver(None) walk and the module-scan fallback."""
        if id(resolver) in visited:
            return
        visited.add(id(resolver))
        try:
            patterns = resolver.url_patterns  # @cached_property; may raise ImproperlyConfigured
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Error reading url_patterns on resolver '%s': %s", resolver_label, exc)
            return

        for item in patterns:
            try:
                if isinstance(item, url_pattern_cls):
                    FunctionWrapper._maybe_patch_pattern(
                        item, original_func, new_func, func_name, func_module, visited_patterns
                    )
                elif isinstance(item, url_resolver_cls):
                    FunctionWrapper._patch_single_resolver(
                        item,
                        resolver_label,
                        original_func,
                        new_func,
                        func_name,
                        func_module,
                        url_pattern_cls,
                        url_resolver_cls,
                        visited,
                        visited_patterns,
                    )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.debug("Error walking pattern in resolver '%s': %s", resolver_label, exc)
                continue

    @staticmethod
    def _maybe_patch_pattern(  # pylint: disable=too-many-arguments
        pattern,
        original_func: Callable,
        new_func: Callable,
        func_name: str,
        func_module,
        visited_patterns: set,
    ) -> None:
        """Patch a single URLPattern.callback if it matches by identity, or by
        ``__name__`` + ``__module__`` (handles ``functools.wraps``-preserving
        decorators like ``@login_required`` and OTel auto-instrumentation
        wrappers). Skips patterns already visited so multiple discovery roots
        don't double-patch."""
        if id(pattern) in visited_patterns:
            return
        visited_patterns.add(id(pattern))
        try:
            cb = getattr(pattern, "callback", None)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Error reading callback on URLPattern: %s", exc)
            return
        if cb is None:
            return

        if cb is original_func:
            try:
                pattern.callback = new_func
                logger.debug("Patched Django URLPattern.callback (identity) for %s", func_name)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.debug("Error setting URLPattern.callback (identity): %s", exc)
            return

        # Identity miss — try name+module fallback. CBV closures created by
        # View.as_view() have __name__ == 'view', not the user's view name,
        # so they're naturally excluded from this match.
        if getattr(cb, "__name__", None) != func_name:
            return
        if func_module is not None and getattr(cb, "__module__", None) != func_module:
            return
        try:
            pattern.callback = new_func
            logger.debug("Patched Django URLPattern.callback (name+module) for %s", func_name)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.debug("Error setting URLPattern.callback (name+module): %s", exc)

    @staticmethod
    def _get_qualified_name(original_func: Callable) -> str:
        """Return a stable qualified name for functions and methods."""
        qualname = getattr(original_func, "__qualname__", None)
        if qualname is not None:
            return qualname
        return getattr(original_func, "__name__", "<anonymous>")

    def _capture_entry_context(  # pylint: disable=no-self-use
        self, original_func: Callable, args: tuple, kwargs: dict, capture_config: CaptureConfig
    ) -> Optional[CapturedContext]:
        """
        Capture function entry context (arguments) as CapturedContext.

        CRITICAL: Never raises exceptions.
        """
        try:
            sig = inspect.signature(original_func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Filter arguments based on capture list:
            # None = do not capture (caller should not invoke this method)
            # [] = capture all arguments
            # ["a", "b"] = capture only the named arguments
            filtered = {}
            for param_name, value in bound_args.arguments.items():
                if capture_config.capture_arguments is not None and len(capture_config.capture_arguments) > 0:
                    if param_name not in capture_config.capture_arguments:
                        continue
                filtered[param_name] = value

            if not filtered:
                return None

            serializer = FunctionWrapper._build_serializer(capture_config)
            arguments = serializer.serialize_variables(filtered)
            return CapturedContext(arguments=arguments)
        except Exception as exc:
            logger.warning("Failed to capture entry context: %s", exc)
            return None

    def _capture_return_context(  # pylint: disable=no-self-use
        self,
        result: Any,
        thrown: Optional[Exception],
        capture_config: CaptureConfig,
        caller_stack: Optional[traceback.StackSummary] = None,
    ) -> Optional[CapturedContext]:
        """
        Capture function return context (return value and/or exception).

        CRITICAL: Never raises exceptions.
        """
        try:
            ctx = CapturedContext()

            if thrown is not None:
                # Exception's own frames (inside the instrumented function), reversed so throw site is first
                tb_frames = traceback.extract_tb(thrown.__traceback__) if thrown.__traceback__ else []
                # Caller stack (above the instrumented function), reversed so nearest caller is first
                all_frames = list(reversed(tb_frames)) + (list(reversed(caller_stack)) if caller_stack else [])
                stack_frames = [
                    StackFrame(
                        file_name=f.filename,
                        function=f.name,
                        line_number=f.lineno or 0,
                    )
                    for f in all_frames
                    if not is_internal_frame(f.filename)
                ][: capture_config.max_stack_frames]
                ctx.throwable = CapturedThrowable(
                    type=type(thrown).__name__,
                    message=str(thrown) or "",
                    stacktrace=stack_frames,
                )

            if result is not None and capture_config.capture_return:
                serializer = FunctionWrapper._build_serializer(capture_config)
                ctx.return_value = serializer.serialize(result)

            return ctx
        except Exception as exc:
            logger.warning("Failed to capture return context: %s", exc)
            return None

    def _build_snapshot(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        module_name: str,
        qualified_name: str,
        original_func: Callable,
        location_hash: Optional[str],
        duration_ns: int,
        entry_context: Optional[CapturedContext],
        return_context: Optional[CapturedContext],
        capture_config: Optional[CaptureConfig],
        instrumentation_type: Optional[str] = None,
    ) -> Snapshot:
        """
        Build a complete Snapshot object.

        CRITICAL: Never raises exceptions (caller catches).
        """
        timestamp_ms = int(time.time() * 1000)

        # Convert duration from nanoseconds to milliseconds per v1 spec.
        # Snapshot.duration is typed Optional[int]; use floor division to keep it int.
        duration_ms = duration_ns // 1_000_000 if duration_ns else None

        # Service and environment from OTel resource attributes
        service_name = os.environ.get("OTEL_SERVICE_NAME")
        if not service_name:
            # Try OTEL_RESOURCE_ATTRIBUTES
            for pair in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    if key.strip() == "service.name":
                        service_name = value.strip()
                        break

        environment = None
        for pair in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
            if "=" in pair:
                key, value = pair.split("=", 1)
                key = key.strip()
                if key == "deployment.environment.name":
                    environment = value.strip()
                    break
                if key == "deployment.environment" and not environment:
                    environment = value.strip()

        # Build instrumentation details per v1 spec
        method_name = qualified_name.split(".")[-1]
        class_name = ".".join(qualified_name.split(".")[:-1]) if "." in qualified_name else None
        file_path = None
        if hasattr(original_func, "__code__"):
            code = original_func.__code__
            file_path = getattr(code, "co_filename", None)

        # Per spec: codeUnit = module path, className = fully qualified
        code_unit = module_name
        class_name_fq = module_name
        if class_name:
            class_name_fq = f"{module_name}.{class_name}"

        instrumentation = InstrumentationDetails(
            location=InstrumentationLocation(
                code_unit=code_unit,
                class_name=class_name_fq,
                method_name=method_name,
                line_number=0,  # 0 = function-level instrumentation per spec
                file_path=file_path,
                language="python",
            ),
        )

        # Read current OTel trace context (don't create a new span)
        trace_ctx = self._get_trace_context()

        # Thread info
        current_thread = threading.current_thread()
        thread_info = ThreadInfo(
            id=threading.get_ident(),
            name=current_thread.name,
        )

        # Stack trace
        stack = None
        if capture_config and capture_config.capture_stack_trace:
            stack = capture_stack_frames(capture_config.max_stack_frames)

        # Captures
        captures = Captures(entry=entry_context, return_context=return_context)

        return Snapshot(
            timestamp=timestamp_ms,
            duration=duration_ms,
            service=service_name or None,
            environment=environment or None,
            location_hash=location_hash or None,
            instrumentation=instrumentation,
            trace=trace_ctx,
            thread=thread_info,
            stack=stack,
            captures=captures,
            instrumentation_type=instrumentation_type,
        )

    @staticmethod
    def _get_trace_context() -> Optional[TraceContext]:
        """Read the current OTel trace/span IDs without creating a new span."""
        try:
            from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel

            span = otel_trace.get_current_span()
            if span and span.get_span_context().is_valid:
                ctx = span.get_span_context()
                trace_id = format(ctx.trace_id, "032x")
                span_id = format(ctx.span_id, "016x")
                return TraceContext(trace_id=trace_id, span_id=span_id)
        except Exception:
            pass
        return None

    @staticmethod
    def _emit_snapshot(snapshot: Snapshot) -> None:
        """Emit snapshot via the global OTLP emitter."""
        try:
            emitter = get_snapshot_emitter()
            if emitter:
                emitter.emit_snapshot(snapshot)
            else:
                logger.debug("No snapshot emitter available, snapshot dropped")
        except Exception as exc:
            logger.warning("Failed to emit snapshot: %s", exc)
