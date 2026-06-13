# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared snapshot construction for both instrumentation engines.

Both ``BytecodeInjectionEngine`` (3.9-3.11) and ``SysMonitoringEngine``
(3.12+) need to assemble a ``Snapshot`` object on every function-entry hit.
The pieces are identical:

* serialise return value (when ``capture_return`` is set)
* build ``InstrumentationDetails`` from ``module_name`` + ``qualified_name``
* read the active OTel trace context (read-only — never start a span)
* capture thread info
* optionally walk the call stack
* emit via the global snapshot emitter

This module owns that pipeline so neither engine has to. ~150 LOC of
duplication eliminated.
"""

import logging
import threading
import time
from typing import Any, Dict, Optional

from amazon.opentelemetry.distro.debugger._data_models import (
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_STRING_LENGTH,
    CaptureConfig,
)
from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    ThreadInfo,
    TraceContext,
)
from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer
from amazon.opentelemetry.distro.debugger._stack_utils import capture_stack_frames

logger = logging.getLogger(__name__)


def _read_trace_context() -> Optional[TraceContext]:
    """Return the active OTel trace context or None — never raise."""
    try:
        from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel

        span = otel_trace.get_current_span()
        if span and span.get_span_context().is_valid:
            sctx = span.get_span_context()
            return TraceContext(
                trace_id=format(sctx.trace_id, "032x"),
                span_id=format(sctx.span_id, "016x"),
            )
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    return None


def _read_otel_resource_attribute(name: str) -> Optional[str]:
    """Pluck a single attribute off the active TracerProvider's resource."""
    try:
        from opentelemetry import trace as otel_trace  # pylint: disable=import-outside-toplevel
        from opentelemetry.sdk.trace import TracerProvider  # pylint: disable=import-outside-toplevel

        provider = otel_trace.get_tracer_provider()
        if isinstance(provider, TracerProvider) and hasattr(provider, "resource"):
            return provider.resource.attributes.get(name)
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    return None


def get_service_name() -> Optional[str]:
    """Service name from the active OTel resource."""
    return _read_otel_resource_attribute("service.name")


def get_environment() -> Optional[str]:
    """Deployment environment from the active OTel resource."""
    return _read_otel_resource_attribute("deployment.environment.name")


def _serializer_for(capture_config: Optional[CaptureConfig]) -> SnapshotSerializer:
    """Build a SnapshotSerializer with capture-config limits or sensible defaults."""
    if capture_config is None:
        return SnapshotSerializer(
            max_fields=DEFAULT_MAX_FIELDS_PER_OBJECT,
            max_string_length=DEFAULT_MAX_STRING_LENGTH,
            max_depth=3,
            max_collection_size=10,
        )
    return SnapshotSerializer(
        max_fields=capture_config.max_fields_per_object or DEFAULT_MAX_FIELDS_PER_OBJECT,
        max_string_length=capture_config.max_string_length or DEFAULT_MAX_STRING_LENGTH,
        max_depth=capture_config.max_object_depth or 3,
        max_collection_size=capture_config.max_collection_width or 10,
    )


def build_function_entry_snapshot(  # pylint: disable=too-many-arguments,too-many-locals
    *,
    entry: Dict[str, Any],
    frame_info: Dict[str, Any],
    retval: Any,
    file_path: Optional[str] = None,
) -> Snapshot:
    """
    Build a function-entry / function-exit / function-exception snapshot.

    Args:
        entry: Engine-side metadata dict for the instrumented function. Must
            contain ``function_key``, ``module_name``, ``qualified_name``,
            ``capture_config``, ``location_hash``, ``instrumentation_type``.
            Engines that store ``original_code`` may also pass it through;
            this factory falls back to ``file_path`` if the dict lacks one.
        frame_info: Per-call state from the entry handler:
            ``{"start_ns": int, "entry_context": Optional[CapturedContext]}``.
        retval: Function return value. ``None`` when called from the exception
            path (where the function raised before returning).
        file_path: Override path for ``InstrumentationLocation.file_path`` —
            engines that don't store ``original_code`` in ``entry`` pass it
            here from their own state.

    Returns:
        A fully-populated ``Snapshot`` ready for ``emitter.emit_snapshot``.
    """
    capture_config = entry.get("capture_config")

    duration_ns = time.time_ns() - frame_info["start_ns"]
    entry_context = frame_info.get("entry_context")

    return_context = None
    if capture_config is not None and capture_config.capture_return and retval is not None:
        return_context = CapturedContext()
        return_context.return_value = _serializer_for(capture_config).serialize(retval)

    module_name = entry["module_name"]
    qualified_name = entry["qualified_name"]
    method_name = qualified_name.split(".")[-1]
    class_part = ".".join(qualified_name.split(".")[:-1]) if "." in qualified_name else None
    class_name_fq = f"{module_name}.{class_part}" if class_part else module_name

    if file_path is None:
        original_code = entry.get("original_code")
        if original_code is not None:
            file_path = getattr(original_code, "co_filename", None)

    instrumentation = InstrumentationDetails(
        location=InstrumentationLocation(
            code_unit=module_name,
            class_name=class_name_fq,
            method_name=method_name,
            line_number=0,  # 0 = function-level per the snapshot spec
            file_path=file_path,
            language="python",
        ),
    )

    stack = None
    if capture_config is not None and capture_config.capture_stack_trace:
        stack = capture_stack_frames(capture_config.max_stack_frames)

    current_thread = threading.current_thread()
    thread_info = ThreadInfo(id=threading.get_ident(), name=current_thread.name)

    captures = Captures(entry=entry_context, return_context=return_context)
    duration_ms = duration_ns // 1_000_000 if duration_ns else None

    return Snapshot(
        timestamp=int(time.time() * 1000),
        duration=duration_ms,
        service=get_service_name(),
        environment=get_environment(),
        location_hash=entry.get("location_hash"),
        instrumentation=instrumentation,
        trace=_read_trace_context(),
        thread=thread_info,
        stack=stack,
        captures=captures,
        instrumentation_type=entry.get("instrumentation_type"),
    )


def emit_snapshot(snapshot: Snapshot) -> None:
    """Emit via the global snapshot emitter; never raise."""
    try:
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter

        emitter = get_snapshot_emitter()
        if emitter is not None:
            emitter.emit_snapshot(snapshot)
    except Exception:  # pylint: disable=broad-exception-caught
        pass
