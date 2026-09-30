# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Snapshot data models for Dynamic Instrumentation.

Implements the AWS DI Snapshot Specification v1.0.
Snapshots replace OTel Spans as the output signal for DI.
"""

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StackFrame:
    """A single frame in a stack trace."""

    file_name: str
    function: str
    line_number: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {"file_path": self.file_name, "function": self.function, "line_number": self.line_number}


@dataclass
class CapturedValue:
    """
    Recursive value representation for captured variables.

    Must contain `type` plus exactly one of:
    value, fields, elements, entries, isNull, or notCapturedReason.
    """

    type: str
    value: Optional[str] = None
    fields: Optional[Dict[str, "CapturedValue"]] = None
    elements: Optional[List["CapturedValue"]] = None
    entries: Optional[List[Dict[str, "CapturedValue"]]] = None
    is_null: bool = False
    not_captured_reason: Optional[str] = None
    truncated: bool = False
    size: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {"type": self.type}
        if self.not_captured_reason is not None:
            result["not_captured_reason"] = self.not_captured_reason
        elif self.is_null:
            result["is_null"] = True
        elif self.value is not None:
            result["value"] = self.value
        elif self.fields is not None:
            result["fields"] = {k: v.to_dict() for k, v in self.fields.items()}
        elif self.elements is not None:
            result["elements"] = [e.to_dict() for e in self.elements]
        elif self.entries is not None:
            result["entries"] = [
                {"key": entry["key"].to_dict(), "value": entry["value"].to_dict()} for entry in self.entries
            ]
        if self.truncated:
            result["truncated"] = True
        if self.size is not None:
            result["size"] = self.size
        return result


@dataclass
class CapturedThrowable:
    """Captured exception information."""

    type: str
    message: str
    stacktrace: List[StackFrame] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "message": self.message,
            "stacktrace": [f.to_dict() for f in self.stacktrace],
        }


@dataclass
class CapturedContext:
    """
    Captured variable context at a specific point in execution.

    Used for both entry and return contexts.
    """

    arguments: Optional[Dict[str, CapturedValue]] = None
    locals: Optional[Dict[str, CapturedValue]] = None
    return_value: Optional[CapturedValue] = None
    throwable: Optional[CapturedThrowable] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if self.arguments is not None:
            result["arguments"] = {k: v.to_dict() for k, v in self.arguments.items()}
        if self.locals is not None:
            result["locals"] = {k: v.to_dict() for k, v in self.locals.items()}
        if self.return_value is not None:
            result["return_value"] = self.return_value.to_dict()
        if self.throwable is not None:
            result["throwable"] = self.throwable.to_dict()
        return result


@dataclass
class Captures:
    """
    Container for all captured data in a snapshot.

    Function-level: entry + return contexts.
    Line-level: lines dict mapping line number to CapturedContext.
    """

    entry: Optional[CapturedContext] = None
    return_context: Optional[CapturedContext] = None
    lines: Optional[Dict[int, CapturedContext]] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if self.entry is not None:
            result["entry"] = self.entry.to_dict()
        if self.return_context is not None:
            result["return"] = self.return_context.to_dict()
        if self.lines is not None:
            result["lines"] = {str(k): v.to_dict() for k, v in self.lines.items()}
        return result


@dataclass
class InstrumentationLocation:
    """Location of the instrumented code.

    Serialized with snake_case keys matching the OTLP body schema:
    code_unit, class_name, method_name, line_number, file_path, language.
    """

    code_unit: str  # package/module path (e.g., "com.example.order_service")
    class_name: str  # fully qualified class name (Python: "com.example.order_service")
    method_name: str  # function/method name
    line_number: int = 0  # 0 for function-level instrumentation
    file_path: Optional[str] = None  # source file name
    language: str = "python"  # runtime language identifier

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "code_unit": self.code_unit,
            "class_name": self.class_name,
            "method_name": self.method_name,
            "line_number": self.line_number,
            "language": self.language,
        }
        if self.file_path is not None:
            result["file_path"] = self.file_path
        return result


@dataclass
class InstrumentationDetails:
    """Instrumentation configuration reference. Contains only location."""

    location: InstrumentationLocation

    def to_dict(self) -> Dict[str, Any]:
        return {"location": self.location.to_dict()}


@dataclass
class TraceContext:
    """Distributed trace correlation."""

    trace_id: str  # 32 hex chars
    span_id: str  # 16 hex chars

    def to_dict(self) -> Dict[str, Any]:
        return {"trace_id": self.trace_id, "span_id": self.span_id}


@dataclass
class ThreadInfo:
    """Thread information at capture time."""

    id: int
    name: str

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "name": self.name}


@dataclass
class Snapshot:
    """
    Top-level snapshot object per the v1 spec.

    Represents a single capture event from dynamic instrumentation.
    """

    timestamp: int  # Unix epoch milliseconds
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    duration: Optional[int] = None  # milliseconds (entry to exit)
    service: Optional[str] = None  # service.name from OTel resource
    environment: Optional[str] = None  # deployment.environment.name from OTel resource
    location_hash: Optional[str] = None  # top-level per spec
    instrumentation: Optional[InstrumentationDetails] = None
    trace: Optional[TraceContext] = None
    thread: Optional[ThreadInfo] = None
    stack: Optional[List[StackFrame]] = None
    captures: Optional[Captures] = None
    instrumentation_type: Optional[str] = None  # "PROBE" or "BREAKPOINT"

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "id": self.id,
            "timestamp": self.timestamp,
        }
        if self.duration is not None:
            result["duration"] = self.duration
        if self.service is not None:
            result["service"] = self.service
        if self.environment is not None:
            result["environment"] = self.environment
        if self.location_hash is not None:
            result["location_hash"] = self.location_hash
        if self.instrumentation is not None:
            result["instrumentation"] = self.instrumentation.to_dict()
        if self.trace is not None:
            result["trace"] = self.trace.to_dict()
        if self.thread is not None:
            result["thread"] = self.thread.to_dict()
        if self.stack is not None:
            result["stack"] = [f.to_dict() for f in self.stack]
        if self.captures is not None:
            result["captures"] = self.captures.to_dict()
        return result
