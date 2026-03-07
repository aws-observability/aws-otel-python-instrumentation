# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Collection

from amazon.opentelemetry.distro.instrumentation.common.utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.instrumentation.crewai._wrappers import (
    _CrewKickoffWrapper,
    _TaskExecuteCoreWrapper,
    _ToolRunWrapper,
    _ToolUseWrapper,
)
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor


class CrewAIInstrumentor(BaseInstrumentor):
    """
    OpenTelemetry instrumentor for CrewAI.

    Instrumentation currently follows OpenTelemetry semantic conventions v1.39 for gen_ai attributes.
    See: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
    Note: Semantic conventions may change in future versions.
    """

    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("crewai >= 1.9.0",)

    # disabling these linters rules as these are instance methods from BaseInstrumentor
    def _instrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)

        try_wrap("crewai", "Crew.kickoff", _CrewKickoffWrapper(tracer))
        try_wrap("crewai", "Task._execute_core", _TaskExecuteCoreWrapper(tracer))
        try_wrap("crewai.tools.tool_usage", "ToolUsage._use", _ToolUseWrapper(tracer))
        try_wrap("crewai.tools.base_tool", "BaseTool.run", _ToolRunWrapper(tracer))
        try_wrap("crewai.tools.base_tool", "Tool.run", _ToolRunWrapper(tracer))

    def _uninstrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        # pylint: disable=import-outside-toplevel
        import crewai
        from crewai.tools import base_tool, tool_usage

        try_unwrap(crewai.Crew, "kickoff")
        try_unwrap(crewai.Task, "_execute_core")
        try_unwrap(tool_usage.ToolUsage, "_use")
        try_unwrap(base_tool.BaseTool, "run")
        try_unwrap(base_tool.Tool, "run")
