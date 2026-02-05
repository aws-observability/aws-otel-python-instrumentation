# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Collection

from wrapt import wrap_function_wrapper

from amazon.opentelemetry.distro.instrumentation.crewai._wrappers import (
    _BaseToolRunWrapper,
    _CrewKickoffWrapper,
    _TaskExecuteCoreWrapper,
)
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap


class CrewAIInstrumentor(BaseInstrumentor):
    """
    OpenTelemetry instrumentor for CrewAI.

    Instrumentation currently follows OpenTelemetry semantic conventions v1.39 for gen_ai attributes.
    See: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
    Note: Semantic conventions may change in future versions.
    """

    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("crewai >= 1.9.0",)

    def _instrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)

        wrap_function_wrapper("crewai", "Crew.kickoff", _CrewKickoffWrapper(tracer))
        wrap_function_wrapper("crewai", "Task._execute_core", _TaskExecuteCoreWrapper(tracer))
        wrap_function_wrapper("crewai.tools.base_tool", "BaseTool.run", _BaseToolRunWrapper(tracer))

    def _uninstrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        # pylint: disable=import-outside-toplevel
        import crewai
        from crewai.tools import base_tool

        unwrap(crewai.Crew, "kickoff")
        unwrap(crewai.Task, "_execute_core")
        unwrap(base_tool.BaseTool, "run")
