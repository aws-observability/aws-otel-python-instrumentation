# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Collection

from amazon.opentelemetry.distro.instrumentation.common.utils import try_unwrap, try_wrap
from amazon.opentelemetry.distro.version import __version__
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor


class LangChainInstrumentor(BaseInstrumentor):
    """
    OpenTelemetry instrumentor for LangChain.

    Instrumentation currently follows OpenTelemetry semantic conventions v1.39 for gen_ai attributes.
    See: https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/
    Note: Semantic conventions may change in future versions.
    """

    def instrumentation_dependencies(self) -> Collection[str]:  # pylint: disable=no-self-use
        return ("langchain >= 0.3.21",)

    # disabling these linters rules as these are instance methods from BaseInstrumentor
    def _instrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.instrumentation.langchain.callback_handler import (
            OpenTelemetryCallbackHandler,
            _BaseCallbackManagerInitWrapper,
        )

        tracer_provider = kwargs.get("tracer_provider") or trace.get_tracer_provider()
        tracer = trace.get_tracer(__name__, __version__, tracer_provider=tracer_provider)

        try_wrap(
            "langchain_core.callbacks",
            "BaseCallbackManager.__init__",
            _BaseCallbackManagerInitWrapper(OpenTelemetryCallbackHandler(tracer)),
        )

    def _uninstrument(self, **kwargs: Any) -> None:  # pylint: disable=no-self-use
        # pylint: disable=import-outside-toplevel
        from langchain_core.callbacks import BaseCallbackManager

        try_unwrap(BaseCallbackManager, "__init__")
