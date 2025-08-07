# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

from typing import Collection

from wrapt import wrap_function_wrapper

from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler import (
    OpenTelemetryCallbackHandler,
)
from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.trace import get_tracer

__all__ = ["OpenTelemetryCallbackHandler"]

_instruments = ("langchain >= 0.1.0",)


class LangChainInstrumentor(BaseInstrumentor):
    def __init__(self):
        super().__init__()
        self.handler = None  # Initialize the handler attribute
        self._wrapped = []

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(__name__, __version__, tracer_provider)

        otel_callback_handler = OpenTelemetryCallbackHandler(tracer)

        wrap_function_wrapper(
            module="langchain_core.callbacks",
            name="BaseCallbackManager.__init__",
            wrapper=_BaseCallbackManagerInitWrapper(otel_callback_handler),
        )

    def _uninstrument(self, **kwargs):
        unwrap("langchain_core.callbacks", "BaseCallbackManager.__init__")
        if hasattr(self, "_wrapped"):
            for module, name in self._wrapped:
                unwrap(module, name)
        self.handler = None


class _BaseCallbackManagerInitWrapper:
    def __init__(self, callback_handler: "OpenTelemetryCallbackHandler"):
        self.callback_handler = callback_handler
        self._wrapped = []

    def __call__(
        self,
        wrapped,
        instance,
        args,
        kwargs,
    ) -> None:
        wrapped(*args, **kwargs)
        for handler in instance.inheritable_handlers:
            if isinstance(handler, OpenTelemetryCallbackHandler):
                return None

        instance.add_handler(self.callback_handler, True)
        return None
