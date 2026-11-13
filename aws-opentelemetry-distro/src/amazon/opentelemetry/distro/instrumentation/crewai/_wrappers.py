# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import inspect
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amazon.opentelemetry.distro.instrumentation.crewai._event_handler import OpenTelemetryEventHandler


class _KickoffWrapper:

    def __init__(self, event_handler: "OpenTelemetryEventHandler"):
        self._event_handler = event_handler

    def __call__(self, wrapped, instance, args, kwargs) -> Any:
        if inspect.iscoroutinefunction(wrapped):
            return self._call_async(wrapped, args, kwargs)
        with self._event_handler._end_kickoff_spans_if_cancelled():
            return wrapped(*args, **kwargs)

    async def _call_async(self, wrapped, args, kwargs) -> Any:
        with self._event_handler._end_kickoff_spans_if_cancelled():
            return await wrapped(*args, **kwargs)
