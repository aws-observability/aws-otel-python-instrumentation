# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from opentelemetry.metrics import Instrument
from opentelemetry.sdk.metrics.view import DropAggregation, View


class ScopeBasedRetainingView(View):
    def __init__(
        self,
        meter_name: Optional[str] = None,
    ) -> None:
        super().__init__(meter_name=meter_name, aggregation=DropAggregation())

    def _match(self, instrument: Instrument) -> bool:
        if instrument.instrumentation_scope.name != self._meter_name:
            return True

        return False
