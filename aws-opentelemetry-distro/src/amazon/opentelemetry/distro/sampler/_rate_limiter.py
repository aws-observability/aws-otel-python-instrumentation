# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from decimal import Decimal
from threading import Lock

from amazon.opentelemetry.distro.sampler._clock import _Clock


class _RateLimiter:
    def __init__(self, max_balance_in_seconds: int, quota: int, clock: _Clock):
        # max_balance_in_seconds is usually 1
        # pylint: disable=invalid-name
        self.MAX_BALANCE_MILLIS = Decimal(max_balance_in_seconds * 1000.0)
        self._clock = clock

        self._quota = Decimal(quota)
        self.__wallet_floor_millis = Decimal(self._clock.now().timestamp() * 1000.0)
        # current "wallet_balance" would be ceiling - floor

        self.__lock = Lock()

    def try_spend(self, cost: float) -> bool:
        if self._quota == 0:
            return False

        quota_per_millis = self._quota / Decimal(1000.0)

        # assume divide by zero not possible
        cost_in_millis = Decimal(cost) / quota_per_millis

        with self.__lock:
            wallet_ceiling_millis = Decimal(self._clock.now().timestamp() * 1000.0)
            current_balance_millis = wallet_ceiling_millis - self.__wallet_floor_millis
            if current_balance_millis > self.MAX_BALANCE_MILLIS:
                current_balance_millis = self.MAX_BALANCE_MILLIS

            pending_remaining_balance_millis = current_balance_millis - cost_in_millis
            if pending_remaining_balance_millis >= 0:
                self.__wallet_floor_millis = wallet_ceiling_millis - pending_remaining_balance_millis
                return True
            # No changes to the wallet state
            return False
