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

    def try_spend(self, cost: float, borrow: bool) -> bool:
        quota_per_millis = self._quota / Decimal(1000.0)

        if borrow and quota_per_millis != 0:
            # When `Borrowing`, pretend that the quota is 1 per second
            quota_per_millis = Decimal(1.0) / Decimal(1000.0)

        with self.__lock:
            wallet_ceiling_millis = Decimal(self._clock.now().timestamp() * 1000.0)
            current_balance_millis = wallet_ceiling_millis - self.__wallet_floor_millis
            if current_balance_millis > self.MAX_BALANCE_MILLIS:
                current_balance_millis = self.MAX_BALANCE_MILLIS

            # Ex:  1.  current_balance_millis=1000ms, quota_per_millis=0.004 (quota=4) -> actual_balance = 4
            #      2.  actual_balance=4, cost=3 -> actual_remaining_balance = 1
            #      3.  actual_remaining_balance=1 -> remaining_balance_millis = 250ms
            actual_balance = current_balance_millis * quota_per_millis
            if actual_balance >= Decimal(cost):
                actual_remaining_balance = actual_balance - Decimal(cost)
                remaining_balance_millis = actual_remaining_balance / quota_per_millis
                self.__wallet_floor_millis = wallet_ceiling_millis - remaining_balance_millis
                return True
            # No changes to the wallet state
            return False
