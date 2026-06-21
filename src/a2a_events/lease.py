"""Client-side automatic lease renewal (spec §14.3, §26.2).

A subscription is only live while its lease is valid, so a subscriber must
renew before it lapses — the spec recommends renewing once 50%–70% of the
lease has elapsed. :class:`AutoLeaseRenewer` runs that loop as a background
task. It is transport-agnostic: it drives any async ``renew(subscriptionId,
leaseSeconds) -> new leaseUntil`` callable, so it works over the in-memory
runtime, JSON-RPC, or HTTP+JSON alike.

``sleep`` and ``now`` are injectable so the loop can be tested against a fake
clock without real waiting.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from types import TracebackType

from .errors import A2AEventsError, ErrorCode

# (subscriptionId, leaseSeconds) -> the new leaseUntil granted by the publisher.
RenewFn = Callable[[str, int], Awaitable[datetime]]


def _default_now() -> datetime:
    return datetime.now(UTC)


class AutoLeaseRenewer:
    """Background task that keeps one subscription's lease from expiring.

    Renews when ``renew_fraction`` of the lease has elapsed (default 0.6, the
    middle of the §14.3 50%–70% window). On a transient renew failure it keeps
    retrying until the lease would expire; a terminal failure (the subscription
    is gone) stops the loop.
    """

    def __init__(
        self,
        renew: RenewFn,
        *,
        renew_fraction: float = 0.6,
        min_sleep: float = 1.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = _default_now,
    ) -> None:
        if not 0.0 < renew_fraction < 1.0:
            raise ValueError("renew_fraction must be in (0, 1)")
        self._renew = renew
        self._renew_fraction = renew_fraction
        self._min_sleep = min_sleep
        self._sleep = sleep
        self._now = now
        self._task: asyncio.Task[None] | None = None
        self._stopped = False
        # Observability: timestamps of successful renewals, and the last error.
        self.renewals: list[datetime] = []
        self.last_error: Exception | None = None

    def start(
        self, subscription_id: str, lease_seconds: int, lease_until: datetime
    ) -> asyncio.Task[None]:
        """Launch the renewal loop as a background task and return it."""
        if self._task is not None:
            raise RuntimeError("renewer already started")
        self._task = asyncio.create_task(
            self._run(subscription_id, lease_seconds, lease_until)
        )
        return self._task

    async def aclose(self) -> None:
        """Stop the loop and await the task's cancellation."""
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            # Swallowing CancelledError is the intended cleanup; ``suppress`` is
            # the idiomatic form (avoids a bare "except: pass" anti-pattern).
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def __aenter__(self) -> AutoLeaseRenewer:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    # --- internals ----------------------------------------------------------
    def _renewal_delay(self, lease_until: datetime, lease_seconds: int) -> float:
        """Seconds to wait before renewing, from the authoritative leaseUntil."""
        remaining = (lease_until - self._now()).total_seconds()
        # Renew once `renew_fraction` of the lease has elapsed, i.e. while
        # (1 - renew_fraction) of it still remains.
        delay = remaining - (1.0 - self._renew_fraction) * lease_seconds
        return max(self._min_sleep, delay)

    async def _run(
        self, subscription_id: str, lease_seconds: int, lease_until: datetime
    ) -> None:
        while not self._stopped:
            await self._sleep(self._renewal_delay(lease_until, lease_seconds))
            if self._stopped:
                return
            try:
                lease_until = await self._renew(subscription_id, lease_seconds)
            except A2AEventsError as exc:
                self.last_error = exc
                if exc.code == ErrorCode.SUBSCRIPTION_NOT_FOUND:
                    return  # terminal: the subscription is gone
                if not await self._retry_pause(lease_until):
                    return  # lease lapsed while failing
                continue
            except Exception as exc:
                self.last_error = exc
                if not await self._retry_pause(lease_until):
                    return
                continue
            self.renewals.append(lease_until)

    async def _retry_pause(self, lease_until: datetime) -> bool:
        """Wait briefly before a retry; return False if the lease has lapsed."""
        remaining = (lease_until - self._now()).total_seconds()
        if remaining <= 0:
            return False
        await self._sleep(min(self._min_sleep, remaining))
        return True
