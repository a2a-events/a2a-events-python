"""Client-side automatic lease renewal (DESIGN.md §14.3)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from conftest import SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import AutoLeaseRenewer, DeliveryMode, DeliveryPreference
from a2a_events.errors import A2AEventsError, ErrorCode

START = datetime(2026, 1, 1, tzinfo=UTC)


class FakeClock:
    """A clock that advances only when something sleeps on it."""

    def __init__(self, start: datetime = START) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.t

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.t += timedelta(seconds=delay)
        await asyncio.sleep(0)  # yield without real waiting


def test_renew_fraction_is_validated():
    with pytest.raises(ValueError):
        AutoLeaseRenewer(lambda _s, _l: None, renew_fraction=0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        AutoLeaseRenewer(lambda _s, _l: None, renew_fraction=1)  # type: ignore[arg-type]


async def test_renews_at_configured_fraction():
    clock = FakeClock()
    lease_seconds = 100
    calls: list[tuple[str, int]] = []
    renewer: AutoLeaseRenewer

    async def renew(sub_id: str, lease: int) -> datetime:  # noqa: S7503
        calls.append((sub_id, lease))
        granted = clock.now() + timedelta(seconds=lease)
        if len(calls) >= 3:
            renewer._stopped = True  # end the loop after three renewals
        return granted

    renewer = AutoLeaseRenewer(
        renew, renew_fraction=0.6, sleep=clock.sleep, now=clock.now
    )
    task = renewer.start(
        "sub_1", lease_seconds, START + timedelta(seconds=lease_seconds)
    )
    await task

    # 60% of a 100s lease elapses before each renewal, in steady state.
    assert clock.sleeps == [60.0, 60.0, 60.0]
    assert calls == [("sub_1", 100)] * 3
    assert len(renewer.renewals) == 3


async def test_seventy_percent_fraction():
    clock = FakeClock()
    renewer: AutoLeaseRenewer

    async def renew(_sub_id: str, lease: int) -> datetime:  # noqa: S7503
        renewer._stopped = True
        return clock.now() + timedelta(seconds=lease)

    renewer = AutoLeaseRenewer(
        renew, renew_fraction=0.7, sleep=clock.sleep, now=clock.now
    )
    await renewer.start("sub_1", 200, START + timedelta(seconds=200))
    assert clock.sleeps == [140.0]  # renew once 70% of 200s has elapsed


async def test_stops_on_terminal_error():
    clock = FakeClock()

    async def renew(_sub_id: str, _lease: int) -> datetime:
        raise A2AEventsError(ErrorCode.SUBSCRIPTION_NOT_FOUND, "gone")

    renewer = AutoLeaseRenewer(renew, sleep=clock.sleep, now=clock.now)
    await renewer.start("sub_1", 100, START + timedelta(seconds=100))

    assert renewer.renewals == []
    assert isinstance(renewer.last_error, A2AEventsError)
    assert renewer.last_error.code == ErrorCode.SUBSCRIPTION_NOT_FOUND


async def test_transient_failure_retries_until_lease_expires():
    clock = FakeClock()
    attempts = {"n": 0}

    async def renew(_sub_id: str, _lease: int) -> datetime:  # noqa: S7503
        attempts["n"] += 1
        raise RuntimeError("publisher unavailable")

    renewer = AutoLeaseRenewer(renew, sleep=clock.sleep, now=clock.now)
    await renewer.start("sub_1", 100, START + timedelta(seconds=100))

    # It keeps trying (more than once) but eventually gives up at expiry.
    assert attempts["n"] > 1
    assert renewer.renewals == []
    assert isinstance(renewer.last_error, RuntimeError)
    assert clock.now() >= START + timedelta(seconds=100)


async def test_aclose_cancels_the_loop():
    clock = FakeClock()
    reached = asyncio.Event()

    async def renew(_sub_id: str, _lease: int) -> datetime:
        reached.set()
        await asyncio.Event().wait()  # block until cancelled
        return clock.now()

    renewer = AutoLeaseRenewer(renew, sleep=clock.sleep, now=clock.now)
    renewer.start("sub_1", 100, START + timedelta(seconds=100))
    await reached.wait()
    await renewer.aclose()

    assert renewer._task is None


async def test_double_start_is_rejected():
    renewer = AutoLeaseRenewer(
        lambda _s, _l: asyncio.sleep(0), sleep=FakeClock().sleep  # type: ignore[arg-type]
    )
    renewer.start("sub_1", 100, START + timedelta(seconds=100))
    with pytest.raises(RuntimeError):
        renewer.start("sub_1", 100, START + timedelta(seconds=100))
    await renewer.aclose()


async def test_drives_a_real_subscription(harness: Harness):
    sub = await harness.publisher.subscribe(
        subscriber_card_url=SUBSCRIBER_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )
    calls = {"n": 0}
    renewer: AutoLeaseRenewer

    async def renew(sub_id: str, lease: int) -> datetime:
        calls["n"] += 1
        renewed = await harness.publisher.renew(sub_id, lease)
        if calls["n"] >= 2:
            renewer._stopped = True
        return renewed.lease_until

    # Instant sleeps; real clock. The renewer should call RenewSubscription
    # and keep the subscription active.
    renewer = AutoLeaseRenewer(renew, sleep=FakeClock().sleep)
    await renewer.start(sub.subscription_id, 3600, sub.lease_until)

    assert calls["n"] == 2
    current = await harness.publisher.get_subscription(sub.subscription_id)
    assert current.status == "active"
