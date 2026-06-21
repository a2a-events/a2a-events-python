"""Pagination, selector limits, and rate limiting (DESIGN.md §14.5, §22, §29)."""

from __future__ import annotations

import pytest

from a2a_events import (
    A2AEventsError,
    A2AEventsPublisher,
    DeliveryResult,
    FieldFilterSelector,
    KeywordSearchSelector,
    PublisherConfig,
    SelectorLimits,
    SigningKey,
    SubscriberCard,
    TokenBucketRateLimiter,
    Topic,
)
from a2a_events.models import DeliveryMode, DeliveryPreference
from a2a_events.pagination import decode_page_token, encode_page_token, paginate
from a2a_events.runtime.contracts import Transport

TOPIC = "agent_card.discovered"


class _AckTransport(Transport):

    async def send_a2a_message(self, endpoint, message):
        return DeliveryResult(ack=True, status_code=204)

    async def send_webhook(self, url, headers, body):
        return DeliveryResult(ack=True, status_code=204)


def _build(**kwargs):
    pub = A2AEventsPublisher(
        agent_card_url="https://pub.example/card",
        transport=_AckTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            card_resolver=lambda u: SubscriberCard(
                card_url=u, a2a_endpoint="https://s.example/a2a"
            ),
            **kwargs,
        ),
    )
    pub.declare_topic(Topic(name=TOPIC, filterableFields=["data.n"]))
    return pub


async def _subscribe(pub, card="https://s.example/card"):
    return await pub.subscribe(
        subscriber_card_url=card,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
    )


# --- page-token primitives -------------------------------------------------
def test_page_token_roundtrip() -> None:
    token = encode_page_token("sub_123")
    assert decode_page_token(token) == "sub_123"
    assert decode_page_token(None) is None


def test_invalid_page_token_raises() -> None:
    with pytest.raises(A2AEventsError) as exc:
        decode_page_token("!!!not-base64!!!")
    assert exc.value.code.value == "INVALID_CURSOR"


def test_paginate_walks_all_pages() -> None:
    items = [f"id{i}" for i in range(5)]
    seen: list[str] = []
    token = None
    pages = 0
    while True:
        page, token = paginate(items, lambda x: x, token, 2)
        seen.extend(page)
        pages += 1
        if token is None:
            break
    assert seen == items and pages == 3  # 2 + 2 + 1


# --- subscription pagination ----------------------------------------------
async def test_paginate_subscriptions() -> None:
    pub = _build(page_size=2)
    for i in range(3):
        await _subscribe(pub, card=f"https://s{i}.example/card")

    page1, token1 = await pub.paginate_subscriptions()
    assert len(page1) == 2 and token1 is not None
    page2, token2 = await pub.paginate_subscriptions(token1)
    assert len(page2) == 1 and token2 is None
    ids = {s.subscription_id for s in page1 + page2}
    assert len(ids) == 3


async def test_paginate_delivery_attempts() -> None:
    pub = _build(page_size=2)
    sub = await _subscribe(pub)
    for i in range(3):
        await pub.publish(TOPIC, "v1", {"n": i})

    first = await pub.list_delivery_attempts(sub.subscription_id, limit=2)
    assert len(first["deliveryAttempts"]) == 2 and first["nextPageToken"] is not None
    second = await pub.list_delivery_attempts(
        sub.subscription_id, first["nextPageToken"], 2
    )
    assert len(second["deliveryAttempts"]) == 1 and second["nextPageToken"] is None


# --- selector limits (§22) -------------------------------------------------
def test_selector_limits_keyword_count() -> None:
    limits = SelectorLimits(max_keywords=2)
    with pytest.raises(A2AEventsError) as exc:
        limits.check(KeywordSearchSelector(keywords=["a", "b", "c"]))
    assert exc.value.code.value == "INVALID_SELECTOR"


def test_selector_limits_keyword_length() -> None:
    limits = SelectorLimits(max_keyword_length=3)
    with pytest.raises(A2AEventsError):
        limits.check(KeywordSearchSelector(keywords=["toolong"]))


def test_selector_limits_field_and_value_counts() -> None:
    limits = SelectorLimits(max_fields=1, max_values_per_field=2)
    with pytest.raises(A2AEventsError):
        limits.check(FieldFilterSelector(where={"data.a": [1], "data.b": [2]}))
    with pytest.raises(A2AEventsError):
        limits.check(FieldFilterSelector(where={"data.a": [1, 2, 3]}))


def test_selector_limits_allow_within_bounds() -> None:
    limits = SelectorLimits()
    limits.check(KeywordSearchSelector(keywords=["ok"]))  # no raise
    limits.check(None)


async def test_subscribe_enforces_selector_limits() -> None:
    pub = _build(selector_limits=SelectorLimits(max_keywords=1))
    with pytest.raises(A2AEventsError) as exc:
        await pub.subscribe(
            subscriber_card_url="https://s.example/card",
            topics=[TOPIC],
            delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
            selector=KeywordSearchSelector(keywords=["a", "b"]),
        )
    assert exc.value.code.value == "INVALID_SELECTOR"


# --- rate limiting (§22) ---------------------------------------------------
def test_token_bucket_allows_then_blocks_then_refills() -> None:
    clock = [0.0]
    limiter = TokenBucketRateLimiter(rate=1.0, capacity=2.0, clock=lambda: clock[0])
    limiter.check("k", "subscribe")
    limiter.check("k", "subscribe")
    with pytest.raises(A2AEventsError) as exc:
        limiter.check("k", "subscribe")
    assert exc.value.code.value == "RATE_LIMITED"
    clock[0] = 1.0  # one second -> one refilled token
    limiter.check("k", "subscribe")  # no raise


async def test_subscribe_rate_limited() -> None:
    limiter = TokenBucketRateLimiter(rate=0.0, capacity=1.0)
    pub = _build(rate_limiter=limiter)
    await _subscribe(pub)  # consumes the only token
    with pytest.raises(A2AEventsError) as exc:
        await _subscribe(pub)
    assert exc.value.code.value == "RATE_LIMITED"


async def test_max_subscriptions_per_subscriber() -> None:
    pub = _build(max_subscriptions_per_subscriber=2)
    await _subscribe(pub)
    await _subscribe(pub)
    with pytest.raises(A2AEventsError) as exc:
        await _subscribe(pub)
    assert exc.value.code.value == "RATE_LIMITED"
