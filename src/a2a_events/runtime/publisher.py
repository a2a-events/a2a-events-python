"""Publisher runtime facade (spec §7.3, §14, §15, §19, §20).

Wires together the topic registry, subscription + lease management, the
selector-matching dispatcher, signed delivery with retry/dead-letter, and
replay. In-memory — the reference semantics, not a production worker
architecture. Delivery is async so it composes with async transports/servers.

Dispatch positions are tracked as integer per-topic high-water marks
(``-1`` = "before the first event"). Public, opaque cursors (``§10.9``) are
exposed only through ``Subscription.cursors`` (the per-topic last-acked map).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from urllib.parse import urlparse

from .. import cursor as cursor_mod
from ..auth import AuthIdentity, DeliveryTokenIssuer, TopicAuthorizer
from ..errors import A2AEventsError, ErrorCode
from ..limits import RateLimiter, SelectorLimits
from ..models import (
    EXTENSION_URI,
    CloudEvent,
    DeliveryMode,
    DeliveryPreference,
    ResolvedDelivery,
    Selector,
    Subscription,
    SubscriptionStatus,
    Topic,
)
from ..observability import Metrics, NullMetrics, trace_id_for
from ..pagination import paginate
from ..selectors import matches, validate_selector
from ..signing import SigningKey, SigningKeySet
from .contracts import (
    DeadLetter,
    DeliveryAttempt,
    DeliveryResult,
    EventRecord,
    EventStore,
    RetryItem,
    RetryQueue,
    SubscriptionStore,
    Transport,
    new_attempt_id,
    new_retry_id,
)
from .memory import InMemoryEventStore, InMemorySubscriptionStore
from .ssrf import SSRFPolicy, check_endpoint

_T = TypeVar("_T")


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class SubscriberCard:
    """The subset of a subscriber's AgentCard the publisher needs (§15, §21.2)."""

    card_url: str
    a2a_endpoint: str | None = None
    receive_url: str | None = None
    accepted_delivery_modes: list[DeliveryMode] = field(
        default_factory=lambda: [DeliveryMode.A2A_MESSAGE, DeliveryMode.WEBHOOK]
    )


@dataclass
class PublisherConfig:
    """Configuration for A2AEventsPublisher."""

    store: EventStore | None = None
    subscription_store: SubscriptionStore | None = None
    card_resolver: Callable[[str], SubscriberCard] | None = None
    ssrf_policy: SSRFPolicy | None = None
    min_lease_seconds: int = 60
    max_lease_seconds: int = 604800
    max_attempts: int = 3
    retry_initial_delay: float = 1.0
    retry_max_delay: float = 300.0
    sleep: Callable[[float], Awaitable[None]] = field(
        default_factory=lambda: asyncio.sleep
    )
    offload_store: bool = False
    store_thread_safe: bool = False
    authorizer: TopicAuthorizer | None = None
    delivery_token_issuer: DeliveryTokenIssuer | None = None
    retry_queue: RetryQueue | None = None
    rate_limiter: RateLimiter | None = None
    selector_limits: SelectorLimits | None = None
    max_subscriptions_per_subscriber: int | None = None
    page_size: int = 100
    metrics: Metrics | None = None
    # When True, ``publish`` only validates + persists the event and returns;
    # network delivery is driven off the publish path by a DispatchWorker (or
    # an explicit ``dispatch_pending()`` call) from the durable per-topic
    # dispatch positions. When False (the default), dispatch runs inline —
    # the development/reference mode, not a production worker architecture.
    deferred_dispatch: bool = False


class A2AEventsPublisher:

    def __init__(
        self,
        agent_card_url: str,
        transport: Transport,
        signing_key: SigningKey | SigningKeySet,
        *,
        config: PublisherConfig | None = None,
    ) -> None:
        self.agent_card_url = agent_card_url
        self.source = "a2a://" + (urlparse(agent_card_url).hostname or "publisher")
        self.transport = transport
        self.keys = (
            signing_key
            if isinstance(signing_key, SigningKeySet)
            else SigningKeySet(signing_key)
        )
        cfg = config or PublisherConfig()
        self.store: EventStore = cfg.store or InMemoryEventStore()
        self.subs: SubscriptionStore = (
            cfg.subscription_store or InMemorySubscriptionStore()
        )
        self._card_resolver = cfg.card_resolver or (
            lambda url: SubscriberCard(card_url=url)
        )
        # Topic configs are immutable once declared (both reference backends are
        # first-declaration-wins), so they are cached here to keep the dispatch
        # hot path free of per-delivery store round-trips.
        self._topics: dict[str, Topic] = {}
        # One dispatch lock per subscription: catch-up (backlog), live publish
        # dispatch, and the DispatchWorker all serialize per subscription, so
        # events reach a subscription strictly in per-topic log order and the
        # subscribe-time backlog can never be skipped by a racing publish.
        self._sub_locks: dict[str, asyncio.Lock] = {}
        self._deferred_dispatch = cfg.deferred_dispatch
        self.ssrf_policy = cfg.ssrf_policy or SSRFPolicy()
        self.min_lease_seconds = cfg.min_lease_seconds
        self.max_lease_seconds = cfg.max_lease_seconds
        self.max_attempts = cfg.max_attempts
        self.retry_initial_delay = cfg.retry_initial_delay
        self.retry_max_delay = cfg.retry_max_delay
        self._sleep = cfg.sleep
        # When the store does blocking I/O (e.g. sync Postgres), run each
        # serving-time store call in a worker thread so it never blocks the
        # event loop. A single-connection store is not safe for concurrent
        # threads, so the lock serializes access; a thread-safe store (e.g. the
        # pooled Postgres backends) sets ``store_thread_safe=True`` to drop the
        # lock and run store calls truly concurrently (§25, async deployments).
        self._offload_store = cfg.offload_store
        self._store_thread_safe = cfg.store_thread_safe
        self._store_lock = asyncio.Lock()
        # Optional control-plane authorization and per-subscription delivery
        # credentials (§21.4, §21.5). When unset, all topics are public and no
        # delivery token is issued — the original zero-auth behavior.
        self.authorizer = cfg.authorizer
        self.delivery_token_issuer = cfg.delivery_token_issuer
        # When set, failed deliveries are persisted to a durable retry queue and
        # re-attempted by a background RetryWorker, instead of retried inline.
        # This survives a crash mid-retry (§19.4). The queue is internally
        # thread-safe, so it needs no asyncio lock under offload.
        self.retry_queue = cfg.retry_queue
        # Resource limits and rate limiting (§22). All optional.
        self.rate_limiter = cfg.rate_limiter
        self.selector_limits = cfg.selector_limits
        self.max_subscriptions_per_subscriber = cfg.max_subscriptions_per_subscriber
        self.page_size = cfg.page_size
        # Observability sink (§32). Defaults to a no-op collector.
        self.metrics: Metrics = cfg.metrics or NullMetrics()

    async def run_offloaded(self, fn: Callable[..., _T], *args: Any) -> _T:
        """Run a (sync) retry-queue call, offloading to a thread when configured.

        The retry queue is thread-safe, so unlike :meth:`_run_store` this needs
        no lock — concurrent claims/enqueues serialize inside the queue.
        """
        if self._offload_store:
            return await asyncio.to_thread(fn, *args)
        return fn(*args)

    async def _run_store(self, fn: Callable[..., _T], *args: Any) -> _T:
        """Invoke a (sync) store call, offloading to a thread when configured."""
        if not self._offload_store:
            return fn(*args)
        if self._store_thread_safe:
            return await asyncio.to_thread(fn, *args)
        async with self._store_lock:
            return await asyncio.to_thread(fn, *args)

    @property
    def dead_letters(self) -> list[DeadLetter]:
        """Events permanently undeliverable to any subscription (§19)."""
        return self.subs.dead_letters()

    @property
    def signing_key(self) -> SigningKey:
        """The active signing key (the one new events are signed with)."""
        return self.keys.active

    def add_signing_key(self, key: SigningKey, *, activate: bool = False) -> None:
        """Publish ``key`` via JWKS; ``activate`` to start signing with it (§21.3)."""
        self.keys.add(key, activate=activate)

    def rotate_signing_key(self, kid: str) -> None:
        """Make the already-published key ``kid`` the active signer (§21.3)."""
        self.keys.activate(kid)

    def retire_signing_key(self, kid: str) -> None:
        """Stop publishing ``kid`` from JWKS (cannot be the active key)."""
        self.keys.retire(kid)

    def signing_jwks(self) -> list[dict[str, str]]:
        """The JWKS ``keys`` array to serve at the publisher's signingKeysUrl."""
        return self.keys.jwks()

    # --- topics -------------------------------------------------------------
    def declare_topic(self, topic: Topic) -> None:
        self.store.declare_topic(topic)

    async def _get_topic(self, name: str) -> Topic:
        """Fetch a topic config, memoizing it (topics are immutable, see §13)."""
        topic = self._topics.get(name)
        if topic is None:
            topic = await self._run_store(self.store.get_topic, name)
            self._topics[name] = topic
        return topic

    async def compact(self, topic: str | None = None) -> int:
        """Physically delete events outside the retention window (§31)."""
        return await self._run_store(self.store.compact, topic)

    async def list_topics(self) -> dict[str, Any]:
        host = urlparse(self.agent_card_url).hostname or "publisher"
        topics = await self._run_store(self.store.topics)
        return {
            "publisher": {"agentCardUrl": self.agent_card_url, "agentId": host},
            "extension": EXTENSION_URI,
            "eventFormat": "cloudevents-1.0",
            "topics": [t.model_dump(by_alias=True, exclude_none=True) for t in topics],
        }

    # --- subscription lifecycle (§14) ---------------------------------------
    async def subscribe(
        self,
        subscriber_card_url: str,
        topics: list[str],
        delivery: DeliveryPreference,
        selector: Selector | None = None,
        from_cursor: str = cursor_mod.LATEST,
        lease_seconds: int = 86400,
        metadata: dict[str, Any] | None = None,
        caller: AuthIdentity | None = None,
    ) -> Subscription:
        self._check_lease(lease_seconds)
        # Duplicate topic names would double-deliver during backfill; a
        # subscription's topic set is a set (§14.1).
        topics = list(dict.fromkeys(topics))
        if self.rate_limiter is not None:
            key = caller.subject if caller else subscriber_card_url
            self.rate_limiter.check(key, "subscribe")
        if self.selector_limits is not None:
            self.selector_limits.check(selector)
        if self.authorizer is not None:
            self.authorizer.authorize_subscribe(caller, topics)
        await self._check_subscription_quota(subscriber_card_url)
        card = self._resolve_card(subscriber_card_url)
        self._check_delivery_mode(delivery.mode, card)
        resolved = self._resolve_delivery_target(delivery, card)

        topic_models = [await self._get_topic(name) for name in topics]
        for topic in topic_models:
            validate_selector(selector, topic)
            if delivery.mode not in topic.delivery_modes:
                raise A2AEventsError(
                    ErrorCode.DELIVERY_MODE_NOT_SUPPORTED,
                    f"Topic {topic.name} does not support delivery mode {delivery.mode.value}.",
                    {"topic": topic.name, "mode": delivery.mode.value},
                )
            # Starting anywhere but "latest" replays the backlog, which a
            # replay-disabled topic must reject (§20.2, §31).
            if from_cursor != cursor_mod.LATEST and not topic.replay:
                raise A2AEventsError(
                    ErrorCode.REPLAY_NOT_SUPPORTED,
                    f"Topic {topic.name} does not support replay; "
                    'subscribe with fromCursor "latest".',
                    {"topic": topic.name},
                )

        now = _now()
        sub = Subscription(
            subscriptionId="sub_" + uuid.uuid4().hex,
            status=SubscriptionStatus.ACTIVE,
            publisherCardUrl=self.agent_card_url,
            subscriberCardUrl=subscriber_card_url,
            topics=topics,
            selector=selector,
            delivery=resolved,
            createdAt=now,
            leaseUntil=now + timedelta(seconds=lease_seconds),
            cursors={},
            metadata=metadata or {},
        )
        start_offsets = {
            t.name: await self._start_offset(t.name, from_cursor) for t in topic_models
        }
        await self._run_store(self.subs.add, sub, start_offsets)

        # Cutover (§14.1): the start offsets captured above are the
        # linearization point — the subscription owns every event after them.
        # Catching up immediately (for *every* fromCursor, including "latest")
        # both backfills the backlog and closes the creation race: an event
        # appended between reading the topic head and persisting the
        # subscription is delivered here rather than falling into a window.
        if not self._deferred_dispatch:
            await self._dispatch_subscription(sub)
        return sub

    def delivery_auth(self, sub: Subscription) -> dict[str, Any] | None:
        """The per-subscription delivery credential to hand back (§21.1, §21.5).

        Returned at subscription creation (and on get/renew) so the subscriber
        can authenticate incoming deliveries. ``None`` when no issuer is wired.
        """
        if self.delivery_token_issuer is None:
            return None
        return {
            "scheme": self.delivery_token_issuer.SCHEME,
            "token": self.delivery_token_issuer.issue(sub.subscription_id),
            "expiresAt": sub.lease_until.isoformat(),
            "rotates": True,
        }

    async def get_subscription(self, subscription_id: str) -> Subscription:
        sub = await self._run_store(self.subs.get, subscription_id)
        if sub is None or sub.status == SubscriptionStatus.DELETED:
            raise A2AEventsError(
                ErrorCode.SUBSCRIPTION_NOT_FOUND,
                f"Subscription {subscription_id} not found.",
                {"subscriptionId": subscription_id},
            )
        if self._expire_if_due(sub):
            await self._run_store(self.subs.update, sub)
        return sub

    async def _get_active_subscription(self, subscription_id: str) -> Subscription:
        """Like :meth:`get_subscription`, but an expired lease is an error.

        Used by the operations that act on live delivery state (replay, ack):
        they fail with ``SUBSCRIPTION_EXPIRED`` (§30) until the subscriber
        renews, instead of silently operating on a lapsed subscription.
        """
        sub = await self.get_subscription(subscription_id)
        if sub.status != SubscriptionStatus.ACTIVE:
            raise A2AEventsError(
                ErrorCode.SUBSCRIPTION_EXPIRED,
                f"Subscription {subscription_id} has expired; renew it first.",
                {"subscriptionId": subscription_id},
            )
        return sub

    async def list_subscriptions(self) -> list[Subscription]:
        subs = await self._run_store(self.subs.list_all)
        for sub in subs:
            if self._expire_if_due(sub):
                await self._run_store(self.subs.update, sub)
        return [s for s in subs if s.status != SubscriptionStatus.DELETED]

    async def paginate_subscriptions(
        self, page_token: str | None = None, limit: int | None = None
    ) -> tuple[list[Subscription], str | None]:
        """One page of active subscriptions plus the next page token (§14.5)."""
        subs = await self.list_subscriptions()
        return paginate(
            subs, lambda s: s.subscription_id, page_token, limit or self.page_size
        )

    async def observability_snapshot(self) -> dict[str, Any]:
        """Current §32 gauges plus any collected counters/latencies."""
        subs = await self._run_store(self.subs.list_all)
        now = _now()
        active = sum(
            1
            for s in subs
            if s.status == SubscriptionStatus.ACTIVE and s.lease_until > now
        )
        expired = sum(
            1
            for s in subs
            if s.status == SubscriptionStatus.EXPIRED
            or (s.status == SubscriptionStatus.ACTIVE and s.lease_until <= now)
        )
        dead_letters = await self._run_store(self.subs.dead_letters)
        snapshot: dict[str, Any] = {
            "subscriptionCount": active,
            "expiredSubscriptionCount": expired,
            "deadLetterCount": len(dead_letters),
        }
        collected = getattr(self.metrics, "snapshot", None)
        if callable(collected):
            snapshot["metrics"] = collected()
        return snapshot

    async def _check_subscription_quota(self, subscriber_card_url: str) -> None:
        """Enforce max subscriptions per subscriber (§22), if configured."""
        if self.max_subscriptions_per_subscriber is None:
            return
        existing = sum(
            1
            for s in await self.list_subscriptions()
            if s.subscriber_card_url == subscriber_card_url
        )
        if existing >= self.max_subscriptions_per_subscriber:
            raise A2AEventsError(
                ErrorCode.RATE_LIMITED,
                "Maximum subscriptions per subscriber reached "
                f"({self.max_subscriptions_per_subscriber}).",
                {
                    "maxSubscriptionsPerSubscriber": (
                        self.max_subscriptions_per_subscriber
                    )
                },
            )

    async def renew(self, subscription_id: str, lease_seconds: int) -> Subscription:
        self._check_lease(lease_seconds)
        sub = await self.get_subscription(subscription_id)
        sub.lease_until = _now() + timedelta(seconds=lease_seconds)
        sub.status = SubscriptionStatus.ACTIVE
        await self._run_store(self.subs.update, sub)
        self.metrics.incr("lease_renewals")
        return sub

    async def delete(self, subscription_id: str) -> Subscription | None:
        # Deletion is idempotent (§14.4).
        sub = await self._run_store(self.subs.get, subscription_id)
        if sub is None:
            return None
        sub.status = SubscriptionStatus.DELETED
        await self._run_store(self.subs.update, sub)
        return sub

    async def ack(self, subscription_id: str, cursor: str) -> Subscription:
        """Explicit ack (§10.10): advance the per-topic acked cursor."""
        sub = await self._get_active_subscription(subscription_id)
        topic = cursor_mod.topic_of(cursor)
        if topic not in sub.topics:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_AUTHORIZED,
                f"Subscription is not authorized for topic {topic}.",
                {"topic": topic},
            )
        await self._advance(sub, topic, cursor)
        return sub

    async def list_delivery_attempts(
        self,
        subscription_id: str,
        page_token: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """The recorded delivery attempts for a subscription (§29 table)."""
        await self.get_subscription(subscription_id)  # authorize / 404 if unknown
        attempts = await self._run_store(self.subs.delivery_attempts, subscription_id)
        page, next_token = paginate(
            attempts,
            lambda a: a.delivery_attempt_id,
            page_token,
            limit or self.page_size,
        )
        return {
            "subscriptionId": subscription_id,
            "deliveryAttempts": [
                {
                    "deliveryAttemptId": a.delivery_attempt_id,
                    "eventId": a.event_id,
                    "cursor": a.cursor,
                    "attempt": a.attempt,
                    "status": a.status,
                    "lastStatusCode": a.last_status_code,
                    "lastError": a.last_error,
                }
                for a in page
            ],
            "nextPageToken": next_token,
        }

    async def replay(
        self,
        subscription_id: str,
        from_cursor: str | None = None,
        to_cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        sub = await self._get_active_subscription(subscription_id)
        if from_cursor and from_cursor not in (cursor_mod.EARLIEST, cursor_mod.LATEST):
            topic = cursor_mod.topic_of(from_cursor)
        else:
            topic = sub.topics[0]
        if topic not in sub.topics:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_AUTHORIZED,
                f"Subscription is not authorized for topic {topic}.",
                {"topic": topic},
            )
        topic_model = await self._get_topic(topic)
        if not topic_model.replay:
            raise A2AEventsError(
                ErrorCode.REPLAY_NOT_SUPPORTED,
                f"Topic {topic} does not support replay.",
                {"topic": topic},
            )
        records, next_cursor = await self._run_store(
            self.store.read, topic, from_cursor, to_cursor, limit
        )
        events = [
            self._build_event(sub, r, schema_url=topic_model.schema_url).model_dump(
                by_alias=True, mode="json", exclude_none=True
            )
            for r in records
            if matches(
                sub.selector, self._raw_event_dict(r), topic_model.filterable_fields
            )
        ]
        return {
            "subscriptionId": subscription_id,
            "events": events,
            "nextCursor": next_cursor,
        }

    # --- publish + dispatch (§15, §19) --------------------------------------
    async def publish(
        self,
        topic: str,
        type: str,
        data: dict[str, Any],
        subject: str | None = None,
    ) -> EventRecord:
        """Validate and persist one event, then dispatch it.

        The event is durable once ``append`` returns: dispatch is driven
        entirely by the per-(subscription, topic) high-water positions, so a
        crash at any later point leaves recoverable work that a restarted
        publisher (or a :class:`~a2a_events.runtime.dispatcher.DispatchWorker`)
        finishes via :meth:`dispatch_pending`. With
        ``PublisherConfig(deferred_dispatch=True)`` no delivery happens here at
        all — publish returns as soon as the event is persisted.
        """
        record = await self._run_store(
            self.store.append, topic, type, self.source, data, subject
        )
        self.metrics.incr("published_events", topic=topic)
        if not self._deferred_dispatch:
            # Inline reference mode: catch every affected subscription up to
            # the head. Subscriptions run concurrently (a slow subscriber does
            # not delay a fast one); each subscription is serialized by its
            # dispatch lock, so per-topic order is preserved.
            await self._dispatch_topic(topic)
        return record

    async def dispatch_pending(self) -> int:
        """Deliver every event still owed to any active subscription (§19).

        The recovery/worker entry point: safe to call at any time, after a
        crash, or periodically from a
        :class:`~a2a_events.runtime.dispatcher.DispatchWorker`. Returns the
        number of events processed (delivered, dead-lettered, handed to the
        retry queue, or skipped by a selector).
        """
        subs = [s for s in await self._run_store(self.subs.list_all) if s.topics]
        results = await asyncio.gather(
            *(self._dispatch_subscription(sub) for sub in subs)
        )
        return sum(results)

    async def _dispatch_topic(self, topic: str) -> None:
        """Catch every subscription of ``topic`` up to the head, concurrently."""
        subs = [
            s for s in await self._run_store(self.subs.list_all) if topic in s.topics
        ]
        await asyncio.gather(*(self._dispatch_subscription(s, [topic]) for s in subs))

    async def _dispatch_subscription(
        self, sub: Subscription, topics: list[str] | None = None
    ) -> int:
        """Catch ``sub`` up to the head of ``topics`` (default: all its topics).

        Serialized per subscription so backlog catch-up and live dispatch can
        never interleave or skip events for the same subscription.
        """
        if not await self._is_active(sub):
            return 0
        # Re-check topic authorization at delivery time so a revoked grant
        # stops future deliveries (§21.4).
        if self.authorizer is not None and not self.authorizer.authorize_delivery(sub):
            return 0
        lock = self._sub_locks.setdefault(sub.subscription_id, asyncio.Lock())
        processed = 0
        async with lock:
            for topic in topics if topics is not None else list(sub.topics):
                processed += await self._catch_up(sub, topic)
        return processed

    async def _catch_up(self, sub: Subscription, topic: str) -> int:
        """Deliver all events of ``topic`` past the subscription's position.

        Pages through the event log from the durable high-water mark in
        ``page_size`` chunks until it reaches the head, delivering matching
        events in log order and advancing the scan position past
        selector-filtered ones. Every step persists position first-class, so a
        crash mid-catch-up resumes exactly where it left off (at-least-once).
        """
        topic_model = await self._get_topic(topic)
        processed = 0
        while True:
            high_water = await self._run_store(
                self.subs.high_water, sub.subscription_id
            )
            position = high_water.get(topic, -1)
            from_cursor = cursor_mod.encode(topic, position) if position >= 0 else None
            try:
                records, _ = await self._run_store(
                    self.store.read, topic, from_cursor, None, self.page_size
                )
            except A2AEventsError as exc:
                if exc.code != ErrorCode.CURSOR_EXPIRED:
                    raise
                # The position aged out of retention: resume from the oldest
                # live event and accept the gap (§31).
                records, _ = await self._run_store(
                    self.store.read, topic, None, None, self.page_size
                )
            if not records:
                return processed
            scan = position
            for record in records:
                offset = cursor_mod.offset_of(record.cursor)
                if offset <= scan:
                    continue
                raw = self._raw_event_dict(record)
                if matches(sub.selector, raw, topic_model.filterable_fields):
                    self.metrics.incr("selector_evaluations", result="match")
                    await self._deliver_one(sub, record)
                else:
                    self.metrics.incr("selector_evaluations", result="miss")
                scan = offset
                processed += 1
            # Persist the scan position once per page so selector-filtered
            # events are never rescanned on the next pass (delivered events
            # already advanced it record-by-record).
            current = await self._run_store(self.subs.high_water, sub.subscription_id)
            if scan > current.get(topic, -1):
                await self._run_store(
                    self.subs.set_high_water, sub.subscription_id, topic, scan
                )

    # --- internals ----------------------------------------------------------
    def _raw_event_dict(self, record: EventRecord) -> dict[str, Any]:
        # The shape selectors resolve against (paths are "data.*").
        return {
            "type": record.event_type,
            "subject": record.subject,
            "data": record.data,
        }

    def _build_event(
        self,
        sub: Subscription,
        record: EventRecord,
        attempt: int = 1,
        schema_url: str | None = None,
    ) -> CloudEvent:
        return CloudEvent(
            id=record.event_id,
            source=record.source,
            type=record.event_type,
            subject=record.subject,
            time=record.created_at,
            data=record.data,
            # Flat scalar extension context attributes (spec §16; CloudEvents
            # 1.0 has no map-typed attributes).
            a2apublisher=self.agent_card_url,
            a2atopic=record.topic,
            a2acursor=record.cursor,
            a2aschemaurl=schema_url,
            a2asubscription=sub.subscription_id,
            a2adeliveryattempt=attempt,
            a2atraceid=trace_id_for(record.event_id),
        )

    async def _attempt_send(
        self, sub: Subscription, record: EventRecord, attempt: int
    ) -> DeliveryResult:
        """Build, sign, and send one delivery attempt of ``record`` to ``sub``."""
        topic_model = await self._get_topic(record.topic)
        event = self._build_event(sub, record, attempt, topic_model.schema_url)
        event_dict = event.model_dump(by_alias=True, mode="json", exclude_none=True)
        timestamp = event_dict["time"]
        signature = self.signing_key.sign(timestamp, event_dict)
        return await self._send(sub, event_dict, signature, timestamp)

    async def _on_delivered(
        self,
        sub: Subscription,
        record: EventRecord,
        attempt: int,
        result: DeliveryResult,
    ) -> None:
        await self._record_attempt(sub, record, attempt, "delivered", result)
        # End-to-end delivery latency: append time -> successful delivery (§32).
        self.metrics.observe(
            "delivery_latency_seconds",
            (_now() - record.created_at).total_seconds(),
            topic=record.topic,
        )
        # Implicit ack on success advances the per-topic cursor (§10.10).
        await self._advance(sub, record.topic, record.cursor, record.event_id)

    async def _on_terminal(
        self,
        sub: Subscription,
        record: EventRecord,
        attempt: int,
        result: DeliveryResult,
    ) -> None:
        await self._record_attempt(sub, record, attempt, "dead_letter", result)
        # Advance the high-water so the dead-lettered event is not retried
        # forever on the next publish (it lives in the dead-letter queue now).
        await self._run_store(
            self.subs.set_high_water,
            sub.subscription_id,
            record.topic,
            cursor_mod.offset_of(record.cursor),
        )

    async def _deliver_one(self, sub: Subscription, record: EventRecord) -> bool:
        if self.retry_queue is not None:
            return await self._deliver_queued(sub, record)
        for attempt in range(1, self.max_attempts + 1):
            result = await self._attempt_send(sub, record, attempt)
            if result.ack:
                await self._on_delivered(sub, record, attempt, result)
                return True
            terminal = not result.retry or attempt == self.max_attempts
            if terminal:
                await self._on_terminal(sub, record, attempt, result)
                return False
            await self._record_attempt(sub, record, attempt, "retry", result)
            # Exponential backoff before the next attempt (§19.4).
            await self._sleep(self._backoff_delay(attempt))
        return False

    async def _deliver_queued(self, sub: Subscription, record: EventRecord) -> bool:
        """First attempt inline; on a retryable failure, enqueue a durable retry.

        The background :class:`~a2a_events.runtime.retry_worker.RetryWorker` drains the
        queue, so a crash before the retry fires does not lose the event.
        """
        result = await self._attempt_send(sub, record, 1)
        if result.ack:
            await self._on_delivered(sub, record, 1, result)
            return True
        if not result.retry or self.max_attempts <= 1:
            await self._on_terminal(sub, record, 1, result)
            return False
        await self._record_attempt(sub, record, 1, "retry", result)
        assert self.retry_queue is not None
        await self.run_offloaded(
            self.retry_queue.enqueue,
            RetryItem(
                retry_id=new_retry_id(),
                subscription_id=sub.subscription_id,
                topic=record.topic,
                cursor=record.cursor,
                event_id=record.event_id,
                attempt=1,
                next_retry_at=_now() + timedelta(seconds=self._backoff_delay(1)),
                last_error=result.reason,
            ),
        )
        # The event is now the retry queue's responsibility: advance the
        # dispatch position so catch-up moves on (high-water = "handed off",
        # not "acked" — the acked cursor advances only on delivery).
        await self._run_store(
            self.subs.set_high_water,
            sub.subscription_id,
            record.topic,
            cursor_mod.offset_of(record.cursor),
        )
        return False

    async def retry_delivery(self, item: RetryItem, queue: RetryQueue) -> None:
        """Re-attempt one queued delivery (called by the RetryWorker, §19.4)."""
        sub = await self._run_store(self.subs.get, item.subscription_id)
        if sub is None or sub.status != SubscriptionStatus.ACTIVE:
            # Subscription gone or no longer active: drop the retry.
            await self.run_offloaded(queue.complete, item.retry_id)
            return
        record = await self._record_at(item.topic, item.cursor)
        if record is None:
            # The event aged out of retention before we could deliver it (§31).
            await self.run_offloaded(queue.complete, item.retry_id)
            return
        attempt = item.attempt + 1
        result = await self._attempt_send(sub, record, attempt)
        if result.ack:
            await self._on_delivered(sub, record, attempt, result)
            await self.run_offloaded(queue.complete, item.retry_id)
            return
        if not result.retry or attempt >= self.max_attempts:
            await self._on_terminal(sub, record, attempt, result)
            await self.run_offloaded(queue.complete, item.retry_id)
            return
        await self._record_attempt(sub, record, attempt, "retry", result)
        await self.run_offloaded(
            queue.reschedule,
            item.retry_id,
            _now() + timedelta(seconds=self._backoff_delay(attempt)),
            attempt,
            result.reason,
        )

    async def _record_at(self, topic: str, cursor: str) -> EventRecord | None:
        """Load the single event record at ``cursor`` (None if expired/missing)."""
        offset = cursor_mod.offset_of(cursor)
        from_cursor = cursor_mod.encode(topic, offset - 1) if offset > 0 else None
        try:
            records, _ = await self._run_store(
                self.store.read, topic, from_cursor, cursor, 1
            )
        except A2AEventsError as exc:
            if exc.code != ErrorCode.CURSOR_EXPIRED:
                return None
            # The *predecessor* offset aged out of retention, but the event at
            # ``cursor`` may itself still be the oldest live event. Re-read from
            # the start of retention so a still-deliverable event isn't dropped.
            try:
                records, _ = await self._run_store(
                    self.store.read, topic, None, cursor, 1
                )
            except A2AEventsError:
                return None
        if records and cursor_mod.offset_of(records[0].cursor) == offset:
            return records[0]
        return None

    async def _record_attempt(
        self,
        sub: Subscription,
        record: EventRecord,
        attempt: int,
        status: str,
        result: DeliveryResult,
    ) -> None:
        # §32: delivery_attempts split by terminal status (delivered/retry/dead_letter).
        self.metrics.incr("delivery_attempts", status=status, topic=record.topic)
        await self._run_store(
            self.subs.record_attempt,
            DeliveryAttempt(
                delivery_attempt_id=new_attempt_id(),
                subscription_id=sub.subscription_id,
                event_id=record.event_id,
                cursor=record.cursor,
                attempt=attempt,
                status=status,
                last_status_code=result.status_code,
                last_error=result.reason,
            ),
        )

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff before retry ``attempt + 1`` (§19.4)."""
        delay: float = self.retry_initial_delay * (2 ** (attempt - 1))
        return min(delay, self.retry_max_delay)

    async def _send(
        self,
        sub: Subscription,
        event_dict: dict[str, Any],
        signature: str,
        timestamp: str,
    ) -> DeliveryResult:
        token = (
            self.delivery_token_issuer.issue(sub.subscription_id)
            if self.delivery_token_issuer is not None
            else None
        )
        if sub.delivery.mode == DeliveryMode.WEBHOOK:
            headers = {
                "A2A-Event-Signature": signature,
                "A2A-Event-Timestamp": timestamp,
                "A2A-Event-Key-ID": self.signing_key.kid,
                "A2A-Subscription-ID": sub.subscription_id,
            }
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            return await self.transport.send_webhook(
                sub.delivery.resolved_url or "", headers, event_dict
            )
        meta: dict[str, Any] = {
            "kind": "event.delivery",
            "signature": signature,
            "timestamp": timestamp,
            "keyId": self.signing_key.kid,
            "subscriptionId": sub.subscription_id,
        }
        if token is not None:
            meta["deliveryToken"] = token
        # A valid A2A v1.0 Message: messageId is required; role uses the A2A
        # enum wire form; the CloudEvent rides in a DataPart.
        message = {
            "message": {
                "messageId": "msg_" + uuid.uuid4().hex,
                "role": "ROLE_AGENT",
                "parts": [{"data": event_dict}],
                "metadata": {EXTENSION_URI: meta},
            }
        }
        return await self.transport.send_a2a_message(
            sub.delivery.resolved_endpoint or "", message
        )

    async def _advance(
        self,
        sub: Subscription,
        topic: str,
        cursor: str,
        event_id: str | None = None,
    ) -> None:
        current = sub.cursors.get(topic)
        if current is None or cursor > current:
            sub.cursors[topic] = cursor
            await self._run_store(self.subs.update, sub)
        offset = cursor_mod.offset_of(cursor)
        high_water = await self._run_store(self.subs.high_water, sub.subscription_id)
        if offset > high_water.get(topic, -1):
            await self._run_store(
                self.subs.set_high_water, sub.subscription_id, topic, offset
            )
        if event_id is None:
            event_id = await self._resolve_event_id(topic, cursor)
        if event_id is not None:
            await self._run_store(
                self.subs.record_ack, sub.subscription_id, event_id, cursor
            )

    async def _resolve_event_id(self, topic: str, cursor: str) -> str | None:
        """Best-effort lookup of the event_id at ``cursor`` for ack auditing."""
        record = await self._record_at(topic, cursor)
        return record.event_id if record is not None else None

    async def _start_offset(self, topic: str, from_cursor: str) -> int:
        """Initial high-water offset (-1 = before first event)."""
        if from_cursor == cursor_mod.EARLIEST:
            return -1
        if from_cursor != cursor_mod.LATEST:
            # A specific cursor is scoped to a single topic (§10.9): it seeds
            # that topic's position; any other subscribed topic starts at
            # "latest". Malformed cursors raise INVALID_CURSOR here.
            if cursor_mod.topic_of(from_cursor) == topic:
                # Validate retention, then resume after the cursor.
                await self._run_store(
                    self.store.read, topic, from_cursor, from_cursor, 1
                )
                return cursor_mod.offset_of(from_cursor)
        if not await self._run_store(self.store.count, topic):
            return -1
        latest = await self._run_store(self.store.latest_cursor, topic)
        return cursor_mod.offset_of(latest)

    # --- guards -------------------------------------------------------------
    def _check_lease(self, lease_seconds: int) -> None:
        if lease_seconds < self.min_lease_seconds:
            raise A2AEventsError(
                ErrorCode.LEASE_TOO_SHORT,
                f"Lease must be >= {self.min_lease_seconds}s.",
                {"minLeaseSeconds": self.min_lease_seconds},
            )
        if lease_seconds > self.max_lease_seconds:
            raise A2AEventsError(
                ErrorCode.LEASE_TOO_LONG,
                f"Lease must be <= {self.max_lease_seconds}s.",
                {"maxLeaseSeconds": self.max_lease_seconds},
            )

    def _resolve_card(self, subscriber_card_url: str) -> SubscriberCard:
        try:
            return self._card_resolver(subscriber_card_url)
        except A2AEventsError:
            raise
        except Exception as exc:
            raise A2AEventsError(
                ErrorCode.SUBSCRIBER_CARD_UNREACHABLE,
                f"Could not resolve subscriber card {subscriber_card_url}.",
                {"subscriberCardUrl": subscriber_card_url},
            ) from exc

    def _check_delivery_mode(self, mode: DeliveryMode, card: SubscriberCard) -> None:
        if mode not in card.accepted_delivery_modes:
            raise A2AEventsError(
                ErrorCode.DELIVERY_MODE_NOT_SUPPORTED,
                f"Subscriber does not accept delivery mode {mode.value}.",
                {"mode": mode.value},
            )

    def _resolve_delivery_target(
        self, delivery: DeliveryPreference, card: SubscriberCard
    ) -> ResolvedDelivery:
        if delivery.mode == DeliveryMode.WEBHOOK:
            if not card.receive_url:
                raise A2AEventsError(
                    ErrorCode.DELIVERY_ENDPOINT_NOT_DECLARED,
                    "Subscriber AgentCard does not declare a receive URL.",
                )
            check_endpoint(card.receive_url, self.ssrf_policy)
            return ResolvedDelivery(mode=delivery.mode, resolvedUrl=card.receive_url)
        if not card.a2a_endpoint:
            raise A2AEventsError(
                ErrorCode.DELIVERY_ENDPOINT_NOT_DECLARED,
                "Subscriber AgentCard does not declare an A2A endpoint.",
            )
        check_endpoint(card.a2a_endpoint, self.ssrf_policy)
        return ResolvedDelivery(mode=delivery.mode, resolvedEndpoint=card.a2a_endpoint)

    async def _is_active(self, sub: Subscription) -> bool:
        if self._expire_if_due(sub):
            await self._run_store(self.subs.update, sub)
        return sub.status == SubscriptionStatus.ACTIVE

    def _expire_if_due(self, sub: Subscription) -> bool:
        """Expire a lapsed lease in place; return whether the status changed."""
        if sub.status == SubscriptionStatus.ACTIVE and sub.lease_until <= _now():
            sub.status = SubscriptionStatus.EXPIRED
            return True
        return False
