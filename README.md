# a2a-events (Python)

The Python implementation and reference runtime for **A2A Events** —
AgentCard-native durable event subscriptions for the
[A2A protocol](https://a2a-protocol.org).

**Subscribe to agents, not URLs.**

> **Spec / source of truth:** the protocol, design, published JSON Schemas, and
> conformance vectors live in the language-neutral
> [`a2a-events`](https://github.com/a2a-events/a2a-events) repo. Start with its
> [`DESIGN.md`](https://github.com/a2a-events/a2a-events/blob/main/DESIGN.md) and
> [docs](https://github.com/a2a-events/a2a-events/tree/main/docs). This repo is
> *an* implementation of that contract.

## Install

The project uses [uv](https://docs.astral.sh/uv/).

```bash
uv sync --extra dev
uv run pytest        # the full test suite
```

Optional extras pull in the transports/backends you need:

- `server` / `client` — the FastAPI publisher & subscriber apps and the httpx transport.
- `postgres` — the durable Postgres event/subscription stores and retry queue.
- `grpc` — the gRPC binding.

## Quick start

A complete in-memory tour (no network, no database) is in
[`examples/quickstart.py`](examples/quickstart.py):

```bash
uv run python examples/quickstart.py
```

## Features

- **Canonical JSON-RPC surface** — the `a2a.events.*` methods (`ListTopics`,
  `Subscribe`, `GetSubscription`, `ListSubscriptions`, `RenewSubscription`,
  `DeleteSubscription`, `Replay`, `Ack`, `ListDeliveryAttempts`). List methods
  return real opaque `nextPageToken` keyset cursors.
- **Resource limits & rate limiting** (§22) — optional `SelectorLimits` bound
  selector size (keyword/field/value counts), a pluggable `RateLimiter`
  (`TokenBucketRateLimiter`) throttles control-plane calls, and
  `max_subscriptions_per_subscriber` caps per-subscriber fan-out — all surfaced
  as `INVALID_SELECTOR` / `RATE_LIMITED`.
- **FastAPI apps** for publisher and subscriber, with the `A2A-Extensions`
  activation handshake and JWKS signing-key discovery. The publisher app also
  exposes the optional **HTTP+JSON binding** (`GET /a2a-events/topics`,
  `POST/GET/DELETE /a2a-events/subscriptions[...]`, `:renew`/`:replay`/`:ack`
  actions) mapping 1:1 to the JSON-RPC methods.
- **Optional gRPC binding** (§12.3) — a `grpc.aio` service
  (`a2a.events.v1.A2AEvents`) with one unary RPC per `a2a.events.*` method, plus
  an `A2AEventsGrpcClient`. It runs through the same JSON-RPC handler, so
  semantics and error mapping are identical; install the `grpc` extra.
- **Delivery** in both modes — canonical A2A-message (`a2a.SendMessage`) and
  webhook — in-memory and over HTTP, with selectors, leases, opaque per-topic
  cursors, replay, and at-least-once semantics with explicit/implicit ack.
- **Contract/implementation seam — bring your own backend.** The publisher
  depends only on the typed Protocols in `a2a_events.runtime.contracts` (the
  backend SPI): `EventStore`, `SubscriptionStore`, `RetryQueue`, and
  `Transport`. Implementations live behind that seam —
  `a2a_events.runtime.memory` is the zero-dependency in-memory reference used by
  default, and `a2a_events.runtime.postgres` is *an* optional batteries-included
  durable reference (install the `postgres` extra), not *the* backend. Since the
  contracts carry no third-party imports, your own Redis/Kafka/NATS/DynamoDB/etc.
  adapter plugs in without touching the publisher.
- **Durable state behind two store seams.** Events live behind the `EventStore`
  contract; subscriptions, acks, high-water positions, and delivery attempts
  behind the `SubscriptionStore` contract. The in-memory and Postgres backends
  pass identical cross-backend store-contract suites, so subscriptions survive a
  publisher restart. The
  Postgres backends use a `psycopg_pool` connection pool, so many offloaded
  store calls run concurrently; per-topic event-append offset allocation is
  serialized with a transaction-scoped advisory lock so concurrent publishes
  never collide on a cursor. The publisher's serving path is async; pass
  `PublisherConfig(offload_store=True, store_thread_safe=True)` (the latter for
  the pooled backends)
  to run blocking store calls in worker threads so they never stall the event
  loop.
- **Signed delivery** — Ed25519 (EdDSA) signatures over the full RFC 8785 (JCS)
  canonical event, including ECMAScript-correct number serialization. Supports
  signing-key rotation: pre-publish the next key via JWKS, activate it, retire
  the old one; subscribers refetch by `kid` on cache miss.
- **Secure delivery hardening** — SSRF guard on resolved delivery endpoints,
  subscriber-side timestamp-skew rejection, and exponential retry backoff with
  dead-lettering.
- **AgentCard discovery & trust** (§12.2, §21.2) — `AgentCardResolver` fetches
  the subscriber's real A2A AgentCard, parses the events extension declaration
  (`role: subscriber`), and resolves delivery endpoints **only** from the card.
  A `CardTrustPolicy` layers on HTTPS-only, same-origin, domain-allowlist,
  A2A `AgentCardSignature` (JWS-over-JCS) verification, and an out-of-band
  domain-ownership challenge.
- **Authorization & identity** (§21.1, §21.4, §21.5) — pluggable control-plane
  authentication (`CallerAuthenticator`) and topic authorization
  (`TopicAuthorizer` / `AllowlistAuthorizer`) evaluated both at subscribe and
  delivery time, so a revoked grant stops future deliveries. The publisher mints
  a per-subscription bearer **delivery token** (`DeliveryTokenIssuer`), returns
  it under `delivery.auth` at creation, and presents it on every delivery; the
  receiver authenticates incoming events against it.
- **Automatic lease renewal** — a transport-agnostic client-side
  `AutoLeaseRenewer` that renews at ~60% of the lease (§14.3).
- **Retention compaction** (§31) — beyond filtering expired events on read, an
  `EventStore.compact()` and a background `RetentionCompactor` physically delete
  events past each topic's `retentionSeconds`. A monotonic per-topic offset
  counter (in-memory field / Postgres `next_offset` column) keeps cursors stable
  so compaction never reuses an offset.
- **Observability** (§32) — a pluggable `Metrics` seam (`InMemoryMetrics` /
  `NullMetrics`) records published-event, delivery-attempt (by status), selector
  match-rate, delivery-latency, and lease-renewal metrics;
  `observability_snapshot()` adds the §32 gauges (active/expired subscription and
  dead-letter counts). Every event carries a deterministic `traceId` in
  `a2aevents.traceId` correlating the §32 tracing fields across attempts.
- **Durable retry architecture** (§19.4) — opt into a `RetryQueue`
  (in-memory or Postgres) and a background `RetryWorker`: a failed delivery is
  persisted with a `next_retry_at` and re-attempted off the publish path, so a
  crash mid-retry doesn't lose the event. Claims use a visibility lease
  (`FOR UPDATE SKIP LOCKED` on Postgres) so a crashed worker's in-flight retries
  become due again. Without a queue, delivery retries inline as before.

## End-to-end (multi-container)

`e2e/run.sh` brings up Postgres, a publisher service, and a subscriber service
in temporary Docker containers on a private network, then runs a host-side
driver that exercises every feature over the real network — AgentCard discovery
and trust, signed HTTP delivery with JWKS discovery, per-subscription delivery
tokens, topic authorization, selectors, replay, ack, `ListDeliveryAttempts`,
renew, key rotation, the SSRF guard, webhook delivery, the durable retry queue
and worker, pagination, retention compaction, observability, cross-container
timestamp-skew rejection, client-side auto lease renewal, and durable
subscriptions surviving a publisher restart:

```bash
./e2e/run.sh
```

## The vendored contract (`schemas/`, `conformance/`)

`schemas/` and `conformance/fixtures/` here are **vendored copies** of the
language-neutral contract owned by the spec repo
([`a2a-events`](https://github.com/a2a-events/a2a-events)). They are committed so
this repo is self-contained — tests and CI need no second checkout.

- The schemas are generated from the typed models with
  `uv run python scripts/export_schemas.py`; `tests/test_conformance.py` fails if
  the committed copy drifts from the models.
- Refresh both from the source of truth with
  `uv run python scripts/sync_spec.py` (copies from a sibling `../a2a-events`
  checkout when present, otherwise fetches from GitHub). Add `--check` in CI to
  fail when the vendored copy falls behind the spec repo.

## License

MIT — see [`LICENSE`](LICENSE).
