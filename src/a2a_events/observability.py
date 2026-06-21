"""Metrics and tracing seams (DESIGN.md §32).

The runtime emits the §32 metrics — published-event and delivery-attempt counts,
delivery success/latency, retry and dead-letter counts, selector match rate,
lease-renewal rate — and gauges (subscription / expired counts) through a small
:class:`Metrics` seam. A publisher constructed without one uses :class:`NullMetrics`
(zero overhead); pass :class:`InMemoryMetrics` to collect them, or adapt the
protocol to Prometheus/OpenTelemetry.

Each event also carries a deterministic ``traceId`` (derived from its event id,
so every delivery attempt and retry of the same event shares it) in
``a2aevents.traceId``, correlating the §32 tracing fields (event.id, topic,
cursor, subscriptionId, deliveryAttemptId, ...).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Protocol, runtime_checkable


def trace_id_for(event_id: str) -> str:
    """Deterministic trace id for an event (stable across attempts, §32)."""
    return "tr_" + event_id.removeprefix("evt_")


def _label_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(labels.items()))


@runtime_checkable
class Metrics(Protocol):
    """Counter + observation sink for the §32 metrics."""

    def incr(self, name: str, value: int = 1, **labels: str) -> None: ...

    def observe(self, name: str, value: float, **labels: str) -> None: ...


class NullMetrics(Metrics):
    """No-op metrics sink (the default — zero overhead).

    Explicitly subclasses ``Metrics`` so the unused (``_``-prefixed) parameters
    count as protocol overrides: pyright matches overrides by signature (not
    by parameter name), and Sonar S1172 exempts overriding methods.
    """

    def incr(self, name: str, value: int = 1, **labels: str) -> None:
        return None

    def observe(self, name: str, value: float, **labels: str) -> None:
        return None


class InMemoryMetrics:
    """Collects counters and observations in process (for tests / inspection)."""

    def __init__(self) -> None:
        self.counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = defaultdict(
            int
        )
        self.observations: dict[
            tuple[str, tuple[tuple[str, str], ...]], list[float]
        ] = defaultdict(list)

    def incr(self, name: str, value: int = 1, **labels: str) -> None:
        self.counters[(name, _label_key(labels))] += value

    def observe(self, name: str, value: float, **labels: str) -> None:
        self.observations[(name, _label_key(labels))].append(value)

    def get(self, name: str, **labels: str) -> int:
        """Total of a counter for the exact label set (0 if unseen)."""
        return self.counters.get((name, _label_key(labels)), 0)

    def total(self, name: str) -> int:
        """Total of a counter across all label sets."""
        return sum(v for (n, _), v in self.counters.items() if n == name)

    def observations_for(self, name: str) -> list[float]:
        out: list[float] = []
        for (n, _), values in self.observations.items():
            if n == name:
                out.extend(values)
        return out

    def snapshot(self) -> dict[str, dict[str, float]]:
        """A flat, human-readable view of all collected metrics."""
        counters = {
            _render(name, labels): float(value)
            for (name, labels), value in self.counters.items()
        }
        latencies = {}
        for (name, labels), values in self.observations.items():
            if values:
                key = _render(name, labels)
                latencies[key] = sum(values) / len(values)
        return {"counters": counters, "averages": latencies}


def _render(name: str, labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return name
    rendered = ",".join(f"{k}={v}" for k, v in labels)
    return f"{name}{{{rendered}}}"
