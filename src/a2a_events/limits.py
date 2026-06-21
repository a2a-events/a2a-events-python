"""Selector resource limits and rate limiting (spec §22).

Selectors can be user-generated and potentially expensive, and control-plane
calls (subscribe, replay, ...) can be abused, so publishers should enforce
limits. Both seams are optional — a publisher without them imposes no limits.

- :class:`SelectorLimits` bounds selector size (keyword count/length, field and
  value counts) and rejects oversize selectors with ``INVALID_SELECTOR``.
- :class:`RateLimiter` / :class:`TokenBucketRateLimiter` throttle control-plane
  calls per principal, raising ``RATE_LIMITED``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .errors import A2AEventsError, ErrorCode
from .models import FieldFilterSelector, KeywordSearchSelector, Selector


@dataclass(frozen=True)
class SelectorLimits:
    """Bounds on selector size (spec §22 resource limits)."""

    max_keywords: int = 50
    max_keyword_length: int = 256
    max_fields: int = 50
    max_values_per_field: int = 100

    def check(self, selector: Selector | None) -> None:
        if selector is None:
            return
        if isinstance(selector, KeywordSearchSelector):
            self._check_keyword(selector)
        elif isinstance(selector, FieldFilterSelector):
            self._check_field_filter(selector)

    def _fail(self, message: str, detail: dict[str, object]) -> None:
        raise A2AEventsError(ErrorCode.INVALID_SELECTOR, message, detail)

    def _check_keyword(self, selector: KeywordSearchSelector) -> None:
        if len(selector.keywords) > self.max_keywords:
            self._fail(
                f"Selector exceeds the maximum of {self.max_keywords} keywords.",
                {"maxKeywords": self.max_keywords},
            )
        for kw in selector.keywords:
            if len(kw) > self.max_keyword_length:
                self._fail(
                    f"Keyword exceeds the maximum length of "
                    f"{self.max_keyword_length} characters.",
                    {"maxKeywordLength": self.max_keyword_length},
                )

    def _check_field_filter(self, selector: FieldFilterSelector) -> None:
        if len(selector.where) > self.max_fields:
            self._fail(
                f"Selector exceeds the maximum of {self.max_fields} fields.",
                {"maxFields": self.max_fields},
            )
        for field, values in selector.where.items():
            if len(values) > self.max_values_per_field:
                self._fail(
                    f"Field {field} exceeds the maximum of "
                    f"{self.max_values_per_field} values.",
                    {"field": field, "maxValuesPerField": self.max_values_per_field},
                )


@runtime_checkable
class RateLimiter(Protocol):
    """Throttles control-plane calls; raises ``RATE_LIMITED`` when exceeded."""

    def check(self, key: str | None, operation: str) -> None: ...


class TokenBucketRateLimiter:
    """A per-principal token-bucket rate limiter (spec §22 max rate).

    Each ``(key, operation)`` gets a bucket of ``capacity`` tokens refilling at
    ``rate`` tokens/second; a call consumes one token and is rejected with
    ``RATE_LIMITED`` when the bucket is empty. Anonymous callers (``key=None``)
    share one bucket per operation.
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._rate = rate
        self._capacity = capacity
        self._clock = clock
        self._buckets: dict[tuple[str, str], tuple[float, float]] = {}
        self._lock = threading.Lock()

    def check(self, key: str | None, operation: str) -> None:
        bucket_key = (key or "<anonymous>", operation)
        now = self._clock()
        with self._lock:
            tokens, last = self._buckets.get(bucket_key, (self._capacity, now))
            tokens = min(self._capacity, tokens + (now - last) * self._rate)
            if tokens < 1.0:
                self._buckets[bucket_key] = (tokens, now)
                raise A2AEventsError(
                    ErrorCode.RATE_LIMITED,
                    f"Rate limit exceeded for operation {operation}.",
                    {"operation": operation},
                )
            self._buckets[bucket_key] = (tokens - 1.0, now)
