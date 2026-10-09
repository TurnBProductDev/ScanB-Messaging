"""Per-person rate limits.

Token buckets in process memory, so the limit is per replica: with N replicas
a person can do N times the rate. That bounds abuse and runaway clients, which
is the job; it is not a quota. A shared limiter (Redis, or the API gateway) is
the upgrade when that difference starts to matter.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from .errors import RateLimited


@dataclass(frozen=True)
class Limit:
    capacity: float          # burst
    per_second: float        # sustained refill


DEFAULT_LIMITS: dict[str, Limit] = {
    "message.send": Limit(capacity=20, per_second=1.0),        # 60/min sustained
    "thread.create": Limit(capacity=10, per_second=0.2),       # 12/min
    "thread.members": Limit(capacity=10, per_second=0.2),
    "typing": Limit(capacity=2, per_second=0.5),
    "task.create": Limit(capacity=10, per_second=0.2),
    "task.update": Limit(capacity=30, per_second=1.0),
    "task.request_update": Limit(capacity=3, per_second=1 / 600),  # a nudge every ten minutes
    "people.search": Limit(capacity=20, per_second=2.0),
}


class RateLimiter:
    def __init__(self, limits: dict[str, Limit] | None = None, clock=time.monotonic) -> None:
        self._limits = limits or DEFAULT_LIMITS
        self._clock = clock
        self._buckets: dict[tuple, tuple[float, float]] = {}

    def check(self, action: str, *key) -> None:
        limit = self._limits.get(action)
        if limit is None:
            return
        now = self._clock()
        bucket_key = (action, *key)
        tokens, last = self._buckets.get(bucket_key, (limit.capacity, now))
        tokens = min(limit.capacity, tokens + (now - last) * limit.per_second)
        if tokens < 1:
            retry = max(1, int((1 - tokens) / limit.per_second))
            raise RateLimited("You are doing that too often. Wait a moment and try again.",
                              retryAfterSeconds=retry)
        self._buckets[bucket_key] = (tokens - 1, now)
        if len(self._buckets) > 50_000:
            self._buckets.clear()   # bounded memory; a reset only ever errs toward allowing
