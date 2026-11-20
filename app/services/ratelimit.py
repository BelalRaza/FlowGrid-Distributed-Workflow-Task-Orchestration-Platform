"""Token-bucket rate limiting, one bucket per API key.

A bucket holds up to ``capacity`` tokens and refills at ``refill_per_second``.
Each request spends one token. This allows short bursts (up to capacity)
while bounding the sustained rate (refill rate).

The Redis implementation runs as a Lua script so read-modify-write is atomic
across any number of API processes, and it reads the clock from Redis itself
(``TIME``) so API servers with skewed clocks still agree.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Protocol

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: float


class RateLimiter(Protocol):
    def hit(self, bucket: str) -> RateLimitResult: ...


_TOKEN_BUCKET_LUA = """
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) * refill)
local allowed = 0
local retry_after = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after = (1 - tokens) / refill
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / refill) + 1)
-- Return strings: Lua numbers are truncated to integers by the Redis protocol.
return {allowed, tostring(tokens), tostring(retry_after)}
"""


class RedisTokenBucket:
    def __init__(self, redis_url: str, capacity: int, refill_per_second: float) -> None:
        self.capacity = capacity
        self.refill = refill_per_second
        self._redis = Redis.from_url(redis_url, socket_connect_timeout=0.5, socket_timeout=0.5)
        self._script = self._redis.register_script(_TOKEN_BUCKET_LUA)

    def hit(self, bucket: str) -> RateLimitResult:
        try:
            allowed, tokens, retry_after = self._script(keys=[bucket], args=[self.capacity, self.refill])
        except RedisError as exc:
            # Fail open: during a Redis outage the API keeps accepting jobs into
            # the PostgreSQL outbox. Rejecting every request because the *rate
            # limiter* is down would turn a partial outage into a total one.
            logger.warning("Rate limiter unavailable; allowing request", extra={"error": str(exc)})
            return RateLimitResult(True, self.capacity, self.capacity, 0.0)
        return RateLimitResult(
            allowed=bool(allowed),
            limit=self.capacity,
            remaining=int(float(tokens)),
            retry_after_seconds=float(retry_after),
        )


class InMemoryTokenBucket:
    """Same algorithm, in-process. For tests and single-process development."""

    def __init__(self, capacity: int, refill_per_second: float, clock=time.monotonic) -> None:
        self.capacity = capacity
        self.refill = refill_per_second
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def hit(self, bucket: str) -> RateLimitResult:
        with self._lock:
            now = self._clock()
            tokens, ts = self._buckets.get(bucket, (float(self.capacity), now))
            tokens = min(self.capacity, tokens + max(0.0, now - ts) * self.refill)
            if tokens >= 1:
                tokens -= 1
                allowed, retry_after = True, 0.0
            else:
                allowed, retry_after = False, (1 - tokens) / self.refill
            self._buckets[bucket] = (tokens, now)
        return RateLimitResult(allowed, self.capacity, math.floor(tokens), retry_after)
