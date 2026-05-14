"""In-process token-bucket rate limiter.

V1 scope: per-process. A token bucket keyed by `(user_id, role_id)`
caps how many `governance.evaluate` calls a single identity can make
per second. Useful for single-process deployments (the embedded
DuckDB + library shape Gibran ships as).

V2 caveat (per HANDOFF.md): in a multi-process deployment, this
becomes false security -- each process has its own bucket, so a user
running N processes gets N times the rate. Cross-process limiting
needs Redis-or-equivalent shared state and is deferred until the
deployment shape is decided.

The limiter is OFF by default. Enable by constructing a `RateLimiter`
with a non-zero `tokens_per_second` and passing it to
`DefaultGovernance(..., rate_limiter=rl)`. With no limiter, every
evaluate proceeds (matches the V1 default).
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class _Bucket:
    tokens: float
    last_refill: float


class RateLimiter:
    """Token bucket keyed by (user_id, role_id)."""

    def __init__(self, *, tokens_per_second: float, burst: int) -> None:
        if tokens_per_second <= 0:
            raise ValueError("tokens_per_second must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self.rate = float(tokens_per_second)
        self.burst = float(burst)
        self._buckets: dict[tuple[str, str], _Bucket] = {}
        self._lock = threading.Lock()

    def acquire(self, user_id: str, role_id: str) -> bool:
        """Try to consume one token for this identity. Returns True if a
        token was available (request proceeds) and False otherwise
        (request should be denied with RATE_LIMITED)."""
        key = (user_id, role_id)
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(tokens=self.burst, last_refill=now)
                self._buckets[key] = bucket
            elapsed = now - bucket.last_refill
            bucket.tokens = min(self.burst, bucket.tokens + elapsed * self.rate)
            bucket.last_refill = now
            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return True
            return False
