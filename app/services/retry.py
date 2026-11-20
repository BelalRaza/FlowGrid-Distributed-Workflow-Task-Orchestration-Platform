"""Retry policy: which errors are worth retrying, and how long to wait."""

from __future__ import annotations

import random
from collections.abc import Callable

from celery.exceptions import SoftTimeLimitExceeded


class TransientJobError(Exception):
    """A failure that may succeed if tried again (network blip, 503, lock timeout)."""


class PermanentJobError(Exception):
    """A failure that will not change on retry (bad data, business rule violation)."""


class JobTimeoutError(TransientJobError):
    """The job exceeded its timeout_seconds."""


class WorkerLostError(TransientJobError):
    """The worker running the job disappeared (lease expired)."""


# Explicit allow-list. Anything not listed here (including programming errors
# such as KeyError or TypeError) is treated as permanent: retrying a bug just
# burns attempts and delays the failure the operator needs to see.
RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TransientJobError,
    SoftTimeLimitExceeded,
    TimeoutError,
    ConnectionError,
)


def is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, RETRYABLE_EXCEPTIONS)


def backoff_delay(
    attempt: int,
    *,
    base: float,
    cap: float,
    rng: Callable[[], float] = random.random,
) -> float:
    """Exponential backoff with "equal jitter".

    attempt 1 -> ~base, attempt 2 -> ~2*base, attempt 3 -> ~4*base ... capped.
    Half of the delay is fixed and half is random, so retries never fire
    immediately but also never fire in lock-step across many failed jobs
    (the "thundering herd" after a shared dependency recovers).
    """
    exp = min(cap, base * (2 ** (attempt - 1)))
    return exp / 2 + rng() * exp / 2
