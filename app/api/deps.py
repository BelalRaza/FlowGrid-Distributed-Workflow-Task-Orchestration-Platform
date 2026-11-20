import math
from functools import lru_cache

from fastapi import Depends, Header, HTTPException, Request, Response, status
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import bind_log_context
from app.db.models import ApiKey
from app.db.session import get_db
from app.services.auth import authenticate
from app.services.jobs import Publisher
from app.services.ratelimit import RateLimiter, RedisTokenBucket
from app.tasks.worker import publish_job


def get_publisher() -> Publisher:
    """The function used to enqueue jobs. Tests override this to simulate a broker outage."""
    return publish_job


@lru_cache
def get_rate_limiter() -> RateLimiter:
    s = get_settings()
    return RedisTokenBucket(s.redis_url, s.rate_limit_capacity, s.rate_limit_refill_per_second)


def _unauthorized(message: str) -> HTTPException:
    return HTTPException(status.HTTP_401_UNAUTHORIZED, detail=message, headers={"WWW-Authenticate": "Bearer"})


def get_api_key(
    request: Request,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> ApiKey:
    """Accepts `Authorization: Bearer <key>` or `X-API-Key: <key>`."""
    raw = x_api_key
    if authorization:
        scheme, _, credentials = authorization.partition(" ")
        if scheme.lower() != "bearer" or not credentials:
            raise _unauthorized("Authorization header must be 'Bearer <api key>'")
        raw = credentials.strip()
    if not raw:
        raise _unauthorized("Missing API key")

    api_key = authenticate(db, raw)
    if api_key is None:
        raise _unauthorized("Invalid or revoked API key")

    request.state.api_key_prefix = api_key.prefix
    bind_log_context(api_key=api_key.prefix)
    return api_key


def require_client(
    response: Response,
    api_key: ApiKey = Depends(get_api_key),
    limiter: RateLimiter = Depends(get_rate_limiter),
) -> ApiKey:
    """Authenticate, then spend one token from this key's rate-limit bucket."""
    result = limiter.hit(f"flowgrid:ratelimit:{api_key.id}")
    headers = {"X-RateLimit-Limit": str(result.limit), "X-RateLimit-Remaining": str(result.remaining)}
    if not result.allowed:
        headers["Retry-After"] = str(max(1, math.ceil(result.retry_after_seconds)))
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded", headers=headers)
    response.headers.update(headers)
    return api_key
