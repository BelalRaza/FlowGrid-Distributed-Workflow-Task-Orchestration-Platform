"""Phase 2 security and API hardening: auth, ownership, rate limiting,
request limits, request IDs and structured logs."""

import json
import logging

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_rate_limiter
from app.cli import main as cli_main
from app.core.config import get_settings
from app.core.logging import ContextFilter, JsonFormatter, log_context
from app.services.auth import authenticate, hash_api_key, revoke_api_key
from app.services.ratelimit import InMemoryTokenBucket

# --------------------------------------------------------------------------- #
# Authentication
# --------------------------------------------------------------------------- #


def test_missing_api_key_is_rejected(anon_client):
    res = anon_client.get("/api/v1/jobs")
    assert res.status_code == 401
    assert res.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize(
    "headers",
    [
        {"Authorization": "Bearer fg_not-a-real-key"},
        {"Authorization": "Bearer something-else"},
        {"Authorization": "Basic dXNlcjpwYXNz"},
        {"Authorization": "Bearer"},
        {"X-API-Key": "fg_wrong"},
    ],
)
def test_invalid_credentials_are_rejected(anon_client, headers):
    assert anon_client.get("/api/v1/jobs", headers=headers).status_code == 401


def test_x_api_key_header_is_accepted(anon_client, api_key):
    assert anon_client.get("/api/v1/jobs", headers={"X-API-Key": api_key[1]}).status_code == 200


def test_revoked_key_is_rejected(client, db, api_key):
    assert client.get("/api/v1/jobs").status_code == 200
    revoke_api_key(db, api_key[0].prefix)
    assert client.get("/api/v1/jobs").status_code == 401


def test_only_the_hash_is_stored(db, api_key):
    row, raw = api_key
    assert row.key_hash == hash_api_key(raw)
    assert raw not in (row.key_hash, row.prefix)
    assert authenticate(db, raw).id == row.id


def test_health_and_docs_are_public(anon_client, monkeypatch):
    from app.api import health

    monkeypatch.setattr(health, "check_redis", lambda: True)
    assert anon_client.get("/health").status_code == 200
    assert anon_client.get("/docs").status_code == 200


# --------------------------------------------------------------------------- #
# Ownership (no cross-client access)
# --------------------------------------------------------------------------- #


def test_clients_cannot_see_each_others_jobs(client, other_client):
    job_id = client.post(
        "/api/v1/jobs", json={"type": "always_fail", "payload": {}, "max_attempts": 1}
    ).json()["job_id"]

    # 404, not 403: never confirm that another client's job ID exists.
    assert other_client.get(f"/api/v1/jobs/{job_id}").status_code == 404
    assert other_client.get(f"/api/v1/jobs/{job_id}/attempts").status_code == 404
    assert other_client.post(f"/api/v1/jobs/{job_id}/replay").status_code == 404
    assert other_client.get("/api/v1/jobs").json()["total"] == 0
    assert other_client.get("/api/v1/jobs/stats").json()["total"] == 0

    assert client.get("/api/v1/jobs").json()["total"] == 1


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #


def test_rate_limit_returns_429_with_retry_after(app, client):
    limiter = InMemoryTokenBucket(capacity=3, refill_per_second=0.5)
    app.dependency_overrides[get_rate_limiter] = lambda: limiter

    statuses = [client.get("/api/v1/jobs").status_code for _ in range(4)]
    assert statuses == [200, 200, 200, 429]

    res = client.get("/api/v1/jobs")
    assert res.status_code == 429
    assert res.headers["Retry-After"] == "2"  # 1 token / 0.5 per second
    assert res.headers["X-RateLimit-Remaining"] == "0"


def test_rate_limit_headers_on_success_and_one_token_per_request(app, client):
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    limiter = InMemoryTokenBucket(capacity=10, refill_per_second=0.001)
    first = client.get("/api/v1/jobs")
    second = client.get("/api/v1/jobs")
    assert first.headers["X-RateLimit-Limit"] == "10"
    assert int(first.headers["X-RateLimit-Remaining"]) == 9
    assert int(second.headers["X-RateLimit-Remaining"]) == 8  # dependency runs once per request


def test_rate_limit_buckets_are_per_api_key(app, client, other_client):
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    limiter = InMemoryTokenBucket(capacity=1, refill_per_second=0.001)
    assert client.get("/api/v1/jobs").status_code == 200
    assert client.get("/api/v1/jobs").status_code == 429
    assert other_client.get("/api/v1/jobs").status_code == 200


def test_token_bucket_refills_over_time():
    now = [0.0]
    bucket = InMemoryTokenBucket(capacity=2, refill_per_second=1, clock=lambda: now[0])
    assert [bucket.hit("k").allowed for _ in range(3)] == [True, True, False]
    now[0] = 1.0
    assert bucket.hit("k").allowed
    assert not bucket.hit("k").allowed
    now[0] = 100.0  # refill never exceeds capacity
    assert [bucket.hit("k").allowed for _ in range(3)] == [True, True, False]


def test_redis_rate_limiter_fails_open_when_redis_is_down():
    from app.services.ratelimit import RedisTokenBucket

    limiter = RedisTokenBucket("redis://127.0.0.1:1/0", capacity=5, refill_per_second=1)
    assert limiter.hit("anything").allowed


# --------------------------------------------------------------------------- #
# Request hardening
# --------------------------------------------------------------------------- #


def test_oversized_body_is_rejected_with_413(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_request_body_bytes", 200)
    big = {"type": "echo", "payload": {"message": "x" * 500}}
    res = client.post("/api/v1/jobs", json=big)
    assert res.status_code == 413
    assert "X-Request-ID" in res.headers


def test_request_id_is_generated_or_propagated(client):
    generated = client.get("/api/v1/jobs").headers["X-Request-ID"]
    assert len(generated) == 32

    assert client.get("/api/v1/jobs", headers={"X-Request-ID": "trace-abc.123"}).headers["X-Request-ID"] == "trace-abc.123"
    # Junk (e.g. log-injection attempts) is replaced, not echoed.
    bad = client.get("/api/v1/jobs", headers={"X-Request-ID": "x\ny"}).headers["X-Request-ID"]
    assert bad != "x\ny" and len(bad) == 32


# --------------------------------------------------------------------------- #
# Structured logging
# --------------------------------------------------------------------------- #


def test_json_logs_include_bound_context_and_extras():
    record = logging.LogRecord("app.test", logging.INFO, __file__, 1, "Attempt %s", ("started",), None)
    record.retryable = True
    with log_context(job_id="j-1", attempt=2):
        ContextFilter().filter(record)
    line = json.loads(JsonFormatter().format(record))
    assert line["msg"] == "Attempt started"
    assert line["level"] == "INFO"
    assert line["job_id"] == "j-1" and line["attempt"] == 2
    assert line["retryable"] is True


def test_context_does_not_leak_after_block():
    with log_context(job_id="inner"):
        pass
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "m", (), None)
    ContextFilter().filter(record)
    assert not hasattr(record, "job_id")


# --------------------------------------------------------------------------- #
# Admin CLI
# --------------------------------------------------------------------------- #


def test_cli_create_list_revoke(capsys, app):
    assert cli_main(["create-api-key", "--name", "cli-test"]) == 0
    raw = capsys.readouterr().out.strip().splitlines()[-1]
    assert raw.startswith("fg_")

    with TestClient(app, headers={"Authorization": f"Bearer {raw}"}) as c:
        assert c.get("/api/v1/me").json()["name"] == "cli-test"

    assert cli_main(["list-api-keys"]) == 0
    assert "cli-test" in capsys.readouterr().out
    assert cli_main(["revoke-api-key", "--prefix", raw[:11]]) == 0
    assert cli_main(["revoke-api-key", "--prefix", raw[:11]]) == 1  # already revoked
