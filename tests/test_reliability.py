"""Phase 2 reliability: retries, backoff, timeouts, leases, fencing, outbox,
idempotency, dead-letter and replay."""

import dataclasses
from datetime import timedelta

import pytest
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_publisher
from app.db.models import IdempotencyKey, Job, JobStatus, Notification, ProcessedRecord, utcnow
from app.services import jobs as jobs_service
from app.services.executor import run_job
from app.services.retry import backoff_delay, is_retryable, TransientJobError
from app.tasks import handlers

# --------------------------------------------------------------------------- #
# Retry policy
# --------------------------------------------------------------------------- #


def test_backoff_grows_exponentially_with_bounded_jitter():
    lo = [backoff_delay(n, base=2, cap=300, rng=lambda: 0.0) for n in range(1, 6)]
    hi = [backoff_delay(n, base=2, cap=300, rng=lambda: 1.0) for n in range(1, 6)]
    assert lo == [1, 2, 4, 8, 16]  # half the exponential delay is fixed...
    assert hi == [2, 4, 8, 16, 32]  # ...and up to half again is random
    assert backoff_delay(20, base=2, cap=300, rng=lambda: 1.0) == 300  # capped


def test_error_classification():
    assert is_retryable(TransientJobError())
    assert is_retryable(ConnectionError())
    assert is_retryable(SoftTimeLimitExceeded())
    assert not is_retryable(KeyError("bug"))  # programming errors are not retried
    assert not is_retryable(ValueError())


# --------------------------------------------------------------------------- #
# Retries end-to-end (eager worker + explicit sweeper passes)
# --------------------------------------------------------------------------- #


def test_transient_failures_are_retried_until_success(client, sweep):
    job_id = client.post("/api/v1/jobs", json={"type": "flaky", "payload": {"fail_times": 2}}).json()["job_id"]

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "RETRY_SCHEDULED" and job["attempts"] == 1
    assert job["next_attempt_at"] is not None

    sweep()  # attempt 2 fails again
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RETRY_SCHEDULED"
    sweep()  # attempt 3 succeeds

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "SUCCEEDED"
    assert job["attempts"] == 3
    assert job["result"] == {"succeeded_on_attempt": 3}

    attempts = client.get(f"/api/v1/jobs/{job_id}/attempts").json()
    assert [a["status"] for a in attempts] == ["FAILED", "FAILED", "SUCCEEDED"]
    assert attempts[0]["retryable"] is True
    assert attempts[0]["error_type"] == "TransientJobError"


def test_retry_is_not_run_before_its_backoff_delay(client, sweep):
    job_id = client.post("/api/v1/jobs", json={"type": "flaky", "payload": {"fail_times": 1}}).json()["job_id"]
    assert sweep(seconds_ahead=0).retries_enqueued == 0
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RETRY_SCHEDULED"


def test_next_attempt_respects_backoff_window(db, submit):
    job = submit("flaky", {"fail_times": 1})
    before = utcnow()
    run_job(job.id, "w")
    db.refresh(job)
    delay = (job.next_attempt_at.replace(tzinfo=None) - before.replace(tzinfo=None)).total_seconds()
    assert 1.0 - 0.5 <= delay <= 2.0 + 0.5  # attempt 1 with base=2 -> [1s, 2s]


def test_exhausted_retries_go_to_dead_letter(client, sweep):
    res = client.post(
        "/api/v1/jobs",
        json={"type": "always_fail", "payload": {"reason": "upstream down", "transient": True}, "max_attempts": 2},
    )
    job_id = res.json()["job_id"]
    sweep()

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "DEAD_LETTER"
    assert job["attempts"] == 2
    assert job["error"] == "TransientJobError: upstream down"
    assert job["finished_at"] is not None

    dlq = client.get("/api/v1/jobs", params={"status": "DEAD_LETTER"}).json()
    assert [j["job_id"] for j in dlq["items"]] == [job_id]


def test_max_attempts_one_means_no_retry(client):
    res = client.post("/api/v1/jobs", json={"type": "flaky", "payload": {"fail_times": 1}, "max_attempts": 1})
    assert client.get(f"/api/v1/jobs/{res.json()['job_id']}").json()["status"] == "DEAD_LETTER"


# --------------------------------------------------------------------------- #
# Timeouts
# --------------------------------------------------------------------------- #


def test_soft_timeout_is_recorded_and_retried(db, submit, monkeypatch):
    def times_out(payload, ctx):
        raise SoftTimeLimitExceeded()

    monkeypatch.setitem(handlers.REGISTRY, "echo", dataclasses.replace(handlers.REGISTRY["echo"], func=times_out))
    job = submit("echo", {"message": "x"}, timeout_seconds=3)

    assert run_job(job.id, "w") == JobStatus.RETRY_SCHEDULED
    db.refresh(job)
    assert job.error == "JobTimeoutError: exceeded timeout of 3s"
    attempt = jobs_service.list_attempts(db, job.id)[0]
    assert attempt.status == "TIMED_OUT" and attempt.retryable is True


# --------------------------------------------------------------------------- #
# Leases: recovering jobs from dead workers
# --------------------------------------------------------------------------- #


def test_expired_lease_is_recovered_and_retried(db, submit, sweep):
    job = submit("echo", {"message": "x"})
    claimed = jobs_service.claim_job(db, job.id, "doomed-worker")  # ...and the worker dies here
    lease_seconds = (claimed.lease_expires_at - claimed.started_at).total_seconds()
    assert lease_seconds == job.timeout_seconds + 15

    assert sweep(seconds_ahead=lease_seconds - 1).leases_recovered == 0  # lease still valid

    result = sweep(seconds_ahead=lease_seconds + 1)
    assert result.leases_recovered == 1
    db.refresh(job)
    assert job.status == JobStatus.RETRY_SCHEDULED.value
    assert job.error.startswith("WorkerLostError: lease expired; worker doomed-worker")
    assert jobs_service.list_attempts(db, job.id)[0].status == "LOST"

    sweep()  # backoff passed: retried on a healthy (eager) worker
    db.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    assert job.attempts == 2
    assert [a.status for a in jobs_service.list_attempts(db, job.id)] == ["LOST", "SUCCEEDED"]


def test_zombie_worker_cannot_overwrite_newer_attempt(db, submit, sweep):
    """Fencing: attempt 1 is declared lost; its late result must be discarded."""
    job = submit("echo", {"message": "x"})
    jobs_service.claim_job(db, job.id, "zombie")
    sweep(seconds_ahead=3600, publish=lambda *_: None)  # lease expired -> retry scheduled
    sweep(seconds_ahead=7200, publish=lambda *_: None)  # retry due -> QUEUED (nobody consumes)
    assert jobs_service.claim_job(db, job.id, "healthy") is not None  # attempt 2 RUNNING

    assert jobs_service.record_success(db, job.id, 1, {"from": "zombie"}) is False
    db.refresh(job)
    assert job.status == JobStatus.RUNNING.value and job.worker_id == "healthy"

    assert jobs_service.record_success(db, job.id, 2, {"from": "healthy"}) is True
    db.refresh(job)
    assert job.result == {"from": "healthy"}


# --------------------------------------------------------------------------- #
# Outbox: broker outages do not lose jobs
# --------------------------------------------------------------------------- #


def _broken_publisher(job_id, timeout_seconds):
    raise ConnectionError("redis down")


def test_broker_outage_leaves_job_pending_then_sweeper_delivers(app, client, db, sweep):
    app.dependency_overrides[get_publisher] = lambda: _broken_publisher
    res = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "outbox"}})
    assert res.status_code == 202  # accepted: it is durable in PostgreSQL
    job_id = res.json()["job_id"]
    assert res.json()["status"] == "PENDING" and res.json()["attempts"] == 0

    # Broker still down: the sweeper tries, fails, and leaves it PENDING.
    assert sweep(seconds_ahead=60, publish=_broken_publisher).pending_republished == 0
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "PENDING"

    # Broker back: the sweeper delivers it.
    assert sweep(seconds_ahead=120).pending_republished == 1
    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "SUCCEEDED" and job["result"] == {"echo": "outbox"}


def test_fresh_pending_jobs_are_not_republished_early(app, client, sweep):
    app.dependency_overrides[get_publisher] = lambda: _broken_publisher
    client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"}})
    assert sweep(seconds_ahead=1).pending_republished == 0


# --------------------------------------------------------------------------- #
# Idempotency keys
# --------------------------------------------------------------------------- #

BODY = {"type": "echo", "payload": {"message": "pay invoice 42"}}


def test_same_idempotency_key_returns_the_same_job(client, db):
    first = client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "inv-42"})
    second = client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "inv-42"})

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert first.json()["job_id"] == second.json()["job_id"]
    assert db.scalar(select(func.count()).select_from(Job)) == 1


def test_idempotency_key_reused_with_different_body_is_rejected(client):
    first = client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "inv-42"})
    other = {"type": "echo", "payload": {"message": "something else"}}
    res = client.post("/api/v1/jobs", json=other, headers={"Idempotency-Key": "inv-42"})
    assert res.status_code == 409
    assert res.json()["detail"]["job_id"] == first.json()["job_id"]


def test_equivalent_bodies_match_after_normalisation(client):
    # Defaults are filled in before hashing, so these are the same request.
    a = {"type": "process_records", "payload": {"batch_id": "b", "record_ids": [1]}}
    b = {"type": "process_records", "payload": {"batch_id": "b", "record_ids": [1], "delay_ms": 0}}
    assert client.post("/api/v1/jobs", json=a, headers={"Idempotency-Key": "k"}).status_code == 202
    assert client.post("/api/v1/jobs", json=b, headers={"Idempotency-Key": "k"}).status_code == 200


def test_idempotency_keys_are_scoped_per_api_key(client, other_client):
    a = client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "shared"}).json()
    b = other_client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "shared"}).json()
    assert a["job_id"] != b["job_id"]


def test_concurrent_duplicate_loses_the_insert_race_gracefully(client, monkeypatch):
    """Simulate two requests passing the 'key exists?' check at the same time.

    The second one's INSERT hits the primary key and must return the first job,
    not a 500 and not a duplicate.
    """
    first = client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "race"}).json()

    original_get = Session.get
    state = {"hidden": False}

    def get_hiding_key_once(self, entity, ident, **kw):
        if entity is IdempotencyKey and not state["hidden"]:
            state["hidden"] = True
            return None  # pretend the other request has not committed yet
        return original_get(self, entity, ident, **kw)

    monkeypatch.setattr(Session, "get", get_hiding_key_once)
    res = client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "race"})
    assert state["hidden"]
    assert res.status_code == 200
    assert res.json()["job_id"] == first["job_id"]


def test_expired_idempotency_keys_are_purged(client, db, sweep):
    client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "old"})
    assert sweep(seconds_ahead=3600).idempotency_keys_purged == 0
    assert sweep(seconds_ahead=25 * 3600).idempotency_keys_purged == 1
    # After expiry the key can be used again and creates a new job.
    assert client.post("/api/v1/jobs", json=BODY, headers={"Idempotency-Key": "old"}).status_code == 202


# --------------------------------------------------------------------------- #
# Idempotent side effects under retry
# --------------------------------------------------------------------------- #


def test_crash_mid_batch_then_retry_does_not_duplicate_side_effects(client, db, sweep):
    res = client.post(
        "/api/v1/jobs",
        json={
            "type": "process_records",
            "payload": {"batch_id": "crashy", "record_ids": [1, 2, 3, 4], "fail_first_attempt_after": 2},
        },
    )
    job_id = res.json()["job_id"]
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RETRY_SCHEDULED"
    # Attempt 1 durably stored and notified 2 of the 4 records before "crashing".
    assert db.scalar(select(func.count()).select_from(Notification)) == 2

    sweep()
    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "SUCCEEDED" and job["attempts"] == 2
    assert job["result"]["notifications_sent"] == 2
    assert job["result"]["notifications_deduplicated"] == 2
    assert db.scalar(select(func.count()).select_from(ProcessedRecord)) == 4
    assert db.scalar(select(func.count()).select_from(Notification)) == 4  # not 6


# --------------------------------------------------------------------------- #
# Dead-letter replay
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("payload", [{"reason": "x"}, {"reason": "x", "transient": True}])
def test_failed_and_dead_letter_jobs_can_be_replayed(client, sweep, payload):
    job_id = client.post(
        "/api/v1/jobs", json={"type": "always_fail", "payload": payload, "max_attempts": 1}
    ).json()["job_id"]

    res = client.post(f"/api/v1/jobs/{job_id}/replay")
    assert res.status_code == 202
    replay = res.json()
    assert replay["job_id"] != job_id
    assert replay["replay_of"] == job_id
    assert replay["payload"] == payload | ({"transient": False} if "transient" not in payload else {})

    # The original stays untouched for audit.
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] in {"FAILED", "DEAD_LETTER"}


def test_successful_job_cannot_be_replayed(client):
    job_id = client.post("/api/v1/jobs", json=BODY).json()["job_id"]
    assert client.post(f"/api/v1/jobs/{job_id}/replay").status_code == 409


def test_sweeper_is_quiet_when_nothing_to_do(sweep):
    assert not sweep(seconds_ahead=0).any()


def test_lease_recovery_is_bounded_by_max_attempts(db, submit, sweep):
    job = submit("echo", {"message": "x"}, max_attempts=1)
    jobs_service.claim_job(db, job.id, "doomed")
    sweep(seconds_ahead=3600)
    db.refresh(job)
    assert job.status == JobStatus.DEAD_LETTER.value
    assert job.finished_at is not None and job.next_attempt_at is None


def test_retry_scheduled_job_keeps_first_start_time(db, submit, sweep):
    job = submit("flaky", {"fail_times": 1})
    run_job(job.id, "w")
    db.refresh(job)
    first_start = job.started_at
    sweep()
    db.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    assert job.started_at == first_start
    assert job.finished_at - job.started_at >= timedelta(0)
