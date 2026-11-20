"""Job state management.

Every status change is a *guarded* UPDATE:

    UPDATE jobs SET status = :to, ...
    WHERE id = :id AND status IN (:allowed_from) [AND attempts = :attempt]

* ``status IN (...)`` keeps the API, workers and the sweeper from overwriting
  each other's transitions when they race.
* ``attempts = :attempt`` is a **fencing token**: once the sweeper has declared
  an attempt lost and a new attempt has started, a late write from the old
  ("zombie") worker matches zero rows and is discarded.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import (
    REPLAYABLE_STATUSES,
    AttemptStatus,
    IdempotencyKey,
    Job,
    JobAttempt,
    JobStatus,
    utcnow,
)
from app.services.retry import backoff_delay
from app.tasks.handlers import get_handler, validate_payload

logger = logging.getLogger(__name__)

# publish(job_id, timeout_seconds): hand a job to the broker. Raises on failure.
Publisher = Callable[[uuid.UUID, int], None]

MAX_ERROR_LENGTH = 4000


class IdempotencyConflictError(Exception):
    """The Idempotency-Key was already used with a different request body."""

    def __init__(self, key: str, job_id: uuid.UUID) -> None:
        super().__init__(f"Idempotency-Key {key!r} was already used with a different request")
        self.key = key
        self.job_id = job_id


class NotReplayableError(Exception):
    def __init__(self, job: Job) -> None:
        super().__init__(f"Only FAILED or DEAD_LETTER jobs can be replayed (job is {job.status})")
        self.job = job


@dataclass(frozen=True)
class SubmitResult:
    job: Job
    # True if an existing job was returned because of a repeated Idempotency-Key.
    replayed: bool = False


# --------------------------------------------------------------------------- #
# Guarded updates
# --------------------------------------------------------------------------- #


def _guarded_update(
    db: Session,
    job_id: uuid.UUID,
    *,
    from_statuses: Iterable[JobStatus],
    to_status: JobStatus,
    extra_where: Iterable[ColumnElement[bool]] = (),
    **values: Any,
) -> bool:
    """Move a job between states inside the caller's transaction (no commit)."""
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status.in_([s.value for s in from_statuses]), *extra_where)
        .values(status=to_status.value, updated_at=utcnow(), **values)
        .execution_options(synchronize_session=False)
    )
    return db.execute(stmt).rowcount == 1


def _transition(db: Session, job_id: uuid.UUID, **kwargs: Any) -> bool:
    ok = _guarded_update(db, job_id, **kwargs)
    db.commit()
    return ok


def _reload(db: Session, job_id: uuid.UUID) -> Job | None:
    # populate_existing: guarded UPDATEs bypass the ORM, so an instance already
    # in this session's identity map would otherwise be returned stale.
    return db.get(Job, job_id, populate_existing=True)


# --------------------------------------------------------------------------- #
# Submission (API side)
# --------------------------------------------------------------------------- #


def request_hash(job_type: str, payload: dict[str, Any], max_attempts: int, timeout_seconds: int) -> str:
    canonical = json.dumps(
        {"type": job_type, "payload": payload, "max_attempts": max_attempts, "timeout_seconds": timeout_seconds},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _idempotent_result(db: Session, record: IdempotencyKey, expected_hash: str) -> SubmitResult:
    if record.request_hash != expected_hash:
        raise IdempotencyConflictError(record.key, record.job_id)
    job = _reload(db, record.job_id)
    assert job is not None  # FK + ON DELETE CASCADE guarantee this
    return SubmitResult(job=job, replayed=True)


def publish_pending(db: Session, job: Job, publish: Publisher) -> bool:
    """Publish a PENDING job and mark it QUEUED.

    This is the "relay" half of a transactional outbox: the job row is already
    committed, so if publishing fails the job simply stays PENDING and the
    sweeper tries again later. Nothing is lost.
    """
    try:
        publish(job.id, job.timeout_seconds)
    except Exception as exc:
        logger.warning(
            "Publish failed; job left PENDING for the sweeper to retry",
            extra={"job_id": str(job.id), "error": f"{type(exc).__name__}: {exc}"},
        )
        return False
    _transition(db, job.id, from_statuses=[JobStatus.PENDING], to_status=JobStatus.QUEUED, queued_at=utcnow())
    return True


def submit_job(
    db: Session,
    *,
    job_type: str,
    payload: dict[str, Any],
    publish: Publisher,
    api_key_id: uuid.UUID | None = None,
    max_attempts: int | None = None,
    timeout_seconds: int | None = None,
    idempotency_key: str | None = None,
    replay_of: uuid.UUID | None = None,
) -> SubmitResult:
    """Validate, persist and enqueue a job.

    The job row (and its idempotency key, if any) is committed in one
    transaction *before* publishing, so a worker can never receive an ID that
    does not exist and a crash can never leave a key pointing at nothing.
    """
    settings = get_settings()
    handler = get_handler(job_type)
    normalized = validate_payload(job_type, payload)
    max_attempts = max_attempts or settings.default_max_attempts
    timeout_seconds = timeout_seconds or handler.default_timeout_seconds

    digest = None
    if idempotency_key is not None:
        if api_key_id is None:
            raise ValueError("Idempotency keys are scoped per API key; api_key_id is required")
        digest = request_hash(job_type, normalized, max_attempts, timeout_seconds)
        existing = db.get(IdempotencyKey, (api_key_id, idempotency_key))
        if existing is not None:
            return _idempotent_result(db, existing, digest)

    job = Job(
        type=job_type,
        payload=normalized,
        status=JobStatus.PENDING.value,
        api_key_id=api_key_id,
        max_attempts=max_attempts,
        timeout_seconds=timeout_seconds,
        replay_of=replay_of,
    )
    db.add(job)
    if digest is not None:
        db.flush()
        db.add(IdempotencyKey(api_key_id=api_key_id, key=idempotency_key, request_hash=digest, job_id=job.id))

    try:
        db.commit()
    except IntegrityError:
        # Two concurrent requests with the same key: the primary key on
        # (api_key_id, key) lets exactly one INSERT win. The loser returns the
        # winner's job instead of creating a duplicate.
        db.rollback()
        if digest is None:
            raise
        existing = db.get(IdempotencyKey, (api_key_id, idempotency_key))
        if existing is None:
            raise
        return _idempotent_result(db, existing, digest)

    publish_pending(db, job, publish)
    job = _reload(db, job.id)
    logger.info("Job submitted", extra={"job_id": str(job.id), "job_type": job.type, "status": job.status})
    return SubmitResult(job=job)


def replay_job(db: Session, job: Job, publish: Publisher) -> Job:
    """Create a fresh job from a FAILED / DEAD_LETTER one. History stays immutable."""
    if JobStatus(job.status) not in REPLAYABLE_STATUSES:
        raise NotReplayableError(job)
    return submit_job(
        db,
        job_type=job.type,
        payload=job.payload,
        publish=publish,
        api_key_id=job.api_key_id,
        max_attempts=job.max_attempts,
        timeout_seconds=job.timeout_seconds,
        replay_of=job.id,
    ).job


# --------------------------------------------------------------------------- #
# Queries (always scoped to the calling API key)
# --------------------------------------------------------------------------- #


def get_job(db: Session, job_id: uuid.UUID, api_key_id: uuid.UUID | None) -> Job | None:
    job = db.get(Job, job_id)
    if job is None or job.api_key_id != api_key_id:
        # Same answer for "doesn't exist" and "not yours": never confirm that
        # another client's job ID is valid.
        return None
    return job


def list_jobs(
    db: Session,
    *,
    api_key_id: uuid.UUID | None,
    status: JobStatus | None = None,
    job_type: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[Job], int]:
    filters = [Job.api_key_id == api_key_id]
    if status is not None:
        filters.append(Job.status == status.value)
    if job_type is not None:
        filters.append(Job.type == job_type)

    total = db.scalar(select(func.count()).select_from(Job).where(*filters)) or 0
    rows = db.scalars(
        select(Job).where(*filters).order_by(Job.created_at.desc(), Job.id).limit(limit).offset(offset)
    ).all()
    return list(rows), total


def job_stats(db: Session, api_key_id: uuid.UUID | None) -> dict[str, int]:
    counts = {s.value: 0 for s in JobStatus}
    rows = db.execute(
        select(Job.status, func.count()).where(Job.api_key_id == api_key_id).group_by(Job.status)
    )
    for status, count in rows:
        counts[status] = count
    return counts


def list_attempts(db: Session, job_id: uuid.UUID) -> list[JobAttempt]:
    return list(
        db.scalars(select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.attempt_number))
    )


# --------------------------------------------------------------------------- #
# Worker-side transitions
# --------------------------------------------------------------------------- #


def claim_job(db: Session, job_id: uuid.UUID, worker_id: str, now: datetime | None = None) -> Job | None:
    """Mark a job RUNNING for this worker and open a new attempt record.

    The job's lease is ``now + timeout + grace``. Because the hard time limit
    kills the task at ``timeout + 5s``, a lease that is still expired after the
    grace period means the worker is gone, not slow.

    In PostgreSQL the UPDATE takes a row lock, so if two workers receive the
    same message only one sees a returned row; the other re-evaluates the WHERE
    clause after the first commits and matches nothing.
    """
    now = now or utcnow()
    job = _reload(db, job_id)
    if job is None:
        return None

    lease = now + timedelta(seconds=job.timeout_seconds + get_settings().lease_grace_seconds)
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status.in_([JobStatus.PENDING.value, JobStatus.QUEUED.value]))
        .values(
            status=JobStatus.RUNNING.value,
            attempts=Job.attempts + 1,
            started_at=func.coalesce(Job.started_at, now),
            worker_id=worker_id,
            lease_expires_at=lease,
            next_attempt_at=None,
            updated_at=now,
        )
        .returning(Job.attempts)
        .execution_options(synchronize_session=False)
    )
    row = db.execute(stmt).first()
    if row is None:
        db.rollback()
        return None

    db.add(JobAttempt(job_id=job_id, attempt_number=row.attempts, worker_id=worker_id, started_at=now))
    db.commit()
    return _reload(db, job_id)


def _finish_attempt(
    db: Session,
    job_id: uuid.UUID,
    attempt: int,
    status: AttemptStatus,
    *,
    now: datetime,
    error: str | None = None,
    error_type: str | None = None,
    retryable: bool | None = None,
) -> None:
    db.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt_number == attempt,
            JobAttempt.status == AttemptStatus.RUNNING.value,
        )
        .values(status=status.value, finished_at=now, error=error, error_type=error_type, retryable=retryable)
        .execution_options(synchronize_session=False)
    )


def record_success(db: Session, job_id: uuid.UUID, attempt: int, result: dict[str, Any]) -> bool:
    now = utcnow()
    ok = _guarded_update(
        db,
        job_id,
        from_statuses=[JobStatus.RUNNING],
        to_status=JobStatus.SUCCEEDED,
        extra_where=[Job.attempts == attempt],  # fencing token
        result=result,
        error=None,
        finished_at=now,
        lease_expires_at=None,
    )
    _finish_attempt(db, job_id, attempt, AttemptStatus.SUCCEEDED, now=now)
    db.commit()
    if not ok:
        logger.warning("Stale success discarded (fenced)", extra={"job_id": str(job_id), "attempt": attempt})
    return ok


def record_failure(
    db: Session,
    job_id: uuid.UUID,
    attempt: int,
    *,
    error_type: str,
    message: str,
    retryable: bool,
    attempt_status: AttemptStatus = AttemptStatus.FAILED,
    now: datetime | None = None,
) -> JobStatus | None:
    """Close an attempt and decide what happens to the job next.

    retryable + attempts left  -> RETRY_SCHEDULED (with exponential backoff)
    retryable + no attempts    -> DEAD_LETTER
    not retryable              -> FAILED
    Returns the new status, or None if the write was fenced off.
    """
    settings = get_settings()
    now = now or utcnow()
    job = _reload(db, job_id)
    if job is None:
        return None

    error = f"{error_type}: {message}"[:MAX_ERROR_LENGTH]
    values: dict[str, Any] = {"error": error, "lease_expires_at": None}
    if retryable and attempt < job.max_attempts:
        to_status = JobStatus.RETRY_SCHEDULED
        delay = backoff_delay(attempt, base=settings.retry_base_delay_seconds, cap=settings.retry_max_delay_seconds)
        values["next_attempt_at"] = now + timedelta(seconds=delay)
    elif retryable:
        to_status = JobStatus.DEAD_LETTER
        values["finished_at"] = now
    else:
        to_status = JobStatus.FAILED
        values["finished_at"] = now

    ok = _guarded_update(
        db,
        job_id,
        from_statuses=[JobStatus.RUNNING],
        to_status=to_status,
        extra_where=[Job.attempts == attempt],  # fencing token
        **values,
    )
    _finish_attempt(
        db, job_id, attempt, attempt_status, now=now, error=error, error_type=error_type, retryable=retryable
    )
    db.commit()
    if not ok:
        logger.warning("Stale failure discarded (fenced)", extra={"job_id": str(job_id), "attempt": attempt})
        return None
    return to_status
