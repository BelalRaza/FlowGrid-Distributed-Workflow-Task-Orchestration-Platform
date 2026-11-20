"""The sweeper: periodic recovery work driven entirely by PostgreSQL state.

Each pass:
1. Recovers RUNNING jobs whose lease expired (the worker died)  -> retry / DEAD_LETTER
2. Re-queues RETRY_SCHEDULED jobs whose backoff delay has passed
3. Re-publishes PENDING jobs that never made it to the broker (outbox relay)
4. Purges expired idempotency keys

Every step uses the same guarded transitions as the API and workers, so it is
safe to run several sweepers at once: if two pick the same job, one UPDATE
matches and the other matches nothing.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import AttemptStatus, IdempotencyKey, Job, JobStatus, utcnow
from app.services import jobs as jobs_service
from app.services.jobs import Publisher

logger = logging.getLogger(__name__)


@dataclass
class SweepResult:
    leases_recovered: int = 0
    retries_enqueued: int = 0
    pending_republished: int = 0
    idempotency_keys_purged: int = 0

    def any(self) -> bool:
        return any(asdict(self).values())


def recover_expired_leases(db: Session, now: datetime) -> int:
    settings = get_settings()
    rows = db.execute(
        select(Job.id, Job.attempts, Job.worker_id)
        .where(Job.status == JobStatus.RUNNING.value, Job.lease_expires_at < now)
        .order_by(Job.lease_expires_at)
        .limit(settings.sweep_batch_size)
    ).all()
    db.rollback()  # end the read transaction before writing

    recovered = 0
    for job_id, attempt, worker_id in rows:
        outcome = jobs_service.record_failure(
            db,
            job_id,
            attempt,
            error_type="WorkerLostError",
            message=f"lease expired; worker {worker_id} presumed dead",
            retryable=True,
            attempt_status=AttemptStatus.LOST,
            now=now,
        )
        if outcome is not None:
            recovered += 1
            logger.warning(
                "Recovered job from lost worker",
                extra={"job_id": str(job_id), "attempt": attempt, "worker_id": worker_id, "outcome": outcome.value},
            )
    return recovered


def enqueue_due_retries(db: Session, publish: Publisher, now: datetime) -> int:
    ids = db.scalars(
        select(Job.id)
        .where(Job.status == JobStatus.RETRY_SCHEDULED.value, Job.next_attempt_at <= now)
        .order_by(Job.next_attempt_at)
        .limit(get_settings().sweep_batch_size)
    ).all()
    db.rollback()

    enqueued = 0
    for job_id in ids:
        # RETRY_SCHEDULED -> PENDING first, then publish. If publishing fails the
        # job sits in PENDING and step 3 retries it: one code path for both.
        if jobs_service._transition(
            db, job_id, from_statuses=[JobStatus.RETRY_SCHEDULED], to_status=JobStatus.PENDING, next_attempt_at=None
        ):
            job = jobs_service._reload(db, job_id)
            if jobs_service.publish_pending(db, job, publish):
                enqueued += 1
    return enqueued


def republish_stale_pending(db: Session, publish: Publisher, now: datetime) -> int:
    settings = get_settings()
    cutoff = now - timedelta(seconds=settings.pending_republish_after_seconds)
    jobs = db.scalars(
        select(Job)
        .where(Job.status == JobStatus.PENDING.value, Job.updated_at < cutoff)
        .order_by(Job.updated_at)
        .limit(settings.sweep_batch_size)
    ).all()

    republished = 0
    for job in jobs:
        if jobs_service.publish_pending(db, job, publish):
            republished += 1
            logger.info("Re-published stranded PENDING job", extra={"job_id": str(job.id)})
        else:
            # Touch updated_at so a broker outage is retried every
            # pending_republish_after_seconds, not on every sweep.
            jobs_service._transition(db, job.id, from_statuses=[JobStatus.PENDING], to_status=JobStatus.PENDING)
    return republished


def purge_idempotency_keys(db: Session, now: datetime) -> int:
    cutoff = now - timedelta(hours=get_settings().idempotency_key_ttl_hours)
    result = db.execute(delete(IdempotencyKey).where(IdempotencyKey.created_at < cutoff))
    db.commit()
    return result.rowcount


def sweep_once(db: Session, publish: Publisher, now: datetime | None = None) -> SweepResult:
    now = now or utcnow()
    result = SweepResult(
        leases_recovered=recover_expired_leases(db, now),
        retries_enqueued=enqueue_due_retries(db, publish, now),
        pending_republished=republish_stale_pending(db, publish, now),
        idempotency_keys_purged=purge_idempotency_keys(db, now),
    )
    if result.any():
        logger.info("Sweep pass", extra=asdict(result))
    return result
