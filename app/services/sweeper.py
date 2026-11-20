"""The sweeper: periodic recovery and scheduling work driven entirely by
PostgreSQL state.

Each pass:
1. Recovers RUNNING jobs whose lease expired (the worker died) -> retry / DEAD_LETTER
2. Fires due cron schedules (exactly once per tick; see services/schedules.py)
3. Starts workflow runs whose run_at has passed
4. Releases SCHEDULED jobs (run_at) and RETRY_SCHEDULED jobs (backoff) that are due
5. Re-publishes PENDING jobs that never reached the broker (outbox relay)
6. Re-publishes QUEUED jobs whose message Redis may have lost
7. Advances workflow runs left stuck by a crash between "step done" and "advance"
8. Purges expired idempotency keys

Every step uses the same guarded transitions as the API and workers, so it is
safe to run several sweepers at once: if two pick the same row, one UPDATE
matches and the other matches nothing.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import AttemptStatus, IdempotencyKey, Job, JobStatus, utcnow
from app.services import jobs as jobs_service
from app.services import schedules as schedule_service
from app.services import workflows as workflow_service
from app.services.jobs import Publisher

logger = logging.getLogger(__name__)


@dataclass
class SweepResult:
    leases_recovered: int = 0
    schedules_fired: int = 0
    runs_started: int = 0
    scheduled_jobs_released: int = 0
    retries_enqueued: int = 0
    pending_republished: int = 0
    queued_republished: int = 0
    runs_advanced: int = 0
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


def release_due(db: Session, publish: Publisher, now: datetime, status: JobStatus) -> int:
    """SCHEDULED / RETRY_SCHEDULED jobs whose time has come -> PENDING -> publish."""
    ids = db.scalars(
        select(Job.id)
        .where(Job.status == status.value, Job.next_attempt_at <= now)
        .order_by(Job.next_attempt_at)
        .limit(get_settings().sweep_batch_size)
    ).all()
    db.rollback()

    released = []
    for job_id in ids:
        # Go through PENDING first: if publishing fails the job is simply a
        # stranded PENDING job, which step 5 already knows how to handle.
        if jobs_service._guarded_update(
            db, job_id, from_statuses=[status], to_status=JobStatus.PENDING, next_attempt_at=None
        ):
            released.append(job_id)
    db.commit()
    jobs_service.publish_many(db, [jobs_service._reload(db, i) for i in released], publish)
    return len(released)


def enqueue_due_retries(db: Session, publish: Publisher, now: datetime) -> int:
    return release_due(db, publish, now, JobStatus.RETRY_SCHEDULED)


def republish_stale_pending(db: Session, publish: Publisher, now: datetime) -> int:
    settings = get_settings()
    cutoff = now - timedelta(seconds=settings.pending_republish_after_seconds)
    jobs = db.scalars(
        select(Job)
        .where(Job.status == JobStatus.PENDING.value, Job.updated_at < cutoff)
        .order_by(Job.updated_at)
        .limit(settings.sweep_batch_size)
    ).all()
    if not jobs:
        db.rollback()
        return 0

    republished = jobs_service.publish_many(db, jobs, publish)
    if republished < len(jobs):
        # Broker still down: touch updated_at so the remaining jobs are retried
        # every pending_republish_after_seconds, not on every sweep.
        db.execute(
            update(Job)
            .where(Job.id.in_([j.id for j in jobs]), Job.status == JobStatus.PENDING.value)
            .values(updated_at=now)
            .execution_options(synchronize_session=False)
        )
        db.commit()
    if republished:
        logger.info("Re-published stranded PENDING jobs", extra={"count": republished})
    return republished


def republish_stale_queued(db: Session, publish: Publisher, now: datetime) -> int:
    """Recover from a broker that *lost* messages (restart without persistence,
    FLUSHALL, failover). The job is still QUEUED in PostgreSQL, so publish it
    again; if the original message does turn up, the guarded claim drops it."""
    settings = get_settings()
    cutoff = now - timedelta(seconds=settings.queued_republish_after_seconds)
    jobs = db.scalars(
        select(Job)
        .where(Job.status == JobStatus.QUEUED.value, Job.queued_at < cutoff)
        .order_by(Job.queued_at)
        .limit(settings.sweep_batch_size)
    ).all()
    db.rollback()

    count = 0
    for job in jobs:
        try:
            publish(job.id, job.timeout_seconds)
        except Exception:
            break
        # Reset the clock so each job is re-sent at most once per interval.
        jobs_service._transition(db, job.id, from_statuses=[JobStatus.QUEUED], to_status=JobStatus.QUEUED,
                                 queued_at=now)
        count += 1
    if count:
        logger.warning("Re-published long-QUEUED jobs (possible broker message loss)", extra={"count": count})
    return count


def fire_schedules(db: Session, publish: Publisher, now: datetime) -> int:
    return schedule_service.fire_due_schedules(db, publish, now)


def start_due_runs(db: Session, publish: Publisher, now: datetime) -> int:
    return sum(workflow_service.start_scheduled_run(db, run_id, publish, now)
               for run_id in workflow_service.due_scheduled_runs(db, now))


def advance_stuck_runs(db: Session, publish: Publisher) -> int:
    advanced = 0
    for run_id in workflow_service.runs_needing_advance(db, get_settings().sweep_batch_size):
        if workflow_service.advance_run(db, run_id, publish) is not None:
            advanced += 1
    return advanced


def purge_idempotency_keys(db: Session, now: datetime) -> int:
    cutoff = now - timedelta(hours=get_settings().idempotency_key_ttl_hours)
    result = db.execute(delete(IdempotencyKey).where(IdempotencyKey.created_at < cutoff))
    db.commit()
    return result.rowcount


def sweep_once(db: Session, publish: Publisher, now: datetime | None = None) -> SweepResult:
    now = now or utcnow()
    result = SweepResult(
        leases_recovered=recover_expired_leases(db, now),
        schedules_fired=fire_schedules(db, publish, now),
        runs_started=start_due_runs(db, publish, now),
        scheduled_jobs_released=release_due(db, publish, now, JobStatus.SCHEDULED),
        retries_enqueued=enqueue_due_retries(db, publish, now),
        pending_republished=republish_stale_pending(db, publish, now),
        queued_republished=republish_stale_queued(db, publish, now),
        runs_advanced=advance_stuck_runs(db, publish),
        idempotency_keys_purged=purge_idempotency_keys(db, now),
    )
    if result.any():
        logger.info("Sweep pass", extra=asdict(result))
    return result
