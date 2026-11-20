"""Job state management.

Every status change goes through ``_transition``, a single guarded UPDATE:

    UPDATE jobs SET status = :to, ...
    WHERE id = :id AND status IN (:allowed_from)

The ``WHERE status IN (...)`` clause is what keeps state correct when the API
and workers race each other. Example: the worker can claim a job (QUEUED ->
RUNNING) before the API has recorded PENDING -> QUEUED. The API's update then
matches zero rows instead of overwriting RUNNING with QUEUED.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db.models import Job, JobStatus, utcnow
from app.tasks.handlers import validate_payload

logger = logging.getLogger(__name__)

Publisher = Callable[[uuid.UUID], None]

MAX_ERROR_LENGTH = 4000


class EnqueueError(Exception):
    def __init__(self, job: Job) -> None:
        super().__init__(f"Job {job.id} was saved but could not be published to the queue")
        self.job = job


def _transition(
    db: Session,
    job_id: uuid.UUID,
    *,
    from_statuses: Iterable[JobStatus],
    to_status: JobStatus,
    **values: Any,
) -> bool:
    """Atomically move a job between states. Returns False if the guard did not match."""
    now = utcnow()
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status.in_([s.value for s in from_statuses]))
        .values(status=to_status.value, updated_at=now, **values)
        .execution_options(synchronize_session=False)
    )
    result = db.execute(stmt)
    db.commit()
    return result.rowcount == 1


def submit_job(db: Session, job_type: str, payload: dict[str, Any], publish: Publisher) -> Job:
    """Validate, persist and enqueue a job.

    The row is committed *before* publishing so a worker can never receive a
    job ID that does not exist yet. If publishing fails the job is marked
    FAILED and ``EnqueueError`` is raised.
    """
    normalized = validate_payload(job_type, payload)

    job = Job(type=job_type, payload=normalized, status=JobStatus.PENDING.value)
    db.add(job)
    db.commit()

    try:
        publish(job.id)
    except Exception as exc:
        logger.exception("Failed to publish job %s", job.id)
        _transition(
            db,
            job.id,
            from_statuses=[JobStatus.PENDING],
            to_status=JobStatus.FAILED,
            error=f"Enqueue failed: {type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH],
            finished_at=utcnow(),
        )
        db.refresh(job)
        raise EnqueueError(job) from exc

    _transition(
        db, job.id, from_statuses=[JobStatus.PENDING], to_status=JobStatus.QUEUED, queued_at=utcnow()
    )
    db.refresh(job)
    logger.info("Job %s (%s) submitted, status=%s", job.id, job.type, job.status)
    return job


def get_job(db: Session, job_id: uuid.UUID) -> Job | None:
    return db.get(Job, job_id)


def list_jobs(
    db: Session,
    *,
    status: JobStatus | None = None,
    job_type: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[Job], int]:
    filters = []
    if status is not None:
        filters.append(Job.status == status.value)
    if job_type is not None:
        filters.append(Job.type == job_type)

    total = db.scalar(select(func.count()).select_from(Job).where(*filters)) or 0
    rows = db.scalars(
        select(Job).where(*filters).order_by(Job.created_at.desc(), Job.id).limit(limit).offset(offset)
    ).all()
    return list(rows), total


def job_stats(db: Session) -> dict[str, int]:
    counts = {s.value: 0 for s in JobStatus}
    for status, count in db.execute(select(Job.status, func.count()).group_by(Job.status)):
        counts[status] = count
    return counts


# --------------------------------------------------------------------------- #
# Worker-side transitions
# --------------------------------------------------------------------------- #


def claim_job(db: Session, job_id: uuid.UUID, worker_id: str) -> Job | None:
    """Mark a job RUNNING for this worker. Returns None if it is not claimable.

    In PostgreSQL the UPDATE takes a row lock, so if two workers receive the same
    message only one of them sees ``rowcount == 1``; the other re-evaluates the
    WHERE clause after the first commits and matches nothing.
    """
    claimed = _transition(
        db,
        job_id,
        from_statuses=[JobStatus.PENDING, JobStatus.QUEUED],
        to_status=JobStatus.RUNNING,
        attempts=Job.attempts + 1,
        started_at=utcnow(),
        worker_id=worker_id,
    )
    # populate_existing: the bulk UPDATE bypasses the ORM, so a Job already in
    # this session's identity map would otherwise be returned stale.
    return db.get(Job, job_id, populate_existing=True) if claimed else None


def mark_succeeded(db: Session, job_id: uuid.UUID, result: dict[str, Any]) -> bool:
    return _transition(
        db,
        job_id,
        from_statuses=[JobStatus.RUNNING],
        to_status=JobStatus.SUCCEEDED,
        result=result,
        error=None,
        finished_at=utcnow(),
    )


def mark_failed(db: Session, job_id: uuid.UUID, error: str) -> bool:
    return _transition(
        db,
        job_id,
        from_statuses=[JobStatus.RUNNING],
        to_status=JobStatus.FAILED,
        error=error[:MAX_ERROR_LENGTH],
        finished_at=utcnow(),
    )
