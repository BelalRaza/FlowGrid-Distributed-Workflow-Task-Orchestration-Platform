"""Runs a single job attempt inside a worker process.

Kept separate from the Celery task so it can be tested directly with any
session factory, without a broker.
"""

from __future__ import annotations

import json
import logging
import uuid

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy.orm import Session, sessionmaker

from app.core.logging import log_context
from app.db.models import AttemptStatus, JobStatus
from app.db.session import get_sessionmaker
from app.services import jobs as jobs_service
from app.services.retry import is_retryable
from app.tasks.handlers import JobContext, get_handler

logger = logging.getLogger(__name__)


def run_job(
    job_id: uuid.UUID,
    worker_id: str,
    session_factory: sessionmaker[Session] | None = None,
) -> JobStatus | None:
    """Claim, execute and record the outcome of one attempt.

    Returns the job's new status, or None if the job could not be claimed
    (unknown ID, already claimed/finished) or the outcome was fenced off
    because this attempt was already declared lost.
    """
    SessionLocal = session_factory or get_sessionmaker()

    # 1. Claim. This short transaction commits before the handler runs, so a
    #    long job never holds a database transaction open.
    with SessionLocal() as db:
        job = jobs_service.claim_job(db, job_id, worker_id)
        if job is None:
            logger.info("Job not claimable (missing, or already claimed/finished); skipping",
                        extra={"job_id": str(job_id), "worker_id": worker_id})
            return None
        job_type, payload, attempt = job.type, dict(job.payload), job.attempts
        max_attempts, timeout = job.max_attempts, job.timeout_seconds

    with log_context(job_id=str(job_id), job_type=job_type, attempt=attempt, worker_id=worker_id):
        logger.info("Attempt started", extra={"max_attempts": max_attempts, "timeout_seconds": timeout})

        # 2. Execute the handler outside any database transaction.
        try:
            ctx = JobContext(job_id=job_id, attempt=attempt, session_factory=SessionLocal)
            result = get_handler(job_type).run(payload, ctx)
            # Fail fast here, not at UPDATE time, if a handler returns something
            # that cannot be stored as JSON.
            result = json.loads(json.dumps(result))
        except SoftTimeLimitExceeded:
            logger.warning("Attempt timed out")
            with SessionLocal() as db:
                return jobs_service.record_failure(
                    db, job_id, attempt,
                    error_type="JobTimeoutError",
                    message=f"exceeded timeout of {timeout}s",
                    retryable=True,
                    attempt_status=AttemptStatus.TIMED_OUT,
                )
        except Exception as exc:
            retryable = is_retryable(exc)
            logger.warning("Attempt failed", exc_info=not retryable,
                           extra={"error_type": type(exc).__name__, "error": str(exc), "retryable": retryable})
            with SessionLocal() as db:
                outcome = jobs_service.record_failure(
                    db, job_id, attempt, error_type=type(exc).__name__, message=str(exc), retryable=retryable
                )
            logger.info("Attempt recorded", extra={"outcome": outcome.value if outcome else "fenced"})
            return outcome

        # 3. Record success (fenced by attempt number).
        with SessionLocal() as db:
            ok = jobs_service.record_success(db, job_id, attempt, result)
        logger.info("Attempt succeeded" if ok else "Attempt result discarded (fenced)")
        return JobStatus.SUCCEEDED if ok else None
