"""Runs a single job inside a worker process.

Kept separate from the Celery task so it can be tested directly with any
session factory, without a broker.
"""

from __future__ import annotations

import json
import logging
import uuid

from sqlalchemy.orm import Session, sessionmaker

from app.db.models import JobStatus
from app.db.session import get_sessionmaker
from app.services import jobs as jobs_service
from app.tasks.handlers import get_handler

logger = logging.getLogger(__name__)


def run_job(
    job_id: uuid.UUID,
    worker_id: str,
    session_factory: sessionmaker[Session] | None = None,
) -> JobStatus | None:
    """Claim, execute and record the outcome of one job.

    Returns the terminal status, or None if the job could not be claimed
    (unknown ID, or already claimed/finished by someone else).
    """
    SessionLocal = session_factory or get_sessionmaker()

    # 1. Claim the job. This short transaction commits before the handler runs,
    #    so a long job never holds a database transaction open.
    with SessionLocal() as db:
        job = jobs_service.claim_job(db, job_id, worker_id)
        if job is None:
            logger.warning("Job %s not claimable (missing or already processed); skipping", job_id)
            return None
        job_type, payload, attempt = job.type, dict(job.payload), job.attempts

    logger.info("Job %s (%s) started on %s, attempt %d", job_id, job_type, worker_id, attempt)

    # 2. Execute the handler outside any database transaction.
    try:
        result = get_handler(job_type).run(payload)
        # Fail fast here, not at UPDATE time, if a handler returns something
        # that cannot be stored as JSON.
        result = json.loads(json.dumps(result))
    except Exception as exc:
        logger.exception("Job %s (%s) failed", job_id, job_type)
        with SessionLocal() as db:
            jobs_service.mark_failed(db, job_id, f"{type(exc).__name__}: {exc}")
        return JobStatus.FAILED

    # 3. Record success.
    with SessionLocal() as db:
        jobs_service.mark_succeeded(db, job_id, result)
    logger.info("Job %s (%s) succeeded", job_id, job_type)
    return JobStatus.SUCCEEDED
