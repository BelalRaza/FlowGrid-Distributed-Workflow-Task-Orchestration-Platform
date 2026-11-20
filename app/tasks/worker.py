"""Celery task definitions and the publisher used by the API and sweeper."""

from __future__ import annotations

import socket
import uuid

from celery.signals import setup_logging

from app.services.executor import run_job
from app.tasks.celery_app import celery_app

EXECUTE_JOB_TASK = "flowgrid.execute_job"

# Hard kill this long after the soft time limit, if the handler ignores it.
HARD_LIMIT_MARGIN_SECONDS = 5


@setup_logging.connect
def _setup_logging(**_: object) -> None:
    # Connecting to this signal stops Celery from installing its own handlers,
    # so worker logs use the same (JSON) format as the API.
    from app.core.logging import configure_logging

    configure_logging()


@celery_app.task(name=EXECUTE_JOB_TASK, acks_late=True, reject_on_worker_lost=True)
def execute_job(job_id: str) -> None:
    # Only the job ID travels through Redis; the worker loads type and payload
    # from PostgreSQL, so the queue never holds state the database disagrees with.
    worker_id = execute_job.request.hostname or socket.gethostname()
    run_job(uuid.UUID(job_id), worker_id=worker_id)


def publish_job(job_id: uuid.UUID, timeout_seconds: int) -> None:
    """Send a job to the broker with its time limits. Fails fast if Redis is down."""
    execute_job.apply_async(
        args=[str(job_id)],
        soft_time_limit=timeout_seconds,
        time_limit=timeout_seconds + HARD_LIMIT_MARGIN_SECONDS,
        retry=True,
        retry_policy={"max_retries": 2, "interval_start": 0, "interval_step": 0.2, "interval_max": 0.5},
    )
