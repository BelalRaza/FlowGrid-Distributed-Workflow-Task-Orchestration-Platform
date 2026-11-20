"""Celery task definitions and the publisher used by the API."""

from __future__ import annotations

import socket
import uuid

from celery.signals import worker_process_init

from app.core.logging import configure_logging
from app.services.executor import run_job
from app.tasks.celery_app import celery_app

EXECUTE_JOB_TASK = "flowgrid.execute_job"


@worker_process_init.connect
def _init_worker_process(**_: object) -> None:
    configure_logging()


@celery_app.task(name=EXECUTE_JOB_TASK)
def execute_job(job_id: str) -> None:
    # Only the job ID travels through Redis; the worker loads type and payload
    # from PostgreSQL, so the queue never holds state the database disagrees with.
    worker_id = execute_job.request.hostname or socket.gethostname()
    run_job(uuid.UUID(job_id), worker_id=worker_id)


def publish_job(job_id: uuid.UUID) -> None:
    """Send a job to the broker. Fails fast (instead of hanging) if Redis is down."""
    execute_job.apply_async(
        args=[str(job_id)],
        retry=True,
        retry_policy={"max_retries": 2, "interval_start": 0, "interval_step": 0.2, "interval_max": 0.5},
    )
