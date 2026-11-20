from celery import Celery

from app.core.config import get_settings

settings = get_settings()

celery_app = Celery("flowgrid", broker=settings.redis_url, include=["app.tasks.worker"])

celery_app.conf.update(
    # PostgreSQL is the source of truth for job status and results, so Celery's
    # own result backend is not used.
    task_ignore_result=True,
    task_default_queue=settings.job_queue,
    task_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # Fetch one message at a time so a long job does not hold others hostage
    # in a busy worker's prefetch buffer.
    worker_prefetch_multiplier=1,
    # Acknowledge after the task finishes, not when it is received. If a worker
    # process dies mid-task the message is redelivered instead of lost. That
    # makes delivery at-least-once, which is why claims are guarded and
    # handlers are idempotent.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Redis re-delivers unacknowledged messages after this long. It must exceed
    # the longest allowed job (max_timeout_seconds) or long jobs get duplicated
    # (which the guarded claim would then reject, but it is wasted work).
    broker_transport_options={"visibility_timeout": 3600},
    broker_connection_retry_on_startup=True,
    task_always_eager=settings.celery_task_always_eager,
)
