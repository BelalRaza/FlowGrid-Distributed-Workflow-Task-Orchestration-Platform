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
    broker_connection_retry_on_startup=True,
    task_always_eager=settings.celery_task_always_eager,
)
