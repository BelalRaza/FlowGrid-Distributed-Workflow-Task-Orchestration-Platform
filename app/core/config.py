from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration, read from environment variables (and .env)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "FlowGrid"
    database_url: str = "postgresql+psycopg://flowgrid:flowgrid@localhost:5432/flowgrid"
    redis_url: str = "redis://localhost:6379/0"
    log_level: str = "INFO"
    # "text" for humans, "json" for log aggregation (Docker Compose uses json).
    log_format: str = "text"

    # Name of the Celery queue that job-execution messages are published to.
    job_queue: str = "flowgrid.jobs"

    # When true, Celery runs tasks inline in the calling process instead of
    # sending them to Redis. Only used by the test suite.
    celery_task_always_eager: bool = False

    # --- Retries --------------------------------------------------------------
    default_max_attempts: int = 3
    max_allowed_attempts: int = 10
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 300.0

    # --- Timeouts & leases ----------------------------------------------------
    max_timeout_seconds: int = 600
    # A RUNNING job whose lease (start + timeout + grace) has passed is treated
    # as abandoned by a dead worker and recovered by the sweeper.
    lease_grace_seconds: int = 15

    # --- Sweeper --------------------------------------------------------------
    sweep_interval_seconds: float = 1.0
    sweep_batch_size: int = 100
    # PENDING jobs older than this are assumed to have missed their publish
    # (e.g. Redis was down) and are re-published.
    pending_republish_after_seconds: int = 10
    # QUEUED jobs older than this are re-published in case Redis lost the
    # message (e.g. a crash without persistence). Duplicates are harmless: the
    # guarded claim runs a job once. Keep this above the normal queue wait.
    queued_republish_after_seconds: int = 300

    # --- Live events (SSE) ------------------------------------------------------
    sse_keepalive_seconds: float = 15.0
    sse_queue_size: int = 1000

    # --- Batches ----------------------------------------------------------------
    max_batch_size: int = 500

    # --- Idempotency ----------------------------------------------------------
    idempotency_key_ttl_hours: int = 24

    # --- Rate limiting (token bucket per API key) ------------------------------
    rate_limit_capacity: int = 100
    rate_limit_refill_per_second: float = 20.0

    # --- Request validation ---------------------------------------------------
    max_request_body_bytes: int = 1_000_000


@lru_cache
def get_settings() -> Settings:
    return Settings()
