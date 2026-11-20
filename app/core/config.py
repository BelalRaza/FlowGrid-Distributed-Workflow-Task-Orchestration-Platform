from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration, read from environment variables (and .env)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "FlowGrid"
    database_url: str = "postgresql+psycopg://flowgrid:flowgrid@localhost:5432/flowgrid"
    redis_url: str = "redis://localhost:6379/0"
    log_level: str = "INFO"

    # Name of the Celery queue that job-execution messages are published to.
    job_queue: str = "flowgrid.jobs"

    # When true, Celery runs tasks inline in the calling process instead of
    # sending them to Redis. Only used by the test suite.
    celery_task_always_eager: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
