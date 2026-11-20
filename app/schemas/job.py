from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.config import get_settings
from app.db.models import AttemptStatus, Job, JobAttempt, JobStatus

_settings = get_settings()


class JobCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "type": "process_records",
                "payload": {"batch_id": "batch-001", "record_ids": [101, 102, 103]},
                "max_attempts": 3,
            }
        },
    )

    type: str = Field(min_length=1, max_length=64, description="An allow-listed job type.")
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(
        default=None,
        ge=1,
        le=_settings.max_allowed_attempts,
        description=f"Total attempts including the first. Default {_settings.default_max_attempts}.",
    )
    timeout_seconds: int | None = Field(
        default=None,
        ge=1,
        le=_settings.max_timeout_seconds,
        description="Per-attempt time limit. Defaults to the job type's default.",
    )


class JobRead(BaseModel):
    job_id: uuid.UUID
    type: str
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    attempts: int
    max_attempts: int
    timeout_seconds: int
    worker_id: str | None
    replay_of: uuid.UUID | None
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    next_attempt_at: datetime | None
    duration_ms: int | None = Field(description="First start → finish, across all attempts.")

    @classmethod
    def from_model(cls, job: Job) -> JobRead:
        duration_ms = None
        if job.started_at and job.finished_at:
            duration_ms = int((job.finished_at - job.started_at).total_seconds() * 1000)
        return cls(
            job_id=job.id,
            type=job.type,
            status=JobStatus(job.status),
            payload=job.payload,
            result=job.result,
            error=job.error,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            timeout_seconds=job.timeout_seconds,
            worker_id=job.worker_id,
            replay_of=job.replay_of,
            created_at=job.created_at,
            queued_at=job.queued_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            next_attempt_at=job.next_attempt_at,
            duration_ms=duration_ms,
        )


class AttemptRead(BaseModel):
    attempt_number: int
    status: AttemptStatus
    worker_id: str | None
    error: str | None
    error_type: str | None
    retryable: bool | None
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None

    @classmethod
    def from_model(cls, a: JobAttempt) -> AttemptRead:
        duration_ms = None
        if a.finished_at:
            duration_ms = int((a.finished_at - a.started_at).total_seconds() * 1000)
        return cls(
            attempt_number=a.attempt_number,
            status=AttemptStatus(a.status),
            worker_id=a.worker_id,
            error=a.error,
            error_type=a.error_type,
            retryable=a.retryable,
            started_at=a.started_at,
            finished_at=a.finished_at,
            duration_ms=duration_ms,
        )


class JobList(BaseModel):
    items: list[JobRead]
    total: int
    limit: int
    offset: int


class JobStats(BaseModel):
    total: int
    by_status: dict[str, int]


class JobTypeInfo(BaseModel):
    name: str
    description: str
    payload_schema: dict[str, Any]
    example: dict[str, Any]
    default_timeout_seconds: int


class WhoAmI(BaseModel):
    name: str
    prefix: str
    rate_limit_capacity: int
    rate_limit_refill_per_second: float
