from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    run_at: datetime | None = Field(
        default=None, description="Run at this time instead of now (UTC if no offset). Status SCHEDULED until then."
    )
    delay_seconds: int | None = Field(
        default=None, ge=1, le=30 * 24 * 3600, description="Run this many seconds from now. Alternative to run_at."
    )

    @model_validator(mode="after")
    def _one_delay(self) -> "JobCreate":
        if self.run_at is not None and self.delay_seconds is not None:
            raise ValueError("Provide at most one of run_at and delay_seconds")
        return self

    def resolved_run_at(self) -> datetime | None:
        if self.delay_seconds is not None:
            return datetime.now(timezone.utc) + timedelta(seconds=self.delay_seconds)
        if self.run_at is not None and self.run_at.tzinfo is None:
            return self.run_at.replace(tzinfo=timezone.utc)
        return self.run_at

    def service_kwargs(self) -> dict[str, Any]:
        return {"job_type": self.type, "payload": self.payload, "max_attempts": self.max_attempts,
                "timeout_seconds": self.timeout_seconds, "run_at": self.resolved_run_at()}


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
    workflow_run_id: uuid.UUID | None = None
    step_id: str | None = None
    schedule_id: uuid.UUID | None = None
    scheduled_for: datetime | None = None
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
            workflow_run_id=job.workflow_run_id,
            step_id=job.step_id,
            schedule_id=job.schedule_id,
            scheduled_for=job.scheduled_for,
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
