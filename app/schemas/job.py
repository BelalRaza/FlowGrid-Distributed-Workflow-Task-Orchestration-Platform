from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.db.models import Job, JobStatus


class JobCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "type": "process_records",
                "payload": {"batch_id": "batch-001", "record_ids": [101, 102, 103]},
            }
        },
    )

    type: str = Field(min_length=1, max_length=64, description="An allow-listed job type.")
    payload: dict[str, Any] = Field(default_factory=dict)


class JobRead(BaseModel):
    job_id: uuid.UUID
    type: str
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    attempts: int
    worker_id: str | None
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None = Field(description="Execution time (started → finished), if finished.")

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
            worker_id=job.worker_id,
            created_at=job.created_at,
            queued_at=job.queued_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
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
