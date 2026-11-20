from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.core.config import get_settings
from app.db.models import JobStatus, RunStatus, Schedule, Workflow, WorkflowRun
from app.schemas.job import JobCreate

_settings = get_settings()


class StepDef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$", description="Unique within the workflow.")
    type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list, max_length=50)
    max_attempts: int | None = Field(default=None, ge=1, le=_settings.max_allowed_attempts)
    timeout_seconds: int | None = Field(default=None, ge=1, le=_settings.max_timeout_seconds)


class WorkflowCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "name": "nightly-import",
                "steps": [
                    {"id": "extract", "type": "echo", "payload": {"message": "start"}},
                    {"id": "batch_a", "type": "process_records", "depends_on": ["extract"],
                     "payload": {"batch_id": "a", "record_ids": [1, 2, 3]}},
                    {"id": "batch_b", "type": "process_records", "depends_on": ["extract"],
                     "payload": {"batch_id": "b", "record_ids": [4, 5]}},
                    {"id": "report", "type": "summarize", "depends_on": ["batch_a", "batch_b"],
                     "payload": {"title": "nightly"}},
                ],
            }
        },
    )

    name: str = Field(min_length=1, max_length=100)
    steps: list[StepDef] = Field(min_length=1, max_length=50)


class WorkflowRead(BaseModel):
    workflow_id: uuid.UUID
    name: str
    steps: list[dict[str, Any]]
    levels: dict[str, int] = Field(description="Depth of each step in the DAG (0 = no dependencies).")
    created_at: datetime

    @classmethod
    def from_model(cls, wf: Workflow, levels: dict[str, int]) -> WorkflowRead:
        return cls(workflow_id=wf.id, name=wf.name, steps=wf.definition["steps"], levels=levels,
                   created_at=wf.created_at)


class RunCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_at: datetime | None = Field(default=None, description="Start later instead of now (UTC if no offset).")


class StepState(BaseModel):
    step_id: str
    job_id: uuid.UUID
    type: str
    status: JobStatus
    depends_on: list[str]
    level: int
    attempts: int
    max_attempts: int
    result: dict[str, Any] | None
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None


class RunRead(BaseModel):
    run_id: uuid.UUID
    workflow_id: uuid.UUID
    workflow_name: str | None = None
    status: RunStatus
    error: str | None
    schedule_id: uuid.UUID | None
    scheduled_for: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None
    steps: list[StepState] | None = None

    @classmethod
    def from_model(cls, run: WorkflowRun, workflow_name: str | None = None,
                   steps: list[StepState] | None = None) -> RunRead:
        duration = None
        if run.started_at and run.finished_at:
            duration = int((run.finished_at - run.started_at).total_seconds() * 1000)
        return cls(
            run_id=run.id, workflow_id=run.workflow_id, workflow_name=workflow_name, status=RunStatus(run.status),
            error=run.error, schedule_id=run.schedule_id, scheduled_for=run.scheduled_for,
            created_at=run.created_at, started_at=run.started_at, finished_at=run.finished_at,
            duration_ms=duration, steps=steps,
        )


class RunList(BaseModel):
    items: list[RunRead]
    total: int
    limit: int
    offset: int


class ScheduleJobTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=_settings.max_allowed_attempts)
    timeout_seconds: int | None = Field(default=None, ge=1, le=_settings.max_timeout_seconds)


class ScheduleCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"example": {"name": "every-5-min", "cron": "*/5 * * * *",
                                       "job": {"type": "echo", "payload": {"message": "tick"}}}},
    )

    name: str = Field(min_length=1, max_length=100)
    cron: str = Field(min_length=9, max_length=100, description="5-field cron, evaluated in UTC.")
    job: ScheduleJobTarget | None = None
    workflow_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def _one_target(self) -> ScheduleCreate:
        if (self.job is None) == (self.workflow_id is None):
            raise ValueError("Provide exactly one of 'job' or 'workflow_id'")
        return self


class ScheduleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool


class ScheduleRead(BaseModel):
    schedule_id: uuid.UUID
    name: str
    cron: str
    enabled: bool
    target_kind: Literal["job", "workflow"]
    job: dict[str, Any] | None
    workflow_id: uuid.UUID | None
    next_run_at: datetime
    last_run_at: datetime | None
    created_at: datetime

    @classmethod
    def from_model(cls, s: Schedule) -> ScheduleRead:
        job = None
        if s.target_kind == "job":
            job = {"type": s.job_type, "payload": s.payload, "max_attempts": s.max_attempts,
                   "timeout_seconds": s.timeout_seconds}
        return cls(schedule_id=s.id, name=s.name, cron=s.cron, enabled=s.enabled, target_kind=s.target_kind,
                   job=job, workflow_id=s.workflow_id, next_run_at=s.next_run_at, last_run_at=s.last_run_at,
                   created_at=s.created_at)


class BatchCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    jobs: list[JobCreate] = Field(min_length=1, max_length=_settings.max_batch_size)


class BatchResult(BaseModel):
    count: int
    job_ids: list[uuid.UUID]
