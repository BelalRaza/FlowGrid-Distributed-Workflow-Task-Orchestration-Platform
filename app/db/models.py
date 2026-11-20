import enum
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

# JSONB on PostgreSQL, plain JSON elsewhere (e.g. SQLite in quick local tests).
JSONType = JSON().with_variant(JSONB(), "postgresql")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _tz():
    return mapped_column(DateTime(timezone=True), nullable=True)


class JobStatus(str, enum.Enum):
    """Lifecycle of a job.

    PENDING          -> committed, not yet confirmed as published (outbox)
    QUEUED           -> message published to the broker
    RUNNING          -> claimed by a worker; protected by a lease
    RETRY_SCHEDULED  -> a transient failure; will be re-queued at next_attempt_at
    SUCCEEDED        -> handler returned a result                 (terminal)
    FAILED           -> permanent (non-retryable) error           (terminal)
    DEAD_LETTER      -> retryable error but attempts exhausted    (terminal)
    """

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTER})
REPLAYABLE_STATUSES = frozenset({JobStatus.FAILED, JobStatus.DEAD_LETTER})


class AttemptStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    LOST = "LOST"  # worker disappeared; detected by lease expiry


def _in(values: type[enum.Enum]) -> str:
    return "status IN (" + ", ".join(f"'{v.value}'" for v in values) + ")"


class ApiKey(Base):
    """A client credential. Only the SHA-256 hash of the key is stored."""

    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    # First characters of the key, safe to display, to identify it in logs/CLI.
    prefix: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    revoked_at: Mapped[datetime | None] = _tz()


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(_in(JobStatus), name="status_valid"),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        CheckConstraint("timeout_seconds >= 1", name="timeout_positive"),
        Index("ix_jobs_status_created_at", "status", "created_at"),
        Index("ix_jobs_created_at", "created_at"),
        Index("ix_jobs_status_next_attempt_at", "status", "next_attempt_at"),
        Index("ix_jobs_status_lease_expires_at", "status", "lease_expires_at"),
        Index("ix_jobs_api_key_id_created_at", "api_key_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    api_key_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("api_keys.id", ondelete="SET NULL"), nullable=True
    )
    type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=JobStatus.PENDING.value)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    replay_of: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    queued_at: Mapped[datetime | None] = _tz()
    started_at: Mapped[datetime | None] = _tz()
    finished_at: Mapped[datetime | None] = _tz()
    next_attempt_at: Mapped[datetime | None] = _tz()
    lease_expires_at: Mapped[datetime | None] = _tz()
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Job id={self.id} type={self.type} status={self.status}>"


class JobAttempt(Base):
    """One execution attempt of a job: who ran it, when, and how it ended."""

    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_job_id_attempt_number"),
        CheckConstraint(_in(AttemptStatus), name="status_valid"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=AttemptStatus.RUNNING.value)
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    retryable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    finished_at: Mapped[datetime | None] = _tz()


class IdempotencyKey(Base):
    """Maps a client-supplied Idempotency-Key to the job it created."""

    __tablename__ = "idempotency_keys"
    __table_args__ = (PrimaryKeyConstraint("api_key_id", "key", name="pk_idempotency_keys"),)

    api_key_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("api_keys.id", ondelete="CASCADE"), nullable=False
    )
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, index=True
    )


# --------------------------------------------------------------------------- #
# Side-effect tables written by the process_records handler. Their unique
# constraints are what make the handler safe to run more than once.
# --------------------------------------------------------------------------- #


class ProcessedRecord(Base):
    __tablename__ = "processed_records"
    __table_args__ = (PrimaryKeyConstraint("batch_id", "record_id", name="pk_processed_records"),)

    batch_id: Mapped[str] = mapped_column(String(128), nullable=False)
    record_id: Mapped[int] = mapped_column(Integer, nullable=False)
    tier: Mapped[str] = mapped_column(String(16), nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)


class Notification(Base):
    """Notification outbox. `dedupe_key` guarantees one notification per event."""

    __tablename__ = "notifications"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    job_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
