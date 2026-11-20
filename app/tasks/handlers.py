"""Allow-listed job handlers.

FlowGrid never executes arbitrary code. A client can only submit a job whose
``type`` is registered here, and its ``payload`` must validate against that
handler's Pydantic model.

Handlers receive a ``JobContext``. Delivery is at-least-once (a job can run
more than once after a crash or timeout), so any handler with side effects
must make them idempotent — see ``process_records`` for the pattern.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import insert as generic_insert
from sqlalchemy.orm import Session, sessionmaker

from app.db.models import Notification, ProcessedRecord
from app.services.retry import PermanentJobError, TransientJobError


class UnknownJobTypeError(Exception):
    def __init__(self, job_type: str) -> None:
        super().__init__(f"Unknown job type: {job_type!r}")
        self.job_type = job_type


class InvalidPayloadError(Exception):
    def __init__(self, job_type: str, errors: list[dict[str, Any]]) -> None:
        super().__init__(f"Invalid payload for job type {job_type!r}")
        self.job_type = job_type
        self.errors = errors


@dataclass(frozen=True)
class JobContext:
    job_id: uuid.UUID
    attempt: int
    session_factory: sessionmaker[Session]
    # For workflow steps: results of the steps this one depends on, by step ID.
    upstream: dict[str, Any] = field(default_factory=dict)


HandlerFunc = Callable[[Any, JobContext], dict[str, Any]]


@dataclass(frozen=True)
class JobHandler:
    name: str
    description: str
    payload_model: type[BaseModel]
    func: HandlerFunc
    example: dict[str, Any]
    default_timeout_seconds: int

    def validate(self, payload: dict[str, Any]) -> BaseModel:
        try:
            return self.payload_model.model_validate(payload)
        except ValidationError as exc:
            raise InvalidPayloadError(
                self.name, exc.errors(include_url=False, include_context=False)
            ) from exc

    def run(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        try:
            model = self.validate(payload)
        except InvalidPayloadError as exc:
            # Stored payloads were validated at submit time, so this means the
            # schema changed underneath an old job. Retrying will not help.
            raise PermanentJobError(str(exc)) from exc
        return self.func(model, ctx)


REGISTRY: dict[str, JobHandler] = {}


def register(
    name: str,
    *,
    payload_model: type[BaseModel],
    description: str,
    example: dict[str, Any],
    default_timeout_seconds: int = 60,
) -> Callable[[HandlerFunc], HandlerFunc]:
    def decorator(func: HandlerFunc) -> HandlerFunc:
        if name in REGISTRY:
            raise RuntimeError(f"Job type {name!r} registered twice")
        REGISTRY[name] = JobHandler(name, description, payload_model, func, example, default_timeout_seconds)
        return func

    return decorator


def get_handler(job_type: str) -> JobHandler:
    try:
        return REGISTRY[job_type]
    except KeyError:
        raise UnknownJobTypeError(job_type) from None


def validate_payload(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Validate a payload and return its normalised JSON form (defaults filled in)."""
    return get_handler(job_type).validate(payload).model_dump(mode="json")


def _insert_ignore(session: Session, model: type, values: dict[str, Any], conflict_cols: list[str]):
    """INSERT ... ON CONFLICT DO NOTHING, for PostgreSQL and SQLite."""
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    elif dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert
    else:  # pragma: no cover
        return generic_insert(model).values(**values)
    return insert(model).values(**values).on_conflict_do_nothing(index_elements=conflict_cols)


# --------------------------------------------------------------------------- #
# Payload models
# --------------------------------------------------------------------------- #


class _StrictPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EchoPayload(_StrictPayload):
    message: str = Field(min_length=1, max_length=1000)


class SleepPayload(_StrictPayload):
    seconds: float = Field(ge=0, le=300, description="How long the worker should sleep.")


class ProcessRecordsPayload(_StrictPayload):
    batch_id: str = Field(min_length=1, max_length=128)
    record_ids: list[int] = Field(min_length=1, max_length=10_000)
    delay_ms: int = Field(
        default=0, ge=0, le=1000, description="Simulated latency per record, in milliseconds."
    )
    fail_first_attempt_after: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Testing aid: on attempt 1 only, raise a transient error after this many "
            "records have been stored, to simulate a crash halfway through a batch."
        ),
    )


class FlakyPayload(_StrictPayload):
    fail_times: int = Field(ge=0, le=10, description="Fail transiently on the first N attempts.")


class SummarizePayload(_StrictPayload):
    title: str = Field(default="summary", min_length=1, max_length=200)


class AlwaysFailPayload(_StrictPayload):
    reason: str = Field(default="Intentional failure for testing", max_length=500)
    transient: bool = Field(
        default=False,
        description="If true, raise a retryable error (ends in DEAD_LETTER); otherwise permanent (FAILED).",
    )


# --------------------------------------------------------------------------- #
# Handlers
# --------------------------------------------------------------------------- #


@register(
    "echo",
    payload_model=EchoPayload,
    description="Returns the message it was given. Useful as a smoke test.",
    example={"message": "hello flowgrid"},
    default_timeout_seconds=10,
)
def echo(payload: EchoPayload, ctx: JobContext) -> dict[str, Any]:
    return {"echo": payload.message}


@register(
    "sleep",
    payload_model=SleepPayload,
    description="Sleeps for N seconds to simulate a long-running job. Pair with timeout_seconds to test timeouts.",
    example={"seconds": 3},
    default_timeout_seconds=310,
)
def sleep(payload: SleepPayload, ctx: JobContext) -> dict[str, Any]:
    time.sleep(payload.seconds)
    return {"slept_seconds": payload.seconds}


@register(
    "flaky",
    payload_model=FlakyPayload,
    description="Fails with a transient error on the first N attempts, then succeeds. Demonstrates retries.",
    example={"fail_times": 2},
    default_timeout_seconds=10,
)
def flaky(payload: FlakyPayload, ctx: JobContext) -> dict[str, Any]:
    if ctx.attempt <= payload.fail_times:
        raise TransientJobError(f"simulated transient failure on attempt {ctx.attempt}")
    return {"succeeded_on_attempt": ctx.attempt}


_TIERS = ("bronze", "silver", "gold")
_COMMIT_EVERY = 100


@register(
    "process_records",
    payload_model=ProcessRecordsPayload,
    description=(
        "Validate → enrich → store → notify for a batch of customer records. Storage and "
        "notifications are idempotent, so retries never duplicate them."
    ),
    example={"batch_id": "batch-001", "record_ids": [101, 102, 103], "delay_ms": 200},
    default_timeout_seconds=300,
)
def process_records(payload: ProcessRecordsPayload, ctx: JobContext) -> dict[str, Any]:
    stored = 0
    notified = 0
    duplicate_notifications = 0
    failures: list[dict[str, Any]] = []
    tiers: dict[str, int] = {}
    seen: set[int] = set()

    with ctx.session_factory() as db:
        for i, record_id in enumerate(payload.record_ids, start=1):
            # 1. validate
            if record_id <= 0:
                failures.append({"record_id": record_id, "error": "record_id must be a positive integer"})
                continue
            if record_id in seen:
                continue
            seen.add(record_id)

            # 2. enrich (stand-in for an external API call; deterministic and offline)
            tier = _TIERS[record_id % len(_TIERS)]

            # 3. store — keyed by (batch_id, record_id): a re-run overwrites nothing.
            db.execute(
                _insert_ignore(
                    db,
                    ProcessedRecord,
                    {"batch_id": payload.batch_id, "record_id": record_id, "tier": tier, "job_id": ctx.job_id},
                    ["batch_id", "record_id"],
                )
            )
            stored += 1
            tiers[tier] = tiers.get(tier, 0) + 1

            # 4. notify — the dedupe key makes "send" happen at most once per record.
            #    RETURNING tells us whether this call inserted the row or the
            #    conflict skipped it (rowcount is not reliable for this across drivers).
            inserted = db.execute(
                _insert_ignore(
                    db,
                    Notification,
                    {
                        "id": uuid.uuid4(),
                        "dedupe_key": f"record-processed:{payload.batch_id}:{record_id}",
                        "job_id": ctx.job_id,
                        "message": f"Record {record_id} in batch {payload.batch_id} processed ({tier})",
                    },
                    ["dedupe_key"],
                ).returning(Notification.id)
            ).first()
            if inserted is not None:
                notified += 1
            else:
                duplicate_notifications += 1

            if i % _COMMIT_EVERY == 0:
                db.commit()
            if payload.delay_ms:
                time.sleep(payload.delay_ms / 1000)
            if (
                ctx.attempt == 1
                and payload.fail_first_attempt_after is not None
                and stored >= payload.fail_first_attempt_after
            ):
                db.commit()  # the work done so far is durable, as in a real crash
                raise TransientJobError(f"simulated crash after storing {stored} records")
        db.commit()

    return {
        "batch_id": payload.batch_id,
        "processed": stored,
        "failed": len(failures),
        "tiers": tiers,
        "notifications_sent": notified,
        "notifications_deduplicated": duplicate_notifications,
        # Cap the stored detail so one huge batch cannot bloat the jobs table.
        "failures": failures[:50],
    }


@register(
    "summarize",
    payload_model=SummarizePayload,
    description=(
        "Workflow fan-in step: sums every numeric field of its upstream steps' results "
        "(e.g. 'processed' across parallel process_records steps)."
    ),
    example={"title": "nightly import"},
    default_timeout_seconds=10,
)
def summarize(payload: SummarizePayload, ctx: JobContext) -> dict[str, Any]:
    totals: dict[str, float] = {}
    for result in ctx.upstream.values():
        for key, value in (result or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                totals[key] = totals.get(key, 0) + value
    return {"title": payload.title, "upstream_steps": sorted(ctx.upstream), "totals": totals}


@register(
    "always_fail",
    payload_model=AlwaysFailPayload,
    description="Always raises. transient=false → FAILED immediately; transient=true → retried, then DEAD_LETTER.",
    example={"reason": "testing failure handling", "transient": False},
    default_timeout_seconds=10,
)
def always_fail(payload: AlwaysFailPayload, ctx: JobContext) -> dict[str, Any]:
    if payload.transient:
        raise TransientJobError(payload.reason)
    raise PermanentJobError(payload.reason)
