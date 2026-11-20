"""Recurring (cron) schedules for jobs and workflows.

Firing a tick must happen **exactly once**, even with several sweepers and
crashes at the worst moment. Two mechanisms make that true:

1. Compare-and-swap on ``next_run_at``. Each sweeper reads a due schedule and
   runs ``UPDATE schedules SET next_run_at = :following WHERE id = :id AND
   next_run_at = :tick``. Only one sweeper's UPDATE can match.
2. The job / run for the tick is inserted **in the same transaction** as the
   CAS, with a unique ``(schedule_id, scheduled_for)``. A crash before commit
   rolls back both (the tick fires on the next pass); a commit makes both
   durable. There is no window in which the schedule advanced but the job was
   never created.

Missed ticks (all sweepers down for an hour) are *coalesced*: the schedule
fires once on recovery and jumps to the next future tick, instead of
replaying a burst of 60 stale runs.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from croniter import croniter
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import Job, Schedule, Workflow, as_utc, utcnow
from app.services import jobs as jobs_service
from app.services import workflows as workflow_service
from app.services.jobs import Publisher
from app.tasks.handlers import validate_payload

logger = logging.getLogger(__name__)


class InvalidScheduleError(Exception):
    pass


def validate_cron(expr: str) -> None:
    # Standard 5-field cron only (minute resolution). croniter also accepts a
    # 6th "seconds" field; refusing it keeps schedules from hammering the system.
    if len(expr.split()) != 5 or not croniter.is_valid(expr):
        raise InvalidScheduleError(f"Invalid cron expression {expr!r} (expected 5 fields, e.g. '*/5 * * * *')")


def next_fire(expr: str, after: datetime) -> datetime:
    return as_utc(croniter(expr, as_utc(after)).get_next(datetime))


def create_schedule(
    db: Session,
    *,
    api_key_id: uuid.UUID | None,
    name: str,
    cron: str,
    job: dict[str, Any] | None = None,
    workflow: Workflow | None = None,
) -> Schedule:
    validate_cron(cron)
    if (job is None) == (workflow is None):
        raise InvalidScheduleError("Provide exactly one target: 'job' or 'workflow_id'")
    schedule = Schedule(
        api_key_id=api_key_id,
        name=name,
        cron=cron,
        enabled=True,
        next_run_at=next_fire(cron, utcnow()),
    )
    if job is not None:
        schedule.target_kind = "job"
        schedule.job_type = job["type"]
        schedule.payload = validate_payload(job["type"], job.get("payload") or {})
        schedule.max_attempts = job.get("max_attempts")
        schedule.timeout_seconds = job.get("timeout_seconds")
    else:
        schedule.target_kind = "workflow"
        schedule.workflow_id = workflow.id
    db.add(schedule)
    db.commit()
    return schedule


def set_enabled(db: Session, schedule: Schedule, enabled: bool) -> Schedule:
    values: dict[str, Any] = {"enabled": enabled, "updated_at": utcnow()}
    if enabled and not schedule.enabled:
        # Resuming does not fire every tick missed while paused.
        values["next_run_at"] = next_fire(schedule.cron, utcnow())
    db.execute(update(Schedule).where(Schedule.id == schedule.id).values(**values))
    db.commit()
    return db.get(Schedule, schedule.id, populate_existing=True)


def _create_target(db: Session, schedule: Schedule, tick: datetime) -> list[Job]:
    if schedule.target_kind == "job":
        job = jobs_service.build_job(
            db,
            job_type=schedule.job_type,
            payload=schedule.payload or {},
            api_key_id=schedule.api_key_id,
            max_attempts=schedule.max_attempts,
            timeout_seconds=schedule.timeout_seconds,
            schedule_id=schedule.id,
            scheduled_for=tick,
        )
        return [job]
    workflow = db.get(Workflow, schedule.workflow_id)
    _, roots = workflow_service.build_run(db, workflow, schedule_id=schedule.id, scheduled_for=tick)
    return roots


def fire_due_schedules(db: Session, publish: Publisher, now: datetime) -> int:
    # Snapshot plain values, not ORM objects: a rollback() inside the loop
    # expires every loaded object, and lazily re-reading next_run_at after
    # another sweeper advanced it would make us CAS against (and fire) the
    # *next* tick early.
    due = db.execute(
        select(Schedule.id, Schedule.next_run_at, Schedule.cron)
        .where(Schedule.enabled.is_(True), Schedule.next_run_at <= now)
        .order_by(Schedule.next_run_at)
        .limit(get_settings().sweep_batch_size)
    ).all()
    db.rollback()

    fired = 0
    for schedule_id, stored_tick, cron in due:
        tick = as_utc(stored_tick)
        following = next_fire(cron, max(now, tick))  # coalesce missed ticks
        won = db.execute(
            update(Schedule)
            .where(Schedule.id == schedule_id, Schedule.enabled.is_(True), Schedule.next_run_at == stored_tick)
            .values(next_run_at=following, last_run_at=now, updated_at=now)
            .execution_options(synchronize_session=False)
        ).rowcount == 1
        if not won:
            db.rollback()  # another sweeper fired this tick
            continue
        schedule = db.get(Schedule, schedule_id, populate_existing=True)
        try:
            pending = _create_target(db, schedule, tick)
            db.commit()
        except Exception as exc:
            # IntegrityError: this tick's job/run already exists (unique
            # schedule_id + scheduled_for), so the tick is served. Anything else:
            # the target became invalid (e.g. a job type was removed). Either way
            # the rollback also undid our CAS, so advance the schedule on its own;
            # otherwise the same tick would be retried on every pass, forever.
            db.rollback()
            if not isinstance(exc, IntegrityError):
                logger.exception("Schedule target could not be created", extra={"schedule_id": str(schedule_id)})
            db.execute(
                update(Schedule)
                .where(Schedule.id == schedule_id, Schedule.next_run_at == stored_tick)
                .values(next_run_at=following, last_run_at=now, updated_at=now)
            )
            db.commit()
            continue
        jobs_service.publish_many(db, [j for j in pending if j.status == "PENDING"], publish)
        fired += 1
        logger.info("Schedule fired", extra={"schedule_id": str(schedule.id), "tick": tick.isoformat(),
                                             "next_run_at": following.isoformat()})
    return fired
