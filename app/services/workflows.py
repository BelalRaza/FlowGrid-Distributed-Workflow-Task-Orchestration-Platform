"""Workflow orchestration: DAG validation, run materialisation and advancement.

A workflow is a list of steps. Each step is an ordinary allow-listed job type
plus ``depends_on`` (other step IDs). Starting a run creates *every* step's job
up front, in one transaction:

    steps with no dependencies  -> PENDING (published immediately)
    steps with dependencies     -> BLOCKED, plus job_dependencies edges

Each step then reuses all of Phase 2's machinery (claims, leases, retries,
timeouts, fencing) unchanged. The only new logic is ``advance_run``, which
runs after a step reaches a terminal state:

    any step FAILED / DEAD_LETTER -> cancel not-yet-started steps, run FAILED
    all steps SUCCEEDED           -> run SUCCEEDED
    otherwise                     -> BLOCKED steps whose parents all succeeded -> PENDING

Correctness note (the "lost wakeup"): a step's success is committed *before*
``advance_run`` reads the DAG, in a separate transaction. If siblings A and B
finish concurrently, whichever advance runs second is guaranteed to see both
successes, so their common child is always unblocked. Both advances may try to
unblock it; the guarded BLOCKED -> PENDING update lets exactly one win. If the
worker dies between the two transactions, the sweeper's ``advance_stuck_runs``
finds the BLOCKED step whose parents are all done and finishes the job.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session, aliased

from app.core.config import get_settings
from app.db.models import (
    FAILED_STATUSES,
    Job,
    JobDependency,
    JobStatus,
    RunStatus,
    Workflow,
    WorkflowRun,
    as_utc,
    utcnow,
)
from app.services import jobs as jobs_service
from app.services.jobs import Publisher
from app.tasks.handlers import InvalidPayloadError, UnknownJobTypeError, validate_payload

logger = logging.getLogger(__name__)

NOT_STARTED = (
    JobStatus.SCHEDULED,
    JobStatus.BLOCKED,
    JobStatus.PENDING,
    JobStatus.QUEUED,
    JobStatus.RETRY_SCHEDULED,
)


class WorkflowValidationError(Exception):
    def __init__(self, errors: list[dict[str, Any]]) -> None:
        super().__init__("Invalid workflow definition")
        self.errors = errors


class NotFoundError(Exception):
    pass


class InvalidRunStateError(Exception):
    pass


# --------------------------------------------------------------------------- #
# DAG validation
# --------------------------------------------------------------------------- #


def topological_order(steps: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Kahn's algorithm. Returns (order, nodes_left_in_cycles)."""
    indegree = {s["id"]: len(s["depends_on"]) for s in steps}
    children: dict[str, list[str]] = defaultdict(list)
    for s in steps:
        for dep in s["depends_on"]:
            children[dep].append(s["id"])

    ready = deque(sorted(sid for sid, n in indegree.items() if n == 0))
    order: list[str] = []
    while ready:
        sid = ready.popleft()
        order.append(sid)
        for child in children[sid]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
    return order, sorted(sid for sid, n in indegree.items() if n > 0)


def step_levels(steps: list[dict[str, Any]]) -> dict[str, int]:
    """Depth of each step (longest path from a root). Used to draw the DAG."""
    order, _ = topological_order(steps)
    deps = {s["id"]: s["depends_on"] for s in steps}
    level: dict[str, int] = {}
    for sid in order:
        level[sid] = 1 + max((level[d] for d in deps[sid]), default=-1)
    return level


def validate_definition(steps: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate a workflow's steps and return the normalised definition.

    Collects *all* problems (duplicate IDs, unknown dependencies, invalid
    payloads, cycles) instead of stopping at the first, so a client can fix a
    definition in one round trip.
    """
    errors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, step in enumerate(steps):
        if step["id"] in seen:
            errors.append({"loc": ["steps", i, "id"], "msg": f"duplicate step id {step['id']!r}"})
        seen.add(step["id"])

    normalized = []
    for i, step in enumerate(steps):
        deps = list(dict.fromkeys(step.get("depends_on") or []))  # de-duplicate, keep order
        for dep in deps:
            if dep == step["id"]:
                errors.append({"loc": ["steps", i, "depends_on"], "msg": "a step cannot depend on itself"})
            elif dep not in seen:
                errors.append({"loc": ["steps", i, "depends_on"], "msg": f"unknown step {dep!r}"})
        try:
            payload = validate_payload(step["type"], step.get("payload") or {})
        except UnknownJobTypeError as exc:
            errors.append({"loc": ["steps", i, "type"], "msg": str(exc)})
            payload = step.get("payload") or {}
        except InvalidPayloadError as exc:
            for e in exc.errors:
                errors.append({"loc": ["steps", i, "payload", *e["loc"]], "msg": e["msg"]})
            payload = step.get("payload") or {}
        normalized.append({**step, "depends_on": deps, "payload": payload})

    if not errors:
        _, cyclic = topological_order(normalized)
        if cyclic:
            errors.append({"loc": ["steps"], "msg": f"dependency cycle involving steps {cyclic}"})
    if errors:
        raise WorkflowValidationError(errors)
    return {"steps": normalized}


def create_workflow(db: Session, *, api_key_id: uuid.UUID | None, name: str, steps: list[dict[str, Any]]) -> Workflow:
    workflow = Workflow(api_key_id=api_key_id, name=name, definition=validate_definition(steps))
    db.add(workflow)
    db.commit()
    return workflow


# --------------------------------------------------------------------------- #
# Runs
# --------------------------------------------------------------------------- #


def _materialize_steps(db: Session, run: WorkflowRun, definition: dict[str, Any]) -> list[Job]:
    """Create every step job (+ edges) in the current transaction. Returns the roots."""
    by_step: dict[str, Job] = {}
    roots: list[Job] = []
    for step in definition["steps"]:
        job = jobs_service.build_job(
            db,
            job_type=step["type"],
            payload=step["payload"],
            api_key_id=run.api_key_id,
            max_attempts=step.get("max_attempts"),
            timeout_seconds=step.get("timeout_seconds"),
            status=JobStatus.BLOCKED if step["depends_on"] else JobStatus.PENDING,
            workflow_run_id=run.id,
            step_id=step["id"],
        )
        by_step[step["id"]] = job
        if not step["depends_on"]:
            roots.append(job)
    db.flush()
    for step in definition["steps"]:
        for dep in step["depends_on"]:
            db.add(JobDependency(job_id=by_step[step["id"]].id, depends_on_job_id=by_step[dep].id))
    return roots


def build_run(
    db: Session,
    workflow: Workflow,
    *,
    run_at: datetime | None = None,
    schedule_id: uuid.UUID | None = None,
    scheduled_for: datetime | None = None,
) -> tuple[WorkflowRun, list[Job]]:
    """Add a run (and, if it starts now, all its step jobs) without committing."""
    run_at = as_utc(run_at)
    run = WorkflowRun(
        id=uuid.uuid4(),
        workflow_id=workflow.id,
        api_key_id=workflow.api_key_id,
        schedule_id=schedule_id,
        scheduled_for=scheduled_for or run_at,
    )
    if run_at is not None and run_at > utcnow():
        run.status = RunStatus.SCHEDULED.value
        db.add(run)
        return run, []
    run.status = RunStatus.RUNNING.value
    run.started_at = utcnow()
    db.add(run)
    db.flush()
    return run, _materialize_steps(db, run, workflow.definition)


def create_run(db: Session, workflow: Workflow, publish: Publisher, run_at: datetime | None = None) -> WorkflowRun:
    run, roots = build_run(db, workflow, run_at=run_at)
    db.commit()
    jobs_service.publish_many(db, roots, publish)
    logger.info("Workflow run created", extra={"workflow_run_id": str(run.id), "status": run.status})
    return db.get(WorkflowRun, run.id, populate_existing=True)


def start_scheduled_run(db: Session, run_id: uuid.UUID, publish: Publisher, now: datetime) -> bool:
    """SCHEDULED -> RUNNING once run_at has passed (called by the sweeper)."""
    claimed = db.execute(
        update(WorkflowRun)
        .where(WorkflowRun.id == run_id, WorkflowRun.status == RunStatus.SCHEDULED.value)
        .values(status=RunStatus.RUNNING.value, started_at=now, updated_at=now)
        .execution_options(synchronize_session=False)
    ).rowcount == 1
    if not claimed:
        db.rollback()
        return False
    run = db.get(WorkflowRun, run_id, populate_existing=True)
    workflow = db.get(Workflow, run.workflow_id)
    roots = _materialize_steps(db, run, workflow.definition)
    db.commit()
    jobs_service.publish_many(db, roots, publish)
    return True


def _run_transition(db: Session, run_id: uuid.UUID, from_: list[RunStatus], to: RunStatus, **values: Any) -> bool:
    now = utcnow()
    return db.execute(
        update(WorkflowRun)
        .where(WorkflowRun.id == run_id, WorkflowRun.status.in_([s.value for s in from_]))
        .values(status=to.value, updated_at=now, **values)
        .execution_options(synchronize_session=False)
    ).rowcount == 1


def _cancel_unstarted(db: Session, run_id: uuid.UUID, reason: str) -> int:
    now = utcnow()
    return db.execute(
        update(Job)
        .where(Job.workflow_run_id == run_id, Job.status.in_([s.value for s in NOT_STARTED]))
        .values(status=JobStatus.CANCELLED.value, error=reason, finished_at=now, next_attempt_at=None, updated_at=now)
        .execution_options(synchronize_session=False)
    ).rowcount


def advance_run(db: Session, run_id: uuid.UUID, publish: Publisher) -> RunStatus | None:
    """Re-evaluate a RUNNING run after one of its steps changed. Idempotent."""
    run = db.get(WorkflowRun, run_id, populate_existing=True)
    if run is None or run.status != RunStatus.RUNNING.value:
        return None

    jobs = db.scalars(
        select(Job).where(Job.workflow_run_id == run_id).execution_options(populate_existing=True)
    ).all()
    status_of = {j.id: JobStatus(j.status) for j in jobs}

    failed = sorted(
        (j for j in jobs if JobStatus(j.status) in (JobStatus.FAILED, JobStatus.DEAD_LETTER)),
        key=lambda j: as_utc(j.finished_at) or utcnow(),
    )
    if failed:
        # Fail fast: stop scheduling new steps. Steps already RUNNING finish on
        # their own; their results are recorded but no longer advance the run.
        first = failed[0]
        cancelled = _cancel_unstarted(db, run_id, f"Cancelled: upstream step '{first.step_id}' {first.status}")
        moved = _run_transition(
            db, run_id, [RunStatus.RUNNING], RunStatus.FAILED,
            error=f"step '{first.step_id}' {first.status}: {first.error}", finished_at=utcnow(),
        )
        db.commit()
        if moved:
            logger.warning("Workflow run failed", extra={"workflow_run_id": str(run_id), "failed_step": first.step_id,
                                                         "steps_cancelled": cancelled})
        return RunStatus.FAILED if moved else None

    if all(s == JobStatus.SUCCEEDED for s in status_of.values()):
        moved = _run_transition(db, run_id, [RunStatus.RUNNING], RunStatus.SUCCEEDED, finished_at=utcnow())
        db.commit()
        if moved:
            logger.info("Workflow run succeeded", extra={"workflow_run_id": str(run_id)})
        return RunStatus.SUCCEEDED if moved else None

    parents: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    for child, parent in db.execute(
        select(JobDependency.job_id, JobDependency.depends_on_job_id)
        .join(Job, Job.id == JobDependency.job_id)
        .where(Job.workflow_run_id == run_id)
    ):
        parents[child].append(parent)

    unblocked = []
    for job in jobs:
        if status_of[job.id] == JobStatus.BLOCKED and all(
            status_of[p] == JobStatus.SUCCEEDED for p in parents[job.id]
        ):
            # Guarded: if two advances race, exactly one moves the step.
            if jobs_service._guarded_update(db, job.id, from_statuses=[JobStatus.BLOCKED], to_status=JobStatus.PENDING):
                unblocked.append(job)
    db.commit()
    if unblocked:
        logger.info("Unblocked workflow steps",
                    extra={"workflow_run_id": str(run_id), "steps": [j.step_id for j in unblocked]})
        jobs_service.publish_many(db, [jobs_service._reload(db, j.id) for j in unblocked], publish)
    return RunStatus.RUNNING


def cancel_run(db: Session, run: WorkflowRun) -> WorkflowRun:
    moved = _run_transition(
        db, run.id, [RunStatus.SCHEDULED, RunStatus.RUNNING], RunStatus.CANCELLED,
        error="Cancelled by client", finished_at=utcnow(),
    )
    if not moved:
        db.rollback()
        raise InvalidRunStateError(f"Run is {run.status}; only SCHEDULED or RUNNING runs can be cancelled")
    _cancel_unstarted(db, run.id, "Cancelled: workflow run cancelled by client")
    db.commit()
    return db.get(WorkflowRun, run.id, populate_existing=True)


def upstream_results(db: Session, job_id: uuid.UUID) -> dict[str, Any]:
    """Results of a step's direct dependencies, keyed by step ID."""
    rows = db.execute(
        select(Job.step_id, Job.result)
        .join(JobDependency, JobDependency.depends_on_job_id == Job.id)
        .where(JobDependency.job_id == job_id)
    )
    return {step_id: result for step_id, result in rows}


# --------------------------------------------------------------------------- #
# Sweeper hooks
# --------------------------------------------------------------------------- #


def runs_needing_advance(db: Session, limit: int) -> set[uuid.UUID]:
    """RUNNING runs whose state implies a transition nobody made (e.g. a worker
    died between committing a step's outcome and advancing the run)."""
    running = WorkflowRun.status == RunStatus.RUNNING.value
    parent = aliased(Job)
    unsatisfied = (
        select(JobDependency.job_id)
        .join(parent, parent.id == JobDependency.depends_on_job_id)
        .where(JobDependency.job_id == Job.id, parent.status != JobStatus.SUCCEEDED.value)
        .exists()
    )
    ready_blocked = (
        select(Job.workflow_run_id)
        .join(WorkflowRun, WorkflowRun.id == Job.workflow_run_id)
        .where(running, Job.status == JobStatus.BLOCKED.value, ~unsatisfied)
    )
    failed_step = (
        select(Job.workflow_run_id)
        .join(WorkflowRun, WorkflowRun.id == Job.workflow_run_id)
        .where(running, Job.status.in_([JobStatus.FAILED.value, JobStatus.DEAD_LETTER.value]))
    )
    all_done = select(WorkflowRun.id).where(
        running,
        ~select(Job.id)
        .where(Job.workflow_run_id == WorkflowRun.id, Job.status != JobStatus.SUCCEEDED.value)
        .exists(),
    )
    ids: set[uuid.UUID] = set()
    for q in (ready_blocked, failed_step, all_done):
        ids.update(db.scalars(q.distinct().limit(limit)))
    db.rollback()
    return ids


def due_scheduled_runs(db: Session, now: datetime) -> list[uuid.UUID]:
    ids = list(db.scalars(
        select(WorkflowRun.id)
        .where(WorkflowRun.status == RunStatus.SCHEDULED.value, WorkflowRun.scheduled_for <= now)
        .order_by(WorkflowRun.scheduled_for)
        .limit(get_settings().sweep_batch_size)
    ))
    db.rollback()
    return ids


__all__ = [
    "FAILED_STATUSES",
    "advance_run",
    "build_run",
    "cancel_run",
    "create_run",
    "create_workflow",
    "step_levels",
    "upstream_results",
    "validate_definition",
]
