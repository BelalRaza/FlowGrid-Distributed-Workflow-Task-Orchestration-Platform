import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_publisher, require_client
from app.db.models import ApiKey, Job, JobStatus, RunStatus, Workflow, WorkflowRun
from app.db.session import get_db
from app.schemas.workflow import RunCreate, RunList, RunRead, StepState, WorkflowCreate, WorkflowRead
from app.services import workflows as workflow_service
from app.services.jobs import Publisher
from app.services.workflows import InvalidRunStateError, WorkflowValidationError

router = APIRouter(prefix="/api/v1", tags=["workflows"], dependencies=[Depends(require_client)])


def _owned_workflow(db: Session, workflow_id: uuid.UUID, api_key: ApiKey) -> Workflow:
    wf = db.get(Workflow, workflow_id)
    if wf is None or wf.api_key_id != api_key.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    return wf


def _owned_run(db: Session, run_id: uuid.UUID, api_key: ApiKey, workflow_id: uuid.UUID | None = None) -> WorkflowRun:
    run = db.get(WorkflowRun, run_id)
    if run is None or run.api_key_id != api_key.id or (workflow_id and run.workflow_id != workflow_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Workflow run not found")
    return run


def _read(wf: Workflow) -> WorkflowRead:
    return WorkflowRead.from_model(wf, workflow_service.step_levels(wf.definition["steps"]))


def _run_detail(db: Session, run: WorkflowRun) -> RunRead:
    wf = db.get(Workflow, run.workflow_id)
    levels = workflow_service.step_levels(wf.definition["steps"])
    deps = {s["id"]: s["depends_on"] for s in wf.definition["steps"]}
    jobs = {j.step_id: j for j in db.scalars(select(Job).where(Job.workflow_run_id == run.id))}
    steps = []
    for step in wf.definition["steps"]:
        job = jobs.get(step["id"])
        if job is None:  # SCHEDULED run: steps not materialised yet
            continue
        steps.append(StepState(
            step_id=step["id"], job_id=job.id, type=job.type, status=JobStatus(job.status),
            depends_on=deps[step["id"]], level=levels[step["id"]], attempts=job.attempts,
            max_attempts=job.max_attempts, result=job.result, error=job.error,
            started_at=job.started_at, finished_at=job.finished_at,
        ))
    return RunRead.from_model(run, wf.name, steps)


@router.post("/workflows", response_model=WorkflowRead, status_code=status.HTTP_201_CREATED,
             responses={422: {"description": "Invalid DAG: duplicate IDs, unknown deps, cycles, bad payloads"}})
def create_workflow(body: WorkflowCreate, api_key: ApiKey = Depends(require_client),
                    db: Session = Depends(get_db)) -> WorkflowRead:
    try:
        wf = workflow_service.create_workflow(
            db, api_key_id=api_key.id, name=body.name, steps=[s.model_dump() for s in body.steps]
        )
    except WorkflowValidationError as exc:
        raise HTTPException(422, detail={"message": str(exc), "errors": exc.errors}) from exc
    return _read(wf)


@router.get("/workflows", response_model=list[WorkflowRead])
def list_workflows(api_key: ApiKey = Depends(require_client), db: Session = Depends(get_db),
                   limit: int = Query(default=50, ge=1, le=200)) -> list[WorkflowRead]:
    rows = db.scalars(
        select(Workflow).where(Workflow.api_key_id == api_key.id).order_by(Workflow.created_at.desc()).limit(limit)
    )
    return [_read(wf) for wf in rows]


@router.get("/workflows/{workflow_id}", response_model=WorkflowRead)
def get_workflow(workflow_id: uuid.UUID, api_key: ApiKey = Depends(require_client),
                 db: Session = Depends(get_db)) -> WorkflowRead:
    return _read(_owned_workflow(db, workflow_id, api_key))


@router.post("/workflows/{workflow_id}/runs", response_model=RunRead, status_code=status.HTTP_202_ACCEPTED)
def start_run(
    workflow_id: uuid.UUID,
    body: RunCreate | None = None,
    api_key: ApiKey = Depends(require_client),
    db: Session = Depends(get_db),
    publish: Publisher = Depends(get_publisher),
) -> RunRead:
    """Start a run now, or schedule it with `run_at`."""
    wf = _owned_workflow(db, workflow_id, api_key)
    run = workflow_service.create_run(db, wf, publish, run_at=body.run_at if body else None)
    return _run_detail(db, run)


@router.get("/workflows/{workflow_id}/runs", response_model=RunList)
def list_runs_of_workflow(workflow_id: uuid.UUID, api_key: ApiKey = Depends(require_client),
                          db: Session = Depends(get_db), limit: int = Query(default=20, ge=1, le=100),
                          offset: int = Query(default=0, ge=0)) -> RunList:
    _owned_workflow(db, workflow_id, api_key)
    return _list_runs(db, api_key, limit, offset, workflow_id=workflow_id)


@router.get("/workflows/{workflow_id}/runs/{run_id}", response_model=RunRead)
def get_run(workflow_id: uuid.UUID, run_id: uuid.UUID, api_key: ApiKey = Depends(require_client),
            db: Session = Depends(get_db)) -> RunRead:
    return _run_detail(db, _owned_run(db, run_id, api_key, workflow_id))


@router.post("/workflows/{workflow_id}/runs/{run_id}/cancel", response_model=RunRead,
             responses={409: {"description": "Run already finished"}})
def cancel_run(workflow_id: uuid.UUID, run_id: uuid.UUID, api_key: ApiKey = Depends(require_client),
               db: Session = Depends(get_db)) -> RunRead:
    """Cancel steps that have not started. Steps already RUNNING finish on their own."""
    run = _owned_run(db, run_id, api_key, workflow_id)
    try:
        run = workflow_service.cancel_run(db, run)
    except InvalidRunStateError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return _run_detail(db, run)


@router.get("/workflow-runs", response_model=RunList)
def list_all_runs(api_key: ApiKey = Depends(require_client), db: Session = Depends(get_db),
                  status_filter: RunStatus | None = Query(default=None, alias="status"),
                  limit: int = Query(default=20, ge=1, le=100), offset: int = Query(default=0, ge=0)) -> RunList:
    """Recent runs across all of this client's workflows."""
    return _list_runs(db, api_key, limit, offset, status_filter=status_filter)


def _list_runs(db, api_key, limit, offset, workflow_id=None, status_filter=None) -> RunList:
    filters = [WorkflowRun.api_key_id == api_key.id]
    if workflow_id:
        filters.append(WorkflowRun.workflow_id == workflow_id)
    if status_filter:
        filters.append(WorkflowRun.status == status_filter.value)
    total = db.scalar(select(func.count()).select_from(WorkflowRun).where(*filters)) or 0
    rows = db.execute(
        select(WorkflowRun, Workflow.name)
        .join(Workflow, Workflow.id == WorkflowRun.workflow_id)
        .where(*filters)
        .order_by(WorkflowRun.created_at.desc(), WorkflowRun.id)
        .limit(limit).offset(offset)
    ).all()
    return RunList(items=[RunRead.from_model(r, name) for r, name in rows], total=total, limit=limit, offset=offset)



