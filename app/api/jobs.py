import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from app.api.deps import get_publisher, require_client
from app.core.config import get_settings
from app.db.models import ApiKey, JobStatus
from app.db.session import get_db
from app.schemas.job import AttemptRead, JobCreate, JobList, JobRead, JobStats, JobTypeInfo, WhoAmI
from app.schemas.workflow import BatchCreate, BatchResult
from app.services import jobs as jobs_service
from app.services.jobs import BatchItemError, IdempotencyConflictError, NotReplayableError, Publisher
from app.tasks.handlers import REGISTRY, InvalidPayloadError, UnknownJobTypeError

# Every route here requires a valid API key and is rate limited per key.
router = APIRouter(prefix="/api/v1", tags=["jobs"], dependencies=[Depends(require_client)])


@router.get("/me", response_model=WhoAmI)
def whoami(api_key: ApiKey = Depends(require_client)) -> WhoAmI:
    s = get_settings()
    return WhoAmI(
        name=api_key.name,
        prefix=api_key.prefix,
        rate_limit_capacity=s.rate_limit_capacity,
        rate_limit_refill_per_second=s.rate_limit_refill_per_second,
    )


@router.get("/job-types", response_model=list[JobTypeInfo])
def list_job_types() -> list[JobTypeInfo]:
    """The allow-list of job types and the payload schema each one accepts."""
    return [
        JobTypeInfo(
            name=h.name,
            description=h.description,
            payload_schema=h.payload_model.model_json_schema(),
            example=h.example,
            default_timeout_seconds=h.default_timeout_seconds,
        )
        for h in REGISTRY.values()
    ]


@router.post(
    "/jobs",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        200: {"description": "Idempotent replay: this Idempotency-Key already created a job"},
        409: {"description": "Idempotency-Key reused with a different request body"},
        422: {"description": "Unknown job type or invalid payload"},
    },
)
def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=255),
    api_key: ApiKey = Depends(require_client),
    db: Session = Depends(get_db),
    publish: Publisher = Depends(get_publisher),
) -> JobRead:
    """Submit a job. Returns 202: accepted, will run asynchronously.

    If the broker is unreachable the job is still accepted (status PENDING) and
    the sweeper publishes it once the broker recovers.
    """
    try:
        result = jobs_service.submit_job(
            db,
            job_type=body.type,
            payload=body.payload,
            publish=publish,
            api_key_id=api_key.id,
            max_attempts=body.max_attempts,
            timeout_seconds=body.timeout_seconds,
            idempotency_key=idempotency_key,
            run_at=body.resolved_run_at(),
        )
    except UnknownJobTypeError as exc:
        raise HTTPException(422, detail={"message": str(exc), "supported_types": sorted(REGISTRY)}) from exc
    except InvalidPayloadError as exc:
        raise HTTPException(422, detail={"message": str(exc), "errors": exc.errors}) from exc
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail={"message": str(exc), "job_id": str(exc.job_id)}
        ) from exc

    if result.replayed:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    return JobRead.from_model(result.job)


@router.post(
    "/jobs/batch",
    response_model=BatchResult,
    status_code=status.HTTP_202_ACCEPTED,
    responses={422: {"description": "Any invalid item rejects the whole batch"}},
)
def create_jobs_batch(
    body: BatchCreate,
    api_key: ApiKey = Depends(require_client),
    db: Session = Depends(get_db),
    publish: Publisher = Depends(get_publisher),
) -> BatchResult:
    """Submit up to `MAX_BATCH_SIZE` jobs atomically (one transaction, one rate-limit token)."""
    try:
        jobs = jobs_service.submit_batch(db, [item.service_kwargs() for item in body.jobs], publish, api_key.id)
    except BatchItemError as exc:
        cause = exc.cause
        detail = {"message": f"jobs[{exc.index}]: {cause}", "index": exc.index}
        if isinstance(cause, InvalidPayloadError):
            detail["errors"] = cause.errors
        raise HTTPException(422, detail=detail) from exc
    return BatchResult(count=len(jobs), job_ids=[j.id for j in jobs])


# Declared before /jobs/{job_id} so "stats" is never parsed as a job ID.
@router.get("/jobs/stats", response_model=JobStats)
def get_job_stats(api_key: ApiKey = Depends(require_client), db: Session = Depends(get_db)) -> JobStats:
    counts = jobs_service.job_stats(db, api_key.id)
    return JobStats(total=sum(counts.values()), by_status=counts)


def _owned_job(db: Session, job_id: uuid.UUID, api_key: ApiKey):
    job = jobs_service.get_job(db, job_id, api_key.id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: uuid.UUID, api_key: ApiKey = Depends(require_client), db: Session = Depends(get_db)) -> JobRead:
    return JobRead.from_model(_owned_job(db, job_id, api_key))


@router.get("/jobs/{job_id}/attempts", response_model=list[AttemptRead])
def get_job_attempts(
    job_id: uuid.UUID, api_key: ApiKey = Depends(require_client), db: Session = Depends(get_db)
) -> list[AttemptRead]:
    _owned_job(db, job_id, api_key)
    return [AttemptRead.from_model(a) for a in jobs_service.list_attempts(db, job_id)]


@router.post(
    "/jobs/{job_id}/replay",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "Job is not FAILED or DEAD_LETTER"}},
)
def replay_job(
    job_id: uuid.UUID,
    api_key: ApiKey = Depends(require_client),
    db: Session = Depends(get_db),
    publish: Publisher = Depends(get_publisher),
) -> JobRead:
    """Re-run a FAILED or DEAD_LETTER job as a new job linked by `replay_of`."""
    job = _owned_job(db, job_id, api_key)
    try:
        new_job = jobs_service.replay_job(db, job, publish)
    except NotReplayableError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return JobRead.from_model(new_job)


@router.get("/jobs", response_model=JobList)
def list_jobs(
    status_filter: JobStatus | None = Query(default=None, alias="status"),
    job_type: str | None = Query(default=None, alias="type", max_length=64),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    api_key: ApiKey = Depends(require_client),
    db: Session = Depends(get_db),
) -> JobList:
    rows, total = jobs_service.list_jobs(
        db, api_key_id=api_key.id, status=status_filter, job_type=job_type, limit=limit, offset=offset
    )
    return JobList(items=[JobRead.from_model(j) for j in rows], total=total, limit=limit, offset=offset)
