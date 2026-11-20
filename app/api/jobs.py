import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_publisher
from app.db.models import JobStatus
from app.db.session import get_db
from app.schemas.job import JobCreate, JobList, JobRead, JobStats, JobTypeInfo
from app.services import jobs as jobs_service
from app.services.jobs import EnqueueError, Publisher
from app.tasks.handlers import REGISTRY, InvalidPayloadError, UnknownJobTypeError

router = APIRouter(prefix="/api/v1", tags=["jobs"])


@router.get("/job-types", response_model=list[JobTypeInfo])
def list_job_types() -> list[JobTypeInfo]:
    """The allow-list of job types and the payload schema each one accepts."""
    return [
        JobTypeInfo(
            name=h.name,
            description=h.description,
            payload_schema=h.payload_model.model_json_schema(),
            example=h.example,
        )
        for h in REGISTRY.values()
    ]


@router.post(
    "/jobs",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
    responses={422: {"description": "Unknown job type or invalid payload"}, 503: {"description": "Queue unavailable"}},
)
def create_job(
    body: JobCreate,
    db: Session = Depends(get_db),
    publish: Publisher = Depends(get_publisher),
) -> JobRead:
    """Submit a job. Returns 202: the job is accepted and will run asynchronously."""
    try:
        job = jobs_service.submit_job(db, body.type, body.payload, publish)
    except UnknownJobTypeError as exc:
        raise HTTPException(
            422,
            detail={"message": str(exc), "supported_types": sorted(REGISTRY)},
        ) from exc
    except InvalidPayloadError as exc:
        raise HTTPException(
            422,
            detail={"message": str(exc), "errors": exc.errors},
        ) from exc
    except EnqueueError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Job queue is unavailable; job was not enqueued", "job_id": str(exc.job.id)},
        ) from exc
    return JobRead.from_model(job)


# Declared before /jobs/{job_id} so "stats" is never parsed as a job ID.
@router.get("/jobs/stats", response_model=JobStats)
def get_job_stats(db: Session = Depends(get_db)) -> JobStats:
    counts = jobs_service.job_stats(db)
    return JobStats(total=sum(counts.values()), by_status=counts)


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)) -> JobRead:
    job = jobs_service.get_job(db, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Job not found")
    return JobRead.from_model(job)


@router.get("/jobs", response_model=JobList)
def list_jobs(
    status_filter: JobStatus | None = Query(default=None, alias="status"),
    job_type: str | None = Query(default=None, alias="type", max_length=64),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> JobList:
    rows, total = jobs_service.list_jobs(
        db, status=status_filter, job_type=job_type, limit=limit, offset=offset
    )
    return JobList(items=[JobRead.from_model(j) for j in rows], total=total, limit=limit, offset=offset)
