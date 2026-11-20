import uuid

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import require_client
from app.db.models import ApiKey, Schedule, Workflow
from app.db.session import get_db
from app.schemas.workflow import ScheduleCreate, ScheduleRead, ScheduleUpdate
from app.services import schedules as schedule_service
from app.services.schedules import InvalidScheduleError
from app.tasks.handlers import InvalidPayloadError, UnknownJobTypeError

router = APIRouter(prefix="/api/v1/schedules", tags=["schedules"], dependencies=[Depends(require_client)])


def _owned(db: Session, schedule_id: uuid.UUID, api_key: ApiKey) -> Schedule:
    s = db.get(Schedule, schedule_id)
    if s is None or s.api_key_id != api_key.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Schedule not found")
    return s


@router.post("", response_model=ScheduleRead, status_code=status.HTTP_201_CREATED)
def create_schedule(body: ScheduleCreate, api_key: ApiKey = Depends(require_client),
                    db: Session = Depends(get_db)) -> ScheduleRead:
    workflow = None
    if body.workflow_id is not None:
        workflow = db.get(Workflow, body.workflow_id)
        if workflow is None or workflow.api_key_id != api_key.id:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    try:
        schedule = schedule_service.create_schedule(
            db, api_key_id=api_key.id, name=body.name, cron=body.cron,
            job=body.job.model_dump() if body.job else None, workflow=workflow,
        )
    except (InvalidScheduleError, UnknownJobTypeError) as exc:
        raise HTTPException(422, detail={"message": str(exc)}) from exc
    except InvalidPayloadError as exc:
        raise HTTPException(422, detail={"message": str(exc), "errors": exc.errors}) from exc
    return ScheduleRead.from_model(schedule)


@router.get("", response_model=list[ScheduleRead])
def list_schedules(api_key: ApiKey = Depends(require_client), db: Session = Depends(get_db)) -> list[ScheduleRead]:
    rows = db.scalars(select(Schedule).where(Schedule.api_key_id == api_key.id).order_by(Schedule.created_at))
    return [ScheduleRead.from_model(s) for s in rows]


@router.get("/{schedule_id}", response_model=ScheduleRead)
def get_schedule(schedule_id: uuid.UUID, api_key: ApiKey = Depends(require_client),
                 db: Session = Depends(get_db)) -> ScheduleRead:
    return ScheduleRead.from_model(_owned(db, schedule_id, api_key))


@router.patch("/{schedule_id}", response_model=ScheduleRead)
def update_schedule(schedule_id: uuid.UUID, body: ScheduleUpdate, api_key: ApiKey = Depends(require_client),
                    db: Session = Depends(get_db)) -> ScheduleRead:
    """Pause (`enabled: false`) or resume. Resuming skips ticks missed while paused."""
    return ScheduleRead.from_model(schedule_service.set_enabled(db, _owned(db, schedule_id, api_key), body.enabled))


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_schedule(schedule_id: uuid.UUID, api_key: ApiKey = Depends(require_client),
                    db: Session = Depends(get_db)) -> Response:
    db.delete(_owned(db, schedule_id, api_key))
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
