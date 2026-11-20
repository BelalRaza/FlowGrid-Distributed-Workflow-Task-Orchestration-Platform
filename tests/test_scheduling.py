"""Phase 3: delayed jobs and recurring (cron) schedules."""

import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, select

from app.db.models import Job, Schedule, as_utc, utcnow
from app.services.schedules import fire_due_schedules
from app.tasks.worker import publish_job

# --------------------------------------------------------------------------- #
# Delayed one-off jobs
# --------------------------------------------------------------------------- #


def test_delayed_job_waits_then_runs(client, sweep):
    job = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "later"}, "delay_seconds": 60}).json()
    assert job["status"] == "SCHEDULED" and job["attempts"] == 0
    assert job["next_attempt_at"] is not None

    assert sweep(seconds_ahead=30).scheduled_jobs_released == 0
    assert sweep(seconds_ahead=90).scheduled_jobs_released == 1
    assert client.get(f"/api/v1/jobs/{job['job_id']}").json()["status"] == "SUCCEEDED"


def test_run_at_in_the_past_runs_immediately(client):
    past = (utcnow() - timedelta(minutes=5)).isoformat()
    job = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"}, "run_at": past}).json()
    assert job["status"] == "SUCCEEDED"


def test_run_at_and_delay_are_mutually_exclusive(client):
    res = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"},
                                            "run_at": utcnow().isoformat(), "delay_seconds": 5})
    assert res.status_code == 422


# --------------------------------------------------------------------------- #
# Recurring schedules
# --------------------------------------------------------------------------- #

EVERY_MINUTE = {"name": "tick", "cron": "* * * * *", "job": {"type": "echo", "payload": {"message": "tick"}}}


def _schedule(client, body=EVERY_MINUTE) -> dict:
    res = client.post("/api/v1/schedules", json=body)
    assert res.status_code == 201, res.json()
    return res.json()


def _jobs_for(db, schedule_id):
    return db.scalars(
        select(Job).where(Job.schedule_id == uuid.UUID(str(schedule_id))).order_by(Job.scheduled_for)
    ).all()


def test_schedule_validation(client):
    def status(body):
        return client.post("/api/v1/schedules", json=body).status_code

    assert status({**EVERY_MINUTE, "cron": "every minute"}) == 422
    assert status({**EVERY_MINUTE, "cron": "*/5 * * * * *"}) == 422  # 6-field (seconds) refused
    assert status({**EVERY_MINUTE, "job": {"type": "echo", "payload": {}}}) == 422  # bad payload
    assert status({"name": "x", "cron": "* * * * *"}) == 422  # no target
    assert status({**EVERY_MINUTE, "workflow_id": "00000000-0000-0000-0000-000000000000"}) == 422  # two targets


def test_next_run_at_is_the_next_cron_tick(client):
    s = _schedule(client, {**EVERY_MINUTE, "cron": "0 3 * * *"})
    nxt = as_utc(datetime.fromisoformat(s["next_run_at"]))
    assert nxt > utcnow() and (nxt.hour, nxt.minute, nxt.second) == (3, 0, 0)


def test_each_tick_fires_exactly_once(client, db):
    s = _schedule(client)
    tick = as_utc(db.get(Schedule, uuid.UUID(s["schedule_id"])).next_run_at)
    now = tick + timedelta(seconds=1)

    assert fire_due_schedules(db, publish_job, now) == 1
    assert fire_due_schedules(db, publish_job, now) == 0  # same tick, second sweeper pass
    jobs = _jobs_for(db, s["schedule_id"])
    assert len(jobs) == 1
    assert as_utc(jobs[0].scheduled_for) == tick
    assert jobs[0].status == "SUCCEEDED"

    assert fire_due_schedules(db, publish_job, now + timedelta(minutes=1)) == 1  # next tick
    assert len(_jobs_for(db, s["schedule_id"])) == 2


def test_already_existing_tick_does_not_wedge_the_schedule(client, db):
    """If a job for the tick already exists (unique schedule_id + scheduled_for),
    the schedule must still advance instead of retrying that tick forever."""
    s = _schedule(client)
    schedule = db.get(Schedule, uuid.UUID(s["schedule_id"]))
    tick = as_utc(schedule.next_run_at)
    db.add(Job(type="echo", payload={"message": "dup"}, status="SUCCEEDED", schedule_id=schedule.id,
               scheduled_for=tick, api_key_id=schedule.api_key_id, max_attempts=1, timeout_seconds=10))
    db.commit()

    assert fire_due_schedules(db, publish_job, tick + timedelta(seconds=1)) == 0
    db.refresh(schedule)
    assert as_utc(schedule.next_run_at) > tick  # moved on
    assert len(_jobs_for(db, s["schedule_id"])) == 1


def test_missed_ticks_are_coalesced(client, db):
    s = _schedule(client)
    schedule = db.get(Schedule, uuid.UUID(s["schedule_id"]))
    tick = as_utc(schedule.next_run_at)
    later = tick + timedelta(minutes=10, seconds=5)  # all sweepers were down for 10 minutes

    assert fire_due_schedules(db, publish_job, later) == 1  # fires once, not 11 times
    db.refresh(schedule)
    assert as_utc(schedule.next_run_at) > later
    assert len(_jobs_for(db, s["schedule_id"])) == 1


def test_pause_and_resume(client, db):
    s = _schedule(client)
    assert client.patch(f"/api/v1/schedules/{s['schedule_id']}", json={"enabled": False}).json()["enabled"] is False
    tick = as_utc(db.get(Schedule, uuid.UUID(s["schedule_id"])).next_run_at)
    assert fire_due_schedules(db, publish_job, tick + timedelta(minutes=5)) == 0

    resumed = client.patch(f"/api/v1/schedules/{s['schedule_id']}", json={"enabled": True}).json()
    assert resumed["enabled"] is True
    assert as_utc(datetime.fromisoformat(resumed["next_run_at"])) > utcnow()


def test_schedule_can_trigger_a_workflow(client, db, sweep):
    wf = client.post("/api/v1/workflows", json={"name": "wf", "steps": [
        {"id": "a", "type": "echo", "payload": {"message": "x"}},
        {"id": "b", "type": "summarize", "depends_on": ["a"]},
    ]}).json()
    s = _schedule(client, {"name": "wf-hourly", "cron": "0 * * * *", "workflow_id": wf["workflow_id"]})
    assert s["target_kind"] == "workflow"

    sweep(seconds_ahead=3601)
    runs = client.get(f"/api/v1/workflows/{wf['workflow_id']}/runs").json()["items"]
    assert len(runs) == 1
    assert runs[0]["schedule_id"] == s["schedule_id"] and runs[0]["status"] == "SUCCEEDED"


def test_delete_and_ownership(client, other_client):
    s = _schedule(client)
    assert other_client.get(f"/api/v1/schedules/{s['schedule_id']}").status_code == 404
    assert other_client.get("/api/v1/schedules").json() == []

    wf = client.post("/api/v1/workflows", json={"name": "w", "steps": [{"id": "a", "type": "echo",
                                                                         "payload": {"message": "x"}}]}).json()
    stolen = {"name": "x", "cron": "* * * * *", "workflow_id": wf["workflow_id"]}
    assert other_client.post("/api/v1/schedules", json=stolen).status_code == 404

    assert client.delete(f"/api/v1/schedules/{s['schedule_id']}").status_code == 204
    assert client.get(f"/api/v1/schedules/{s['schedule_id']}").status_code == 404


def test_scheduled_jobs_count_is_bounded_by_ticks(client, db):
    s = _schedule(client, {**EVERY_MINUTE, "cron": "*/15 * * * *"})
    tick = as_utc(db.get(Schedule, uuid.UUID(s["schedule_id"])).next_run_at)
    for minutes in range(0, 60, 5):  # sweeper passes every 5 minutes for an hour
        fire_due_schedules(db, publish_job, tick + timedelta(minutes=minutes, seconds=1))
    count = db.scalar(select(func.count()).select_from(Job).where(Job.schedule_id == uuid.UUID(s["schedule_id"])))
    assert count == 4  # :00, :15, :30, :45
