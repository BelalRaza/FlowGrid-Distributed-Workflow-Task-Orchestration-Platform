"""Phase 3: workflow DAGs — validation, execution, failure, recovery, cancel."""

from datetime import timedelta

from sqlalchemy import select

from app.db.models import Job, JobStatus, RunStatus, utcnow
from app.services import jobs as jobs_service
from app.services import workflows as workflow_service

DIAMOND = {
    "name": "nightly-import",
    "steps": [
        {"id": "extract", "type": "echo", "payload": {"message": "start"}},
        {"id": "batch_a", "type": "process_records", "depends_on": ["extract"],
         "payload": {"batch_id": "wf-a", "record_ids": [1, 2, 3]}},
        {"id": "batch_b", "type": "process_records", "depends_on": ["extract"],
         "payload": {"batch_id": "wf-b", "record_ids": [4, 5]}},
        {"id": "report", "type": "summarize", "depends_on": ["batch_a", "batch_b"], "payload": {"title": "nightly"}},
    ],
}


def _create(client, definition=DIAMOND) -> str:
    res = client.post("/api/v1/workflows", json=definition)
    assert res.status_code == 201, res.json()
    return res.json()["workflow_id"]


def _start(client, wf_id, **body) -> dict:
    res = client.post(f"/api/v1/workflows/{wf_id}/runs", json=body or None)
    assert res.status_code == 202, res.json()
    return res.json()


def _run(client, wf_id, run_id) -> dict:
    return client.get(f"/api/v1/workflows/{wf_id}/runs/{run_id}").json()


def _steps(run) -> dict[str, str]:
    return {s["step_id"]: s["status"] for s in run["steps"]}


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def _errors(client, steps):
    res = client.post("/api/v1/workflows", json={"name": "bad", "steps": steps})
    assert res.status_code == 422
    return [e["msg"] for e in res.json()["detail"]["errors"]]


def test_cycle_is_rejected(client):
    msgs = _errors(client, [
        {"id": "a", "type": "echo", "payload": {"message": "x"}, "depends_on": ["c"]},
        {"id": "b", "type": "echo", "payload": {"message": "x"}, "depends_on": ["a"]},
        {"id": "c", "type": "echo", "payload": {"message": "x"}, "depends_on": ["b"]},
        {"id": "ok", "type": "echo", "payload": {"message": "x"}},
    ])
    assert msgs == ["dependency cycle involving steps ['a', 'b', 'c']"]


def test_all_structural_errors_reported_at_once(client):
    res = client.post("/api/v1/workflows", json={"name": "bad", "steps": [
        {"id": "a", "type": "echo", "payload": {"message": "x"}},
        {"id": "a", "type": "echo", "payload": {"message": "x"}},
        {"id": "b", "type": "echo", "payload": {}, "depends_on": ["b", "ghost"]},
        {"id": "c", "type": "rm_rf", "payload": {}},
    ]})
    errors = res.json()["detail"]["errors"]
    by_loc = {tuple(e["loc"]): e["msg"] for e in errors}
    assert by_loc[("steps", 1, "id")] == "duplicate step id 'a'"
    assert by_loc[("steps", 2, "depends_on")] in {"a step cannot depend on itself", "unknown step 'ghost'"}
    assert ("steps", 2, "payload", "message") in by_loc  # payload validated per step
    assert "Unknown job type" in by_loc[("steps", 3, "type")]
    assert len([e for e in errors if e["loc"][:3] == ["steps", 2, "depends_on"]]) == 2


def test_step_id_format_and_size_limits(client):
    bad_id = client.post("/api/v1/workflows", json={"name": "x", "steps": [{"id": "has space", "type": "echo"}]})
    assert bad_id.status_code == 422
    too_many = [{"id": f"s{i}", "type": "echo", "payload": {"message": "x"}} for i in range(51)]
    assert client.post("/api/v1/workflows", json={"name": "x", "steps": too_many}).status_code == 422


def test_workflow_read_includes_levels(client):
    wf = client.get(f"/api/v1/workflows/{_create(client)}").json()
    assert wf["levels"] == {"extract": 0, "batch_a": 1, "batch_b": 1, "report": 2}
    assert wf["steps"][1]["payload"]["delay_ms"] == 0  # payloads normalised at definition time


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #


def test_diamond_runs_in_dependency_order_and_passes_results(client):
    wf_id = _create(client)
    run = _start(client, wf_id)
    run = _run(client, wf_id, run["run_id"])

    assert run["status"] == "SUCCEEDED"
    assert _steps(run) == {s: "SUCCEEDED" for s in ("extract", "batch_a", "batch_b", "report")}
    report = next(s for s in run["steps"] if s["step_id"] == "report")
    # The fan-in step received both parents' results and summed them.
    assert report["result"]["upstream_steps"] == ["batch_a", "batch_b"]
    assert report["result"]["totals"]["processed"] == 5

    # Dependency order is visible in the timestamps.
    started = {s["step_id"]: s["started_at"] for s in run["steps"]}
    finished = {s["step_id"]: s["finished_at"] for s in run["steps"]}
    assert finished["extract"] <= started["batch_a"] and finished["extract"] <= started["batch_b"]
    assert max(finished["batch_a"], finished["batch_b"]) <= started["report"]


def test_steps_are_ordinary_jobs(client):
    wf_id = _create(client)
    run_id = _start(client, wf_id)["run_id"]
    jobs = client.get("/api/v1/jobs", params={"type": "summarize"}).json()["items"]
    assert jobs[0]["workflow_run_id"] == run_id and jobs[0]["step_id"] == "report"


def test_failed_step_fails_run_and_cancels_unstarted_steps(client):
    wf_id = _create(client, {"name": "fails", "steps": [
        {"id": "bad", "type": "always_fail", "payload": {"reason": "bad row"}},
        {"id": "good", "type": "echo", "payload": {"message": "x"}},
        {"id": "after", "type": "echo", "payload": {"message": "x"}, "depends_on": ["bad", "good"]},
    ]})
    run = _run(client, wf_id, _start(client, wf_id)["run_id"])
    assert run["status"] == "FAILED"
    assert run["error"] == "step 'bad' FAILED: PermanentJobError: bad row"
    assert _steps(run) == {"bad": "FAILED", "good": "CANCELLED", "after": "CANCELLED"}


def test_transient_step_failure_retries_inside_the_run(client, sweep):
    wf_id = _create(client, {"name": "flaky", "steps": [
        {"id": "first", "type": "flaky", "payload": {"fail_times": 1}},
        {"id": "second", "type": "echo", "payload": {"message": "x"}, "depends_on": ["first"]},
    ]})
    run_id = _start(client, wf_id)["run_id"]
    run = _run(client, wf_id, run_id)
    assert run["status"] == "RUNNING"
    assert _steps(run) == {"first": "RETRY_SCHEDULED", "second": "BLOCKED"}

    sweep()  # backoff elapsed: retry succeeds, which unblocks "second"
    run = _run(client, wf_id, run_id)
    assert run["status"] == "SUCCEEDED" and _steps(run) == {"first": "SUCCEEDED", "second": "SUCCEEDED"}


def test_dead_lettered_step_fails_the_run(client, sweep):
    wf_id = _create(client, {"name": "dlq", "steps": [
        {"id": "only", "type": "always_fail", "payload": {"transient": True}, "max_attempts": 2},
    ]})
    run_id = _start(client, wf_id)["run_id"]
    sweep()
    run = _run(client, wf_id, run_id)
    assert run["status"] == "FAILED" and _steps(run) == {"only": "DEAD_LETTER"}


# --------------------------------------------------------------------------- #
# Concurrency and crash safety
# --------------------------------------------------------------------------- #


def _definition_run(db, api_key, publish=lambda *_: None):
    wf = workflow_service.create_workflow(db, api_key_id=api_key[0].id, name="d", steps=DIAMOND["steps"])
    return wf, workflow_service.create_run(db, wf, publish)


def _finish(db, run_id, step_id):
    job = db.scalar(select(Job).where(Job.workflow_run_id == run_id, Job.step_id == step_id))
    if job.status == JobStatus.BLOCKED.value:
        jobs_service._transition(db, job.id, from_statuses=[JobStatus.BLOCKED], to_status=JobStatus.QUEUED)
    claimed = jobs_service.claim_job(db, job.id, "w")
    assert jobs_service.record_success(db, job.id, claimed.attempts, {"processed": 1})


def test_crash_between_step_success_and_advance_is_repaired_by_sweeper(db, api_key, sweep):
    """The worker committed the step's success, then died before advance_run."""
    _, run = _definition_run(db, api_key)
    for step in ("extract", "batch_a", "batch_b"):
        _finish(db, run.id, step)  # successes recorded, nobody advanced the run

    report = db.scalar(select(Job).where(Job.workflow_run_id == run.id, Job.step_id == "report"))
    assert report.status == JobStatus.BLOCKED.value  # stuck: the wake-up was lost

    result = sweep(seconds_ahead=0)
    assert result.runs_advanced == 1
    db.refresh(report)
    assert report.status == JobStatus.SUCCEEDED.value  # unblocked, published, run (eager)
    db.refresh(run)
    assert run.status == RunStatus.SUCCEEDED.value


def test_concurrent_advances_unblock_a_step_exactly_once(db, api_key):
    published = []
    record = lambda job_id, timeout: published.append(job_id)  # noqa: E731
    _, run = _definition_run(db, api_key, publish=record)
    for step in ("extract", "batch_a", "batch_b"):
        _finish(db, run.id, step)
    published.clear()

    # Both parents' workers call advance_run for the same state.
    workflow_service.advance_run(db, run.id, record)
    workflow_service.advance_run(db, run.id, record)

    report = db.scalar(select(Job).where(Job.workflow_run_id == run.id, Job.step_id == "report"))
    assert published == [report.id]  # published once, not twice


def test_cancel_run_cancels_unstarted_steps(app, client):
    from app.api.deps import get_publisher

    app.dependency_overrides[get_publisher] = lambda: (lambda *_: None)  # nothing consumes the queue
    wf_id = _create(client)
    run_id = _start(client, wf_id)["run_id"]

    res = client.post(f"/api/v1/workflows/{wf_id}/runs/{run_id}/cancel")
    assert res.status_code == 200
    run = res.json()
    assert run["status"] == "CANCELLED"
    assert set(_steps(run).values()) == {"CANCELLED"}
    assert client.post(f"/api/v1/workflows/{wf_id}/runs/{run_id}/cancel").status_code == 409


def test_cancelled_step_is_never_executed_even_if_its_message_arrives(db, api_key):
    from app.services.executor import run_job

    _, run = _definition_run(db, api_key)
    workflow_service.cancel_run(db, run)
    extract = db.scalar(select(Job).where(Job.workflow_run_id == run.id, Job.step_id == "extract"))
    assert run_job(extract.id, "late-worker") is None  # the queued message is a no-op


# --------------------------------------------------------------------------- #
# Scheduling and access control
# --------------------------------------------------------------------------- #


def test_run_with_run_at_starts_later(client, sweep):
    wf_id = _create(client)
    run = _start(client, wf_id, run_at=(utcnow() + timedelta(minutes=10)).isoformat())
    assert run["status"] == "SCHEDULED" and run["steps"] == []

    assert sweep(seconds_ahead=60).runs_started == 0
    assert sweep(seconds_ahead=11 * 60).runs_started == 1
    assert _run(client, wf_id, run["run_id"])["status"] == "SUCCEEDED"


def test_run_listing(client):
    wf_id = _create(client)
    _start(client, wf_id)
    _start(client, wf_id)
    assert client.get(f"/api/v1/workflows/{wf_id}/runs").json()["total"] == 2
    all_runs = client.get("/api/v1/workflow-runs", params={"status": "SUCCEEDED"}).json()
    assert all_runs["total"] == 2 and all_runs["items"][0]["workflow_name"] == "nightly-import"


def test_other_clients_cannot_see_or_run_workflows(client, other_client):
    wf_id = _create(client)
    run_id = _start(client, wf_id)["run_id"]
    assert other_client.get(f"/api/v1/workflows/{wf_id}").status_code == 404
    assert other_client.post(f"/api/v1/workflows/{wf_id}/runs").status_code == 404
    assert other_client.get(f"/api/v1/workflows/{wf_id}/runs/{run_id}").status_code == 404
    assert other_client.get("/api/v1/workflow-runs").json()["total"] == 0


def test_workflow_steps_cannot_be_replayed_individually(client):
    wf_id = _create(client, {"name": "f", "steps": [{"id": "bad", "type": "always_fail", "payload": {}}]})
    run = _run(client, wf_id, _start(client, wf_id)["run_id"])
    res = client.post(f"/api/v1/jobs/{run['steps'][0]['job_id']}/replay")
    assert res.status_code == 409 and "new workflow run" in res.json()["detail"]
