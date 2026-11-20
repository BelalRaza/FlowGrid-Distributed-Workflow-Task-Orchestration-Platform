import uuid

from sqlalchemy import func, select

from app.db.models import Job


def _count_jobs(db) -> int:
    return db.scalar(select(func.count()).select_from(Job))


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #


def test_submit_job_runs_to_success(client):
    res = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "hi"}})
    assert res.status_code == 202
    job_id = res.json()["job_id"]

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "SUCCEEDED"
    assert job["result"] == {"echo": "hi"}
    assert job["error"] is None
    assert job["attempts"] == 1
    assert job["max_attempts"] == 3
    assert job["timeout_seconds"] == 10  # echo's default
    assert job["worker_id"]
    assert job["started_at"] and job["finished_at"]
    assert job["duration_ms"] is not None and job["duration_ms"] >= 0


def test_submit_with_custom_attempts_and_timeout(client):
    res = client.post(
        "/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"}, "max_attempts": 5, "timeout_seconds": 2}
    )
    body = res.json()
    assert body["max_attempts"] == 5 and body["timeout_seconds"] == 2


def test_process_records_reports_per_record_failures(client):
    res = client.post(
        "/api/v1/jobs",
        json={"type": "process_records", "payload": {"batch_id": "b1", "record_ids": [1, 2, 3, -4, 0]}},
    )
    body = res.json()
    assert body["status"] == "SUCCEEDED"
    assert body["result"]["processed"] == 3
    assert body["result"]["failed"] == 2
    assert {f["record_id"] for f in body["result"]["failures"]} == {-4, 0}


def test_payload_defaults_are_persisted(client):
    res = client.post(
        "/api/v1/jobs",
        json={"type": "process_records", "payload": {"batch_id": "b1", "record_ids": [1]}},
    )
    assert res.json()["payload"] == {
        "batch_id": "b1",
        "record_ids": [1],
        "delay_ms": 0,
        "fail_first_attempt_after": None,
    }


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def test_permanent_failure_is_not_retried(client):
    res = client.post("/api/v1/jobs", json={"type": "always_fail", "payload": {"reason": "boom"}})
    assert res.status_code == 202

    job = client.get(f"/api/v1/jobs/{res.json()['job_id']}").json()
    assert job["status"] == "FAILED"
    assert job["error"] == "PermanentJobError: boom"
    assert job["result"] is None
    assert job["attempts"] == 1
    assert job["next_attempt_at"] is None
    assert job["finished_at"] is not None


def test_unknown_job_type_is_rejected_without_creating_a_job(client, db):
    res = client.post("/api/v1/jobs", json={"type": "rm_rf", "payload": {}})
    assert res.status_code == 422
    assert "echo" in res.json()["detail"]["supported_types"]
    assert _count_jobs(db) == 0


def test_invalid_payload_is_rejected_without_creating_a_job(client, db):
    res = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"msg": "typo"}})
    assert res.status_code == 422
    locs = {tuple(e["loc"]) for e in res.json()["detail"]["errors"]}
    assert ("message",) in locs  # required field missing
    assert ("msg",) in locs  # unknown field rejected (extra="forbid")
    assert _count_jobs(db) == 0


def test_out_of_range_values_are_rejected(client):
    assert client.post("/api/v1/jobs", json={"type": "sleep", "payload": {"seconds": 9999}}).status_code == 422
    assert client.post(
        "/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"}, "max_attempts": 0}
    ).status_code == 422
    assert client.post(
        "/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"}, "timeout_seconds": 100000}
    ).status_code == 422


def test_unknown_top_level_field_is_rejected(client):
    res = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "x"}, "priority": 1})
    assert res.status_code == 422


def test_get_unknown_job_returns_404(client):
    assert client.get(f"/api/v1/jobs/{uuid.uuid4()}").status_code == 404


def test_get_job_with_malformed_id_returns_422(client):
    assert client.get("/api/v1/jobs/not-a-uuid").status_code == 422


# --------------------------------------------------------------------------- #
# Listing, stats, metadata
# --------------------------------------------------------------------------- #


def test_list_jobs_filters_and_paginates(client):
    for i in range(3):
        client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": str(i)}})
    client.post("/api/v1/jobs", json={"type": "always_fail", "payload": {}})

    page = client.get("/api/v1/jobs", params={"limit": 2}).json()
    assert page["total"] == 4 and len(page["items"]) == 2

    failed = client.get("/api/v1/jobs", params={"status": "FAILED"}).json()
    assert failed["total"] == 1 and failed["items"][0]["type"] == "always_fail"

    echo = client.get("/api/v1/jobs", params={"type": "echo", "offset": 2}).json()
    assert echo["total"] == 3 and len(echo["items"]) == 1


def test_list_jobs_rejects_bad_query(client):
    assert client.get("/api/v1/jobs", params={"status": "DONE"}).status_code == 422
    assert client.get("/api/v1/jobs", params={"limit": 1000}).status_code == 422


def test_stats_counts_by_status(client):
    client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "a"}})
    client.post("/api/v1/jobs", json={"type": "always_fail", "payload": {}})
    client.post("/api/v1/jobs", json={"type": "flaky", "payload": {"fail_times": 1}})

    stats = client.get("/api/v1/jobs/stats").json()
    assert stats["total"] == 3
    assert stats["by_status"]["SUCCEEDED"] == 1
    assert stats["by_status"]["FAILED"] == 1
    assert stats["by_status"]["RETRY_SCHEDULED"] == 1
    assert stats["by_status"]["DEAD_LETTER"] == 0


def test_job_types_lists_allow_list_with_schemas(client):
    types = {t["name"]: t for t in client.get("/api/v1/job-types").json()}
    assert {"echo", "sleep", "flaky", "process_records", "always_fail"} <= set(types)
    assert "message" in types["echo"]["payload_schema"]["properties"]
    assert types["process_records"]["default_timeout_seconds"] == 300


def test_whoami(client):
    me = client.get("/api/v1/me").json()
    assert me["name"] == "test-client" and me["prefix"].startswith("fg_")


def test_health_reports_dependencies(anon_client, monkeypatch):
    from app.api import health

    monkeypatch.setattr(health, "check_redis", lambda: True)
    assert anon_client.get("/health").json() == {"status": "ok", "database": "ok", "redis": "ok"}

    monkeypatch.setattr(health, "check_redis", lambda: False)
    res = anon_client.get("/health")
    assert res.status_code == 503 and res.json()["redis"] == "unavailable"


def test_dashboard_is_served_without_auth(anon_client):
    res = anon_client.get("/")
    assert res.status_code == 200
    assert "FlowGrid" in res.text
