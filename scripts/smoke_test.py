"""End-to-end smoke test against a running FlowGrid stack.

Unlike the pytest suite (eager Celery), this goes through the real path:
HTTP API -> PostgreSQL -> Redis -> Celery worker -> PostgreSQL.

    docker compose up -d --build   # or: make up
    python scripts/smoke_test.py [--base-url http://localhost:8000]

Uses only the standard library so it runs with any Python 3.11+.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

TERMINAL = {"SUCCEEDED", "FAILED"}


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            return res.status, json.loads(res.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def submit(base: str, job_type: str, payload: dict) -> str:
    status, body = call(base, "POST", "/api/v1/jobs", {"type": job_type, "payload": payload})
    assert status == 202, f"expected 202, got {status}: {body}"
    return body["job_id"]


def wait(base: str, job_id: str, timeout: float = 30) -> dict:
    deadline = time.monotonic() + timeout
    seen = []
    while time.monotonic() < deadline:
        _, job = call(base, "GET", f"/api/v1/jobs/{job_id}")
        if not seen or seen[-1] != job["status"]:
            seen.append(job["status"])
        if job["status"] in TERMINAL:
            job["_observed"] = seen
            return job
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s (states seen: {seen})")


def check(name: str, fn) -> bool:
    try:
        detail = fn()
    except AssertionError as exc:
        print(f"  FAIL  {name}: {exc}")
        return False
    print(f"  ok    {name}" + (f"  ({detail})" if detail else ""))
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    base = parser.parse_args().base_url.rstrip("/")

    print(f"FlowGrid smoke test against {base}")

    def health():
        status, body = call(base, "GET", "/health")
        assert status == 200 and body["status"] == "ok", body
        return "postgres + redis reachable"

    def echo_succeeds():
        job = wait(base, submit(base, "echo", {"message": "smoke"}))
        assert job["status"] == "SUCCEEDED", job
        assert job["result"] == {"echo": "smoke"}, job["result"]
        assert job["attempts"] == 1 and job["worker_id"], job
        return f"worker={job['worker_id']}, {job['duration_ms']} ms"

    def failure_is_recorded():
        job = wait(base, submit(base, "always_fail", {"reason": "smoke failure"}))
        assert job["status"] == "FAILED", job
        assert job["error"] == "JobHandlerError: smoke failure", job["error"]
        return job["error"]

    def lifecycle_is_observable():
        job = wait(base, submit(base, "sleep", {"seconds": 1.5}))
        assert job["status"] == "SUCCEEDED", job
        assert "RUNNING" in job["_observed"], job["_observed"]
        assert job["duration_ms"] >= 1500, job["duration_ms"]
        return " -> ".join(job["_observed"])

    def batch_processing():
        ids = list(range(1, 101)) + [-1, -2]
        job = wait(base, submit(base, "process_records", {"batch_id": "smoke", "record_ids": ids}))
        assert job["result"]["processed"] == 100 and job["result"]["failed"] == 2, job["result"]
        return f"processed={job['result']['processed']} failed={job['result']['failed']}"

    def invalid_payload_rejected():
        status, body = call(base, "POST", "/api/v1/jobs", {"type": "echo", "payload": {}})
        assert status == 422, (status, body)
        status, _ = call(base, "POST", "/api/v1/jobs", {"type": "shell", "payload": {}})
        assert status == 422
        return "422 for bad payload and unknown type"

    def many_jobs():
        n = 25
        start = time.monotonic()
        ids = [submit(base, "process_records", {"batch_id": f"b{i}", "record_ids": [1, 2, 3], "delay_ms": 50})
               for i in range(n)]
        jobs = [wait(base, i, timeout=60) for i in ids]
        elapsed = time.monotonic() - start
        assert all(j["status"] == "SUCCEEDED" for j in jobs)
        assert all(j["attempts"] == 1 for j in jobs), "a job ran more than once"
        return f"{n} jobs in {elapsed:.1f}s"

    checks = [
        ("health check", health),
        ("echo job succeeds", echo_succeeds),
        ("failing job is recorded as FAILED", failure_is_recorded),
        ("RUNNING state is observable", lifecycle_is_observable),
        ("process_records batch", batch_processing),
        ("invalid input rejected", invalid_payload_rejected),
        ("burst of jobs, each run exactly once", many_jobs),
    ]
    results = [check(name, fn) for name, fn in checks]
    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
