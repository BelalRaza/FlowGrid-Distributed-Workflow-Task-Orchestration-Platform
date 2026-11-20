"""End-to-end smoke test against a running FlowGrid stack.

Unlike the pytest suite (eager Celery, explicit sweeper calls), this goes
through the real path: HTTP API -> PostgreSQL -> Redis -> Celery worker ->
PostgreSQL, with the real sweeper process driving retries.

    make up
    make smoke            # creates throwaway API keys via the admin CLI

Set FLOWGRID_API_KEY to use an existing key instead.
"""

from __future__ import annotations

import argparse
import http.client
import os
import urllib.request
import sys
import time
import uuid
from urllib.parse import urlsplit

from flowgrid_client import Client, SSEReader, create_api_key, run_checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    base = parser.parse_args().base_url

    c = Client(base, os.environ.get("FLOWGRID_API_KEY") or create_api_key("smoke-test"))
    print(f"FlowGrid smoke test against {base}")

    # Created first and checked last, so waiting for the next minute boundary
    # overlaps with the other checks instead of adding a minute of idle time.
    cron_marker = f"cron-{uuid.uuid4().hex[:8]}"
    status, cron_schedule, _ = c.call("POST", "/api/v1/schedules", {
        "name": cron_marker, "cron": "* * * * *", "job": {"type": "echo", "payload": {"message": cron_marker}}})
    assert status == 201, cron_schedule

    def health():
        status, body, _ = c.call("GET", "/health", auth=False)
        assert status == 200 and body["status"] == "ok", body
        return "postgres + redis reachable"

    def auth_required():
        assert c.call("GET", "/api/v1/jobs", auth=False)[0] == 401
        assert c.call("GET", "/api/v1/jobs", headers={"Authorization": "Bearer fg_nope"}, auth=False)[0] == 401
        return "401 without / with a bad key"

    def echo_succeeds():
        job = c.wait(c.submit("echo", {"message": "smoke"})["job_id"])
        assert job["status"] == "SUCCEEDED" and job["result"] == {"echo": "smoke"}, job
        assert job["attempts"] == 1 and job["worker_id"], job
        return f"worker={job['worker_id']}"

    def permanent_failure_not_retried():
        job = c.wait(c.submit("always_fail", {"reason": "bad input"})["job_id"])
        assert job["status"] == "FAILED" and job["attempts"] == 1, job
        return job["error"]

    def transient_failures_retried():
        job = c.wait(c.submit("flaky", {"fail_times": 2})["job_id"], timeout=30)
        assert job["status"] == "SUCCEEDED" and job["attempts"] == 3, job
        statuses = [a["status"] for a in c.attempts(job["job_id"])]
        assert statuses == ["FAILED", "FAILED", "SUCCEEDED"], statuses
        return " -> ".join(job["_observed"])

    def dead_letter():
        job = c.wait(
            c.submit("always_fail", {"reason": "upstream 503", "transient": True}, max_attempts=2)["job_id"]
        )
        assert job["status"] == "DEAD_LETTER" and job["attempts"] == 2, job
        replay = c.call("POST", f"/api/v1/jobs/{job['job_id']}/replay")[1]
        assert replay["replay_of"] == job["job_id"], replay
        return "replayable as " + replay["job_id"][:8]

    def idempotency():
        key = {"Idempotency-Key": f"smoke-{uuid.uuid4()}"}
        a = c.submit("echo", {"message": "once"}, headers=key)
        b = c.submit("echo", {"message": "once"}, headers=key, expect=200)
        assert a["job_id"] == b["job_id"]
        c.submit("echo", {"message": "different"}, headers=key, expect=409)
        return "same key -> same job; different body -> 409"

    def idempotent_side_effects():
        batch = f"smoke-{uuid.uuid4().hex[:8]}"
        job = c.wait(
            c.submit(
                "process_records",
                {"batch_id": batch, "record_ids": list(range(1, 201)), "fail_first_attempt_after": 120},
            )["job_id"]
        )
        r = job["result"]
        assert job["status"] == "SUCCEEDED" and job["attempts"] == 2, job
        assert r["notifications_sent"] == 80 and r["notifications_deduplicated"] == 120, r
        return "crash after 120/200, retry sent only the missing 80 notifications"

    def invalid_input_rejected():
        assert c.call("POST", "/api/v1/jobs", {"type": "echo", "payload": {}})[0] == 422
        assert c.call("POST", "/api/v1/jobs", {"type": "shell", "payload": {}})[0] == 422
        # Declare a 2 MB body but send only the headers. The server must answer
        # 413 without waiting for (or reading) the body. Uploading a real 2 MB
        # body races the server closing the socket and fails with EPIPE.
        url = urlsplit(base)
        conn = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=5)
        conn.putrequest("POST", "/api/v1/jobs")
        conn.putheader("Authorization", f"Bearer {c.api_key}")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(2_000_000))
        conn.endheaders()
        status = conn.getresponse().status
        conn.close()
        assert status == 413, status
        return "422 bad payload / unknown type, 413 oversized body (before reading it)"

    def many_jobs():
        n = 25
        start = time.monotonic()
        ids = [c.submit("process_records", {"batch_id": f"burst-{uuid.uuid4().hex[:6]}", "record_ids": [1, 2, 3],
                                            "delay_ms": 50})["job_id"] for _ in range(n)]
        jobs = [c.wait(i, timeout=60) for i in ids]
        elapsed = time.monotonic() - start
        assert all(j["status"] == "SUCCEEDED" and j["attempts"] == 1 for j in jobs)
        return f"{n} jobs in {elapsed:.1f}s, each run exactly once"

    def rate_limited():
        # A separate key so throttling it does not affect the checks above.
        burst = Client(base, create_api_key("smoke-rate-limit"))
        responses = [burst.call("GET", "/api/v1/jobs/stats") for _ in range(150)]
        codes = [r[0] for r in responses]
        limited = codes.count(429)
        assert codes[0] == 200 and limited > 0, codes
        first_429_headers = next(r[2] for r in responses if r[0] == 429)
        assert first_429_headers.get("retry-after"), first_429_headers
        return f"{codes.count(200)} allowed, {limited} throttled (429), Retry-After={first_429_headers['retry-after']}s"

    def workflow_diamond():
        wf = c.call("POST", "/api/v1/workflows", {"name": "smoke-diamond", "steps": [
            {"id": "extract", "type": "echo", "payload": {"message": "go"}},
            {"id": "a", "type": "process_records", "depends_on": ["extract"],
             "payload": {"batch_id": f"wf-{uuid.uuid4().hex[:6]}", "record_ids": list(range(1, 51))}},
            {"id": "b", "type": "process_records", "depends_on": ["extract"],
             "payload": {"batch_id": f"wf-{uuid.uuid4().hex[:6]}", "record_ids": list(range(1, 31))}},
            {"id": "report", "type": "summarize", "depends_on": ["a", "b"]},
        ]})[1]
        run = c.call("POST", f"/api/v1/workflows/{wf['workflow_id']}/runs")[1]
        run = c.wait_run(wf["workflow_id"], run["run_id"])
        assert run["status"] == "SUCCEEDED", run
        report = next(s for s in run["steps"] if s["step_id"] == "report")
        assert report["result"]["totals"]["processed"] == 80, report["result"]
        return f"4 steps, fan-out/fan-in in {run['duration_ms']} ms, report.totals.processed=80"

    def workflow_fail_fast():
        wf = c.call("POST", "/api/v1/workflows", {"name": "smoke-fail", "steps": [
            {"id": "bad", "type": "always_fail", "payload": {"reason": "bad input"}},
            {"id": "next", "type": "echo", "payload": {"message": "x"}, "depends_on": ["bad"]},
        ]})[1]
        run = c.call("POST", f"/api/v1/workflows/{wf['workflow_id']}/runs")[1]
        run = c.wait_run(wf["workflow_id"], run["run_id"])
        steps = {s["step_id"]: s["status"] for s in run["steps"]}
        assert run["status"] == "FAILED" and steps == {"bad": "FAILED", "next": "CANCELLED"}, run
        return run["error"]

    def invalid_dag():
        status, body, _ = c.call("POST", "/api/v1/workflows", {"name": "cyclic", "steps": [
            {"id": "a", "type": "echo", "payload": {"message": "x"}, "depends_on": ["b"]},
            {"id": "b", "type": "echo", "payload": {"message": "x"}, "depends_on": ["a"]}]})
        assert status == 422, body
        return body["detail"]["errors"][0]["msg"]

    def delayed_job():
        job = c.submit("echo", {"message": "later"}, delay_seconds=3)
        assert job["status"] == "SCHEDULED", job
        start = time.monotonic()
        done = c.wait(job["job_id"], timeout=20)
        assert done["status"] == "SUCCEEDED", done
        return f"SCHEDULED -> SUCCEEDED after {time.monotonic() - start:.1f}s (delay 3s)"

    def batch():
        status, body, _ = c.call("POST", "/api/v1/jobs/batch",
                                 {"jobs": [{"type": "echo", "payload": {"message": str(i)}} for i in range(200)]})
        assert status == 202 and body["count"] == 200, body
        last = c.wait(body["job_ids"][-1], timeout=60)
        assert last["status"] == "SUCCEEDED"
        return "200 jobs in one request (one transaction, one rate-limit token)"

    def live_events():
        reader = SSEReader(base, c.api_key)
        reader.start()
        assert reader.ready.wait(5), "SSE stream did not open"
        t0 = time.monotonic()
        job_id = c.submit("sleep", {"seconds": 0.5})["job_id"]
        deadline = time.monotonic() + 15
        seen = []
        while time.monotonic() < deadline:
            seen = [e for _, e in reader.events if e.get("id") == job_id]
            if seen and seen[-1]["status"] == "SUCCEEDED":
                break
            time.sleep(0.05)
        reader.close()
        statuses = [e["status"] for e in seen]
        assert statuses[-1:] == ["SUCCEEDED"] and "RUNNING" in statuses, statuses
        first_at = next(t for t, e in reader.events if e.get("id") == job_id)
        return f"{' -> '.join(statuses)}; first event {(first_at - t0) * 1000:.0f} ms after submit"

    def metrics():
        assert c.call("GET", "/health/live", auth=False)[0] == 200
        req = urllib.request.Request(base + "/metrics")
        with urllib.request.urlopen(req, timeout=10) as res:
            text = res.read().decode()
        assert "flowgrid_database_up 1.0" in text
        assert 'flowgrid_jobs{status="SUCCEEDED"}' in text
        assert 'route="/api/v1/jobs/{job_id}"' in text
        series = sum(1 for line in text.splitlines() if line.startswith("flowgrid_"))
        return f"{series} flowgrid_* series, routes labelled by template"

    def cron_fires():
        deadline = time.monotonic() + 75
        while time.monotonic() < deadline:
            items = c.call("GET", "/api/v1/jobs?limit=100")[1]["items"]
            fired = [j for j in items if j.get("schedule_id") == cron_schedule["schedule_id"]]
            if fired and fired[0]["status"] == "SUCCEEDED":
                c.call("DELETE", f"/api/v1/schedules/{cron_schedule['schedule_id']}")
                return f"fired for tick {fired[0]['scheduled_for']} ({len(fired)} job)"
            time.sleep(1)
        raise AssertionError("schedule '* * * * *' did not fire within 75s")

    return run_checks([
        ("health check", health),
        ("authentication required", auth_required),
        ("echo job succeeds", echo_succeeds),
        ("permanent failure is not retried", permanent_failure_not_retried),
        ("transient failures are retried with backoff", transient_failures_retried),
        ("exhausted retries -> DEAD_LETTER -> replay", dead_letter),
        ("idempotency keys", idempotency),
        ("crash mid-batch: retry has no duplicate side effects", idempotent_side_effects),
        ("invalid input rejected", invalid_input_rejected),
        ("burst of jobs", many_jobs),
        ("workflow: diamond DAG with fan-in results", workflow_diamond),
        ("workflow: failed step cancels the rest", workflow_fail_fast),
        ("workflow: cyclic DAG rejected", invalid_dag),
        ("delayed job (delay_seconds)", delayed_job),
        ("batch submit", batch),
        ("live events over SSE", live_events),
        ("prometheus metrics", metrics),
        ("cron schedule fires", cron_fires),
        ("rate limiting", rate_limited),
    ])


if __name__ == "__main__":
    sys.exit(main())
