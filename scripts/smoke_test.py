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
import sys
import time
import uuid
from urllib.parse import urlsplit

from flowgrid_client import Client, create_api_key, run_checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    base = parser.parse_args().base_url

    c = Client(base, os.environ.get("FLOWGRID_API_KEY") or create_api_key("smoke-test"))
    print(f"FlowGrid smoke test against {base}")

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
        ("rate limiting", rate_limited),
    ])


if __name__ == "__main__":
    sys.exit(main())
