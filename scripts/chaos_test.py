"""Failure-injection tests against the running Docker stack.

Each scenario breaks something for real (kills a container, stops Redis,
forces a timeout) and asserts the system recovers to the correct state.

    make up
    make chaos

Takes about a minute: lease expiry is deliberately not instant.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from flowgrid_client import Client, compose, create_api_key, psql, run_checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    base = parser.parse_args().base_url
    c = Client(base, create_api_key("chaos-test"))
    print(f"FlowGrid chaos test against {base}\n")

    def worker_killed_mid_job():
        job = c.submit("sleep", {"seconds": 6}, timeout_seconds=8)
        c.wait(job["job_id"], until={"RUNNING"}, timeout=15)
        compose("kill", "-s", "SIGKILL", "worker")  # no graceful shutdown, no ack
        killed_at = time.monotonic()
        compose("start", "worker")

        done = c.wait(job["job_id"], timeout=60)
        attempts = c.attempts(job["job_id"])
        statuses = [a["status"] for a in attempts]
        assert done["status"] == "SUCCEEDED", done
        assert statuses == ["LOST", "SUCCEEDED"], statuses
        assert attempts[0]["error_type"] == "WorkerLostError", attempts[0]
        return (
            f"lease expired -> {' -> '.join(done['_observed'])}; attempts {statuses}; "
            f"recovered {time.monotonic() - killed_at:.0f}s after kill"
        )

    def timeout_then_dead_letter():
        job = c.submit("sleep", {"seconds": 30}, timeout_seconds=2, max_attempts=2)
        done = c.wait(job["job_id"], timeout=40)
        statuses = [a["status"] for a in c.attempts(job["job_id"])]
        assert done["status"] == "DEAD_LETTER", done
        assert statuses == ["TIMED_OUT", "TIMED_OUT"], statuses
        return f"{done['error']}; attempts {statuses}"

    def redis_outage():
        compose("stop", "redis")
        try:
            status, health, _ = c.call("GET", "/health", auth=False)
            assert status == 503 and health["redis"] == "unavailable", health
            t0 = time.monotonic()
            job = c.submit("echo", {"message": "written during outage"})  # still 202
            submit_ms = (time.monotonic() - t0) * 1000
            assert job["status"] == "PENDING", job
            time.sleep(2)
            assert c.job(job["job_id"])["status"] == "PENDING"
        finally:
            compose("start", "redis")
        done = c.wait(job["job_id"], timeout=60)
        assert done["status"] == "SUCCEEDED" and done["result"] == {"echo": "written during outage"}, done
        return f"accepted as PENDING in {submit_ms:.0f} ms while Redis was down, delivered by sweeper after restart"

    def duplicate_side_effects_under_crash():
        batch = f"chaos-{uuid.uuid4().hex[:8]}"
        job = c.submit(
            "process_records", {"batch_id": batch, "record_ids": list(range(1, 501)), "fail_first_attempt_after": 250}
        )
        done = c.wait(job["job_id"], timeout=30)
        assert done["status"] == "SUCCEEDED" and done["attempts"] == 2, done
        rows = int(psql(f"SELECT count(*) FROM processed_records WHERE batch_id = '{batch}'"))
        notes = int(psql(f"SELECT count(*) FROM notifications WHERE dedupe_key LIKE 'record-processed:{batch}:%'"))
        assert rows == 500 and notes == 500, (rows, notes)
        return f"500 records, 2 attempts -> {rows} stored rows, {notes} notifications (no duplicates)"

    def duplicate_message_delivery():
        # Publish the same job ID to the broker twice (what acks_late redelivery
        # does after a crash). The guarded claim must run it exactly once.
        job = c.wait(c.submit("echo", {"message": "dup"})["job_id"])
        compose(
            "exec", "-T", "api", "python", "-c",
            f"from app.tasks.worker import publish_job; import uuid; "
            f"[publish_job(uuid.UUID('{job['job_id']}'), 10) for _ in range(3)]",
        )
        time.sleep(2)
        after = c.job(job["job_id"])
        assert after["attempts"] == 1 and after["status"] == "SUCCEEDED", after
        return "3 duplicate messages delivered, attempts still 1"

    return run_checks([
        ("worker SIGKILLed mid-job -> lease recovery", worker_killed_mid_job),
        ("timeout on every attempt -> DEAD_LETTER", timeout_then_dead_letter),
        ("Redis outage -> outbox delivers later", redis_outage),
        ("crash mid-batch -> no duplicate side effects", duplicate_side_effects_under_crash),
        ("duplicate message delivery -> runs once", duplicate_message_delivery),
    ])


if __name__ == "__main__":
    sys.exit(main())
