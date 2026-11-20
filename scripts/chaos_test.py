"""Failure-injection tests against the running Docker stack.

Each scenario breaks something for real (kills a container, stops Redis,
forces a timeout) and asserts the system recovers to the correct state.

    make up
    make chaos

Takes about three minutes: lease expiry and message-loss recovery are
deliberately not instant, and the multi-sweeper check spans two cron ticks.
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

    # Three sweepers race to fire the same every-minute schedule for the whole
    # run; checked at the end (exactly one job per tick).
    compose("up", "-d", "--no-recreate", "--scale", "sweeper=3", "sweeper")
    race_marker = f"race-{uuid.uuid4().hex[:8]}"
    race = c.call("POST", "/api/v1/schedules", {
        "name": race_marker, "cron": "* * * * *", "job": {"type": "echo", "payload": {"message": race_marker}}})[1]

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

    def broker_loses_queued_messages():
        compose("stop", "worker")
        try:
            ids = [c.submit("echo", {"message": f"lost-{i}"})["job_id"] for i in range(5)]
            assert all(c.job(i)["status"] == "QUEUED" for i in ids)
            compose("exec", "-T", "redis", "redis-cli", "FLUSHALL")  # the messages are gone
            assert compose("exec", "-T", "redis", "redis-cli", "LLEN", "flowgrid.jobs").strip() == "0"
        finally:
            compose("start", "worker")
        lost_at = time.monotonic()
        jobs = [c.wait(i, timeout=120) for i in ids]
        assert all(j["status"] == "SUCCEEDED" and j["attempts"] == 1 for j in jobs), jobs
        return f"5 QUEUED messages wiped by FLUSHALL; re-published by the sweeper, all done {time.monotonic() - lost_at:.0f}s later"

    def worker_killed_mid_workflow():
        wf = c.call("POST", "/api/v1/workflows", {"name": "chaos-wf", "steps": [
            {"id": "slow", "type": "sleep", "payload": {"seconds": 5}, "timeout_seconds": 7},
            {"id": "after", "type": "summarize", "depends_on": ["slow"]},
        ]})[1]
        run = c.call("POST", f"/api/v1/workflows/{wf['workflow_id']}/runs")[1]
        slow_job = next(s["job_id"] for s in run["steps"] if s["step_id"] == "slow")
        c.wait(slow_job, until={"RUNNING"}, timeout=15)
        compose("kill", "-s", "SIGKILL", "worker")
        compose("start", "worker")
        run = c.wait_run(wf["workflow_id"], run["run_id"], timeout=90)
        assert run["status"] == "SUCCEEDED", run
        attempts = [a["status"] for a in c.attempts(slow_job)]
        assert attempts == ["LOST", "SUCCEEDED"], attempts
        return f"step 'slow' attempts {attempts}; dependent step ran after recovery; run SUCCEEDED"

    def concurrent_sweepers_fire_each_tick_once():
        # The schedule has been live since the start; make sure it spans 2+ ticks.
        deadline = time.monotonic() + 130
        while time.monotonic() < deadline:
            ticks = psql(
                "SELECT scheduled_for, count(*) FROM jobs "
                f"WHERE schedule_id = '{race['schedule_id']}' GROUP BY scheduled_for ORDER BY 1"
            ).splitlines()
            if len(ticks) >= 2:
                break
            time.sleep(2)
        c.call("DELETE", f"/api/v1/schedules/{race['schedule_id']}")
        compose("up", "-d", "--no-recreate", "--scale", "sweeper=1", "sweeper")
        counts = [int(t.split("|")[1]) for t in ticks]
        assert len(counts) >= 2 and all(n == 1 for n in counts), ticks
        return f"3 sweepers, {len(counts)} ticks, jobs per tick = {counts}"

    return run_checks([
        ("worker SIGKILLed mid-job -> lease recovery", worker_killed_mid_job),
        ("timeout on every attempt -> DEAD_LETTER", timeout_then_dead_letter),
        ("Redis outage -> outbox delivers later", redis_outage),
        ("crash mid-batch -> no duplicate side effects", duplicate_side_effects_under_crash),
        ("duplicate message delivery -> runs once", duplicate_message_delivery),
        ("Redis FLUSHALL loses queued messages -> re-published", broker_loses_queued_messages),
        ("worker SIGKILLed mid-workflow -> run completes", worker_killed_mid_workflow),
        ("3 concurrent sweepers -> each cron tick fires once", concurrent_sweepers_fire_each_tick_once),
    ])


if __name__ == "__main__":
    sys.exit(main())
