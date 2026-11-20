"""Phase 3: live events (SSE), metrics, batch submission, broker message loss."""

import asyncio
import json
import os
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import func, select

from app.api.deps import get_publisher
from app.api.events import get_event_bus
from app.core.config import get_settings
from app.db.models import Job
from app.services.events import EventBus

# --------------------------------------------------------------------------- #
# Event bus
# --------------------------------------------------------------------------- #


def test_bus_delivers_only_to_the_owning_api_key():
    async def scenario():
        bus = EventBus()
        async with bus.subscribe("key-a") as a, bus.subscribe("key-b") as b:
            bus.publish({"kind": "job", "api_key_id": "key-a", "id": "1"})
            assert (await a.get())["id"] == "1"
            assert b.empty()
            bus.broadcast({"kind": "resync"})
            assert (await a.get())["kind"] == "resync" and (await b.get())["kind"] == "resync"
        assert bus.subscriber_count == 0

    asyncio.run(scenario())


def test_slow_consumer_gets_resync_instead_of_blocking():
    async def scenario():
        bus = EventBus(queue_size=3)
        async with bus.subscribe("k") as q:
            for i in range(10):
                bus.publish({"kind": "job", "api_key_id": "k", "id": i})
            drained = [q.get_nowait() for _ in range(q.qsize())]
            assert {"kind": "resync"} in drained
            assert len(drained) <= 3

    asyncio.run(scenario())


class _ScriptedBus:
    """Yields a fixed list of events then closes, so the SSE response ends."""

    def __init__(self, events):
        self.events = events

    @asynccontextmanager
    async def subscribe(self, api_key_id):
        q: asyncio.Queue = asyncio.Queue()
        for e in self.events:
            q.put_nowait(e)
        q.put_nowait(None)
        yield q


def test_sse_stream_format(app, client, api_key):
    app.dependency_overrides[get_event_bus] = lambda: _ScriptedBus(
        [{"kind": "job", "id": "j1", "status": "RUNNING"}, {"kind": "workflow_run", "id": "r1", "status": "SUCCEEDED"}]
    )
    res = client.get("/api/v1/events")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/event-stream")
    frames = [f for f in res.text.split("\n\n") if f]
    assert frames[0] == "retry: 3000"
    assert frames[1].startswith("event: ready\n")
    assert frames[2] == 'event: job\ndata: {"kind": "job", "id": "j1", "status": "RUNNING"}'
    assert frames[3].startswith("event: workflow_run\n")


def test_sse_requires_auth(anon_client):
    assert anon_client.get("/api/v1/events").status_code == 401


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL", "").startswith("postgresql"),
                    reason="LISTEN/NOTIFY trigger is PostgreSQL-only")
def test_postgres_trigger_notifies_on_commit_only(db, submit):
    import psycopg

    from app.services.events import _libpq_dsn

    with psycopg.connect(_libpq_dsn(os.environ["TEST_DATABASE_URL"]), autocommit=True) as conn:
        conn.execute("LISTEN flowgrid_events")
        job = submit("echo", {"message": "notify"})  # insert + PENDING -> QUEUED, committed
        events = [json.loads(n.payload) for n in conn.notifies(timeout=2, stop_after=2)]
        assert [e["status"] for e in events] == ["PENDING", "QUEUED"]
        assert events[0]["id"] == str(job.id) and events[0]["kind"] == "job"

        db.execute(Job.__table__.update().where(Job.id == job.id).values(status="RUNNING"))
        db.rollback()  # rolled back: must produce no event
        assert list(conn.notifies(timeout=0.5, stop_after=1)) == []


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #


def test_metrics_endpoint(client, anon_client):
    job_id = client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "m"}}).json()["job_id"]
    client.get(f"/api/v1/jobs/{job_id}")

    text = anon_client.get("/metrics").text
    assert "flowgrid_database_up 1.0" in text
    assert 'flowgrid_jobs{status="SUCCEEDED"} 1.0' in text
    assert 'flowgrid_job_attempts_total{status="SUCCEEDED"} 1.0' in text
    assert "flowgrid_sweeper_lag_seconds 0.0" in text
    # Labelled by route template, never by the raw job ID (bounded cardinality).
    assert 'route="/api/v1/jobs/{job_id}"' in text
    assert job_id not in text


def test_liveness_has_no_dependencies(anon_client):
    assert anon_client.get("/health/live").json() == {"status": "ok"}


# --------------------------------------------------------------------------- #
# Batch submission
# --------------------------------------------------------------------------- #


def test_batch_submit(client):
    jobs = [{"type": "echo", "payload": {"message": str(i)}} for i in range(5)]
    res = client.post("/api/v1/jobs/batch", json={"jobs": jobs})
    assert res.status_code == 202 and res.json()["count"] == 5
    statuses = {client.get(f"/api/v1/jobs/{i}").json()["status"] for i in res.json()["job_ids"]}
    assert statuses == {"SUCCEEDED"}


def test_batch_is_all_or_nothing(client, db):
    jobs = [{"type": "echo", "payload": {"message": "ok"}}, {"type": "echo", "payload": {}}]
    res = client.post("/api/v1/jobs/batch", json={"jobs": jobs})
    assert res.status_code == 422 and res.json()["detail"]["index"] == 1
    assert db.scalar(select(func.count()).select_from(Job)) == 0


def test_batch_size_limit(client):
    jobs = [{"type": "echo", "payload": {"message": "x"}}] * 501
    assert client.post("/api/v1/jobs/batch", json={"jobs": jobs}).status_code == 422


def test_batch_with_broker_down_leaves_jobs_pending(app, client):
    calls = []

    def broken(job_id, timeout):
        calls.append(job_id)
        raise ConnectionError("down")

    app.dependency_overrides[get_publisher] = lambda: broken
    res = client.post("/api/v1/jobs/batch", json={"jobs": [{"type": "echo", "payload": {"message": "x"}}] * 20})
    assert res.status_code == 202
    assert len(calls) == 1  # stopped at the first failure instead of timing out 20 times
    assert client.get("/api/v1/jobs/stats").json()["by_status"]["PENDING"] == 20


# --------------------------------------------------------------------------- #
# Broker message loss
# --------------------------------------------------------------------------- #


def test_long_queued_jobs_are_republished_once_per_interval(submit, sweep):
    job = submit("echo", {"message": "lost in redis"})  # QUEUED, message never consumed
    sent = []

    def recorder(job_id, timeout):
        sent.append(job_id)

    # Derive timings from the setting: Compose overrides the 300s default.
    after = get_settings().queued_republish_after_seconds
    assert sweep(seconds_ahead=after - 5, publish=recorder).queued_republished == 0
    assert sweep(seconds_ahead=after + 1, publish=recorder).queued_republished == 1
    assert sent == [job.id]
    assert sweep(seconds_ahead=after + 2, publish=recorder).queued_republished == 0  # clock reset


def test_sse_stream_closes_when_key_is_revoked(app, client, db, api_key, monkeypatch):
    """A stream authenticated before a revocation must not keep streaming."""
    from app.services.auth import revoke_api_key

    class _QuietBus:
        @asynccontextmanager
        async def subscribe(self, api_key_id):
            yield asyncio.Queue()  # no events: only keepalives and re-checks

    from app.api.deps import require_client

    monkeypatch.setattr(get_settings(), "sse_keepalive_seconds", 0.05)
    app.dependency_overrides[get_event_bus] = lambda: _QuietBus()
    # Simulate "authenticated when the stream opened, revoked afterwards":
    # the request passes auth via the override, but the key is already revoked
    # by the time the stream re-checks it.
    app.dependency_overrides[require_client] = lambda: api_key[0]
    revoke_api_key(db, api_key[0].prefix)
    res = client.get("/api/v1/events")
    assert "event: revoked" in res.text  # stream ended itself instead of running forever
