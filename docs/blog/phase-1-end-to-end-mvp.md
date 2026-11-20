# Building FlowGrid, Phase 1: One Reliable Path from API to Worker

*FlowGrid build log · Phase 1 of 3 · November 2026*

> **TL;DR:** Phase 1 delivers a working end-to-end job system: a FastAPI service validates and persists jobs in PostgreSQL, publishes them through Redis, and Celery workers execute allow-listed handlers and record the outcome. A small dashboard shows it all live. The interesting parts weren't the features. They were the state-machine guards that stop the API and workers from overwriting each other, and three bugs that only showed up once things ran for real.

---

## 1. What problem Phase 1 solves

Some work shouldn't happen inside an HTTP request: processing 10,000 customer records, calling slow external APIs, sending notifications. Run it in the request handler and you get timeouts, no way to retry, and no record of what happened if the process dies halfway.

The standard answer is a **job queue**: the API accepts the work, a background worker does it, and a database remembers the outcome. Phase 1 builds exactly that path and nothing more:

```
submit → validate → persist → enqueue → execute → record outcome → query
```

No retries, no workflows, no auth yet. Those are Phases 2 and 3. The goal was **one path that's correct and that I understand completely** before adding reliability features on top.

## 2. Architecture

```
          ┌──────────────┐   POST /api/v1/jobs    ┌───────────────┐
          │  Dashboard / │ ─────────────────────▶ │    FastAPI    │
          │    client    │ ◀── GET /jobs/{id} ─── │      API      │
          └──────────────┘                        └───┬───────┬───┘
                                    1. INSERT (PENDING)│       │ 2. publish job_id
                                    3. UPDATE → QUEUED │       ▼
                                              ┌────────▼──┐ ┌───────┐
                                              │PostgreSQL │ │ Redis │
                                              │ (truth)   │ │(pipe) │
                                              └────────▲──┘ └───┬───┘
                                    5. RUNNING →       │        │ 4. deliver job_id
                                       SUCCEEDED/FAILED│        ▼
                                                  ┌────┴──────────────┐
                                                  │   Celery worker   │
                                                  │ (allow-listed     │
                                                  │  handlers only)   │
                                                  └───────────────────┘
```

| Component | Role in Phase 1 |
| --- | --- |
| **FastAPI** (`app/api/`) | Validates requests, creates jobs, serves status, list, stats, health and the dashboard |
| **PostgreSQL** (`app/db/`) | The single source of truth for every job's status, payload, result, error and timestamps |
| **Redis** | Message broker only: carries job IDs from the API to the workers |
| **Celery worker** (`app/tasks/`) | Claims a job, runs its handler, records the result |
| **Alembic** (`migrations/`) | Versioned schema; runs as a one-shot `migrate` container before the API and worker start |
| **Dashboard** (`app/static/index.html`) | Single static page: submit jobs, filter the job table, inspect results. Polls every 2s |

### Decision: PostgreSQL is the truth, Redis is just a pipe

The Celery message carries **only the job ID**:

```python
@celery_app.task(name="flowgrid.execute_job")
def execute_job(job_id: str) -> None:
    worker_id = execute_job.request.hostname or socket.gethostname()
    run_job(uuid.UUID(job_id), worker_id=worker_id)
```

The worker loads the type and payload from PostgreSQL. I also set `task_ignore_result=True`, so Celery's result backend is never used. As a result, there's exactly one place to ask "what happened to job X?", and the queue can never hold a different version of a job than the database does. Redis can lose its data and no job *history* is lost (in-flight messages are a separate problem; see §7).

## 3. The job lifecycle as a state machine

```
 PENDING ──publish ok──▶ QUEUED ──worker claims──▶ RUNNING ──▶ SUCCEEDED
    │                                                   └────▶ FAILED
    └──publish failed──▶ FAILED
```

Why both `PENDING` *and* `QUEUED`? Writing to PostgreSQL and publishing to Redis are **two separate systems with no shared transaction**. Here's the order of operations in `submit_job`:

1. `INSERT` the job as `PENDING` and **commit**. The row must exist before any worker can receive its ID.
2. Publish to Redis.
3. `UPDATE` to `QUEUED`.

If the API process crashes between steps 1 and 2, the job stays `PENDING`. That's an **honest** state: "saved, not confirmed as queued". If I had inserted it as `QUEUED` directly, a crash at the same point would leave a row that claims to be queued but sits in no queue at all. Phase 2/3 can add a reconciler that re-publishes stale `PENDING` jobs. The data model already makes them easy to find.

If publishing raises an error (Redis is down), the job is marked `FAILED` with `Enqueue failed: ...` and the API returns **503** with the job ID, so the client knows exactly what happened.

### Every transition is a guarded UPDATE

All status changes go through one function:

```python
def _transition(db, job_id, *, from_statuses, to_status, **values) -> bool:
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status.in_([s.value for s in from_statuses]))
        .values(status=to_status.value, updated_at=utcnow(), **values)
    )
    result = db.execute(stmt)
    db.commit()
    return result.rowcount == 1
```

The `WHERE status IN (...)` clause is the whole trick. It turns "set the status" into "set the status *only if the job is in a state where that transition is legal*". It handles three real races:

**Race 1: the worker beats the API.** After step 2, a fast worker can claim the job (`QUEUED/PENDING → RUNNING`) before the API runs step 3. Without the guard, the API would overwrite `RUNNING` with `QUEUED` and the job would look stuck forever. With the guard, the API's update matches zero rows and does nothing. There's a test for exactly this (`test_api_does_not_overwrite_status_set_by_a_fast_worker`), and the test suite runs Celery in eager mode, where the worker *always* finishes before step 3, so the guard is exercised on every API test.

**Race 2: two workers get the same message.** The claim is `PENDING/QUEUED → RUNNING`. In PostgreSQL an `UPDATE` takes a row lock. The second worker blocks, then re-evaluates the `WHERE` clause after the first commits, sees `RUNNING`, and matches nothing. Only one worker ever sees `rowcount == 1`.

**Race 3: a late write after a terminal state.** `mark_succeeded` and `mark_failed` only match `RUNNING`, so nothing can move a `SUCCEEDED` job back to `FAILED`.

This doesn't make execution exactly-once, and §7 explains why. It does make **status** consistent.

### Short transactions, long jobs

The worker uses three separate, short transactions: *claim*, then run the handler **outside any transaction**, then *record the outcome*. A 30-second job never holds a database connection or row lock for 30 seconds.

## 4. Allow-listed handlers, not arbitrary code

A client can only submit a `type` that exists in a registry, and the `payload` must validate against that handler's Pydantic model:

```python
@register(
    "process_records",
    payload_model=ProcessRecordsPayload,   # batch_id, record_ids (1..10,000), delay_ms
    description="Simulates the validate → enrich → store → notify pipeline...",
    example={"batch_id": "batch-001", "record_ids": [101, 102, 103], "delay_ms": 200},
)
def process_records(payload: ProcessRecordsPayload) -> dict: ...
```

Phase 1 ships four handlers: `echo`, `sleep`, `process_records` and `always_fail` (for exercising the failure path).

A few details that matter:

- **Validation happens at submit time.** Unknown types and bad payloads get a `422` with field-level errors, and *no job row is created*. A bad request never becomes queued work. The tests assert the jobs table is still empty.
- **`extra="forbid"`** on every payload model, so a typo like `{"msg": "hi"}` is rejected instead of silently ignored.
- **The normalized payload is what gets stored.** Defaults are filled in (`delay_ms: 0`), so the record shows exactly what the worker will run.
- **Per-record failures vs. job failures.** In `process_records`, a bad record ID counts as a failed *record*, and the job still `SUCCEEDED` with `{"processed": 100, "failed": 2}`. A handler *raising* is a failed *job*. Keeping those two apart matters a lot once retries arrive in Phase 2: you don't retry 10,000 records because one was invalid.
- **Stored failure detail is capped at 50 entries** so one bad batch can't bloat a row.
- **`GET /api/v1/job-types`** exposes each handler's JSON Schema and example. The dashboard builds its form from this endpoint.

## 5. The API

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/api/v1/jobs` | **202 Accepted**, not 201: the resource exists, but the work hasn't happened yet |
| `GET` | `/api/v1/jobs/{id}` | Status, result, error, attempts, worker, all timestamps, `duration_ms` |
| `GET` | `/api/v1/jobs?status=&type=&limit=&offset=` | Newest first; `limit` capped at 100 |
| `GET` | `/api/v1/jobs/stats` | Counts per status |
| `GET` | `/api/v1/job-types` | The allow-list |
| `GET` | `/health` | Checks PostgreSQL and Redis; 503 if either is down |

A small FastAPI gotcha: `/jobs/stats` must be **declared before** `/jobs/{job_id}`. Routes are matched in order, and `"stats"` would otherwise match the `{job_id}` route and fail UUID validation with a confusing 422.

The schema indexes `(status, created_at)` and `created_at` to support the list and filter queries, and a `CHECK` constraint keeps any status outside the five legal values out of the database, even if a bug in the code tries to write one.

## 6. Three bugs the build surfaced

These are the most useful part of this write-up, because each one taught something general.

### Bug 1: stale reads from SQLAlchemy's identity map

`test_only_one_claim_succeeds` failed. `claim_job` ran the guarded `UPDATE` and then called `db.get(Job, id)` to return the job. The returned object still said `QUEUED` with no `worker_id`.

**Why:** the `Session` already held that `Job` object (from `submit_job`), and an ORM-level bulk `UPDATE` with `synchronize_session=False` doesn't touch objects already in memory. `db.get()` checks the identity map first and returned the cached, stale copy.

**Fix:** `db.get(Job, job_id, populate_existing=True)` forces a reload from the database. In production every worker task opens a fresh session, so it would never have shown up there. That's exactly the kind of latent bug that bites later, when someone reuses a session.

### Bug 2: a dependency conflict the unit tests couldn't see

All 30 tests passed. Then the worker container crashed on startup:

```
kombu.exceptions.VersionMismatch: Redis transport requires redis-py versions 6.1.0 or later. You have 5.3.1
```

I had pinned `redis<6`. Current kombu (Celery's transport layer) needs redis-py ≥ 6.1. The tests didn't catch it because they run Celery in **eager mode**, which never creates a broker connection.

**Lesson:** eager mode is great for testing *your* logic, but it skips the transport entirely. That's why Phase 1 also has `scripts/smoke_test.py`, which drives the real HTTP → Postgres → Redis → worker path and is part of the definition of done.

### Bug 3: an em dash broke Docker builds

`docker compose up --build` failed with:

```
header key "x-docker-expose-session-sharedkey" contains value with non-printable ASCII characters
```

The project folder is named `FlowGrid — Distributed Workflow & ...`, and that `—` is non-ASCII. Compose v5 builds through BuildKit "bake", which puts the project path into a gRPC header, and gRPC headers must be ASCII. Single-service builds happened to work. Building several services in one call didn't.

**Fix:** two changes, one of them a real improvement anyway:

1. Only the `api` service has `build:`. `migrate` and `worker` reuse the same `flowgrid-app:latest` image (`pull_policy: never`). They run identical code, so building three images was wasteful in the first place.
2. The `Makefile` exports `COMPOSE_BAKE=false` as a safeguard.

## 7. What Phase 1 deliberately does *not* guarantee

Being explicit about this matters more than the feature list.

- **Messages can be lost if a worker dies mid-task.** Celery's default is to acknowledge a message *when it's received*, not when it's finished. If a worker is killed mid-job, the job stays `RUNNING` forever and the message is gone. Phase 2 will switch to late acknowledgement (`acks_late`), which in turn means a job *can* be delivered twice. That's why the guarded claim already exists, and why idempotency is a Phase 2 item.
- **No retries.** `attempts` is tracked (it's always 1 today) so the schema is ready for Phase 2's backoff policy.
- **Stranded `PENDING` jobs aren't swept.** The state exists, the reconciler doesn't yet.
- **No timeouts.** A handler that hangs holds a worker slot forever.
- **No auth or rate limiting.** The API is open. That's fine for localhost, but not something to deploy.
- **The dashboard polls** every 2 seconds. Live updates via SSE are a Phase 3 item.

## 8. Testing strategy and verified results

| Layer | Where | What it covers |
| --- | --- | --- |
| Handler unit tests | `tests/test_handlers.py` | Pure functions: pipeline logic, validation errors, failure cap |
| State-machine tests | `tests/test_job_state.py` | Exclusive claim, duplicate delivery is a no-op, terminal states are final, the API/worker race, worker running older code than the API |
| API tests | `tests/test_jobs_api.py` | Happy path, failed jobs, 422s create no rows, broker outage → 503 + `FAILED`, pagination/filters, stats, health |
| End-to-end | `scripts/smoke_test.py` | The real stack: Redis transport, worker process, observable `RUNNING` state, a burst of jobs |

The pytest suite runs against **SQLite** (`make test`, no services needed) or **PostgreSQL** (`make test-pg`). Both pass: **30/30**.

Results from the final verification run on my machine (Docker Desktop, one worker container, `--concurrency=2`):

```
FlowGrid smoke test against http://localhost:8000
  ok    health check  (postgres + redis reachable)
  ok    echo job succeeds  (worker=celery@b7268acbbdb4, 42 ms)
  ok    failing job is recorded as FAILED  (JobHandlerError: smoke failure)
  ok    RUNNING state is observable  (RUNNING -> SUCCEEDED)
  ok    process_records batch  (processed=100 failed=2)
  ok    invalid input rejected  (422 for bad payload and unknown type)
  ok    burst of jobs, each run exactly once  (25 jobs in 2.2s)

7/7 checks passed
```

Also verified:

- `alembic check` reports **no drift** between the hand-written migration and the SQLAlchemy models.
- After `docker compose restart` of postgres, redis, api and worker, `/api/v1/jobs/stats` returned identical counts (29 jobs) before and after. History is durable.
- The dashboard showed a `sleep` job as `RUNNING` and then `SUCCEEDED` live, with no browser console errors.

> The "25 jobs in 2.2s" figure is a smoke-test observation, not a benchmark. Each job sleeps 50 ms × 3 records, with 2 worker processes, and the client submits and polls sequentially. Real throughput measurements are a Phase 3 deliverable.

## 9. How to run it

```bash
make up                                   # postgres, redis, migrate, api, worker
open http://localhost:8000                # dashboard  (/docs for OpenAPI)
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
make test        # pytest on SQLite
make test-pg     # pytest on PostgreSQL inside Docker
make smoke       # end-to-end through Redis and a real worker
```

## 10. What's next: Phase 2 (reliability)

Phase 1 left a clear list of gaps, and Phase 2 addresses them directly:

1. **Late acknowledgement + redelivery**, so a killed worker doesn't lose its job.
2. **Retries with exponential backoff** and a retry limit, plus a `job_attempts` table so each attempt has its own record.
3. **Idempotency keys** on `POST /jobs`, so a client retrying a request doesn't create two jobs.
4. **Dead-letter handling** for jobs that exhaust their retries.
5. **Timeouts**, **structured logs**, **authentication** and **rate limiting**.
6. **Chaos tests**: kill a worker mid-job and verify what happens, rather than assuming.

The guarded state transitions built in Phase 1 are the foundation all of that sits on.
