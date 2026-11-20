# Building FlowGrid, Phase 3: From Jobs to Workflows

*FlowGrid build log · Phase 3 of 3 · November 2026*

> **TL;DR:** Phase 3 turns reliable single jobs into an orchestration platform. It adds workflow DAGs with parallel branches and data passing between steps, delayed jobs and cron schedules that fire exactly once per tick even with competing sweepers, live status updates streamed from a PostgreSQL trigger over Server-Sent Events, Prometheus metrics with alert rules, a batch API, and a throughput benchmark at 1, 2 and 4 workers. The design principle from Phase 1 still carries everything: **PostgreSQL is the source of truth, and every state change is a guarded UPDATE.** Workflows, schedules and live events are all built on top of that, and the chaos suite proves it by killing workers mid-workflow, wiping Redis and racing three sweepers against one schedule.

---

## 1. What was missing after Phase 2

Phase 2 made *a job* survive crashes, outages and duplicate delivery. But real work is rarely one job:

- **"Validate, then enrich A and B in parallel, then report"** needs dependencies between jobs.
- **"Run every night at 03:00"** needs time.
- **"Watch it happen"** needs push updates, not a dashboard polling every 2 seconds.
- **"Is it healthy? Does it scale?"** needs metrics and measurements, not adjectives.

Phase 3 also closes a gap that Phase 2's own chaos test never exercised (§7).

## 2. Workflows as DAGs

A workflow is a list of steps. Each step is an ordinary allow-listed job type plus `depends_on`:

```json
{"name": "nightly-import", "steps": [
  {"id": "extract", "type": "echo",            "payload": {"message": "start"}},
  {"id": "batch_a", "type": "process_records", "depends_on": ["extract"], "payload": {...}},
  {"id": "batch_b", "type": "process_records", "depends_on": ["extract"], "payload": {...}},
  {"id": "report",  "type": "summarize",       "depends_on": ["batch_a", "batch_b"]}
]}
```

### Validation reports every problem at once

`POST /workflows` validates the whole definition before storing it:

- duplicate step IDs
- unknown or self-referencing dependencies
- unknown job types
- each step's payload, checked against its handler's schema
- **cycles**, via Kahn's algorithm (repeatedly remove nodes with no incoming edges; anything left over is part of a cycle)

All problems come back in a single 422, each with a precise location like `["steps", 2, "payload", "message"]`, so a client can fix a definition in one round trip. Payloads are normalised at definition time, so a run executes exactly what was stored.

### Every step is created up front

Starting a run creates **every** step's job in one transaction:

| Step | Initial status |
| --- | --- |
| No dependencies | `PENDING` (published immediately) |
| Has dependencies | `BLOCKED`, plus a `job_dependencies` row per edge |

Creating steps lazily as their parents finish was the alternative. Up-front creation was chosen for three reasons:

1. **Atomicity.** A run either exists completely or not at all. There is no "step 3 was never created because the process died" state.
2. **Reuse.** Each step is a normal job, so retries, timeouts, leases, fencing, attempts and dead-lettering all work for workflow steps with **zero** workflow-specific code.
3. **Visibility.** The dashboard can draw the whole DAG, including steps that are still `BLOCKED`.

The only new piece of logic is `advance_run`, which runs after a step reaches a terminal state:

| Run state | Action |
| --- | --- |
| Any step `FAILED` / `DEAD_LETTER` | Cancel all not-yet-started steps (`CANCELLED`); run → `FAILED` |
| Every step `SUCCEEDED` | Run → `SUCCEEDED` |
| Otherwise | `BLOCKED` steps whose parents all succeeded → `PENDING` → publish |

Steps receive their parents' results in `ctx.upstream`. The `summarize` handler uses that to fan in: in the smoke test, a 4-step diamond processed 50 + 30 records in two parallel branches, and `report.totals.processed` came back as exactly 80.

**Failure semantics are "fail fast":** the first failed step stops the run, and steps that haven't started are cancelled rather than run pointlessly. Steps that are already running finish on their own, because you can't safely un-run a side effect. Their results are recorded but don't move the run. A transient error in a step *doesn't* fail the run: the step retries with backoff inside the run, and its dependents stay `BLOCKED` until it succeeds.

## 3. The lost wakeup, and why it can't happen here

The classic DAG bug: `report` depends on `batch_a` and `batch_b`, and both finish at the same instant on two workers. Each worker asks, "are all of report's parents done?"

The bug occurs if each worker **marks itself done and checks its siblings in the same transaction**. Worker A's transaction can't see B's uncommitted success, and B can't see A's. Both conclude "not yet", both commit, and `report` sits `BLOCKED` forever. Nobody is left to wake it up.

My first plan was a row lock on the run to serialise completions. Working through the ordering showed it isn't needed:

1. A step's success is **committed in its own transaction first**.
2. `advance_run` runs in a **new** transaction afterwards, and under READ COMMITTED every statement sees everything already committed.

So if A's success commits before B's, then B's check runs after B's own commit, which is after A's commit, and B's check *must* see A as done. The two "both miss" orderings are impossible.

The opposite race, where **both** checks see everything done and both try to unblock `report`, is handled by the guard we already had: `UPDATE ... SET status='PENDING' WHERE status='BLOCKED'` matches for exactly one of them. `test_concurrent_advances_unblock_a_step_exactly_once` calls `advance_run` twice on the same state and asserts `report` was published **once**.

That leaves one real gap: **a worker dies after committing its success but before calling `advance_run`**. No race is involved, just a crash in the wrong place. The sweeper covers it with one query:

```sql
-- RUNNING runs with a BLOCKED step whose parents have all succeeded
... WHERE job.status = 'BLOCKED'
    AND NOT EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs p ON p.id = d.depends_on_job_id
                    WHERE d.job_id = job.id AND p.status <> 'SUCCEEDED')
```

`test_crash_between_step_success_and_advance_is_repaired_by_sweeper` records three successes without ever advancing, then shows one sweep finishing the run. The chaos suite does it for real by SIGKILLing a worker in the middle of a workflow step: the step comes back as `LOST → SUCCEEDED`, its dependent runs, and the run succeeds.

## 4. Time: delayed jobs and cron schedules

### Delayed jobs reuse the retry machinery

`POST /jobs` with `run_at` or `delay_seconds` creates the job as `SCHEDULED` with `next_attempt_at` set. That's exactly how Phase 2 represents a retry waiting for its backoff (`RETRY_SCHEDULED` + `next_attempt_at`). The sweeper's release path handles both: `→ PENDING → publish`. Workflow runs accept `run_at` too; a scheduled run's steps are created when it starts.

### Cron: exactly once per tick, by construction

A schedule has a 5-field cron expression (UTC, minute resolution; the 6-field "seconds" form is refused) and a target, either a job or a workflow. The hard requirement: with several sweepers and crashes at the worst possible moment, **each tick produces exactly one job**. Two mechanisms make that true by construction rather than by luck:

**1. Compare-and-swap on `next_run_at`.**

```sql
UPDATE schedules SET next_run_at = :following
WHERE id = :id AND enabled AND next_run_at = :tick   -- only one sweeper can match
```

**2. The job or run is inserted in the *same transaction* as the CAS,** and `(schedule_id, scheduled_for)` is unique. A crash before commit rolls back both, and the tick fires on the next pass. A commit makes both durable. There's no window in which the schedule advanced but the job doesn't exist.

The chaos suite runs **three sweepers** against an every-minute schedule for several minutes: **jobs per tick = [1, 1]**.

**Missed ticks are coalesced.** If every sweeper was down for ten minutes, a schedule fires *once* on recovery and jumps to the next future tick. Replaying ten stale runs in a burst is almost never what you want. Resuming a paused schedule likewise skips the ticks it missed while paused.

Two bugs in this code were caught in review before any test ran (§8).

## 5. Live progress: a database trigger, SSE, and one connection per process

### Events come from PostgreSQL itself

```sql
CREATE TRIGGER jobs_notify AFTER INSERT OR UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION flowgrid_notify_job();   -- pg_notify('flowgrid_events', {...})
```

Why a trigger rather than publishing events from Python?

- **Every writer is covered for free:** the API, workers and sweeper all emit events without a line of event code.
- **`NOTIFY` is transactional.** It's delivered only when the transaction commits, and never if it rolls back, so the stream can never announce a state the database doesn't hold. A PostgreSQL-only test asserts this: an update followed by a rollback produces no event.

### Fan-out without exhausting the pool

Each API process holds **one** `LISTEN` connection and fans events out to in-memory per-client queues, filtered by API key. So 500 open dashboards cost one database connection, not 500. A slow client whose queue fills up gets a single `resync` event ("refetch everything") instead of blocking the bus for everyone else.

One detail would have hurt in production: SSE responses stay open for hours, and the request's database session, used only for authentication, would otherwise **pin a pooled connection for the life of the stream**. With SQLAlchemy's default pool of 15, the 16th open dashboard would hang every other request. The endpoint closes its session before it starts streaming.

### The dashboard can't use `EventSource`

The browser's `EventSource` API **can't send an `Authorization` header**. The common workaround is putting the API key in the URL, which leaks it into access logs, proxies and browser history. The dashboard instead reads the stream with `fetch()` and parses SSE frames itself. Events are treated as hints: each one triggers a debounced refetch over the REST API, and polling only kicks in while the stream is down.

In the smoke test the first event arrived **6–9 ms after submit**, and the stream showed `PENDING → QUEUED → RUNNING → SUCCEEDED`.

## 6. Metrics: count in the database, label by template

`/metrics` exposes two kinds of series:

- **Gauges read from PostgreSQL at scrape time:** jobs by status, runs by status, attempt outcomes, oldest queued job age, oldest pending job age, **sweeper lag** (how overdue the most overdue scheduled job is), expired leases, enabled schedules. Workers run in many processes and containers, and counting in-process would need Prometheus' multiprocess mode, and would still lose counts from a container that died. The database already knows the truth. (Every API replica reports the same values, so aggregate with `max()`, not `sum()`.)
- **HTTP request counts and latency, labelled by route *template*.** The label is `/api/v1/jobs/{job_id}`, never the raw path. Raw paths would create a new time series for every job ID: unbounded cardinality, the classic way to take a Prometheus server down. The test asserts that the job ID doesn't appear anywhere in `/metrics`.

`deploy/prometheus/alerts.yml` has 7 rules, each aimed at a failure mode Phases 1–3 taught us to expect: sweeper lag, queue backlog, publish failures, dead letters growing, expired leases, database unreachable, and 5xx rate. Prometheus loaded all 7 as healthy. To check that a metric means what it claims, I stopped the sweeper and let a delayed job go overdue: `flowgrid_sweeper_lag_seconds` read **11.1**, and fell back to **0** within seconds of restarting the sweeper.

## 7. The gap Phase 2 never tested: Redis *losing* messages

Phase 2's chaos test stopped Redis and showed the outbox delivering jobs afterwards. But `docker compose stop` is **graceful**: Redis saves its dataset on SIGTERM, so every queued message came back after the restart. The test proved "Redis is unreachable" is safe. It never tested "Redis lost data", which is what a crash without persistence, a failover to a lagging replica, or an operator's `FLUSHALL` actually does.

In Phase 2, a job that was `QUEUED` in PostgreSQL but whose message vanished from Redis would have stayed `QUEUED` **forever**. Nothing in the system re-sent it.

The fix is another sweeper duty: re-publish jobs that have been `QUEUED` longer than `QUEUED_REPUBLISH_AFTER_SECONDS` (default 300s), and reset their clock. If the original message *does* still exist, the duplicate is harmless: the guarded claim lets exactly one delivery run. The new chaos scenario stops the workers, queues 5 jobs, runs `FLUSHALL`, confirms the Redis queue is empty, and restarts the workers. All 5 jobs complete with **`attempts == 1`**.

The trade-off: a job waiting legitimately in a long backlog also gets re-sent once per interval. Set the threshold above your normal worst-case queue wait.

## 8. Bugs found along the way

**1. A rollback that would have fired the next cron tick early (caught in review).** The first `fire_due_schedules` loaded due schedules as ORM objects, then looped over them, rolling back whenever it lost a CAS race. **A rollback expires every object loaded in the session.** The next schedule's `next_run_at` would then be lazily re-read from the database, and if another sweeper had just advanced it, this sweeper would CAS against the *future* tick and fire it early. The fix snapshots plain values (`id, next_run_at, cron`) before the loop. This is a SQLAlchemy behaviour worth remembering: expiry on rollback is unconditional, regardless of `expire_on_commit`.

**2. A schedule that could wedge itself (caught in review).** If a tick's job already existed (`IntegrityError`), the rollback undid the duplicate insert *and* the CAS that advanced the schedule. The same tick would then be retried on every pass, forever. Now any failure to create the target still advances the schedule, in its own transaction. There's a test for exactly this.

**3. A test that depended on a default.** `test_long_queued_jobs_are_republished_once_per_interval` passed on SQLite and failed on PostgreSQL. The PostgreSQL run happens inside the API container, which inherits Compose's `QUEUED_REPUBLISH_AFTER_SECONDS=60`, while the test had hardcoded the 300s default. Tests now derive timings from the setting. This is the third phase in a row where running the suite in a second environment caught something.

**4. A dashboard that retried a dead key forever (found in the browser).** The browser console showed a stream of 401s. Local storage still held a key revoked at the end of Phase 2, and the fallback poll kept retrying it every 3 seconds. Now a 401 forgets the stored key. Verified in the browser by revoking a key while the page was open: exactly one 401, then silence.

**5. SSE streams outlived key revocation (found because of bug 4).** Revoking a key didn't affect streams that were already open, because authentication ran only when the stream connected. A leaked key's open stream would keep receiving that client's events indefinitely. The stream now re-checks the key on its keepalive interval and ends with a `revoked` event. There's a test for it.

**6. A benchmark that couldn't scale down.** The readiness check waited for a fresh worker `ready.` log line, which never appears when you scale *down*. Only the cleanup step failed, but it's the kind of bug that makes a benchmark "work once".

## 9. Scale: the benchmark

`POST /jobs/batch` submits up to 500 jobs in one transaction for one rate-limit token, which is how the "process 10,000 records" story from the original pitch should be submitted. `make bench` uses it to measure two workloads at 1, 2 and 4 worker containers (2 processes each), on one 8-CPU Docker VM shared with PostgreSQL, Redis, the API and the sweeper. Latencies come from PostgreSQL timestamps, not from client-side polling.

| Worker containers (processes) | Overhead: 1,000 no-op jobs | I/O-bound: 400 × 0.2 s jobs | I/O vs ideal | I/O end-to-end p50 / p95 |
| --- | --- | --- | --- | --- |
| 1 (2) | 257 jobs/s | 9.1 jobs/s | 91% | 22.2 s / 41.9 s |
| 2 (4) | 396 jobs/s | 17.9 jobs/s | 90% | 11.3 s / 21.3 s |
| 4 (8) | 466 jobs/s | 35.2 jobs/s | 88% | 5.8 s / 10.9 s |

Every one of the 4,200 jobs succeeded, none retried, and none ran twice.

What the numbers say:

- **I/O-bound work scales almost linearly.** Each doubling of workers doubles throughput and halves latency, at 88–91% of the theoretical `processes ÷ 0.2 s`. The missing ~10% is FlowGrid's own per-job cost: several committed transitions plus a broker round trip.
- **Pure overhead scales sub-linearly (257 → 396 → 466).** No-op jobs measure the platform itself, and the platform shares 8 CPUs with everything else. As workers grow, PostgreSQL and the API take a bigger share of the machine. On separate hosts, the database becomes the bottleneck to watch: every job is about 4–5 small committed writes.
- **Latency here is queue wait.** With 400 jobs submitted at once, end-to-end latency is almost entirely waiting for a free worker (queue-wait p50 22.0 s vs end-to-end 22.2 s on one worker). That's exactly what `flowgrid_oldest_queued_job_age_seconds` measures, and why it drives the "add workers" alert.

These are laptop numbers, honestly labelled, not a production capacity claim.

## 10. Verification

| Check | Result |
| --- | --- |
| pytest on SQLite | **121 passed, 1 skipped** (the PostgreSQL-only `NOTIFY` test) |
| pytest on PostgreSQL | **122 / 122** |
| Migration `0002 → 0003` on the live database; `downgrade 0002 → upgrade head` | Clean; `alembic check` reports no drift; both triggers installed |
| `make smoke` | **19 / 19** (workflows, fail-fast, cycle rejection, delay, batch, SSE, metrics, a real cron tick, plus all Phase 1–2 checks) |
| `make chaos` | **8 / 8** |
| `make bench` | Table above |
| Prometheus | Target `up`; all 7 alert rules loaded and healthy; sweeper-lag metric verified (11.1 s → 0) |
| Dashboard in the browser | Diamond DAG rendered live (`BLOCKED` → parallel `RUNNING` → all `SUCCEEDED`, 1.4 s), schedules created, revocation behaviour verified |

The chaos run:

```
  ok    worker SIGKILLed mid-job -> lease recovery  [31.2s]  (attempts ['LOST', 'SUCCEEDED']; recovered 31s after kill)
  ok    timeout on every attempt -> DEAD_LETTER  [6.8s]  (attempts ['TIMED_OUT', 'TIMED_OUT'])
  ok    Redis outage -> outbox delivers later  [11.5s]  (accepted as PENDING in 233 ms while Redis was down)
  ok    crash mid-batch -> no duplicate side effects  [2.9s]  (500 records, 2 attempts -> 500 rows, 500 notifications)
  ok    duplicate message delivery -> runs once  [3.4s]  (3 duplicate messages delivered, attempts still 1)
  ok    Redis FLUSHALL loses queued messages -> re-published  [62.6s]  (5 QUEUED messages wiped; all done 58s later)
  ok    worker SIGKILLed mid-workflow -> run completes  [29.8s]  (step 'slow' attempts ['LOST', 'SUCCEEDED']; run SUCCEEDED)
  ok    3 concurrent sweepers -> each cron tick fires once  [3.3s]  (3 sweepers, 2 ticks, jobs per tick = [1, 1])

8/8 checks passed
```

## 11. Known limits

- **Recovery time after a crash is `timeout + 15 s`.** There's no heartbeat; a workflow inherits that delay at each crashed step.
- **Live events aren't durable.** A disconnected client misses them and must refetch; the dashboard does this automatically.
- **No partial re-runs of a workflow.** A failed run is re-run from the start. Steps are idempotent where it matters, so redoing completed steps is safe, but not free.
- **Cron is UTC-only with minute resolution.** There are no per-schedule time zones.
- **The PgBouncer caveat.** Transaction-mode pooling breaks `LISTEN`; see the [deployment guide](../deployment.md).

## 12. Looking back across the three phases

The same handful of ideas carried the whole project:

1. **One source of truth.** Redis only ever carries job IDs. Everything that matters — status, attempts, schedules, DAG edges, even the event stream — comes from PostgreSQL. That's why losing Redis entirely is a recoverable event.
2. **Guarded transitions.** `UPDATE ... WHERE status IN (...)` started in Phase 1 as a fix for one race between the API and a worker. It then made duplicate deliveries harmless (Phase 2), multiple sweepers safe (Phase 2), concurrent DAG unblocking correct (Phase 3), and cron ticks exactly-once (Phase 3, with a CAS).
3. **Recovery is a loop, not a code path.** Instead of trying to handle every failure at the moment it happens, the sweeper repeatedly asks the database, "what state implies work nobody did?", and does it. Crashes anywhere become delays rather than losses.
4. **Test the failure, not the feature.** Each phase's most important bugs were found by a second database, a real container being killed, a browser console, or a second look at a rollback. None were found by the happy path.

The full build log: [Phase 1](phase-1-end-to-end-mvp.md) · [Phase 2](phase-2-reliability-and-security.md) · Phase 3 · [Deployment guide](../deployment.md) · [Benchmark results](../benchmarks/phase-3-results.md)
