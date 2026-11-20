# Building FlowGrid, Phase 2: Making Failure Boring

*FlowGrid build log · Phase 2 of 3 · November 2026*

> **TL;DR:** Phase 1 worked when nothing went wrong. Phase 2 makes it work when things do. It adds retries with exponential backoff, per-job timeouts, a dead-letter state with replay, leases that recover jobs from dead workers, an outbox relay that survives Redis outages, idempotency keys, idempotent side effects, a fencing token against "zombie" workers, API-key auth with per-client data isolation, Redis token-bucket rate limiting, and structured JSON logs. Every one of these is backed by a test, and the scary ones by a chaos test that SIGKILLs containers and checks the system recovers to the right state.

---

## 1. Where Phase 1 left us

The Phase 1 post ended with an honest list of gaps. Every Phase 2 feature exists to close one of them:

| Phase 1 gap | What actually happened | Phase 2 fix |
| --- | --- | --- |
| Worker dies mid-job | Job stuck in `RUNNING` forever; message already acked and gone | Late acks **+** leases recovered by a sweeper |
| A transient error | Job `FAILED` immediately | Error classification, exponential backoff, `RETRY_SCHEDULED` |
| Retries never stop | — | `max_attempts`, then `DEAD_LETTER` (replayable) |
| Handler hangs | Holds a worker slot forever | Soft and hard time limits per job |
| Redis down at submit | `503`, job marked `FAILED` | **Outbox:** `202 PENDING`, sweeper publishes later |
| Client retries a POST | Duplicate job | `Idempotency-Key` |
| Job re-runs after a crash | Duplicate side effects | Upserts + notification dedupe keys |
| Open API | Anyone can read anyone's jobs | API keys, per-key ownership, rate limits |

## 2. The new state machine

```
PENDING ──publish──▶ QUEUED ──claim──▶ RUNNING ──────────────▶ SUCCEEDED
   ▲                                     │
   │  backoff elapsed (sweeper)          ├── transient error / timeout / lease expired
   └─────────── RETRY_SCHEDULED ◀────────┤      and attempts < max_attempts
                                         ├── same, attempts exhausted ──────▶ DEAD_LETTER
                                         └── permanent error ───────────────▶ FAILED
```

Two terminal failure states, deliberately separate:

- **`FAILED`**: the error is *permanent* (bad data, a business-rule violation, a bug). Retrying won't change anything, so we don't.
- **`DEAD_LETTER`**: the error was *transient*, but we ran out of patience. An operator should look, fix whatever is upstream, and **replay**.

Replay (`POST /jobs/{id}/replay`) creates a **new** job with `replay_of` pointing at the original. The dead job stays untouched, so the audit trail of what happened and when is never rewritten.

Every attempt now gets its own row in `job_attempts` (`RUNNING → SUCCEEDED | FAILED | TIMED_OUT | LOST`) with the worker, the error type and whether it was retryable. `GET /jobs/{id}/attempts` gives you the job's full history.

## 3. Retries: what to retry, and when

### What to retry is an allow-list

```python
RETRYABLE_EXCEPTIONS = (TransientJobError, SoftTimeLimitExceeded, TimeoutError, ConnectionError)
```

Anything not on this list, **including `KeyError`, `TypeError` and every other programming error**, is permanent. That's a deliberate choice. Many retry libraries retry everything by default, which means a typo in a handler burns all its attempts and the real failure surfaces minutes late, buried under backoff. Handlers opt in to retries by raising `TransientJobError`.

### When to retry: exponential backoff with "equal jitter"

```python
def backoff_delay(attempt, *, base, cap, rng=random.random):
    exp = min(cap, base * 2 ** (attempt - 1))
    return exp / 2 + rng() * exp / 2
```

With `base=2s`, attempt 1 waits 1–2s, attempt 2 waits 2–4s, attempt 3 waits 4–8s, and so on, capped at 300s. Half the delay is fixed (retries never fire instantly) and half is random. The random half matters: when a shared dependency comes back after an outage, a thousand failed jobs shouldn't all hit it in the same millisecond. The test pins this down by injecting `rng=0` and `rng=1`.

### Retries are scheduled in the database, not in the broker

The obvious implementation is Celery's `apply_async(countdown=delay)`. I didn't use it. With a Redis broker, a countdown task sits *inside a worker's memory* until its ETA. Kill that worker and the retry is gone, which is exactly the failure Phase 2 is supposed to survive.

Instead, a failure writes `status = RETRY_SCHEDULED, next_attempt_at = now + delay` to PostgreSQL. A separate **sweeper** process (§5) moves due jobs back to `PENDING` and publishes them. The retry schedule now lives in the same durable store as everything else.

## 4. Timeouts double as leases

Each job has `timeout_seconds` (a per-type default, overridable per job). When it's published:

```python
execute_job.apply_async(args=[job_id], soft_time_limit=timeout, time_limit=timeout + 5)
```

- **Soft limit:** Celery raises `SoftTimeLimitExceeded` inside the handler. The executor records the attempt as `TIMED_OUT`, which is retryable.
- **Hard limit, 5s later:** if the handler ignores the soft limit (stuck in C code, swallowing exceptions), Celery kills the process.

When a worker claims a job, it writes a **lease**:

```
lease_expires_at = now + timeout_seconds + LEASE_GRACE_SECONDS (15s)
```

This is the key insight of Phase 2: **because the hard limit guarantees no attempt can legitimately run longer than `timeout + 5s`, a lease still unexpired after `timeout + 15s` can only mean the worker is gone**. Most lease systems need a heartbeat thread that periodically extends the lease. Here the timeout already bounds the runtime, so the lease is a simple deadline. One constraint must hold: the grace (15s) has to exceed the hard-kill margin (5s). Otherwise the sweeper could recover a job that's still being killed.

## 5. The sweeper

`python -m app.sweeper` runs every second and does four things, all driven by PostgreSQL state:

1. **Recover expired leases.** `RUNNING` jobs past `lease_expires_at` are treated as a failed, retryable attempt (`WorkerLostError`). The attempt becomes `LOST`, and the job moves to `RETRY_SCHEDULED` or `DEAD_LETTER`.
2. **Re-queue due retries.** `RETRY_SCHEDULED` jobs whose `next_attempt_at` has passed are moved to `PENDING` and published.
3. **Relay the outbox.** `PENDING` jobs older than 10s missed their publish, so they're published again.
4. **Purge idempotency keys** older than 24h.

Step 2 deliberately goes through `PENDING` rather than publishing straight from `RETRY_SCHEDULED`. If the publish fails, the job is simply a stranded `PENDING` job, which step 3 already knows how to handle. One recovery path covers both cases.

**Can you run two sweepers?** Yes, with no leader election, because every action is a guarded `UPDATE ... WHERE status = ...` (introduced in Phase 1). If two sweepers pick the same job, one update matches a row and the other matches nothing. The worst case is a duplicate *publish*, which the guarded claim turns into a no-op.

### From 503 to 202: the outbox

In Phase 1, if Redis was down at submit time, the API returned 503 and marked the job `FAILED`. That's honest, but unhelpful: PostgreSQL was up, and we already had the job safely stored.

Now the API commits the job as `PENDING`, *tries* to publish, and returns **202** either way. If the publish failed, the sweeper relays it once Redis is back. This is the transactional outbox pattern, with the jobs table itself serving as the outbox. The chaos test (§10) stops Redis, submits a job, and gets `202 PENDING` back in **236 ms**. After Redis restarts, the job completes with no intervention.

## 6. At-least-once delivery, and the three defences against it

Phase 2 sets `task_acks_late=True` and `task_reject_on_worker_lost=True`: a message is only acknowledged after the task finishes. That fixes "worker dies, message lost", but it's a trade, not a free win. **Delivery is now at-least-once.** A job *will* sometimes be delivered more than once:

- A worker child process is killed → Celery requeues the message immediately.
- A whole worker container dies → the unacked message reappears after Redis's `visibility_timeout` (1h).
- The sweeper re-publishes a `PENDING` job whose first publish actually succeeded.

Three layers make that harmless:

**Defence 1: the guarded claim.** Claiming is `UPDATE ... WHERE status IN ('PENDING','QUEUED')`. A second delivery of a job that's already `RUNNING` or finished matches zero rows and is dropped. The chaos test publishes the same job ID three extra times: `attempts` stays at 1.

**Defence 2: the fencing token.** This one is subtle. Suppose attempt 1's worker doesn't die but *freezes* (a long GC pause, a VM migration, a network partition from the database). Its lease expires, the sweeper declares attempt 1 `LOST`, and attempt 2 starts on a healthy worker. Then the zombie wakes up and tries to write its result.

Every outcome write is fenced by the attempt number:

```python
_guarded_update(db, job_id,
    from_statuses=[JobStatus.RUNNING], to_status=JobStatus.SUCCEEDED,
    extra_where=[Job.attempts == attempt],   # ← the fencing token
    ...)
```

The zombie writes with `attempts = 1`, but the row says `attempts = 2`, so the update matches nothing and is logged as `Stale success discarded (fenced)`. This is the same idea as fencing tokens in distributed locks (Kleppmann's well-known critique of Redlock): **a lock or lease alone can't stop a paused process from acting on stale authority. A monotonically increasing token checked at the point of the write can.** `test_zombie_worker_cannot_overwrite_newer_attempt` reproduces the scenario.

**Defence 3: idempotent side effects.** Defences 1 and 2 protect *FlowGrid's own state*. They can't un-send an email. If attempt 1 wrote 120 records and sent 120 notifications before crashing, attempt 2 will redo that work. The handler has to make redoing it harmless:

```python
# store: keyed by (batch_id, record_id); a re-run inserts nothing new
insert(ProcessedRecord).values(...).on_conflict_do_nothing(index_elements=["batch_id", "record_id"])

# notify: one notification per business event, ever
insert(Notification).values(dedupe_key=f"record-processed:{batch_id}:{record_id}", ...)
    .on_conflict_do_nothing(index_elements=["dedupe_key"]).returning(Notification.id)
```

`process_records` has a `fail_first_attempt_after` testing aid that commits N records and then raises a transient error on attempt 1. In the chaos run, a 500-record batch crashed at record 250 and was retried. The result: **500 rows in `processed_records`, 500 rows in `notifications`, not 750.** The job's result reports `notifications_sent: 250, notifications_deduplicated: 250`.

The dedupe key is a *business* key (`batch_id:record_id`), not the job ID. A replayed job, or a second job for the same batch, also won't re-notify. That's the semantics you actually want.

> **Honest limit:** the `notifications` table is an outbox. Nothing in FlowGrid actually *delivers* notifications yet. Exactly-once delivery to a real email/SMS provider would also need the provider to accept an idempotency key. No queue can promise that on its own.

## 7. Idempotency keys for clients

Network retries happen on the *client* side too: a request times out, the client retries, and the server might process both. `Idempotency-Key` handles that:

| Request | Response |
| --- | --- |
| New key | `202`, job created, key stored **in the same transaction** as the job |
| Same key, same body | `200`, the original job, header `Idempotent-Replayed: true` |
| Same key, different body | `409 Conflict`, with the original job's ID |

Some details that matter:

- **Keys are scoped per API key.** The primary key is `(api_key_id, key)`, so two clients can both use `"order-1"`.
- **The body hash is computed after normalisation.** `{"record_ids":[1]}` and `{"record_ids":[1],"delay_ms":0}` are the same request once defaults are filled in, so they hash the same.
- **Concurrency is handled by the database, not by a lock.** Two simultaneous requests can both pass the "does this key exist?" check. Only one `INSERT` can win the primary key. The loser gets an `IntegrityError`, rolls back, reads the winner's row, and returns the winner's job. The test simulates exactly this interleaving by hiding the key from the first lookup.
- **Keys expire after 24h** (the sweeper purges them). After that, the same key creates a new job. That's the same contract Stripe documents for its idempotency keys.

## 8. Security: API keys, ownership, rate limits

### API keys, hashed with SHA-256 (not bcrypt)

Keys look like `fg_<43 chars>`: 256 bits from `secrets.token_urlsafe(32)`. The database stores only `sha256(key)`, plus an 11-character prefix for identifying keys in logs and the CLI. The raw key is printed once at creation time.

Why not bcrypt or argon2? Slow hashes exist to make *guessing low-entropy human passwords* expensive. A 256-bit random key can't be guessed however fast the hash is, so a slow hash would just add latency to every request. (Revocation is a timestamp, and `python -m app.cli revoke-api-key` takes effect on the next request.)

Why API keys and not JWT? FlowGrid's clients are *programs* submitting jobs. API keys are the standard tool for machine-to-machine access, and they're instantly revocable. JWTs make more sense for user sessions in a multi-user UI, which isn't in scope.

### Ownership: 404, not 403

Every job is stamped with the `api_key_id` that created it, and every query filters on it. Asking for another client's job returns **404**, the same response as a job that doesn't exist. A 403 would confirm that the ID is valid, and that leaks information.

> Jobs created in Phase 1 (before keys existed) have no owner after the migration, so no key can see them. That's the safe default for unowned data.

### Rate limiting: a token bucket in Lua

Each API key gets a bucket of 100 tokens that refills at 20 per second. That allows bursts of up to 100 requests while capping the sustained rate at 20/s. The bucket lives in Redis and is updated by a Lua script, which makes the read-modify-write atomic across any number of API processes. The script reads the clock with Redis's own `TIME` command, so two API servers with skewed clocks still agree on how many tokens are left.

Every response carries `X-RateLimit-Limit` and `X-RateLimit-Remaining`. A throttled one returns **429** with `Retry-After`. In the smoke test, a burst of 150 requests got **112 allowed, 38 throttled, `Retry-After: 1`**.

**The limiter fails open.** If Redis is down, requests are allowed (and a warning is logged). That's a deliberate trade: with the outbox, a Redis outage is a *partial* outage in which the API can still accept jobs. Failing closed would let the rate limiter turn it into a total one.

### Request hardening

- Request bodies over 1 MB are rejected with **413** before they're read (based on `Content-Length`).
- `max_attempts` (1–10) and `timeout_seconds` (1–600) are bounded in the schema.
- Every response carries an `X-Request-ID`. A caller-supplied ID is propagated only if it matches `[A-Za-z0-9._-]{1,64}`, so nobody can inject newlines into the logs through a header.

## 9. Structured logs that follow the job

With `LOG_FORMAT=json` (the default in Compose), every line is a JSON object. The useful part is that **context is attached automatically**. The executor wraps each attempt in `log_context(job_id=..., attempt=..., worker_id=...)`, which is a `ContextVar`, and a logging filter stamps those fields onto every record emitted inside it, including records from handler code that knows nothing about logging context:

```json
{"ts": "2026-11-20T21:36:38.681+00:00", "level": "WARNING", "logger": "app.services.executor",
 "msg": "Attempt failed", "error_type": "TransientJobError",
 "error": "simulated transient failure on attempt 2", "retryable": true,
 "job_id": "149fc995-e61f-424c-af58-41438c23d8cd", "job_type": "flaky", "attempt": 2,
 "worker_id": "celery@6765502fb6d8"}
```

API requests get the same treatment with `request_id` and the caller's key prefix. Celery's own logs go through the same formatter: connecting to Celery's `setup_logging` signal stops it from installing its own handlers.

## 10. How it was verified

| Layer | Result |
| --- | --- |
| pytest on **SQLite** | **80 / 80** passed |
| pytest on **PostgreSQL** | **80 / 80** passed |
| `alembic check` after upgrade | No drift between migration and models |
| Migration on live Phase 1 data | 30 existing jobs upgraded `0001 → 0002`, backfilled with `max_attempts=1` |
| `downgrade 0001 → upgrade head` round trip | Clean, no drift |
| `make smoke` (real stack) | **11 / 11** |
| `make chaos` (real failures) | **5 / 5** |
| Dashboard in the browser | Retry flow visible live (`RETRY_SCHEDULED` → `SUCCEEDED`, attempt history), no console errors |

The chaos run, verbatim:

```
FlowGrid chaos test against http://localhost:8000
  ok    worker SIGKILLed mid-job -> lease recovery  [31.8s]  (lease expired -> RUNNING -> RETRY_SCHEDULED
        -> QUEUED -> RUNNING -> SUCCEEDED; attempts ['LOST', 'SUCCEEDED']; recovered 32s after kill)
  ok    timeout on every attempt -> DEAD_LETTER  [7.0s]  (JobTimeoutError: exceeded timeout of 2s;
        attempts ['TIMED_OUT', 'TIMED_OUT'])
  ok    Redis outage -> outbox delivers later  [11.0s]  (accepted as PENDING in 236 ms while Redis was
        down, delivered by sweeper after restart)
  ok    crash mid-batch -> no duplicate side effects  [3.0s]  (500 records, 2 attempts -> 500 stored
        rows, 500 notifications (no duplicates))
  ok    duplicate message delivery -> runs once  [3.4s]  (3 duplicate messages delivered, attempts still 1)

5/5 checks passed
```

A second run on the final build reproduced it: 5/5 again, with recovery 31s after the kill and a 254 ms submit during the Redis outage.

The 32 seconds of recovery in the first scenario adds up as expected: the lease was `8s timeout + 15s grace = 23s` after the attempt started, then a 1–2s backoff, then the 6s job ran again. **Recovery time after a worker crash is bounded by `timeout + grace`.** That's the cost of not having heartbeats, and it's why per-job timeouts should be set close to the job's real runtime rather than generously.

## 11. Bugs the build surfaced

**1. `rowcount` lies about `ON CONFLICT DO NOTHING`, but only on PostgreSQL.** The handler originally used `result.rowcount == 1` to decide whether a notification was newly inserted. All 80 tests passed on SQLite. On PostgreSQL, three failed: every notification counted as a duplicate. For ORM-level `INSERT` statements, `rowcount` isn't reliable across drivers. The fix is `RETURNING id`, which returns a row if and only if the insert happened, and works the same on both databases. **This is exactly why the suite runs on two databases:** SQLite for speed, PostgreSQL because it's the one that actually ships.

**2. A rate-limit test that couldn't fail closed.** The first version of the 429 test overrode the limiter dependency with `lambda: InMemoryTokenBucket(...)`, which builds a brand-new, full bucket on *every request*, so it never throttled anything. The production code avoids this with `@lru_cache` on `get_rate_limiter()`, but the test bug is the same mistake you could easily make in production: a rate limiter constructed per request is no rate limiter at all.

**3. `urllib` header case.** The smoke test's 429 check reported `Retry-After=None` even though the header was there. `urllib` keeps header-name case on successful responses but lowercases it on `HTTPError` responses. The fix was to normalize header names in the test client. It's a reminder that when a check fails, first verify the check itself.

**4. Testing early rejection is itself racy.** The first 413 check uploaded a real 2 MB body. It passed twice and then failed with `BrokenPipeError`. The middleware did exactly what it should, answering 413 from the headers alone and closing the connection, but that meant the server hung up while the client was still uploading. Whether the client read the 413 first or hit a broken pipe came down to timing. The fix tests the property directly: `http.client` sends only headers declaring `Content-Length: 2000000`, then reads the response. Getting an immediate 413 with zero body bytes sent *proves* the body was never read. It passed 3/3 consecutive runs afterwards.

## 12. What Phase 2 still doesn't guarantee

- **Exactly-once execution.** Not possible, and not claimed. FlowGrid guarantees at-least-once delivery, at-most-once *state transitions* (guards + fencing), and gives handlers the tools for idempotent side effects. Handlers that call external systems must pass idempotency keys downstream.
- **Fast recovery for long jobs.** Recovery time is `timeout + 15s`. A job with a 10-minute timeout that crashes at second 1 waits about 10 minutes to be retried. Heartbeat-extended leases would fix this, at the cost of a heartbeat thread per job.
- **Sweeper liveness isn't monitored.** If every sweeper is down, retries and recovery pause (nothing is lost; they resume on restart). Phase 3's metrics should alert on "oldest due retry" age.
- **The body-size limit is based on `Content-Length`.** Chunked uploads without that header aren't capped by this middleware.
- **Rate limits are per key, not per IP**, and unauthenticated requests (which get a 401) aren't rate limited.

## 13. Next: Phase 3 (workflow orchestration)

Phase 2 made single jobs reliable. Phase 3 composes them:

1. **Multi-step workflows (DAGs):** tasks with dependencies, where step B runs only after A succeeds, built on the same claim / lease / retry machinery.
2. **Scheduled jobs:** `run_at` and cron-style schedules. The sweeper is already a natural home for this.
3. **Multiple workers and throughput tests:** real measurements as workers scale.
4. **Live monitoring:** Server-Sent Events instead of dashboard polling, and operational metrics (queue depth, retry age, attempt outcomes).
5. **Deployment documentation.**
