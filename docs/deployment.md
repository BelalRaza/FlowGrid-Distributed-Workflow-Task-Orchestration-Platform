# Deploying FlowGrid

This guide covers running FlowGrid outside of `make up`: what each component needs, how to scale it, what to monitor, and how to operate it day to day. Everything here follows from how the system is built. The reasons are in the [build log](blog/).

## 1. Components

| Component | Command | Stateless? | Replicas | Notes |
| --- | --- | --- | --- | --- |
| **Migrations** | `alembic upgrade head` | — | Run once per deploy, **before** the others | A one-shot job (Compose: `migrate` service; Kubernetes: a Job or init step) |
| **API** | `uvicorn app.main:app --host 0.0.0.0 --port 8000` | Yes | 2+ behind a load balancer | Holds one PostgreSQL `LISTEN` connection for live events |
| **Worker** | `celery -A app.tasks.celery_app worker --concurrency=N --without-gossip --without-mingle` | Yes | Scale with load | Needs a long stop grace period (see §4) |
| **Sweeper** | `python -m app.sweeper` | Yes | 1–2 | Safe to run several: every action is a guarded `UPDATE` or compare-and-swap |
| **PostgreSQL** | managed (RDS, Cloud SQL, …) | **No: source of truth** | Primary + standby | Backups and point-in-time recovery matter here, not in Redis |
| **Redis** | managed or self-hosted | Mostly | 1 (+ replica) | Losing it loses only in-flight messages; FlowGrid recovers them (§5) |

All application components use the same image (`Dockerfile`). Configuration is entirely environment variables; see [`.env.example`](../.env.example).

## 2. Configuration checklist

| Variable | Production guidance |
| --- | --- |
| `DATABASE_URL` | Use a dedicated role with least privilege; pass via a secret, not the image |
| `REDIS_URL` | Use `rediss://` (TLS) and a password when Redis is not on a private network |
| `LOG_FORMAT=json` | Ship stdout to your log pipeline; every line has `request_id` / `job_id` / `attempt` |
| `LEASE_GRACE_SECONDS` | Must exceed the hard-kill margin (5s). Recovery time after a crash is `timeout + grace` |
| `MAX_TIMEOUT_SECONDS` | Keep below Redis' `visibility_timeout` (3600s in `celery_app.py`) |
| `QUEUED_REPUBLISH_AFTER_SECONDS` | Above your worst normal queue wait (default 300s). Compose sets 60s only for the chaos demo |
| `RATE_LIMIT_CAPACITY` / `RATE_LIMIT_REFILL_PER_SECOND` | Per API key. Batch submits cost one token for up to 500 jobs |
| `SSE_KEEPALIVE_SECONDS` | Below your load balancer's idle timeout (e.g. ALB: 60s) |

**Secrets:** API keys are created with `python -m app.cli create-api-key --name <client>` (run it as a one-off task) and are shown once. Only SHA-256 hashes are stored. Revoke with `revoke-api-key --prefix fg_xxxxxxxx`; open SSE streams notice within `SSE_KEEPALIVE_SECONDS`.

## 3. Networking

* Terminate TLS at the load balancer. API keys travel in the `Authorization` header and must never cross the network in plain text.
* `/metrics` is unauthenticated and reveals aggregate counts across all clients. **Do not expose it publicly**: allow it only from the Prometheus network, or block the path at the load balancer.
* `/api/v1/events` is a long-lived streaming response. Disable response buffering for it at the proxy (FlowGrid already sends `X-Accel-Buffering: no` for nginx) and make sure idle timeouts exceed the keepalive interval.
* Health checks: use `/health/live` for **liveness** (no dependency checks, so a database blip does not restart every API container) and `/health` for **readiness** (PostgreSQL + Redis).

## 4. Scaling and shutdown

**Workers** are the main scaling knob. Each process runs one job at a time (`worker_prefetch_multiplier=1`):

* I/O-bound jobs: throughput ≈ processes ÷ average job time. The [benchmark](benchmarks/phase-3-results.md) measured 88–91% of that ideal at 2–8 processes.
* CPU-bound jobs: about one process per core.
* Watch `flowgrid_oldest_queued_job_age_seconds`. If it keeps growing, add workers.

**Graceful shutdown:** on SIGTERM, Celery stops taking messages and finishes the ones in progress. Give workers a stop grace period at least as long as your longest job timeout (Compose: `stop_grace_period: 2m`; Kubernetes: `terminationGracePeriodSeconds`). A job killed anyway is not lost: its lease expires and the sweeper retries it.

**Database connections:** every API and worker process has its own SQLAlchemy pool (default 5 + 10 overflow), plus one `LISTEN` connection per API process. Size `max_connections` for `(api_procs + worker_procs + sweepers) × 15` or put a pooler in front.

> ⚠️ **PgBouncer in transaction mode breaks `LISTEN/NOTIFY`.** If you use a pooler, either give the API a direct connection (or a session-mode pool) for its listener, or live events will silently stop. Everything else works through transaction pooling.

## 5. Failure modes and what happens

| Failure | Behaviour | Recovery time |
| --- | --- | --- |
| Worker process / container dies mid-job | Lease expires → attempt marked `LOST` → retried with backoff | `timeout + LEASE_GRACE_SECONDS` |
| Redis down | API still accepts jobs (`PENDING`, outbox); rate limiter fails open; `/health` → 503 | Delivered within ~`PENDING_REPUBLISH_AFTER_SECONDS` of Redis returning |
| Redis loses data (restart without persistence, failover) | `QUEUED` jobs re-published by the sweeper | `QUEUED_REPUBLISH_AFTER_SECONDS` |
| PostgreSQL down | API returns errors; workers cannot claim; nothing new is lost (messages wait in Redis) | Resumes when PostgreSQL returns |
| Sweeper down | Retries, delayed jobs, schedules and crash recovery pause (nothing lost); `flowgrid_sweeper_lag_seconds` grows | Catches up on restart; missed cron ticks fire **once** (coalesced) |
| Worker crashes between a workflow step succeeding and unblocking its dependents | Sweeper finds the `BLOCKED` step whose parents are done and unblocks it | Next sweep (~1s) |

Redis persistence (AOF) reduces how often the "Redis loses data" recovery path is needed, but it is not required for correctness.

## 6. Monitoring

Start with `make monitoring` (Compose profile) or point your Prometheus at each API replica's `/metrics`. The rules in [`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml):

| Alert | Fires when | Usually means |
| --- | --- | --- |
| `FlowGridDatabaseUnreachable` | `flowgrid_database_up == 0` for 1m | PostgreSQL down, or the API is down |
| `FlowGridSweeperLagging` | due jobs > 60s overdue for 2m | No sweeper running, or it is overloaded |
| `FlowGridQueueBacklog` | oldest `QUEUED` job > 5 min | Not enough workers |
| `FlowGridBrokerPublishFailing` | oldest `PENDING` job > 2 min | Redis unreachable from the API / sweeper |
| `FlowGridDeadLettersGrowing` | > 10 dead letters in 30 min | A downstream dependency is failing |
| `FlowGridExpiredLeases` | expired leases not recovered for 5m | Sweeper not keeping up |
| `FlowGridHighErrorRate` | > 5% HTTP 5xx for 5m | API or database problems |

The job/run gauges are read from PostgreSQL at scrape time, so every API replica reports the same values. Aggregate them with `max()`, not `sum()`.

## 7. Runbook

**Dead letters piling up.** List them with `GET /api/v1/jobs?status=DEAD_LETTER`, read the attempt history (`/jobs/{id}/attempts`), fix the upstream cause, then replay: `POST /api/v1/jobs/{id}/replay` creates a new job and leaves the original untouched for audit. For workflows, start a new run.

**A job looks stuck.**
* `PENDING` → can't reach Redis (check `/health`).
* `QUEUED` → no free workers (check the queue-age metric) or the message was lost (re-published automatically).
* `RUNNING` past its timeout → the lease will expire and the sweeper retries it.
* `BLOCKED` → a parent step hasn't succeeded yet; check the run.

**Revoking a leaked key.** Run `python -m app.cli revoke-api-key --prefix <prefix>`. Requests fail immediately, and open SSE streams close within the keepalive interval.

**Deploying a new version.** Run migrations first, then roll the API, workers and sweeper. Check each migration for compatibility with the *old* code before a zero-downtime rollout. `0003` is safe: it only adds nullable columns, tables and allowed statuses. `0002` is not: it added `NOT NULL` columns (`max_attempts`, `timeout_seconds`) and dropped their defaults after the backfill, so Phase 1 code can no longer insert jobs. Run upgrades like that in a short maintenance window, or keep the server defaults until the old code is gone (expand → migrate → contract). When a job type is *removed*, drain or replay its queued jobs first: a worker without the handler fails them permanently (`UnknownJobTypeError`) rather than guessing.

## 8. Known limits

* **Recovery after a crash takes `timeout + grace`,** because there is no heartbeat. Keep timeouts close to real runtimes.
* **Live events are hints, not a durable log.** A client that disconnects should refetch state (the dashboard does).
* **Cron runs in UTC with minute resolution.**
* **The body-size limit relies on `Content-Length`.** Enforce a hard limit at the proxy as well.
* **Rate limits are per API key, not per IP.** Unauthenticated requests (401) are cheap, but add IP-level limiting at the edge if you are exposed to the internet.
