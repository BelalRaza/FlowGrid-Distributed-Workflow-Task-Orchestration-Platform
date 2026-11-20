"""Throughput benchmark: how does FlowGrid scale with worker containers?

For each worker count (default 1, 2, 4; each container runs WORKER_CONCURRENCY=2
processes) it runs two workloads, each under its own API key so the numbers
are isolated:

* overhead  - N `echo` jobs: no work at all, so this measures the platform's
              own per-job cost (HTTP, PostgreSQL transitions, Redis, Celery).
* io-bound  - N `sleep 0.2s` jobs: stands in for jobs that wait on I/O
              (external APIs). Ideal throughput = processes / 0.2s.

Jobs are submitted with the batch endpoint. Latencies come from PostgreSQL
timestamps, not client-side polling:
    queue wait = started_at - created_at,   end-to-end = finished_at - created_at

    make up && make bench      # results -> docs/benchmarks/phase-3-results.md
"""

from __future__ import annotations

import argparse
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flowgrid_client import PROJECT_DIR, Client, compose, create_api_key, psql

WORKLOADS = {
    "overhead": {"type": "echo", "payload": {"message": "bench"}},
    "io-bound": {"type": "sleep", "payload": {"seconds": 0.2}},
}


def scale_workers(n: int) -> None:
    before = len(compose("ps", "worker", "--status", "running", "-q").split())
    since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    compose("up", "-d", "--no-recreate", "--scale", f"worker={n}", "worker")
    # Scaling up: wait until every *new* container logged "ready". Scaling
    # down starts nothing new, so only the container count matters.
    new = max(0, n - before)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        running = compose("ps", "worker", "--status", "running", "-q").split()
        ready = compose("logs", "worker", "--since", since).count(" ready.") if new else 0
        if len(running) == n and ready >= new:
            time.sleep(2)  # let the new consumers finish subscribing
            return
        time.sleep(1)
    raise RuntimeError(f"workers did not reach {n}")


def run_workload(base: str, workers: int, name: str, count: int) -> dict:
    key_name = f"bench-{name}-{workers}w-{int(time.time())}"
    c = Client(base, create_api_key(key_name))
    template = WORKLOADS[name]
    t0 = time.monotonic()
    for start in range(0, count, 500):
        chunk = [template] * min(500, count - start)
        status, body, _ = c.call("POST", "/api/v1/jobs/batch", {"jobs": chunk})
        assert status == 202, body
    while True:
        stats = c.call("GET", "/api/v1/jobs/stats")[1]["by_status"]
        if stats["SUCCEEDED"] + stats["FAILED"] + stats["DEAD_LETTER"] >= count:
            break
        if time.monotonic() - t0 > 600:
            raise RuntimeError(f"timed out: {stats}")
        time.sleep(0.25)

    row = psql(f"""
        SELECT count(*),
               count(*) FILTER (WHERE j.status = 'SUCCEEDED'),
               count(*) FILTER (WHERE j.attempts > 1),
               EXTRACT(EPOCH FROM max(j.finished_at) - min(j.created_at)),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM j.started_at - j.created_at)),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM j.started_at - j.created_at)),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM j.finished_at - j.created_at)),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM j.finished_at - j.created_at)),
               percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM j.finished_at - j.created_at)),
               count(DISTINCT j.worker_id)
        FROM jobs j JOIN api_keys k ON k.id = j.api_key_id
        WHERE k.name = '{key_name}'
    """).split("|")
    total, ok, retried, span, qw50, qw95, e50, e95, e99, distinct_workers = row
    return {
        "workers": workers, "processes": workers * 2, "workload": name, "jobs": int(total),
        "succeeded": int(ok), "retried": int(retried), "span_s": float(span),
        "throughput": int(total) / float(span), "queue_p50": float(qw50), "queue_p95": float(qw95),
        "e2e_p50": float(e50), "e2e_p95": float(e95), "e2e_p99": float(e99),
        "worker_hosts": int(distinct_workers),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--overhead-jobs", type=int, default=1000)
    parser.add_argument("--io-jobs", type=int, default=400)
    args = parser.parse_args()
    counts = {"overhead": args.overhead_jobs, "io-bound": args.io_jobs}

    results = []
    try:
        for n in [int(x) for x in args.workers.split(",")]:
            print(f"-- scaling to {n} worker container(s) x 2 processes")
            scale_workers(n)
            for name in WORKLOADS:
                r = run_workload(args.base_url, n, name, counts[name])
                results.append(r)
                print(f"   {name:<9} {r['jobs']:>5} jobs  {r['throughput']:7.1f} jobs/s  "
                      f"e2e p50 {r['e2e_p50']:.2f}s p95 {r['e2e_p95']:.2f}s  "
                      f"(ok {r['succeeded']}, retried {r['retried']}, {r['worker_hosts']} worker hosts)")
    finally:
        scale_workers(1)

    ncpu = compose("exec", "-T", "postgres", "nproc").strip()
    lines = [
        "# Phase 3 throughput benchmark",
        "",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} by `scripts/benchmark.py`. "
        f"Host: {platform.system()} {platform.machine()}, Docker VM with {ncpu} CPUs; every service "
        "(PostgreSQL, Redis, API, sweeper, workers) shares that VM. Each worker container runs 2 Celery processes.",
        "",
        "| Workload | Worker containers (processes) | Jobs | Throughput (jobs/s) | Queue wait p50 / p95 | "
        "End-to-end p50 / p95 / p99 | Retried |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in results:
        lines.append(
            f"| {r['workload']} | {r['workers']} ({r['processes']}) | {r['jobs']} | {r['throughput']:.1f} | "
            f"{r['queue_p50']:.2f}s / {r['queue_p95']:.2f}s | "
            f"{r['e2e_p50']:.2f}s / {r['e2e_p95']:.2f}s / {r['e2e_p99']:.2f}s | {r['retried']} |"
        )
    lines += ["", "Ideal io-bound throughput = processes / 0.2 s, i.e. 5 jobs/s per process: 10 / 20 / 40 jobs/s for 1 / 2 / 4 containers. Overhead jobs do no work, so they measure the platform's own per-job cost; their scaling is limited by the single shared VM (PostgreSQL, Redis and the API compete with the workers for the same CPUs).", ""]
    out = PROJECT_DIR / "docs" / "benchmarks" / "phase-3-results.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print(f"\nwrote {out.relative_to(PROJECT_DIR)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
