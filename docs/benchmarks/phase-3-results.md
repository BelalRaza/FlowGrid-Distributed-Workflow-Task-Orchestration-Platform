# Phase 3 throughput benchmark

Generated 2026-11-20 22:15 UTC by `scripts/benchmark.py`. Host: Darwin arm64, Docker VM with 8 CPUs; every service (PostgreSQL, Redis, API, sweeper, workers) shares that VM. Each worker container runs 2 Celery processes.

| Workload | Worker containers (processes) | Jobs | Throughput (jobs/s) | Queue wait p50 / p95 | End-to-end p50 / p95 / p99 | Retried |
| --- | --- | --- | --- | --- | --- | --- |
| overhead | 1 (2) | 1000 | 256.6 | 1.81s / 3.18s | 1.82s / 3.18s / 3.32s | 0 |
| io-bound | 1 (2) | 400 | 9.1 | 21.96s / 41.74s | 22.17s / 41.94s / 43.69s | 0 |
| overhead | 2 (4) | 1000 | 396.0 | 1.12s / 1.85s | 1.13s / 1.86s / 1.99s | 0 |
| io-bound | 2 (4) | 400 | 17.9 | 11.10s / 21.04s | 11.31s / 21.26s / 22.14s | 0 |
| overhead | 4 (8) | 1000 | 466.2 | 0.91s / 1.43s | 0.92s / 1.44s / 1.50s | 0 |
| io-bound | 4 (8) | 400 | 35.2 | 5.61s / 10.68s | 5.82s / 10.89s / 11.35s | 0 |

Ideal io-bound throughput = processes / 0.2 s, i.e. 5 jobs/s per process: 10 / 20 / 40 jobs/s for 1 / 2 / 4 containers. Overhead jobs do no work, so they measure the platform's own per-job cost; their scaling is limited by the single shared VM (PostgreSQL, Redis and the API compete with the workers for the same CPUs).
