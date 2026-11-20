# FlowGrid build log

Technical write-ups for each phase of FlowGrid: what was built, why it was designed that way, what broke along the way, and what was verified.

| Phase | Post | Status |
| --- | --- | --- |
| 1 — End-to-end MVP | [One reliable path from API to worker](phase-1-end-to-end-mvp.md): guarded state transitions, PostgreSQL as source of truth | ✅ Complete |
| 2 — Reliability & security | [Making failure boring](phase-2-reliability-and-security.md): retries, leases, outbox, idempotency, fencing, auth, rate limits | ✅ Complete |
| 3 — Workflow orchestration | [From jobs to workflows](phase-3-workflow-orchestration.md): DAGs, exactly-once cron, live events, metrics, benchmark | ✅ Complete |

Related: [Deployment guide](../deployment.md) · [Benchmark results](../benchmarks/phase-3-results.md)
