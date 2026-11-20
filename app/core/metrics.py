"""Prometheus metrics.

Two kinds:

* **Database-derived gauges**, computed at scrape time (jobs by status, queue
  age, sweeper lag, expired leases...). Workers run in many processes and
  containers; counting in-process would need Prometheus' multiprocess mode and
  still miss work done by a container that died. PostgreSQL already knows the
  truth, so the API reads it. Every API replica reports the same values: use
  ``max()`` across instances in PromQL.
* **In-process HTTP metrics** per API replica, labelled by *route template*
  (``/api/v1/jobs/{job_id}``), never the raw path. Raw paths would create one
  time series per job ID and eventually take Prometheus down (unbounded
  cardinality).
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from sqlalchemy import func, select

from app.db.models import AttemptStatus, Job, JobAttempt, JobStatus, RunStatus, Schedule, WorkflowRun, as_utc, utcnow
from app.db.session import get_sessionmaker

logger = logging.getLogger(__name__)


def _age_seconds(oldest) -> float:
    return max(0.0, (utcnow() - as_utc(oldest)).total_seconds()) if oldest else 0.0


class DatabaseCollector:
    def __init__(self, subscriber_count: Callable[[], int]) -> None:
        self._subscriber_count = subscriber_count

    def describe(self):  # avoid a DB query at registration time
        return []

    def collect(self):
        up = GaugeMetricFamily("flowgrid_database_up", "1 if the metrics query reached PostgreSQL")
        try:
            families = list(self._collect_db())
        except Exception:
            logger.exception("Metrics collection failed")
            up.add_metric([], 0)
            yield up
            return
        up.add_metric([], 1)
        yield up
        yield from families
        sse = GaugeMetricFamily("flowgrid_sse_clients", "Open live-event (SSE) connections on this API instance")
        sse.add_metric([], self._subscriber_count())
        yield sse

    def _collect_db(self):
        now = utcnow()
        with get_sessionmaker()() as db:
            jobs = GaugeMetricFamily("flowgrid_jobs", "Jobs by status", labels=["status"])
            counts = {s.value: 0 for s in JobStatus}
            counts.update(dict(db.execute(select(Job.status, func.count()).group_by(Job.status)).all()))
            for status, n in counts.items():
                jobs.add_metric([status], n)
            yield jobs

            runs = GaugeMetricFamily("flowgrid_workflow_runs", "Workflow runs by status", labels=["status"])
            rcounts = {s.value: 0 for s in RunStatus}
            rcounts.update(dict(db.execute(select(WorkflowRun.status, func.count()).group_by(WorkflowRun.status)).all()))
            for status, n in rcounts.items():
                runs.add_metric([status], n)
            yield runs

            attempts = CounterMetricFamily("flowgrid_job_attempts", "Job attempts by outcome", labels=["status"])
            acounts = {s.value: 0 for s in AttemptStatus}
            acounts.update(dict(db.execute(select(JobAttempt.status, func.count()).group_by(JobAttempt.status)).all()))
            for status, n in acounts.items():
                attempts.add_metric([status], n)
            yield attempts

            def gauge(name: str, doc: str, value: float) -> GaugeMetricFamily:
                g = GaugeMetricFamily(name, doc)
                g.add_metric([], value)
                return g

            yield gauge(
                "flowgrid_oldest_queued_job_age_seconds",
                "How long the oldest QUEUED job has waited for a worker (queue backlog)",
                _age_seconds(db.scalar(select(func.min(Job.queued_at)).where(Job.status == JobStatus.QUEUED.value))),
            )
            yield gauge(
                "flowgrid_oldest_pending_job_age_seconds",
                "How long the oldest PENDING job has waited to be published (broker trouble)",
                _age_seconds(db.scalar(select(func.min(Job.updated_at)).where(Job.status == JobStatus.PENDING.value))),
            )
            yield gauge(
                "flowgrid_sweeper_lag_seconds",
                "How overdue the most overdue SCHEDULED/RETRY_SCHEDULED job is (sweeper down or overloaded)",
                _age_seconds(db.scalar(
                    select(func.min(Job.next_attempt_at)).where(
                        Job.status.in_([JobStatus.SCHEDULED.value, JobStatus.RETRY_SCHEDULED.value]),
                        Job.next_attempt_at <= now,
                    )
                )),
            )
            yield gauge(
                "flowgrid_expired_leases",
                "RUNNING jobs past their lease (should be ~0; recovered by the sweeper)",
                db.scalar(select(func.count()).select_from(Job).where(
                    Job.status == JobStatus.RUNNING.value, Job.lease_expires_at < now)) or 0,
            )
            yield gauge(
                "flowgrid_schedules_enabled", "Enabled recurring schedules",
                db.scalar(select(func.count()).select_from(Schedule).where(Schedule.enabled.is_(True))) or 0,
            )


class Metrics:
    def __init__(self, subscriber_count: Callable[[], int]) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "flowgrid_http_requests", "HTTP requests", ["method", "route", "status"], registry=self.registry
        )
        self.latency = Histogram(
            "flowgrid_http_request_duration_seconds",
            "HTTP request latency",
            ["method", "route"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
            registry=self.registry,
        )
        self.registry.register(DatabaseCollector(subscriber_count))

    def observe(self, method: str, route: str, status: int, seconds: float) -> None:
        self.requests.labels(method, route, str(status)).inc()
        self.latency.labels(method, route).observe(seconds)
