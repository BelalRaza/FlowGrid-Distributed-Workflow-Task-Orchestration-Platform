"""State-machine tests: the guarded transitions that keep job status correct."""

import uuid

from app.db.models import JobStatus
from app.db.session import get_sessionmaker
from app.services import jobs as jobs_service
from app.services.executor import run_job


def _noop_publisher(job_id):
    pass


def test_job_is_queued_after_successful_publish(db):
    job = jobs_service.submit_job(db, "echo", {"message": "x"}, _noop_publisher)
    assert job.status == JobStatus.QUEUED.value
    assert job.queued_at is not None
    assert job.attempts == 0


def test_only_one_claim_succeeds(db):
    job = jobs_service.submit_job(db, "echo", {"message": "x"}, _noop_publisher)

    first = jobs_service.claim_job(db, job.id, "worker-a")
    second = jobs_service.claim_job(db, job.id, "worker-b")

    assert first is not None and first.worker_id == "worker-a"
    assert second is None


def test_duplicate_delivery_does_not_rerun_a_finished_job(db):
    job = jobs_service.submit_job(db, "echo", {"message": "x"}, _noop_publisher)

    assert run_job(job.id, "worker-a") == JobStatus.SUCCEEDED
    # Same message delivered again (e.g. broker redelivery): must be a no-op.
    assert run_job(job.id, "worker-b") is None

    db.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    assert job.attempts == 1
    assert job.worker_id == "worker-a"


def test_api_does_not_overwrite_status_set_by_a_fast_worker(db):
    """The worker may claim the job before the API records PENDING -> QUEUED."""

    def fast_worker_publisher(job_id):
        with get_sessionmaker()() as worker_db:
            assert jobs_service.claim_job(worker_db, job_id, "fast-worker") is not None

    job = jobs_service.submit_job(db, "echo", {"message": "x"}, fast_worker_publisher)
    assert job.status == JobStatus.RUNNING.value  # not clobbered back to QUEUED


def test_terminal_status_cannot_be_changed(db):
    job = jobs_service.submit_job(db, "echo", {"message": "x"}, _noop_publisher)
    run_job(job.id, "worker-a")

    assert jobs_service.mark_failed(db, job.id, "late error") is False
    db.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value


def test_run_job_with_unknown_id_is_a_noop():
    assert run_job(uuid.uuid4(), "worker-a") is None


def test_unregistered_handler_fails_the_job(db, monkeypatch):
    """A worker running older code than the API must fail the job, not crash."""
    job = jobs_service.submit_job(db, "echo", {"message": "x"}, _noop_publisher)

    from app.tasks import handlers

    monkeypatch.delitem(handlers.REGISTRY, "echo")
    assert run_job(job.id, "old-worker") == JobStatus.FAILED
    db.refresh(job)
    assert job.error.startswith("UnknownJobTypeError")
