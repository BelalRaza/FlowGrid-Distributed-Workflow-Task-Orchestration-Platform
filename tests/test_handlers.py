"""Unit tests for handlers: no broker or HTTP involved."""

import uuid

import pytest
from sqlalchemy import func, select

from app.db.models import Notification, ProcessedRecord
from app.db.session import get_sessionmaker
from app.services.retry import PermanentJobError, TransientJobError
from app.tasks.handlers import (
    InvalidPayloadError,
    JobContext,
    UnknownJobTypeError,
    get_handler,
    validate_payload,
)


def ctx(attempt: int = 1, job_id: uuid.UUID | None = None) -> JobContext:
    return JobContext(job_id=job_id or uuid.uuid4(), attempt=attempt, session_factory=get_sessionmaker())


def test_echo():
    assert get_handler("echo").run({"message": "hello"}, ctx()) == {"echo": "hello"}


def test_process_records_pipeline(engine):
    result = get_handler("process_records").run({"batch_id": "b", "record_ids": [3, 4, 5, -1]}, ctx())
    assert result["processed"] == 3
    assert result["failed"] == 1
    assert result["tiers"] == {"bronze": 1, "silver": 1, "gold": 1}
    assert result["notifications_sent"] == 3
    assert result["failures"] == [{"record_id": -1, "error": "record_id must be a positive integer"}]


def test_process_records_caps_stored_failures(engine):
    result = get_handler("process_records").run({"batch_id": "b", "record_ids": [-1] * 200}, ctx())
    assert result["failed"] == 200
    assert len(result["failures"]) == 50


def test_process_records_is_idempotent_when_rerun(db):
    """Running the same batch twice must not duplicate stored rows or notifications."""
    payload = {"batch_id": "rerun", "record_ids": [1, 2, 3, 3]}
    first = get_handler("process_records").run(payload, ctx())
    second = get_handler("process_records").run(payload, ctx(attempt=2))

    assert first["notifications_sent"] == 3
    assert second["notifications_sent"] == 0
    assert second["notifications_deduplicated"] == 3
    assert db.scalar(select(func.count()).select_from(ProcessedRecord)) == 3
    assert db.scalar(select(func.count()).select_from(Notification)) == 3


def test_flaky_fails_then_succeeds():
    with pytest.raises(TransientJobError):
        get_handler("flaky").run({"fail_times": 2}, ctx(attempt=2))
    assert get_handler("flaky").run({"fail_times": 2}, ctx(attempt=3)) == {"succeeded_on_attempt": 3}


def test_always_fail_permanent_and_transient():
    with pytest.raises(PermanentJobError, match="nope"):
        get_handler("always_fail").run({"reason": "nope"}, ctx())
    with pytest.raises(TransientJobError):
        get_handler("always_fail").run({"reason": "nope", "transient": True}, ctx())


def test_stored_payload_that_no_longer_validates_is_permanent():
    with pytest.raises(PermanentJobError):
        get_handler("echo").run({"wrong": 1}, ctx())


def test_unknown_type():
    with pytest.raises(UnknownJobTypeError):
        validate_payload("does_not_exist", {})


def test_invalid_payload_collects_errors():
    with pytest.raises(InvalidPayloadError) as exc:
        validate_payload("process_records", {"batch_id": "", "record_ids": []})
    assert {e["loc"][0] for e in exc.value.errors} == {"batch_id", "record_ids"}
