"""Unit tests for handlers: no database, broker or HTTP involved."""

import pytest

from app.tasks.handlers import (
    InvalidPayloadError,
    JobHandlerError,
    UnknownJobTypeError,
    get_handler,
    validate_payload,
)


def test_echo():
    assert get_handler("echo").run({"message": "hello"}) == {"echo": "hello"}


def test_process_records_pipeline():
    result = get_handler("process_records").run({"batch_id": "b", "record_ids": [3, 4, 5, -1]})
    assert result["processed"] == 3
    assert result["failed"] == 1
    assert result["tiers"] == {"bronze": 1, "silver": 1, "gold": 1}
    assert result["failures"] == [{"record_id": -1, "error": "record_id must be a positive integer"}]


def test_process_records_caps_stored_failures():
    result = get_handler("process_records").run({"batch_id": "b", "record_ids": [-1] * 200})
    assert result["failed"] == 200
    assert len(result["failures"]) == 50


def test_always_fail_raises():
    with pytest.raises(JobHandlerError, match="nope"):
        get_handler("always_fail").run({"reason": "nope"})


def test_unknown_type():
    with pytest.raises(UnknownJobTypeError):
        validate_payload("does_not_exist", {})


def test_invalid_payload_collects_errors():
    with pytest.raises(InvalidPayloadError) as exc:
        validate_payload("process_records", {"batch_id": "", "record_ids": []})
    assert {e["loc"][0] for e in exc.value.errors} == {"batch_id", "record_ids"}
