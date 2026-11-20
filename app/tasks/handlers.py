"""Allow-listed job handlers.

FlowGrid never executes arbitrary code. A client can only submit a job whose
``type`` is registered here, and its ``payload`` must validate against that
handler's Pydantic model. Handlers are plain functions so they can be unit
tested without Celery, Redis or a database.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class UnknownJobTypeError(Exception):
    def __init__(self, job_type: str) -> None:
        super().__init__(f"Unknown job type: {job_type!r}")
        self.job_type = job_type


class InvalidPayloadError(Exception):
    def __init__(self, job_type: str, errors: list[dict[str, Any]]) -> None:
        super().__init__(f"Invalid payload for job type {job_type!r}")
        self.job_type = job_type
        self.errors = errors


class JobHandlerError(Exception):
    """Raised by a handler to signal that the job itself failed."""


@dataclass(frozen=True)
class JobHandler:
    name: str
    description: str
    payload_model: type[BaseModel]
    func: Callable[[Any], dict[str, Any]]
    example: dict[str, Any]

    def validate(self, payload: dict[str, Any]) -> BaseModel:
        try:
            return self.payload_model.model_validate(payload)
        except ValidationError as exc:
            raise InvalidPayloadError(
                self.name, exc.errors(include_url=False, include_context=False)
            ) from exc

    def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.func(self.validate(payload))


REGISTRY: dict[str, JobHandler] = {}


def register(
    name: str, *, payload_model: type[BaseModel], description: str, example: dict[str, Any]
) -> Callable[[Callable[[Any], dict[str, Any]]], Callable[[Any], dict[str, Any]]]:
    def decorator(func: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
        if name in REGISTRY:
            raise RuntimeError(f"Job type {name!r} registered twice")
        REGISTRY[name] = JobHandler(name, description, payload_model, func, example)
        return func

    return decorator


def get_handler(job_type: str) -> JobHandler:
    try:
        return REGISTRY[job_type]
    except KeyError:
        raise UnknownJobTypeError(job_type) from None


def validate_payload(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Validate a payload and return its normalised JSON form (defaults filled in)."""
    return get_handler(job_type).validate(payload).model_dump(mode="json")


# --------------------------------------------------------------------------- #
# Payload models
# --------------------------------------------------------------------------- #


class _StrictPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EchoPayload(_StrictPayload):
    message: str = Field(min_length=1, max_length=1000)


class SleepPayload(_StrictPayload):
    seconds: float = Field(ge=0, le=30, description="How long the worker should sleep.")


class ProcessRecordsPayload(_StrictPayload):
    batch_id: str = Field(min_length=1, max_length=128)
    record_ids: list[int] = Field(min_length=1, max_length=10_000)
    delay_ms: int = Field(
        default=0, ge=0, le=1000, description="Simulated latency per record, in milliseconds."
    )


class AlwaysFailPayload(_StrictPayload):
    reason: str = Field(default="Intentional failure for testing", max_length=500)


# --------------------------------------------------------------------------- #
# Handlers
# --------------------------------------------------------------------------- #


@register(
    "echo",
    payload_model=EchoPayload,
    description="Returns the message it was given. Useful as a smoke test.",
    example={"message": "hello flowgrid"},
)
def echo(payload: EchoPayload) -> dict[str, Any]:
    return {"echo": payload.message}


@register(
    "sleep",
    payload_model=SleepPayload,
    description="Sleeps for N seconds to simulate a long-running job.",
    example={"seconds": 3},
)
def sleep(payload: SleepPayload) -> dict[str, Any]:
    time.sleep(payload.seconds)
    return {"slept_seconds": payload.seconds}


class _RecordError(Exception):
    pass


_TIERS = ("bronze", "silver", "gold")


def _validate_record(record_id: int) -> dict[str, Any]:
    if record_id <= 0:
        raise _RecordError("record_id must be a positive integer")
    return {"id": record_id}


def _enrich_record(record: dict[str, Any]) -> dict[str, Any]:
    # Stand-in for a call to an external enrichment API (deterministic, offline).
    return {**record, "tier": _TIERS[record["id"] % len(_TIERS)]}


def _store_record(record: dict[str, Any]) -> None:
    # Stand-in for persisting the enriched record. Real storage arrives with
    # idempotent writes in Phase 2.
    return None


def _notify(record: dict[str, Any]) -> None:
    # Stand-in for sending a notification.
    return None


@register(
    "process_records",
    payload_model=ProcessRecordsPayload,
    description=(
        "Simulates the validate → enrich → store → notify pipeline for a batch of "
        "customer records. Non-positive IDs fail validation individually."
    ),
    example={"batch_id": "batch-001", "record_ids": [101, 102, 103], "delay_ms": 200},
)
def process_records(payload: ProcessRecordsPayload) -> dict[str, Any]:
    processed = 0
    failures: list[dict[str, Any]] = []
    tiers: dict[str, int] = {}

    for record_id in payload.record_ids:
        try:
            record = _enrich_record(_validate_record(record_id))
            _store_record(record)
            _notify(record)
        except _RecordError as exc:
            failures.append({"record_id": record_id, "error": str(exc)})
        else:
            processed += 1
            tiers[record["tier"]] = tiers.get(record["tier"], 0) + 1
        if payload.delay_ms:
            time.sleep(payload.delay_ms / 1000)

    return {
        "batch_id": payload.batch_id,
        "processed": processed,
        "failed": len(failures),
        "tiers": tiers,
        # Cap the stored detail so one huge batch cannot bloat the jobs table.
        "failures": failures[:50],
    }


@register(
    "always_fail",
    payload_model=AlwaysFailPayload,
    description="Always raises an error. Used to exercise the failure path.",
    example={"reason": "testing failure handling"},
)
def always_fail(payload: AlwaysFailPayload) -> dict[str, Any]:
    raise JobHandlerError(payload.reason)
