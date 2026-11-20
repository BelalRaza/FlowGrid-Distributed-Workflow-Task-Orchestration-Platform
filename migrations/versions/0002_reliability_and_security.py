"""phase 2: retries, attempts, dead-letter, idempotency, api keys, side-effect tables

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

JOB_STATUSES_V1 = ("PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED")
JOB_STATUSES_V2 = JOB_STATUSES_V1 + ("RETRY_SCHEDULED", "DEAD_LETTER")
ATTEMPT_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST")


def _in(values) -> str:
    return "status IN (" + ", ".join(f"'{v}'" for v in values) + ")"


def upgrade() -> None:
    # --- api_keys ---------------------------------------------------------------
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("prefix", sa.String(length=16), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api_keys")),
        sa.UniqueConstraint("key_hash", name=op.f("uq_api_keys_key_hash")),
    )
    op.create_index(op.f("ix_api_keys_prefix"), "api_keys", ["prefix"])

    # --- jobs: new columns --------------------------------------------------------
    # Existing Phase 1 jobs ran with no retries, so they are backfilled with
    # max_attempts=1. The server defaults exist only for the backfill.
    op.add_column("jobs", sa.Column("api_key_id", sa.Uuid(), nullable=True))
    op.add_column("jobs", sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="1"))
    op.add_column("jobs", sa.Column("timeout_seconds", sa.Integer(), nullable=False, server_default="60"))
    op.add_column("jobs", sa.Column("replay_of", sa.Uuid(), nullable=True))
    op.add_column("jobs", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.alter_column("jobs", "max_attempts", server_default=None)
    op.alter_column("jobs", "timeout_seconds", server_default=None)

    op.create_foreign_key(
        op.f("fk_jobs_api_key_id_api_keys"), "jobs", "api_keys", ["api_key_id"], ["id"], ondelete="SET NULL"
    )
    op.create_foreign_key(
        op.f("fk_jobs_replay_of_jobs"), "jobs", "jobs", ["replay_of"], ["id"], ondelete="SET NULL"
    )

    op.drop_constraint(op.f("ck_jobs_status_valid"), "jobs", type_="check")
    op.create_check_constraint(op.f("ck_jobs_status_valid"), "jobs", _in(JOB_STATUSES_V2))
    op.create_check_constraint(op.f("ck_jobs_max_attempts_positive"), "jobs", "max_attempts >= 1")
    op.create_check_constraint(op.f("ck_jobs_timeout_positive"), "jobs", "timeout_seconds >= 1")

    op.create_index("ix_jobs_status_next_attempt_at", "jobs", ["status", "next_attempt_at"])
    op.create_index("ix_jobs_status_lease_expires_at", "jobs", ["status", "lease_expires_at"])
    op.create_index("ix_jobs_api_key_id_created_at", "jobs", ["api_key_id", "created_at"])

    # --- job_attempts -------------------------------------------------------------
    op.create_table(
        "job_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_type", sa.String(length=100), nullable=True),
        sa.Column("retryable", sa.Boolean(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(_in(ATTEMPT_STATUSES), name=op.f("ck_job_attempts_status_valid")),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name=op.f("fk_job_attempts_job_id_jobs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job_attempts")),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_job_id_attempt_number"),
    )
    op.create_index(op.f("ix_job_attempts_job_id"), "job_attempts", ["job_id"])

    # --- idempotency_keys -----------------------------------------------------------
    op.create_table(
        "idempotency_keys",
        sa.Column("api_key_id", sa.Uuid(), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["api_key_id"], ["api_keys.id"], name=op.f("fk_idempotency_keys_api_key_id_api_keys"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name=op.f("fk_idempotency_keys_job_id_jobs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("api_key_id", "key", name="pk_idempotency_keys"),
    )
    op.create_index(op.f("ix_idempotency_keys_created_at"), "idempotency_keys", ["created_at"])

    # --- side-effect tables for process_records ------------------------------------
    op.create_table(
        "processed_records",
        sa.Column("batch_id", sa.String(length=128), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=False),
        sa.Column("tier", sa.String(length=16), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("batch_id", "record_id", name="pk_processed_records"),
    )
    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
        sa.UniqueConstraint("dedupe_key", name=op.f("uq_notifications_dedupe_key")),
    )


def downgrade() -> None:
    op.drop_table("notifications")
    op.drop_table("processed_records")
    op.drop_index(op.f("ix_idempotency_keys_created_at"), table_name="idempotency_keys")
    op.drop_table("idempotency_keys")
    op.drop_index(op.f("ix_job_attempts_job_id"), table_name="job_attempts")
    op.drop_table("job_attempts")

    op.drop_index("ix_jobs_api_key_id_created_at", table_name="jobs")
    op.drop_index("ix_jobs_status_lease_expires_at", table_name="jobs")
    op.drop_index("ix_jobs_status_next_attempt_at", table_name="jobs")
    op.drop_constraint(op.f("ck_jobs_timeout_positive"), "jobs", type_="check")
    op.drop_constraint(op.f("ck_jobs_max_attempts_positive"), "jobs", type_="check")
    # Jobs in Phase 2-only states cannot be represented in the Phase 1 schema.
    op.execute(
        "UPDATE jobs SET status = 'FAILED' WHERE status IN ('RETRY_SCHEDULED', 'DEAD_LETTER')"
    )
    op.drop_constraint(op.f("ck_jobs_status_valid"), "jobs", type_="check")
    op.create_check_constraint(op.f("ck_jobs_status_valid"), "jobs", _in(JOB_STATUSES_V1))
    op.drop_constraint(op.f("fk_jobs_replay_of_jobs"), "jobs", type_="foreignkey")
    op.drop_constraint(op.f("fk_jobs_api_key_id_api_keys"), "jobs", type_="foreignkey")
    for column in ("lease_expires_at", "next_attempt_at", "replay_of", "timeout_seconds", "max_attempts", "api_key_id"):
        op.drop_column("jobs", column)

    op.drop_index(op.f("ix_api_keys_prefix"), table_name="api_keys")
    op.drop_table("api_keys")
