"""phase 3: workflows (DAGs), workflow runs, job dependencies, schedules, NOTIFY triggers

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

JOB_STATUSES_V2 = ("PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "RETRY_SCHEDULED", "DEAD_LETTER")
JOB_STATUSES_V3 = ("SCHEDULED", "BLOCKED") + JOB_STATUSES_V2 + ("CANCELLED",)
RUN_STATUSES = ("SCHEDULED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED")

# Frozen copy of app/db/triggers.py at this revision (migrations must not
# import application code that may change later).
JOB_FUNCTION = """
CREATE OR REPLACE FUNCTION flowgrid_notify_job() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status
     OR NEW.attempts IS DISTINCT FROM OLD.attempts THEN
    PERFORM pg_notify('flowgrid_events', json_build_object(
      'kind', 'job', 'id', NEW.id, 'api_key_id', NEW.api_key_id, 'status', NEW.status,
      'type', NEW.type, 'attempts', NEW.attempts, 'workflow_run_id', NEW.workflow_run_id,
      'step_id', NEW.step_id, 'at', NEW.updated_at)::text);
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
"""
RUN_FUNCTION = """
CREATE OR REPLACE FUNCTION flowgrid_notify_run() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status THEN
    PERFORM pg_notify('flowgrid_events', json_build_object(
      'kind', 'workflow_run', 'id', NEW.id, 'api_key_id', NEW.api_key_id,
      'status', NEW.status, 'workflow_id', NEW.workflow_id, 'at', NEW.updated_at)::text);
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
"""


def _in(values) -> str:
    return "status IN (" + ", ".join(f"'{v}'" for v in values) + ")"


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("api_key_id", sa.Uuid(), nullable=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("definition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["api_key_id"], ["api_keys.id"], name=op.f("fk_workflows_api_key_id_api_keys"),
                                ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflows")),
    )
    op.create_index("ix_workflows_api_key_id_created_at", "workflows", ["api_key_id", "created_at"])

    op.create_table(
        "schedules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("api_key_id", sa.Uuid(), nullable=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("cron", sa.String(length=100), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("target_kind", sa.String(length=16), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("max_attempts", sa.Integer(), nullable=True),
        sa.Column("timeout_seconds", sa.Integer(), nullable=True),
        sa.Column("workflow_id", sa.Uuid(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("target_kind IN ('job', 'workflow')", name=op.f("ck_schedules_target_kind_valid")),
        sa.CheckConstraint(
            "(target_kind = 'job' AND job_type IS NOT NULL AND workflow_id IS NULL) OR "
            "(target_kind = 'workflow' AND workflow_id IS NOT NULL AND job_type IS NULL)",
            name=op.f("ck_schedules_target_consistent"),
        ),
        sa.ForeignKeyConstraint(["api_key_id"], ["api_keys.id"], name=op.f("fk_schedules_api_key_id_api_keys"),
                                ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workflow_id"], ["workflows.id"], name=op.f("fk_schedules_workflow_id_workflows"),
                                ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_schedules")),
    )
    op.create_index(op.f("ix_schedules_api_key_id"), "schedules", ["api_key_id"])
    op.create_index("ix_schedules_enabled_next_run_at", "schedules", ["enabled", "next_run_at"])

    op.create_table(
        "workflow_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("workflow_id", sa.Uuid(), nullable=False),
        sa.Column("api_key_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("schedule_id", sa.Uuid(), nullable=True),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(_in(RUN_STATUSES), name=op.f("ck_workflow_runs_status_valid")),
        sa.ForeignKeyConstraint(["workflow_id"], ["workflows.id"],
                                name=op.f("fk_workflow_runs_workflow_id_workflows"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["api_key_id"], ["api_keys.id"],
                                name=op.f("fk_workflow_runs_api_key_id_api_keys"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["schedule_id"], ["schedules.id"],
                                name=op.f("fk_workflow_runs_schedule_id_schedules"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_runs")),
        sa.UniqueConstraint("schedule_id", "scheduled_for", name="uq_workflow_runs_schedule_id_scheduled_for"),
    )
    op.create_index(op.f("ix_workflow_runs_workflow_id"), "workflow_runs", ["workflow_id"])
    op.create_index("ix_workflow_runs_api_key_id_created_at", "workflow_runs", ["api_key_id", "created_at"])
    op.create_index("ix_workflow_runs_status_scheduled_for", "workflow_runs", ["status", "scheduled_for"])

    # --- jobs: workflow membership + schedule origin --------------------------------
    op.add_column("jobs", sa.Column("workflow_run_id", sa.Uuid(), nullable=True))
    op.add_column("jobs", sa.Column("step_id", sa.String(length=64), nullable=True))
    op.add_column("jobs", sa.Column("schedule_id", sa.Uuid(), nullable=True))
    op.add_column("jobs", sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True))
    op.create_foreign_key(op.f("fk_jobs_workflow_run_id_workflow_runs"), "jobs", "workflow_runs",
                          ["workflow_run_id"], ["id"], ondelete="CASCADE")
    op.create_foreign_key(op.f("fk_jobs_schedule_id_schedules"), "jobs", "schedules",
                          ["schedule_id"], ["id"], ondelete="SET NULL")
    op.create_index(op.f("ix_jobs_workflow_run_id"), "jobs", ["workflow_run_id"])
    op.create_index("ix_jobs_status_queued_at", "jobs", ["status", "queued_at"])
    op.create_unique_constraint("uq_jobs_workflow_run_id_step_id", "jobs", ["workflow_run_id", "step_id"])
    op.create_unique_constraint("uq_jobs_schedule_id_scheduled_for", "jobs", ["schedule_id", "scheduled_for"])
    op.drop_constraint(op.f("ck_jobs_status_valid"), "jobs", type_="check")
    op.create_check_constraint(op.f("ck_jobs_status_valid"), "jobs", _in(JOB_STATUSES_V3))

    op.create_table(
        "job_dependencies",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("depends_on_job_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], name=op.f("fk_job_dependencies_job_id_jobs"),
                                ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["depends_on_job_id"], ["jobs.id"],
                                name=op.f("fk_job_dependencies_depends_on_job_id_jobs"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("job_id", "depends_on_job_id", name="pk_job_dependencies"),
    )
    op.create_index(op.f("ix_job_dependencies_depends_on_job_id"), "job_dependencies", ["depends_on_job_id"])

    # --- NOTIFY triggers --------------------------------------------------------------
    op.execute(JOB_FUNCTION)
    op.execute(RUN_FUNCTION)
    op.execute("CREATE TRIGGER jobs_notify AFTER INSERT OR UPDATE ON jobs "
               "FOR EACH ROW EXECUTE FUNCTION flowgrid_notify_job()")
    op.execute("CREATE TRIGGER workflow_runs_notify AFTER INSERT OR UPDATE ON workflow_runs "
               "FOR EACH ROW EXECUTE FUNCTION flowgrid_notify_run()")


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS workflow_runs_notify ON workflow_runs")
    op.execute("DROP TRIGGER IF EXISTS jobs_notify ON jobs")
    op.execute("DROP FUNCTION IF EXISTS flowgrid_notify_run()")
    op.execute("DROP FUNCTION IF EXISTS flowgrid_notify_job()")

    op.drop_index(op.f("ix_job_dependencies_depends_on_job_id"), table_name="job_dependencies")
    op.drop_table("job_dependencies")

    # Workflow step jobs and Phase 3-only states cannot exist in the Phase 2 schema.
    op.execute("DELETE FROM jobs WHERE workflow_run_id IS NOT NULL")
    op.execute("UPDATE jobs SET status = 'FAILED' WHERE status IN ('SCHEDULED', 'BLOCKED', 'CANCELLED')")
    op.drop_constraint(op.f("ck_jobs_status_valid"), "jobs", type_="check")
    op.create_check_constraint(op.f("ck_jobs_status_valid"), "jobs", _in(JOB_STATUSES_V2))
    op.drop_constraint("uq_jobs_schedule_id_scheduled_for", "jobs", type_="unique")
    op.drop_constraint("uq_jobs_workflow_run_id_step_id", "jobs", type_="unique")
    op.drop_index("ix_jobs_status_queued_at", table_name="jobs")
    op.drop_index(op.f("ix_jobs_workflow_run_id"), table_name="jobs")
    op.drop_constraint(op.f("fk_jobs_schedule_id_schedules"), "jobs", type_="foreignkey")
    op.drop_constraint(op.f("fk_jobs_workflow_run_id_workflow_runs"), "jobs", type_="foreignkey")
    for column in ("scheduled_for", "schedule_id", "step_id", "workflow_run_id"):
        op.drop_column("jobs", column)

    op.drop_index("ix_workflow_runs_status_scheduled_for", table_name="workflow_runs")
    op.drop_index("ix_workflow_runs_api_key_id_created_at", table_name="workflow_runs")
    op.drop_index(op.f("ix_workflow_runs_workflow_id"), table_name="workflow_runs")
    op.drop_table("workflow_runs")
    op.drop_index("ix_schedules_enabled_next_run_at", table_name="schedules")
    op.drop_index(op.f("ix_schedules_api_key_id"), table_name="schedules")
    op.drop_table("schedules")
    op.drop_index("ix_workflows_api_key_id_created_at", table_name="workflows")
    op.drop_table("workflows")
