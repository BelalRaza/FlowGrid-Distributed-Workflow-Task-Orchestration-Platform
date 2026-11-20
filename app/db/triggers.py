"""PostgreSQL triggers that publish job / workflow-run changes with NOTIFY.

Why a trigger and not application code?
* It fires for every writer (API, workers, sweeper) with no code changes.
* NOTIFY is transactional: listeners hear about a change only if and when it
  commits, never for a rolled-back change. The event stream therefore cannot
  show a state the database does not hold.

Payloads are kept small (pg_notify has an 8000-byte limit); listeners re-read
details over the API.
"""

from sqlalchemy import DDL, Table, event

CHANNEL = "flowgrid_events"

JOB_FUNCTION = f"""
CREATE OR REPLACE FUNCTION flowgrid_notify_job() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status
     OR NEW.attempts IS DISTINCT FROM OLD.attempts THEN
    PERFORM pg_notify('{CHANNEL}', json_build_object(
      'kind', 'job', 'id', NEW.id, 'api_key_id', NEW.api_key_id, 'status', NEW.status,
      'type', NEW.type, 'attempts', NEW.attempts, 'workflow_run_id', NEW.workflow_run_id,
      'step_id', NEW.step_id, 'at', NEW.updated_at)::text);
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
"""

RUN_FUNCTION = f"""
CREATE OR REPLACE FUNCTION flowgrid_notify_run() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status THEN
    PERFORM pg_notify('{CHANNEL}', json_build_object(
      'kind', 'workflow_run', 'id', NEW.id, 'api_key_id', NEW.api_key_id,
      'status', NEW.status, 'workflow_id', NEW.workflow_id, 'at', NEW.updated_at)::text);
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
"""

JOB_TRIGGER = (
    "CREATE TRIGGER jobs_notify AFTER INSERT OR UPDATE ON jobs "
    "FOR EACH ROW EXECUTE FUNCTION flowgrid_notify_job()"
)
RUN_TRIGGER = (
    "CREATE TRIGGER workflow_runs_notify AFTER INSERT OR UPDATE ON workflow_runs "
    "FOR EACH ROW EXECUTE FUNCTION flowgrid_notify_run()"
)


def attach(jobs: Table, runs: Table) -> None:
    for table, function, trigger in ((jobs, JOB_FUNCTION, JOB_TRIGGER), (runs, RUN_FUNCTION, RUN_TRIGGER)):
        event.listen(table, "after_create", DDL(function).execute_if(dialect="postgresql"))
        event.listen(table, "after_create", DDL(trigger).execute_if(dialect="postgresql"))
