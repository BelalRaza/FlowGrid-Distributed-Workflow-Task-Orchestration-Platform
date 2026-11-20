"""Shared test fixtures.

Tests run Celery in eager mode: ``publish_job`` executes the task inline, so
the real worker code path (claim -> run handler -> record outcome) is exercised
without a Redis broker. Retries are driven explicitly by calling the sweeper
with a ``now`` in the future, so no test ever sleeps through a backoff delay.

Database: set TEST_DATABASE_URL to run against PostgreSQL (recommended, and what
`make test-pg` uses). Otherwise a throwaway SQLite file is used so the suite can
run with no services at all.
"""

import os
import tempfile
from datetime import timedelta
from pathlib import Path

_sqlite_path = Path(tempfile.gettempdir()) / f"flowgrid-test-{os.getpid()}.db"
os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL", f"sqlite:///{_sqlite_path}")
os.environ["CELERY_TASK_ALWAYS_EAGER"] = "true"
# Point Redis somewhere unreachable so no test accidentally depends on it.
os.environ["REDIS_URL"] = "redis://127.0.0.1:1/0"

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.api.deps import get_rate_limiter  # noqa: E402
from app.db import session as db_session  # noqa: E402
from app.db.base import Base  # noqa: E402
from app.db.models import Job, utcnow  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import jobs as jobs_service  # noqa: E402
from app.services.auth import create_api_key  # noqa: E402
from app.services.ratelimit import InMemoryTokenBucket  # noqa: E402
from app.services.sweeper import sweep_once  # noqa: E402
from app.tasks.worker import publish_job  # noqa: E402


@pytest.fixture(scope="session")
def engine():
    engine = create_engine(os.environ["DATABASE_URL"])
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    db_session.configure_engine(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()
    _sqlite_path.unlink(missing_ok=True)


@pytest.fixture(autouse=True)
def clean_db(engine):
    yield
    with Session(engine) as s:
        for table in reversed(Base.metadata.sorted_tables):
            s.execute(table.delete())
        s.commit()


@pytest.fixture
def db(engine):
    with db_session.get_sessionmaker()() as s:
        yield s


@pytest.fixture
def api_key(db):
    """(ApiKey row, raw key string)"""
    return create_api_key(db, "test-client")


@pytest.fixture
def other_api_key(db):
    return create_api_key(db, "other-client")


@pytest.fixture
def limiter():
    return InMemoryTokenBucket(capacity=10_000, refill_per_second=10_000)


@pytest.fixture
def app(limiter):
    app = create_app()
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    return app


def _client(app, raw_key=None):
    headers = {"Authorization": f"Bearer {raw_key}"} if raw_key else {}
    return TestClient(app, headers=headers)


@pytest.fixture
def client(app, api_key):
    with _client(app, api_key[1]) as c:
        yield c


@pytest.fixture
def other_client(app, other_api_key):
    with _client(app, other_api_key[1]) as c:
        yield c


@pytest.fixture
def anon_client(app):
    with _client(app) as c:
        yield c


def noop_publisher(job_id, timeout_seconds):
    """Publishes nothing: leaves the job QUEUED, as if waiting in Redis."""


@pytest.fixture
def submit(db, api_key):
    """Service-level submit owned by the test API key. Defaults to a no-op broker."""

    def _submit(job_type, payload, publish=noop_publisher, **kwargs) -> Job:
        return jobs_service.submit_job(
            db, job_type=job_type, payload=payload, publish=publish, api_key_id=api_key[0].id, **kwargs
        ).job

    return _submit


@pytest.fixture
def sweep(db):
    """Run one sweeper pass `seconds_ahead` in the future, with the eager broker."""

    def _sweep(seconds_ahead: float = 3600, publish=publish_job):
        return sweep_once(db, publish, now=utcnow() + timedelta(seconds=seconds_ahead))

    return _sweep
