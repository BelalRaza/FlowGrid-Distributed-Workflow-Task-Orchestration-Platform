"""Shared test fixtures.

Tests run Celery in eager mode: ``publish_job`` executes the task inline, so
the real worker code path (claim -> run handler -> record outcome) is exercised
without a Redis broker.

Database: set TEST_DATABASE_URL to run against PostgreSQL (recommended, and what
`docker compose run` uses). Otherwise a throwaway SQLite file is used so the
suite can run with no services at all.
"""

import os
import tempfile
from pathlib import Path

_sqlite_path = Path(tempfile.gettempdir()) / f"flowgrid-test-{os.getpid()}.db"
os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL", f"sqlite:///{_sqlite_path}")
os.environ["CELERY_TASK_ALWAYS_EAGER"] = "true"
# Point Redis somewhere unreachable so no test accidentally depends on it.
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:1/0")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, delete  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.db import session as db_session  # noqa: E402
from app.db.base import Base  # noqa: E402
from app.db.models import Job  # noqa: E402
from app.main import create_app  # noqa: E402


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
        s.execute(delete(Job))
        s.commit()


@pytest.fixture
def db(engine):
    with db_session.get_sessionmaker()() as s:
        yield s


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c
