"""Sweeper process entry point:  python -m app.sweeper"""

from __future__ import annotations

import logging
import signal
import time

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import get_sessionmaker
from app.services.sweeper import sweep_once
from app.tasks.worker import publish_job

logger = logging.getLogger("app.sweeper")

_running = True


def _stop(signum, _frame) -> None:
    global _running
    logger.info("Sweeper received signal %s, stopping after current pass", signum)
    _running = False


def main() -> None:
    configure_logging()
    settings = get_settings()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    logger.info("Sweeper started", extra={"interval_seconds": settings.sweep_interval_seconds})

    SessionLocal = get_sessionmaker()
    while _running:
        try:
            with SessionLocal() as db:
                sweep_once(db, publish_job)
        except Exception:
            # A database blip must not kill the sweeper; the next pass retries.
            logger.exception("Sweep pass failed")
        time.sleep(settings.sweep_interval_seconds)


if __name__ == "__main__":
    main()
