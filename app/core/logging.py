"""Structured logging.

Every log line can carry context (request_id, job_id, attempt, worker_id...)
without passing it through every function call: values bound with
``log_context`` live in a ContextVar and are added to each record by a filter.

LOG_FORMAT=json emits one JSON object per line (for log aggregation);
LOG_FORMAT=text keeps a readable format for local development.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from app.core.config import get_settings

_context: ContextVar[dict[str, Any]] = ContextVar("flowgrid_log_context", default={})

# Attributes every LogRecord has; anything else was passed via `extra=`.
_STANDARD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


@contextmanager
def log_context(**values: Any) -> Iterator[None]:
    token = _context.set({**_context.get(), **values})
    try:
        yield
    finally:
        _context.reset(token)


def bind_log_context(**values: Any) -> None:
    """Bind values for the rest of the current context (e.g. one HTTP request)."""
    _context.set({**_context.get(), **values})


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


def _extras(record: logging.LogRecord) -> dict[str, Any]:
    return {k: v for k, v in record.__dict__.items() if k not in _STANDARD_ATTRS and not k.startswith("_")}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **_extras(record),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = _extras(record)
        if extras:
            base += "  " + " ".join(f"{k}={v}" for k, v in extras.items())
        return base


def configure_logging() -> None:
    settings = get_settings()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter() if settings.log_format.lower() == "json" else TextFormatter())
    handler.addFilter(ContextFilter())

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(settings.log_level.upper())
    # Our request middleware logs each request with more context.
    logging.getLogger("uvicorn.access").disabled = True
