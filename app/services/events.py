"""Live events: PostgreSQL LISTEN/NOTIFY -> in-process fan-out -> SSE clients.

    DB trigger --NOTIFY--> PostgresListener (one connection per API process)
                               |
                           EventBus.publish()
                               |-- subscriber queue (api key A) --> SSE stream
                               `-- subscriber queue (api key B) --> SSE stream

One LISTEN connection per API process, not per browser tab, so 500 open
dashboards cost one database connection, not 500.

NOTIFY is not durable: events emitted while a client is disconnected are
gone. That is acceptable because events are *hints*. The dashboard refetches
state on (re)connect and treats events as "something changed, refresh".
A slow client whose queue fills up gets a single ``resync`` event instead of
blocking the bus.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from app.db.triggers import CHANNEL

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, queue_size: int = 1000) -> None:
        self._queue_size = queue_size
        self._subscribers: dict[int, tuple[str | None, asyncio.Queue]] = {}
        self._next_id = 0
        self.published = 0

    @asynccontextmanager
    async def subscribe(self, api_key_id: uuid.UUID | None) -> AsyncIterator[asyncio.Queue]:
        sid = self._next_id
        self._next_id += 1
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers[sid] = (str(api_key_id) if api_key_id else None, queue)
        try:
            yield queue
        finally:
            self._subscribers.pop(sid, None)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @staticmethod
    def _offer(queue: asyncio.Queue, event: dict[str, Any]) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            # Slow consumer: drop its backlog and tell it to refetch.
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait({"kind": "resync"})

    def publish(self, event: dict[str, Any]) -> None:
        """Deliver to subscribers of the event's API key. Never blocks."""
        self.published += 1
        owner = event.get("api_key_id")
        for key, queue in list(self._subscribers.values()):
            if key == owner:  # clients only ever see their own jobs
                self._offer(queue, event)

    def broadcast(self, event: dict[str, Any]) -> None:
        """Deliver to every subscriber (only for content-free signals like resync)."""
        for _, queue in list(self._subscribers.values()):
            self._offer(queue, event)

    def close(self) -> None:
        for _, queue in list(self._subscribers.values()):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                queue.get_nowait()
                queue.put_nowait(None)


def _libpq_dsn(sqlalchemy_url: str) -> str:
    # "postgresql+psycopg://..." -> "postgresql://..."
    scheme, rest = sqlalchemy_url.split("://", 1)
    return "postgresql://" + rest if scheme.startswith("postgresql") else sqlalchemy_url


async def run_postgres_listener(bus: EventBus, database_url: str) -> None:
    """LISTEN forever, reconnecting with backoff. Cancelled on app shutdown."""
    import psycopg

    dsn = _libpq_dsn(database_url)
    delay = 1.0
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                await conn.execute(f"LISTEN {CHANNEL}")
                logger.info("Event listener connected", extra={"channel": CHANNEL})
                delay = 1.0
                # Tell connected clients to refetch: events may have been missed
                # while the listener was disconnected.
                bus.broadcast({"kind": "resync"})
                async for notify in conn.notifies():
                    try:
                        bus.publish(json.loads(notify.payload))
                    except ValueError:
                        logger.warning("Bad NOTIFY payload", extra={"payload": notify.payload[:200]})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Event listener disconnected; retrying", extra={"error": str(exc), "retry_in": delay})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
