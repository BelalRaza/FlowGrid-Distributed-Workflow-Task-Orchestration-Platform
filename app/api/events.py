import asyncio
import json
import time
import uuid

from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.orm import Session

from app.api.deps import require_client
from app.core.config import get_settings
from app.db.models import ApiKey
from app.db.session import get_db, get_sessionmaker
from app.services.events import EventBus

router = APIRouter(tags=["monitoring"])


def get_event_bus(request: Request) -> EventBus:
    return request.app.state.event_bus


def _key_still_valid(api_key_id: uuid.UUID) -> bool:
    with get_sessionmaker()() as db:
        key = db.get(ApiKey, api_key_id)
        return key is not None and key.revoked_at is None


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@router.get(
    "/api/v1/events",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}, "description": "Server-Sent Events stream"}},
)
async def events(
    request: Request,
    api_key: ApiKey = Depends(require_client),
    db: Session = Depends(get_db),
    bus: EventBus = Depends(get_event_bus),
) -> StreamingResponse:
    """Live job and workflow-run status changes for this API key (Server-Sent Events).

    Events are hints ("job X is now SUCCEEDED"); fetch details over the REST
    API. A `resync` event means events may have been missed: refetch state.
    """
    # Authentication used a pooled DB connection. Give it back now: an SSE
    # stream can stay open for hours and must not pin a connection, or a
    # handful of open dashboards would exhaust the pool.
    db.close()
    keepalive = get_settings().sse_keepalive_seconds

    async def stream():
        yield "retry: 3000\n\n"  # browser reconnect delay
        yield _sse("ready", {"api_key": api_key.prefix})
        last_check = time.monotonic()
        async with bus.subscribe(api_key.id) as queue:
            while True:
                if await request.is_disconnected():
                    break
                # The key was checked when the stream opened; a stream can
                # outlive a revocation by hours, so re-check periodically.
                if time.monotonic() - last_check >= keepalive:
                    last_check = time.monotonic()
                    if not await asyncio.to_thread(_key_still_valid, api_key.id):
                        yield _sse("revoked", {"detail": "API key revoked"})
                        break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=keepalive)
                except asyncio.TimeoutError:
                    # Comments keep proxies and load balancers from closing an idle stream.
                    yield ": keepalive\n\n"
                    continue
                if event is None:  # bus closed (shutdown)
                    break
                yield _sse(event.get("kind", "message"), event)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/metrics", include_in_schema=False, response_class=PlainTextResponse)
def metrics(request: Request) -> PlainTextResponse:
    """Prometheus scrape endpoint. Expose it only on an internal network."""
    return PlainTextResponse(generate_latest(request.app.state.metrics.registry), media_type=CONTENT_TYPE_LATEST)
