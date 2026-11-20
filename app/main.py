import asyncio
import contextlib
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from app import __version__
from app.api import events, health, jobs, schedules, workflows
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.metrics import Metrics
from app.core.middleware import BodySizeLimitMiddleware, RequestContextMiddleware
from app.services.events import EventBus, run_postgres_listener

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()
    listener = None
    if settings.database_url.startswith("postgresql"):
        listener = asyncio.create_task(run_postgres_listener(app.state.event_bus, settings.database_url))
    try:
        yield
    finally:
        app.state.event_bus.close()
        if listener is not None:
            listener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener


def create_app() -> FastAPI:
    app = FastAPI(
        title="FlowGrid",
        description=(
            "Distributed workflow and background task processing platform.\n\n"
            "All `/api/v1` routes require an API key: `Authorization: Bearer fg_...` "
            "(create one with `make api-key`)."
        ),
        version=__version__,
        lifespan=lifespan,
    )
    app.state.event_bus = EventBus(queue_size=get_settings().sse_queue_size)
    app.state.metrics = Metrics(subscriber_count=lambda: app.state.event_bus.subscriber_count)

    # Starlette runs the last-added middleware first: request context wraps
    # everything, so even 413 responses get a request ID, a log line and metrics.
    app.add_middleware(BodySizeLimitMiddleware)
    app.add_middleware(RequestContextMiddleware)

    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(workflows.router)
    app.include_router(schedules.router)
    app.include_router(events.router)

    @app.get("/", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
