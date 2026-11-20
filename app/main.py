from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from app import __version__
from app.api import health, jobs
from app.core.logging import configure_logging
from app.core.middleware import BodySizeLimitMiddleware, RequestContextMiddleware

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging()
    yield


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
    # Starlette runs the last-added middleware first: request context wraps
    # everything, so even 413 responses get a request ID and an access log.
    app.add_middleware(BodySizeLimitMiddleware)
    app.add_middleware(RequestContextMiddleware)

    app.include_router(health.router)
    app.include_router(jobs.router)

    @app.get("/", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
