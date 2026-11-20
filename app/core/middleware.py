"""HTTP middleware: request IDs, access logs, and a request body size limit."""

from __future__ import annotations

import logging
import re
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.core.config import get_settings
from app.core.logging import log_context

logger = logging.getLogger("app.access")

_VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assigns a request ID, binds it to every log line, and logs the request."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Accept a caller's X-Request-ID (useful for tracing across services) but
        # only if it is short and plain, so it cannot inject junk into logs.
        incoming = request.headers.get("x-request-id", "")
        request_id = incoming if _VALID_REQUEST_ID.match(incoming) else uuid.uuid4().hex

        start = time.perf_counter()
        with log_context(request_id=request_id):
            response = await call_next(request)
            elapsed = time.perf_counter() - start
            duration_ms = round(elapsed * 1000, 1)

            # Label metrics by the matched route *template* ("/api/v1/jobs/{job_id}"),
            # never the raw path, or every job ID becomes its own time series.
            metrics = getattr(request.app.state, "metrics", None)
            if metrics is not None:
                route = request.scope.get("route")
                metrics.observe(request.method, getattr(route, "path", "unmatched"), response.status_code, elapsed)

            if request.url.path not in ("/health", "/health/live", "/metrics"):
                logger.info(
                    "%s %s %s",
                    request.method,
                    request.url.path,
                    response.status_code,
                    extra={
                        "method": request.method,
                        "path": request.url.path,
                        "status": response.status_code,
                        "duration_ms": duration_ms,
                        "api_key": getattr(request.state, "api_key_prefix", None),
                    },
                )
        response.headers["X-Request-ID"] = request_id
        return response


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject oversized requests before reading them (413), based on Content-Length."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        limit = get_settings().max_request_body_bytes
        length = request.headers.get("content-length")
        if length is not None:
            try:
                too_big = int(length) > limit
            except ValueError:
                return JSONResponse({"detail": "Invalid Content-Length"}, status_code=400)
            if too_big:
                return JSONResponse(
                    {"detail": f"Request body exceeds {limit} bytes"}, status_code=413
                )
        return await call_next(request)
