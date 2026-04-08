"""FastAPI application factory."""
from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ingestion.review.escalation import escalation_loop

_STATIC_DIR = Path(__file__).parent.parent / "static"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-load the embedding model so first request isn't slow
    try:
        from ingestion.embeddings.encoder import Encoder
        Encoder.get()
        logger.info("Embedding model loaded")
    except Exception as e:
        logger.warning(f"Could not pre-load embedding model: {e}")

    # Start escalation background loop
    task = asyncio.create_task(escalation_loop())

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def create_app() -> FastAPI:
    app = FastAPI(
        title="Product Knowledge Ingestion & Intelligence Repository",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Request ID middleware
    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        request_id = str(uuid.uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    # Standard error responses
    @app.exception_handler(404)
    async def not_found(request: Request, exc):
        return JSONResponse(status_code=404, content={"detail": "Not found"})

    @app.exception_handler(500)
    async def server_error(request: Request, exc):
        logger.exception("Internal server error")
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    # Register routers
    from ingestion.api.routers import context, entities, linked_context, search, ingest, review, documents, chat

    prefix = "/v1"
    app.include_router(context.router, prefix=prefix, tags=["context"])
    app.include_router(entities.router, prefix=prefix, tags=["entities"])
    app.include_router(linked_context.router, prefix=prefix, tags=["linked-context"])
    app.include_router(search.router, prefix=prefix, tags=["search"])
    app.include_router(ingest.router, prefix=prefix, tags=["ingest"])
    app.include_router(review.router, prefix=prefix, tags=["review"])
    app.include_router(documents.router, prefix=prefix, tags=["documents"])
    app.include_router(chat.router, prefix=prefix, tags=["chat"])

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # Serve the UI at /
    @app.get("/", include_in_schema=False)
    def ui():
        return FileResponse(_STATIC_DIR / "index.html")

    # Mount static assets (CSS, JS, images if added later)
    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    return app


app = create_app()
