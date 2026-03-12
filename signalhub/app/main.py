from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from signalhub.app.api.routes import router
from signalhub.app.config import load_settings
from signalhub.app.database.db import Database
from signalhub.app.scheduler.polling import build_scheduler, run_virtuals_poll


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


logger = logging.getLogger(__name__)


async def run_initial_poll(database: Database, settings) -> None:
    try:
        await run_virtuals_poll(database=database, settings=settings)
    except Exception:
        logger.exception("initial virtuals poll failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    database = Database(settings.db_path)
    database.init_db()
    database.upsert_source(
        name=settings.source_name,
        source_type=settings.source_type,
        endpoint=settings.virtuals_endpoint,
        interval_seconds=settings.poll_interval_seconds,
        enabled=settings.source_enabled,
    )

    app.state.settings = settings
    app.state.database = database

    scheduler = build_scheduler(database, settings)
    app.state.scheduler = scheduler
    scheduler_started = False

    if settings.source_enabled:
        scheduler.start()
        scheduler_started = True
        asyncio.create_task(run_initial_poll(database=database, settings=settings))

    try:
        yield
    finally:
        if scheduler_started:
            scheduler.shutdown(wait=False)


def create_app() -> FastAPI:
    app = FastAPI(
        title="SignalHub - Virtuals Monitor",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/dashboard")

    return app


app = create_app()
