from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from signalhub.app.api.routes import router
from signalhub.app.config import load_settings
from signalhub.app.database.db import Database
from signalhub.app.explorer import BaseLaunchTraceService
from signalhub.app.exports import TokenPoolExportService
from signalhub.app.scheduler.polling import PollingController
from signalhub.app.subscriptions import ChainstackLaunchMonitor


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


logger = logging.getLogger(__name__)


async def run_initial_poll(controller: PollingController) -> None:
    try:
        await controller.scan_once(trigger="startup")
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
    app.state.base_trace_service = BaseLaunchTraceService(settings)
    app.state.token_pool_exporter = TokenPoolExportService(database, settings.token_pool_export_path)
    app.state.token_pool_exporter.refresh()

    controller = PollingController(database, settings, app.state.token_pool_exporter)
    launch_monitor = ChainstackLaunchMonitor(
        database,
        settings,
        app.state.base_trace_service,
        app.state.token_pool_exporter,
    )
    app.state.polling_controller = controller
    app.state.launch_monitor = launch_monitor
    controller.start()
    launch_monitor.start()

    try:
        if settings.source_enabled and controller.mode == "auto":
            asyncio.create_task(run_initial_poll(controller))
        yield
    finally:
        await launch_monitor.shutdown()
        controller.shutdown()


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
