from __future__ import annotations

import asyncio
import logging

from signalhub.app.config import Settings
from signalhub.app.database.db import Database
from signalhub.app.database.models import utc_now_iso
from signalhub.app.parsers.virtuals_parser import VirtualsParser
from signalhub.app.processors.event_processor import EventProcessor
from signalhub.app.sources.virtuals_source import VirtualsSource


logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
except ModuleNotFoundError:
    class AsyncIOScheduler:
        """Fallback scheduler for environments without APScheduler."""

        def __init__(self, timezone: str = "UTC") -> None:
            self.timezone = timezone
            self._interval_seconds = 0
            self._job = None
            self._task: asyncio.Task | None = None
            self._running = False
            self._job_lock = asyncio.Lock()

        def add_job(
            self,
            func,
            trigger: str,
            *,
            seconds: int,
            kwargs: dict,
            id: str,
            replace_existing: bool = True,
            max_instances: int = 1,
            coalesce: bool = True,
        ) -> None:
            del trigger, id, replace_existing, max_instances, coalesce
            self._interval_seconds = seconds
            self._job = (func, kwargs)

        def start(self) -> None:
            if self._running or self._job is None:
                return
            self._running = True
            self._task = asyncio.create_task(self._run_loop())

        async def _run_loop(self) -> None:
            assert self._job is not None
            func, kwargs = self._job
            while self._running:
                await asyncio.sleep(self._interval_seconds)
                async with self._job_lock:
                    try:
                        await func(**kwargs)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("fallback scheduled job failed")

        def shutdown(self, wait: bool = False) -> None:
            del wait
            self._running = False
            if self._task is not None:
                self._task.cancel()


async def run_virtuals_poll(database: Database, settings: Settings) -> dict[str, int]:
    source = VirtualsSource(settings)
    parser = VirtualsParser()
    processor = EventProcessor(database)
    run_time = utc_now_iso()

    payload = await source.fetch_projects()
    projects = parser.parse_projects(payload)
    summary = processor.process_projects(projects)
    database.update_source_last_run(settings.source_name, run_time)

    logger.info(
        "virtuals poll completed received=%s new=%s updated=%s status_changed=%s",
        summary["received"],
        summary["new_projects"],
        summary["updated_projects"],
        summary["status_changes"],
    )

    return summary


def build_scheduler(database: Database, settings: Settings) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        run_virtuals_poll,
        "interval",
        seconds=settings.poll_interval_seconds,
        kwargs={"database": database, "settings": settings},
        id="virtuals-poll",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    return scheduler
