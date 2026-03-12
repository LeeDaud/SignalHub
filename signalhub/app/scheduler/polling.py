from __future__ import annotations

import asyncio
import logging
from typing import Any

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
    parser = VirtualsParser(app_base_url=settings.virtuals_app_base_url)
    processor = EventProcessor(database)
    run_time = utc_now_iso()

    base_records = await source.fetch_projects()
    refresh_ids = database.list_projects_for_detail_refresh(
        limit=settings.virtuals_detail_refresh_limit
    )
    detail_records = await source.fetch_project_details(refresh_ids)
    merged_records = source.merge_project_records(base_records, detail_records)

    projects = parser.parse_projects(merged_records)
    summary = processor.process_projects(projects)
    summary["detail_refreshed"] = len(detail_records)
    summary["tracked_for_refresh"] = len(refresh_ids)

    database.update_source_last_run(settings.source_name, run_time)

    logger.info(
        "virtuals poll completed received=%s new=%s updated=%s status_changed=%s detail_refreshed=%s",
        summary["received"],
        summary["new_projects"],
        summary["updated_projects"],
        summary["status_changes"],
        summary["detail_refreshed"],
    )

    return summary


class PollingController:
    def __init__(self, database: Database, settings: Settings) -> None:
        self.database = database
        self.settings = settings
        self.mode = "auto" if settings.source_enabled else "manual"
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self._scan_lock = asyncio.Lock()
        self._started = False
        self.last_summary: dict[str, Any] | None = None
        self.last_error: str | None = None
        self.last_started_at: str | None = None
        self.last_completed_at: str | None = None

        self.scheduler.add_job(
            self.scheduled_scan,
            "interval",
            seconds=settings.poll_interval_seconds,
            kwargs={},
            id="virtuals-poll",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    def start(self) -> None:
        if self._started:
            return
        self.scheduler.start()
        self._started = True

    def shutdown(self) -> None:
        if not self._started:
            return
        self.scheduler.shutdown(wait=False)
        self._started = False

    async def scheduled_scan(self) -> None:
        if self.mode != "auto":
            return
        await self.scan_once(trigger="auto")

    async def scan_once(self, *, trigger: str) -> dict[str, Any]:
        async with self._scan_lock:
            self.last_started_at = utc_now_iso()
            self.last_error = None
            try:
                summary = await run_virtuals_poll(self.database, self.settings)
                self.last_completed_at = utc_now_iso()
                self.last_summary = {
                    **summary,
                    "trigger": trigger,
                    "started_at": self.last_started_at,
                    "completed_at": self.last_completed_at,
                }
                return self.last_summary
            except Exception as exc:
                self.last_completed_at = utc_now_iso()
                self.last_error = str(exc)
                logger.exception("polling scan failed trigger=%s", trigger)
                raise

    def set_mode(self, mode: str) -> dict[str, Any]:
        normalized = mode.strip().lower()
        if normalized not in {"auto", "manual"}:
            raise ValueError("mode must be 'auto' or 'manual'")
        should_trigger_scan = self.mode != "auto" and normalized == "auto"
        self.mode = normalized
        if should_trigger_scan and not self._scan_lock.locked():
            try:
                asyncio.create_task(self.scan_once(trigger="mode_switch"))
            except RuntimeError:
                logger.exception("failed to schedule mode switch scan")
        return self.get_status()

    def get_status(self) -> dict[str, Any]:
        source = self.database.get_source(self.settings.source_name) or {}
        return {
            "mode": self.mode,
            "running": self.mode == "auto",
            "is_scanning": self._scan_lock.locked(),
            "poll_interval_seconds": self.settings.poll_interval_seconds,
            "last_run": source.get("last_run"),
            "last_started_at": self.last_started_at,
            "last_completed_at": self.last_completed_at,
            "last_error": self.last_error,
            "last_summary": self.last_summary,
        }
