from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse

from signalhub.app.config import Settings
from signalhub.app.database.db import Database


router = APIRouter()


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


@router.get("/healthz")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/events")
async def list_events(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    type: str | None = Query(default=None),
    source: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    database = get_database(request)
    return database.list_events(
        limit=limit,
        offset=offset,
        event_type=type,
        source=source,
    )


@router.get("/projects")
async def list_projects(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    database = get_database(request)
    return database.list_entities(limit=limit, offset=offset, status=status)


@router.get("/projects/{project_id}")
async def get_project(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.get("/system/status")
async def system_status(request: Request) -> dict[str, Any]:
    database = get_database(request)
    settings = get_settings(request)
    source = database.get_source(settings.source_name) or {}

    return {
        "app_name": settings.app_name,
        "source": {
            "name": settings.source_name,
            "type": settings.source_type,
            "endpoint": settings.virtuals_endpoint,
            "enabled": settings.source_enabled,
            "sample_mode": settings.sample_mode,
            "last_run": source.get("last_run"),
        },
        "polling_interval_seconds": settings.poll_interval_seconds,
        "projects_tracked": database.count_entities(),
        "events_recorded": database.count_events(),
        "recent_new_projects": database.list_recent_new_projects(limit=5),
    }


@router.get("/dashboard")
async def dashboard(request: Request) -> FileResponse:
    settings = get_settings(request)
    dashboard_path = Path(settings.dashboard_path)
    if not dashboard_path.exists():
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return FileResponse(dashboard_path)
