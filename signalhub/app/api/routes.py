from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Literal
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from signalhub.app.config import Settings
from signalhub.app.database.db import Database
from signalhub.app.database.models import utc_now_iso
from signalhub.app.scheduler.polling import PollingController


router = APIRouter()


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_polling_controller(request: Request) -> PollingController:
    return request.app.state.polling_controller


class PollingModePayload(BaseModel):
    mode: Literal["auto", "manual"]


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
    upcoming_only: bool = Query(default=False),
    order_by: str = Query(default="last_seen_desc"),
) -> list[dict[str, Any]]:
    database = get_database(request)
    projects = database.list_entities(
        limit=limit,
        offset=offset,
        status=status,
        upcoming_only=upcoming_only,
        order_by=order_by,
    )
    return [_decorate_project(project) for project in projects]


@router.get("/launches/upcoming")
async def list_upcoming_launches(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    database = get_database(request)
    projects = database.list_upcoming_launches(limit=limit, offset=offset)
    return [_decorate_project(project) for project in projects]


@router.get("/projects/{project_id}")
async def get_project(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return _decorate_project(project)


@router.get("/projects/{project_id}/contract")
async def get_project_contract(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return {
        "project_id": project["project_id"],
        "display_title": _display_title(project),
        "name": project["name"],
        "symbol": project["symbol"],
        "contract_address": project["contract_address"],
        "status": project["status"],
        "launch_time": project["launch_time"],
        "url": project["url"],
    }


@router.get("/bot/feed/upcoming")
async def bot_upcoming_feed(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    within_hours: int = Query(default=72, ge=1, le=720),
    contract_ready_only: bool = Query(default=False),
) -> dict[str, Any]:
    database = get_database(request)
    projects = database.list_upcoming_launches_for_feed(
        limit=limit,
        within_hours=within_hours,
        contract_ready_only=contract_ready_only,
    )
    items = [_bot_project(project) for project in projects]
    return {
        "source": "virtuals",
        "generated_at": utc_now_iso(),
        "count": len(items),
        "within_hours": within_hours,
        "contract_ready_only": contract_ready_only,
        "projects": items,
    }


@router.get("/bot/feed/events")
async def bot_event_feed(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    since: str | None = Query(default=None),
    event_types: str | None = Query(default=None),
) -> dict[str, Any]:
    database = get_database(request)
    type_filter = tuple(
        item.strip()
        for item in (event_types or "").split(",")
        if item.strip()
    )
    events = database.list_events_for_feed(
        limit=limit,
        since=since,
        event_types=type_filter,
    )
    items = [_bot_event(event) for event in events]
    return {
        "source": "virtuals",
        "generated_at": utc_now_iso(),
        "count": len(items),
        "since": since,
        "event_types": list(type_filter),
        "events": items,
        "latest_time": items[0]["time"] if items else None,
        "oldest_time": items[-1]["time"] if items else None,
    }


@router.get("/bot/feed/snapshot")
async def bot_snapshot_feed(
    request: Request,
    project_limit: int = Query(default=50, ge=1, le=500),
    event_limit: int = Query(default=50, ge=1, le=500),
    within_hours: int = Query(default=72, ge=1, le=720),
) -> dict[str, Any]:
    settings = get_settings(request)
    database = get_database(request)
    controller = get_polling_controller(request)
    source = database.get_source(settings.source_name) or {}

    projects = database.list_upcoming_launches_for_feed(
        limit=project_limit,
        within_hours=within_hours,
        contract_ready_only=False,
    )
    events = database.list_events_for_feed(limit=event_limit)
    control = controller.get_status()

    project_items = [_bot_project(project) for project in projects]
    event_items = [_bot_event(event) for event in events]

    return {
        "source": {
            "name": settings.source_name,
            "type": settings.source_type,
            "mode": settings.virtuals_mode,
            "endpoint": settings.virtuals_endpoint,
            "last_run": source.get("last_run"),
        },
        "generated_at": utc_now_iso(),
        "polling_interval_seconds": settings.poll_interval_seconds,
        "control": {
            "mode": control["mode"],
            "running": control["running"],
            "is_scanning": control["is_scanning"],
            "last_run": control["last_run"],
            "last_error": control["last_error"],
        },
        "summary": {
            "projects_tracked": database.count_entities(),
            "upcoming_projects": database.count_upcoming_launches(),
            "events_recorded": database.count_events(),
        },
        "launches": {
            "count": len(project_items),
            "within_hours": within_hours,
            "items": project_items,
        },
        "events": {
            "count": len(event_items),
            "items": event_items,
        },
    }


@router.get("/control/polling")
async def get_polling_status(request: Request) -> dict[str, Any]:
    controller = get_polling_controller(request)
    return controller.get_status()


@router.post("/control/polling/mode")
async def set_polling_mode(payload: PollingModePayload, request: Request) -> dict[str, Any]:
    controller = get_polling_controller(request)
    try:
        return controller.set_mode(payload.mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/control/polling/scan")
async def trigger_scan(request: Request) -> dict[str, Any]:
    controller = get_polling_controller(request)
    return await controller.scan_once(trigger="manual")


@router.get("/system/status")
async def system_status(request: Request) -> dict[str, Any]:
    database = get_database(request)
    settings = get_settings(request)
    source = database.get_source(settings.source_name) or {}
    controller = get_polling_controller(request)
    control = controller.get_status()

    return {
        "app_name": settings.app_name,
        "source": {
            "name": settings.source_name,
            "type": settings.source_type,
            "endpoint": settings.virtuals_endpoint,
            "enabled": settings.source_enabled,
            "sample_mode": settings.sample_mode,
            "mode": settings.virtuals_mode,
            "last_run": source.get("last_run"),
        },
        "control": control,
        "polling_interval_seconds": settings.poll_interval_seconds,
        "projects_tracked": database.count_entities(),
        "upcoming_projects": database.count_upcoming_launches(),
        "events_recorded": database.count_events(),
        "recent_new_projects": [
            _decorate_project(project)
            for project in database.list_recent_new_projects(limit=5)
        ],
        "upcoming_launches": [
            _decorate_project(project)
            for project in database.list_upcoming_launches(limit=5)
        ],
    }


@router.get("/dashboard")
async def dashboard(request: Request) -> FileResponse:
    settings = get_settings(request)
    dashboard_path = Path(settings.dashboard_path)
    if not dashboard_path.exists():
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return FileResponse(dashboard_path)


def _display_title(project: dict[str, Any]) -> str:
    token_name = project.get("symbol") or project.get("name") or project.get("project_id")
    return f"${token_name}"


def _decorate_project(project: dict[str, Any]) -> dict[str, Any]:
    return {
        **project,
        "display_title": _display_title(project),
    }


def _bot_project(project: dict[str, Any]) -> dict[str, Any]:
    launch_time = project.get("launch_time")
    return {
        "project_id": project["project_id"],
        "display_title": _display_title(project),
        "name": project["name"],
        "symbol": project["symbol"],
        "status": project["status"],
        "launch_time": launch_time,
        "seconds_to_launch": _seconds_to_launch(launch_time),
        "contract_address": project["contract_address"],
        "contract_ready": bool(project["contract_address"]),
        "url": project["url"],
        "creator": project["creator"],
        "description": project["description"],
        "created_time": project["created_time"],
        "last_seen": project["last_seen"],
    }


def _bot_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    return {
        "event_id": event["id"],
        "type": event["type"],
        "time": event["time"],
        "target": event["target"],
        "name": payload.get("name"),
        "symbol": payload.get("symbol"),
        "status": payload.get("status") or payload.get("new_status"),
        "contract_address": payload.get("contract_address", ""),
        "url": payload.get("url", ""),
        "changes": payload.get("changes"),
        "payload": payload,
    }


def _seconds_to_launch(launch_time: str | None) -> int | None:
    if not launch_time:
        return None
    try:
        launch_dt = datetime.fromisoformat(launch_time.replace("Z", "+00:00"))
    except ValueError:
        return None
    now = datetime.now(timezone.utc)
    return int((launch_dt - now).total_seconds())
