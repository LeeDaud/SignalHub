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
from signalhub.app.scoring import ScoreEngine


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
    target: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    database = get_database(request)
    return [
        _decorate_event(event)
        for event in database.list_events(
            limit=limit,
            offset=offset,
            event_type=type,
            source=source,
            target=target,
        )
    ]


@router.get("/projects")
async def list_projects(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    stage: str | None = Query(default=None),
    upcoming_only: bool = Query(default=False),
    watchlist_only: bool = Query(default=False),
    order_by: str = Query(default="last_seen_desc"),
) -> list[dict[str, Any]]:
    database = get_database(request)
    projects = database.list_entities(
        limit=limit,
        offset=offset,
        status=status,
        stage=stage,
        upcoming_only=upcoming_only,
        watchlist_only=watchlist_only,
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


@router.get("/projects/{project_id}/score")
async def get_project_score(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return {
        "project_id": project["project_id"],
        "display_title": _display_title(project),
        "score": _coerce_score(project.get("project_score")),
        "score_grade": _score_grade(project.get("project_score")),
        "risk_level": _score_risk(project.get("project_score"), project.get("risk_level")),
        "lifecycle_stage": project["lifecycle_stage"],
    }


@router.get("/projects/{project_id}/changes")
async def get_project_changes(
    project_id: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    changes = database.list_project_changes(project_id, limit=limit)
    return {
        "project_id": project_id,
        "count": len(changes),
        "changes": changes,
    }


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


@router.get("/projects/{project_id}/analysis")
async def get_project_analysis(
    project_id: str,
    request: Request,
    event_limit: int = Query(default=20, ge=1, le=100),
    change_limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    return {
        "project": _decorate_project(project),
        "score": {
            "score": _coerce_score(project.get("project_score")),
            "score_grade": _score_grade(project.get("project_score")),
            "risk_level": _score_risk(project.get("project_score"), project.get("risk_level")),
        },
        "changes": database.list_project_changes(project_id, limit=change_limit),
        "addresses": database.list_project_addresses(project_id),
        "events": [
            _decorate_event(event)
            for event in database.list_events(limit=event_limit, target=project_id)
        ],
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


@router.get("/watchlist")
async def list_watchlist(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    database = get_database(request)
    return [_decorate_project(project) for project in database.list_watchlist(limit=limit)]


@router.post("/watchlist/{project_id}")
async def add_watchlist(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    if not database.set_watchlist(project_id, True):
        raise HTTPException(status_code=404, detail="Project not found")
    project = database.get_entity_detail(project_id)
    assert project is not None
    return {"project_id": project_id, "watchlist": True, "project": _decorate_project(project)}


@router.delete("/watchlist/{project_id}")
async def remove_watchlist(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    if not database.set_watchlist(project_id, False):
        raise HTTPException(status_code=404, detail="Project not found")
    project = database.get_entity_detail(project_id)
    assert project is not None
    return {"project_id": project_id, "watchlist": False, "project": _decorate_project(project)}


@router.get("/lifecycle/stats")
async def lifecycle_stats(request: Request) -> dict[str, Any]:
    database = get_database(request)
    return {
        "count": database.count_entities(),
        "stages": database.count_lifecycle_stages(),
    }


@router.get("/rankings/top-projects")
async def top_scored_projects(
    request: Request,
    limit: int = Query(default=10, ge=1, le=100),
) -> list[dict[str, Any]]:
    database = get_database(request)
    return [_decorate_project(project) for project in database.list_top_scored_projects(limit=limit)]


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
        "watchlist_count": database.count_watchlist(),
        "lifecycle_stats": database.count_lifecycle_stages(),
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


def _coerce_score(value: Any) -> int:
    return ScoreEngine.normalize_score(value)


def _score_grade(value: Any) -> str:
    return ScoreEngine.grade(value)


def _score_risk(value: Any, fallback: str | None = None) -> str:
    if value is None and fallback:
        return str(fallback)
    return ScoreEngine.risk_level(value)


def _decorate_project(project: dict[str, Any]) -> dict[str, Any]:
    score_value = _coerce_score(project.get("project_score"))
    return {
        **project,
        "project_score": score_value,
        "score_grade": _score_grade(score_value),
        "risk_level": _score_risk(score_value, project.get("risk_level")),
        "display_title": _display_title(project),
    }


def _bot_project(project: dict[str, Any]) -> dict[str, Any]:
    launch_time = project.get("launch_time")
    score_value = _coerce_score(project.get("project_score"))
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
        "team": project.get("team", ""),
        "links": project.get("links", []),
        "tokenomics": project.get("tokenomics", ""),
        "created_time": project["created_time"],
        "last_seen": project["last_seen"],
        "lifecycle_stage": project.get("lifecycle_stage"),
        "project_score": score_value,
        "score_grade": _score_grade(score_value),
        "risk_level": _score_risk(score_value, project.get("risk_level")),
        "watchlist": bool(project.get("watchlist")),
    }


def _decorate_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    if payload.get("project_score") is not None:
        score_value = _coerce_score(payload.get("project_score"))
        payload["project_score"] = score_value
        payload["score_grade"] = payload.get("score_grade") or _score_grade(score_value)
        payload["risk_level"] = _score_risk(score_value, payload.get("risk_level"))
    elif payload.get("score_grade"):
        payload["score_grade"] = str(payload["score_grade"]).strip().upper()
    return {
        **event,
        "payload": payload,
    }


def _bot_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = _decorate_event(event).get("payload") or {}
    return {
        "event_id": event["id"],
        "type": event["type"],
        "time": event["time"],
        "target": event["target"],
        "name": payload.get("name"),
        "symbol": payload.get("symbol"),
        "status": payload.get("status") or payload.get("new_status"),
        "lifecycle_stage": payload.get("lifecycle_stage") or payload.get("new_stage"),
        "project_score": payload.get("project_score", 0),
        "score_grade": payload.get("score_grade"),
        "risk_level": payload.get("risk_level"),
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
