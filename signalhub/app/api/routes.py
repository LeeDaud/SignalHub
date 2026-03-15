from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Literal
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from signalhub.app.config import Settings
from signalhub.app.database.db import Database
from signalhub.app.database.models import utc_now_iso
from signalhub.app.explorer import BaseLaunchTraceService
from signalhub.app.exports import TokenPoolExportService
from signalhub.app.scheduler.polling import PollingController
from signalhub.app.scoring import ScoreEngine
from signalhub.app.subscriptions import ChainstackLaunchMonitor


router = APIRouter()


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_polling_controller(request: Request) -> PollingController:
    return request.app.state.polling_controller


def get_base_trace_service(request: Request) -> BaseLaunchTraceService:
    return request.app.state.base_trace_service


def get_launch_monitor(request: Request) -> ChainstackLaunchMonitor:
    return request.app.state.launch_monitor


def get_token_pool_exporter(request: Request) -> TokenPoolExportService:
    return request.app.state.token_pool_exporter


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
    internal_market = database.get_internal_market_detail(project_id) or {}
    return {
        "project_id": project["project_id"],
        "display_title": _display_title(project),
        "name": project["name"],
        "symbol": project["symbol"],
        "token_address": project.get("token_address", ""),
        "contract_address": project["contract_address"],
        "internal_market_address": project.get("internal_market_address") or internal_market.get("internal_market_address", ""),
        "pool_address": project.get("internal_market_address") or internal_market.get("internal_market_address", ""),
        "intermediate_address": internal_market.get("intermediate_address", ""),
        "status": project["status"],
        "launch_time": project["launch_time"],
        "url": project["url"],
    }


@router.get("/projects/{project_id}/launch-trace")
async def get_project_launch_trace(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    project = database.get_entity_detail(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    token_address = str(project.get("token_address") or project.get("contract_address") or "").strip()
    if not token_address:
        raise HTTPException(status_code=409, detail="Project token address is not available yet")

    monitor = get_launch_monitor(request)
    try:
        trace = await monitor.trace_project(
            project,
            trigger="manual_api",
            force=True,
            raise_on_error=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if trace is None:
        trace = {}

    return {
        "project_id": project["project_id"],
        "display_title": _display_title(project),
        "name": project["name"],
        "symbol": project["symbol"],
        "status": project["status"],
        "token_address": token_address,
        "contract_address": project.get("contract_address", ""),
        "trace": trace,
    }


@router.get("/projects/{project_id}/internal-market")
async def get_project_internal_market(project_id: str, request: Request) -> dict[str, Any]:
    database = get_database(request)
    detail = database.get_internal_market_detail(project_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return {
        "project_id": detail["project_id"],
        "display_title": _display_title(detail),
        "name": detail["name"],
        "symbol": detail["symbol"],
        "status": detail["status"],
        "token_address": detail.get("token_address", ""),
        "contract_address": detail["contract_address"],
        "launch_time": detail["launch_time"],
        "internal_market_address": detail["internal_market_address"],
        "pool_address": detail["internal_market_address"],
        "launch_tx_hash": detail["launch_tx_hash"],
        "launch_block_number": detail["launch_block_number"],
        "intermediate_address": detail.get("intermediate_address", ""),
        "launch_sender": detail["launch_sender"],
        "launch_target": detail["launch_target"],
        "mint_recipient": detail["mint_recipient"],
        "confidence": detail["confidence"],
        "evidence": detail["evidence"],
        "subscription_status": detail["subscription_status"],
        "last_seen": detail["last_seen"],
        "last_error": detail["last_error"],
        "notes": detail["notes"],
    }


@router.get("/explorers/base/launch-trace/{contract_address}")
async def get_base_launch_trace(
    contract_address: str,
    request: Request,
    launch_time_hint: str | None = Query(default=None),
    created_time_hint: str | None = Query(default=None),
    project_status: str | None = Query(default=None),
) -> dict[str, Any]:
    service = get_base_trace_service(request)
    try:
        return await service.trace_token_launch(
            contract_address,
            launch_time_hint=launch_time_hint,
            created_time_hint=created_time_hint,
            project_status=project_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


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


@router.get("/bot/feed/internal-markets")
async def bot_internal_market_feed(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    database = get_database(request)
    items = [
        _bot_internal_market_item(item)
        for item in database.list_internal_market_details(limit=limit)
    ]
    return {
        "source": "virtuals",
        "generated_at": utc_now_iso(),
        "count": len(items),
        "items": items,
    }


@router.get("/bot/feed/token-pools")
async def bot_token_pool_feed(
    request: Request,
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    exporter = get_token_pool_exporter(request)
    payload = _build_token_pool_payload(exporter, limit=limit)
    items = payload.get("items", [])
    confirmed_count = int(payload.get("confirmed_count") or 0)
    return {
        "source": "virtuals",
        "generated_at": payload.get("generated_at") or utc_now_iso(),
        "count": len(items),
        "confirmed_count": confirmed_count,
        "items": items,
        "total_available": payload.get("count", len(items)),
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


@router.get("/bot/feed/unified")
@router.get("/bot/feed/snapshot")
async def bot_snapshot_feed(
    request: Request,
    project_limit: int = Query(default=50, ge=1, le=500),
    event_limit: int = Query(default=50, ge=1, le=500),
    internal_market_limit: int = Query(default=100, ge=1, le=500),
    token_pool_limit: int = Query(default=500, ge=1, le=5000),
    within_hours: int = Query(default=72, ge=1, le=720),
) -> dict[str, Any]:
    settings = get_settings(request)
    database = get_database(request)
    controller = get_polling_controller(request)
    exporter = get_token_pool_exporter(request)
    source = database.get_source(settings.source_name) or {}

    projects = database.list_upcoming_launches_for_feed(
        limit=project_limit,
        within_hours=within_hours,
        contract_ready_only=False,
    )
    internal_markets = database.list_internal_market_details(limit=internal_market_limit)
    token_pool_payload = _build_token_pool_payload(exporter, limit=token_pool_limit)
    events = database.list_events_for_feed(limit=event_limit)
    control = controller.get_status()

    project_items = [_bot_project(project) for project in projects]
    internal_market_items = [_bot_internal_market_item(item) for item in internal_markets]
    event_items = [_bot_event(event) for event in events]
    token_pool_items = token_pool_payload.get("items", [])
    confirmed_pool_count = int(token_pool_payload.get("confirmed_count") or 0)

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
            "confirmed_internal_markets": confirmed_pool_count,
            "token_pool_items": int(token_pool_payload.get("count") or len(token_pool_items)),
        },
        "launches": {
            "count": len(project_items),
            "within_hours": within_hours,
            "items": project_items,
        },
        "internal_markets": {
            "count": len(internal_market_items),
            "items": internal_market_items,
        },
        "token_pools": {
            "count": len(token_pool_items),
            "confirmed_count": confirmed_pool_count,
            "total_available": token_pool_payload.get("count", len(token_pool_items)),
            "items": token_pool_items,
        },
        "events": {
            "count": len(event_items),
            "items": event_items,
            "latest_time": event_items[0]["time"] if event_items else None,
            "oldest_time": event_items[-1]["time"] if event_items else None,
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
    launch_monitor = get_launch_monitor(request)
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
        "chainstack_subscription": launch_monitor.get_status(),
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


@router.get("/exports/token-pools.json")
async def token_pools_export(request: Request) -> JSONResponse:
    exporter = get_token_pool_exporter(request)
    payload = exporter.build_payload()
    exporter.refresh()
    return JSONResponse(content=payload)


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
        "token_address": project.get("token_address", ""),
        "internal_market_address": project.get("internal_market_address", ""),
        "pool_address": project.get("internal_market_address", ""),
        "contract_ready": bool(project.get("token_address") or project["contract_address"]),
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


def _bot_internal_market_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "project_id": item["project_id"],
        "display_title": _display_title(item),
        "name": item["name"],
        "symbol": item["symbol"],
        "status": item["status"],
        "token_address": item.get("token_address", ""),
        "contract_address": item["contract_address"],
        "internal_market_address": item["internal_market_address"],
        "pool_address": item["internal_market_address"],
        "intermediate_address": item.get("intermediate_address", ""),
        "launch_tx_hash": item["launch_tx_hash"],
        "launch_block_number": item["launch_block_number"],
        "launch_sender": item["launch_sender"],
        "launch_target": item["launch_target"],
        "mint_recipient": item["mint_recipient"],
        "confidence": item["confidence"],
        "evidence": item["evidence"],
        "subscription_status": item["subscription_status"],
        "last_seen": item["last_seen"],
        "url": item["url"],
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
        "token_address": payload.get("token_address", ""),
        "pool_address": payload.get("pool_address") or payload.get("internal_market_address", ""),
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


def _build_token_pool_payload(
    exporter: TokenPoolExportService,
    *,
    limit: int,
) -> dict[str, Any]:
    payload = exporter.build_payload()
    items = list(payload.get("items", []))[:limit]
    confirmed_count = sum(1 for item in items if str(item.get("pool_address") or "").strip())
    return {
        **payload,
        "items": items,
        "confirmed_count": confirmed_count,
    }
