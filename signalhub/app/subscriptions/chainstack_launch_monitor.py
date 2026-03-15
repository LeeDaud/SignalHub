from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

import websockets

from signalhub.app.config import Settings
from signalhub.app.database.db import Database
from signalhub.app.database.models import (
    ProjectAddress,
    ProjectLaunchTrace,
    SignalEvent,
    build_event_id,
    links_to_json,
    utc_now_iso,
)
from signalhub.app.explorer.basescan_trace import TRANSFER_TOPIC, BaseLaunchTraceService
from signalhub.app.exports import TokenPoolExportService


logger = logging.getLogger(__name__)


class ChainstackLaunchMonitor:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        trace_service: BaseLaunchTraceService,
        exporter: TokenPoolExportService | None = None,
    ) -> None:
        self.database = database
        self.settings = settings
        self.trace_service = trace_service
        self.exporter = exporter
        self.enabled = bool(
            settings.chainstack_subscription_enabled
            and settings.chainstack_base_https_url
            and settings.chainstack_base_wss_url
        )
        self.refresh_seconds = settings.chainstack_subscription_refresh_seconds
        self.backfill_enabled = bool(
            settings.chainstack_trace_backfill_enabled
            and settings.chainstack_base_https_url
        )
        self.backfill_batch_size = settings.chainstack_trace_backfill_batch_size
        self.backfill_cooldown_seconds = settings.chainstack_trace_backfill_cooldown_seconds
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._connection: websockets.WebSocketClientProtocol | None = None
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._subscription_by_project: dict[str, str] = {}
        self._project_by_subscription: dict[str, dict[str, Any]] = {}
        self._request_id = 0
        self._connected = False
        self._last_error: str | None = None
        self._last_connected_at: str | None = None
        self._last_message_at: str | None = None
        self._project_locks: dict[str, asyncio.Lock] = {}
        self._last_trace_attempt_at: dict[str, float] = {}
        self._prelaunch_statuses = {
            self._normalize_status(item)
            for item in settings.chainstack_prelaunch_statuses
            if self._normalize_status(item)
        }

    def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def shutdown(self) -> None:
        self._stop_event.set()
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    def get_status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "connected": self._connected,
            "subscriptions": len(self._subscription_by_project),
            "wss_url_configured": bool(self.settings.chainstack_base_wss_url),
            "https_url_configured": bool(self.settings.chainstack_base_https_url),
            "trace_backfill_enabled": self.backfill_enabled,
            "trace_backfill_batch_size": self.backfill_batch_size,
            "trace_backfill_cooldown_seconds": self.backfill_cooldown_seconds,
            "last_connected_at": self._last_connected_at,
            "last_message_at": self._last_message_at,
            "last_error": self._last_error,
        }

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = str(exc)
                self._connected = False
                logger.exception("chainstack launch monitor failed")
                await asyncio.sleep(5)

    async def _connect_and_run(self) -> None:
        assert self.settings.chainstack_base_wss_url is not None
        self._reset_connection_state()
        async with websockets.connect(
            self.settings.chainstack_base_wss_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as websocket:
            self._connection = websocket
            self._connected = True
            self._last_connected_at = utc_now_iso()
            self._last_error = None
            reader = asyncio.create_task(self._reader_loop())
            try:
                while not self._stop_event.is_set():
                    await self._reconcile_subscriptions()
                    await asyncio.sleep(self.refresh_seconds)
            finally:
                reader.cancel()
                with suppress(asyncio.CancelledError):
                    await reader
                self._connected = False
                self._connection = None
                self._reset_connection_state()

    def _reset_connection_state(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()
        self._subscription_by_project.clear()
        self._project_by_subscription.clear()

    async def _reader_loop(self) -> None:
        assert self._connection is not None
        async for raw_message in self._connection:
            self._last_message_at = utc_now_iso()
            payload = json.loads(raw_message)
            if "id" in payload:
                future = self._pending.pop(int(payload["id"]), None)
                if future and not future.done():
                    if payload.get("error"):
                        future.set_exception(RuntimeError(str(payload["error"])))
                    else:
                        future.set_result(payload.get("result"))
                continue

            params = payload.get("params") or {}
            if payload.get("method") == "eth_subscription":
                await self._handle_subscription_message(params)

    async def _handle_subscription_message(self, params: dict[str, Any]) -> None:
        subscription_id = str(params.get("subscription") or "").strip()
        log_payload = params.get("result") or {}
        project = self._project_by_subscription.get(subscription_id)
        if not project or not isinstance(log_payload, dict):
            return
        await self._process_log(project, log_payload)

    async def _process_log(self, project: dict[str, Any], log_payload: dict[str, Any]) -> None:
        tx_hash = str(log_payload.get("transactionHash") or "").strip()
        await self.trace_project(
            project,
            trigger="subscription_log",
            tx_hash=tx_hash,
            force=True,
        )

    async def trace_project(
        self,
        project: dict[str, Any],
        *,
        trigger: str,
        tx_hash: str | None = None,
        force: bool = False,
        raise_on_error: bool = False,
    ) -> dict[str, Any] | None:
        project_id = str(project["project_id"])
        lock = self._project_locks.setdefault(project_id, asyncio.Lock())
        if lock.locked():
            if raise_on_error:
                raise RuntimeError(f"Trace already in progress for project {project_id}")
            return None

        async with lock:
            existing_trace = self.database.get_project_launch_trace(project_id) or {}
            if tx_hash and existing_trace.get("launch_tx_hash") == tx_hash and existing_trace.get("internal_market_address"):
                return None
            if not force and not self._should_trace_project(project_id):
                return None

            self._mark_trace_attempt(project_id)
            try:
                trace = await self.trace_service.trace_token_launch(
                    str(project.get("token_address") or project.get("contract_address") or ""),
                    launch_time_hint=project.get("launch_time"),
                    created_time_hint=project.get("created_time"),
                    project_status=project.get("status"),
                )
            except Exception as exc:
                logger.exception("chainstack trace failed project_id=%s trigger=%s", project_id, trigger)
                self._upsert_trace_error(project, existing_trace, str(exc))
                if raise_on_error:
                    if isinstance(exc, RuntimeError):
                        raise
                    raise RuntimeError(str(exc)) from exc
                return None

            self.persist_trace_result(project, trace, existing_trace=existing_trace)
            return trace

    def persist_trace_result(
        self,
        project: dict[str, Any],
        trace: dict[str, Any],
        *,
        existing_trace: dict[str, Any] | None = None,
    ) -> ProjectLaunchTrace:
        project_id = str(project["project_id"])
        current_trace = existing_trace or self.database.get_project_launch_trace(project_id) or {}
        now = utc_now_iso()
        launch_trace = self._build_launch_trace(project, trace, current_trace, now)
        self.database.upsert_project_launch_trace(launch_trace)
        if launch_trace.internal_market_address:
            self.database.set_internal_market_address(
                project_id,
                launch_trace.internal_market_address,
            )
            if self.exporter is not None:
                self.exporter.refresh()

        new_addresses = self._build_new_addresses(project_id, trace, now)
        self.database.upsert_project_addresses(new_addresses)

        latest_project = self.database.get_entity_detail(project_id) or project
        if launch_trace.launch_tx_hash and launch_trace.launch_tx_hash != (current_trace.get("launch_tx_hash") or ""):
            self.database.create_event(self._build_launch_event(latest_project, launch_trace))

        if (
            launch_trace.internal_market_address
            and launch_trace.internal_market_address.lower()
            != str(current_trace.get("internal_market_address") or "").lower()
        ):
            self.database.create_event(self._build_internal_market_event(latest_project, launch_trace))

        return launch_trace

    def _upsert_trace_error(
        self,
        project: dict[str, Any],
        existing_trace: dict[str, Any],
        error_message: str,
    ) -> None:
        now = utc_now_iso()
        trace = ProjectLaunchTrace(
            project_id=str(project["project_id"]),
            token_contract=str(project.get("token_address") or project.get("contract_address") or ""),
            launch_tx_hash=str(existing_trace.get("launch_tx_hash") or ""),
            launch_block_number=existing_trace.get("launch_block_number"),
            internal_market_address=str(existing_trace.get("internal_market_address") or ""),
            intermediate_address=str(existing_trace.get("intermediate_address") or ""),
            mint_recipient=str(existing_trace.get("mint_recipient") or ""),
            launch_sender=str(existing_trace.get("launch_sender") or ""),
            launch_target=str(existing_trace.get("launch_target") or ""),
            confidence=str(existing_trace.get("confidence") or ""),
            evidence=str(existing_trace.get("evidence") or ""),
            subscription_status="error",
            first_seen=str(existing_trace.get("first_seen") or now),
            last_seen=now,
            last_error=error_message,
            notes_json=links_to_json(existing_trace.get("notes") or []),
        )
        self.database.upsert_project_launch_trace(trace)

    def _build_launch_trace(
        self,
        project: dict[str, Any],
        trace: dict[str, Any],
        existing_trace: dict[str, Any],
        now: str,
    ) -> ProjectLaunchTrace:
        launch_transaction = trace.get("launch_transaction") or {}
        earliest_transfer = trace.get("earliest_transfer") or {}
        launch_candidate = trace.get("launch_candidate") or {}
        status = "traced"
        if launch_candidate.get("address"):
            status = "confirmed"
        elif existing_trace.get("internal_market_address"):
            status = "confirmed"
        elif launch_transaction.get("hash"):
            status = "launch_seen"

        return ProjectLaunchTrace(
            project_id=str(project["project_id"]),
            token_contract=str(
                trace.get("token_contract")
                or project.get("token_address")
                or project.get("contract_address")
                or ""
            ),
            launch_tx_hash=str(
                launch_transaction.get("hash")
                or earliest_transfer.get("tx_hash")
                or existing_trace.get("launch_tx_hash")
                or ""
            ),
            launch_block_number=launch_transaction.get("block_number") or earliest_transfer.get("block_number"),
            internal_market_address=str(
                launch_candidate.get("address")
                or existing_trace.get("internal_market_address")
                or ""
            ),
            intermediate_address=str(
                launch_candidate.get("intermediate_address")
                or existing_trace.get("intermediate_address")
                or ""
            ),
            mint_recipient=str(launch_candidate.get("mint_recipient") or existing_trace.get("mint_recipient") or ""),
            launch_sender=str(launch_candidate.get("launch_sender") or existing_trace.get("launch_sender") or ""),
            launch_target=str(launch_candidate.get("launch_target") or existing_trace.get("launch_target") or ""),
            confidence=str(launch_candidate.get("confidence") or existing_trace.get("confidence") or ""),
            evidence=str(launch_candidate.get("evidence") or existing_trace.get("evidence") or ""),
            subscription_status=status,
            first_seen=str(existing_trace.get("first_seen") or now),
            last_seen=now,
            last_error="",
            notes_json=links_to_json(trace.get("notes") or []),
        )

    def _build_new_addresses(
        self,
        project_id: str,
        trace: dict[str, Any],
        first_seen: str,
    ) -> list[ProjectAddress]:
        launch_candidate = trace.get("launch_candidate") or {}
        candidates = [
            (launch_candidate.get("launch_sender"), "launch_sender"),
            (launch_candidate.get("launch_target"), "launch_target"),
            (launch_candidate.get("intermediate_address"), "launch_intermediate"),
            (launch_candidate.get("mint_recipient"), "mint_recipient"),
            (launch_candidate.get("address"), "internal_market"),
        ]
        addresses: list[ProjectAddress] = []
        for address, address_type in candidates:
            value = str(address or "").strip()
            if not value:
                continue
            if self.database.has_project_address(project_id, value, address_type, chain="BASE"):
                continue
            addresses.append(
                ProjectAddress(
                    project_id=project_id,
                    address=value,
                    address_type=address_type,
                    chain="BASE",
                    first_seen=first_seen,
                )
            )
        return addresses

    def _build_launch_event(
        self,
        project: dict[str, Any],
        trace: ProjectLaunchTrace,
    ) -> SignalEvent:
        return SignalEvent(
            event_id=build_event_id(),
            source="chainstack_launch_monitor",
            type="project_launch_detected",
            target=trace.project_id,
            time=utc_now_iso(),
            payload={
                "name": project.get("name"),
                "symbol": project.get("symbol"),
                "url": project.get("url"),
                "status": project.get("status"),
                "token_address": project.get("token_address"),
                "contract_address": project.get("contract_address"),
                "launch_time": project.get("launch_time"),
                "launch_tx_hash": trace.launch_tx_hash,
                "launch_block_number": trace.launch_block_number,
                "intermediate_address": trace.intermediate_address,
                "launch_sender": trace.launch_sender,
                "launch_target": trace.launch_target,
                "mint_recipient": trace.mint_recipient,
                "internal_market_address": trace.internal_market_address,
                "pool_address": trace.internal_market_address,
            },
        )

    def _build_internal_market_event(
        self,
        project: dict[str, Any],
        trace: ProjectLaunchTrace,
    ) -> SignalEvent:
        return SignalEvent(
            event_id=build_event_id(),
            source="chainstack_launch_monitor",
            type="project_internal_market_detected",
            target=trace.project_id,
            time=utc_now_iso(),
            payload={
                "name": project.get("name"),
                "symbol": project.get("symbol"),
                "url": project.get("url"),
                "status": project.get("status"),
                "token_address": project.get("token_address"),
                "contract_address": project.get("contract_address"),
                "launch_time": project.get("launch_time"),
                "launch_tx_hash": trace.launch_tx_hash,
                "internal_market_address": trace.internal_market_address,
                "pool_address": trace.internal_market_address,
                "confidence": trace.confidence,
                "evidence": trace.evidence,
                "intermediate_address": trace.intermediate_address,
                "launch_sender": trace.launch_sender,
                "launch_target": trace.launch_target,
                "mint_recipient": trace.mint_recipient,
            },
        )

    async def _reconcile_subscriptions(self) -> None:
        if self._connection is None:
            return

        desired_projects = {
            item["project_id"]: item
            for item in self._sorted_subscription_projects(
                self.database.list_projects_for_chainstack_subscription(limit=500)
            )
        }

        for project_id, project in desired_projects.items():
            if project_id in self._subscription_by_project:
                continue
            result = await self._rpc(
                "eth_subscribe",
                [
                    "logs",
                    {
                        "address": project.get("token_address") or project.get("contract_address"),
                        "topics": [TRANSFER_TOPIC],
                    },
                ],
            )
            subscription_id = str(result or "").strip()
            if not subscription_id:
                continue
            self._subscription_by_project[project_id] = subscription_id
            self._project_by_subscription[subscription_id] = project

        for project_id, subscription_id in list(self._subscription_by_project.items()):
            if project_id in desired_projects:
                continue
            with suppress(Exception):
                await self._rpc("eth_unsubscribe", [subscription_id])
            self._subscription_by_project.pop(project_id, None)
            self._project_by_subscription.pop(subscription_id, None)

        await self._backfill_projects(list(desired_projects.values()))

    async def _backfill_projects(self, projects: list[dict[str, Any]]) -> None:
        if not self.backfill_enabled:
            return

        traced = 0
        for project in self._sorted_subscription_projects(projects):
            if traced >= self.backfill_batch_size:
                break
            project_id = str(project.get("project_id") or "")
            if not project_id or not self._should_trace_project(project_id):
                continue
            result = await self.trace_project(project, trigger="backfill")
            if result is not None:
                traced += 1

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        if self._connection is None:
            raise RuntimeError("Chainstack WSS connection is not available")
        self._request_id += 1
        request_id = self._request_id
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = future
        await self._connection.send(json.dumps(payload))
        return await asyncio.wait_for(future, timeout=20)

    def _sorted_subscription_projects(
        self,
        projects: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return sorted(
            projects,
            key=lambda item: (
                0 if self._is_prelaunch_status(item.get("status")) else 1,
                self._launch_phase_rank(item.get("launch_time")),
                self._launch_sort_value(item.get("launch_time")),
                str(item.get("project_id") or ""),
            ),
        )

    def _is_prelaunch_status(self, value: Any) -> bool:
        return self._normalize_status(value) in self._prelaunch_statuses

    def _normalize_status(self, value: Any) -> str:
        text = str(value or "").strip().upper()
        for token in (" ", "-", "_"):
            text = text.replace(token, "")
        return text

    def _launch_phase_rank(self, launch_time: Any) -> int:
        launch_dt = self._parse_iso_datetime(launch_time)
        if launch_dt is None:
            return 2
        now = datetime.now(timezone.utc)
        return 0 if launch_dt >= now else 1

    def _launch_sort_value(self, launch_time: Any) -> float:
        launch_dt = self._parse_iso_datetime(launch_time)
        if launch_dt is None:
            return float("inf")
        timestamp = launch_dt.timestamp()
        if launch_dt >= datetime.now(timezone.utc):
            return timestamp
        return -timestamp

    def _parse_iso_datetime(self, value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _should_trace_project(self, project_id: str) -> bool:
        last_attempt = self._last_trace_attempt_at.get(project_id)
        if last_attempt is None:
            return True
        return (time.monotonic() - last_attempt) >= self.backfill_cooldown_seconds

    def _mark_trace_attempt(self, project_id: str) -> None:
        self._last_trace_attempt_at[project_id] = time.monotonic()
