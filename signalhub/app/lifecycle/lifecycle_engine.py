from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from signalhub.app.database.models import ProjectEntity


class LifecycleEngine:
    def resolve_stage(
        self,
        existing: ProjectEntity | None,
        incoming: ProjectEntity,
        *,
        raw_data: dict[str, Any],
        content_changed: bool,
    ) -> str:
        previous_stage = existing.lifecycle_stage if existing else ""
        derived_stage = self._derive_stage(incoming, raw_data)
        if derived_stage:
            return self._prefer_progressive_stage(previous_stage, derived_stage)

        if existing is None:
            return "detected"

        if content_changed or existing.raw_hash != incoming.raw_hash:
            if previous_stage in {"", "detected", "info_updated"}:
                return "info_updated"

        return previous_stage or "detected"

    def _derive_stage(self, project: ProjectEntity, raw_data: dict[str, Any]) -> str | None:
        status = (project.status or "").strip().upper()

        if self._flag(raw_data, "inactive", "isInactive", "archived"):
            return "inactive"

        if self._flag(raw_data, "launchClosed", "isLaunchClosed", "saleClosed"):
            return "launch_closed"

        if project.contract_address and (
            status in {"ACTIVE", "LAUNCHED", "TRADING", "LIVE"}
            or "TRADE" in status
        ):
            return "token_trading"

        if self._flag(raw_data, "launchLive", "isLive", "tradingLive"):
            return "launch_live"

        if self._flag(raw_data, "launchOpen", "isOpen", "saleOpen", "openForLaunch") or "OPEN" in status:
            return "launch_open"

        if project.launch_time:
            if self._launch_due(project.launch_time) and status in {"ACTIVE", "LIVE"}:
                return "launch_live"
            return "launch_announced"

        if status in {"INACTIVE", "ARCHIVED"}:
            return "inactive"

        return None

    def _prefer_progressive_stage(self, previous: str, incoming: str) -> str:
        priority = {
            "detected": 0,
            "info_updated": 1,
            "launch_announced": 2,
            "launch_open": 3,
            "launch_live": 4,
            "launch_closed": 5,
            "token_trading": 6,
            "inactive": 7,
        }
        if not previous:
            return incoming
        return incoming if priority.get(incoming, -1) >= priority.get(previous, -1) else previous

    def _launch_due(self, launch_time: str) -> bool:
        try:
            launch_dt = datetime.fromisoformat(launch_time.replace("Z", "+00:00"))
        except ValueError:
            return False
        return launch_dt <= datetime.now(timezone.utc)

    def _flag(self, raw_data: dict[str, Any], *keys: str) -> bool:
        for key in keys:
            value = raw_data.get(key)
            if isinstance(value, bool):
                if value:
                    return True
                continue
            if str(value or "").strip().lower() in {"1", "true", "yes", "open", "live"}:
                return True
        return False
