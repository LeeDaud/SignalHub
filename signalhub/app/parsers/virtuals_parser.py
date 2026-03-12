from __future__ import annotations

from typing import Any

from signalhub.app.database.models import (
    ProjectEntity,
    build_raw_hash,
    build_url_hash,
    utc_now_iso,
)


class VirtualsParser:
    def parse_projects(self, payload: Any) -> list[ProjectEntity]:
        rows = self._extract_items(payload)
        last_seen = utc_now_iso()
        projects: list[ProjectEntity] = []
        seen_project_ids: set[str] = set()

        for row in rows:
            if not isinstance(row, dict):
                continue
            entity = self._parse_project(row, last_seen)
            if entity is None or entity.project_id in seen_project_ids:
                continue
            projects.append(entity)
            seen_project_ids.add(entity.project_id)

        return projects

    def _extract_items(self, payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            for key in ("projects", "data", "items", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value

        return []

    def _parse_project(self, row: dict[str, Any], last_seen: str) -> ProjectEntity | None:
        name = self._as_text(row.get("name") or row.get("title"))
        symbol = self._as_text(row.get("symbol") or row.get("ticker"))
        url = self._as_text(row.get("url") or row.get("link"))
        status = self._as_text(row.get("status"), default="unknown")
        description = self._as_text(row.get("description") or row.get("summary"))
        creator = self._extract_creator(row.get("creator") or row.get("owner"))
        created_time = self._extract_time(row)

        project_id = self._resolve_project_id(row, url, name, symbol)
        if not project_id:
            return None

        return ProjectEntity(
            project_id=project_id,
            name=name or project_id,
            symbol=symbol,
            url=url,
            status=status,
            description=description,
            creator=creator,
            created_time=created_time,
            last_seen=last_seen,
            raw_hash=build_raw_hash(name or project_id, status, description),
        )

    def _resolve_project_id(
        self,
        row: dict[str, Any],
        url: str,
        name: str,
        symbol: str,
    ) -> str:
        candidate_keys = (
            "project_id",
            "projectId",
            "id",
            "slug",
            "contract_address",
            "contractAddress",
            "token_address",
            "tokenAddress",
        )

        for key in candidate_keys:
            value = self._as_text(row.get(key))
            if value:
                return value

        if url:
            return f"url_{build_url_hash(url)}"

        fallback = f"{name}|{symbol}".strip("|")
        if fallback:
            return f"generated_{build_url_hash(fallback)}"

        return ""

    def _extract_time(self, row: dict[str, Any]) -> str | None:
        for key in (
            "created_time",
            "createdAt",
            "created_at",
            "published_at",
            "launch_time",
            "timestamp",
        ):
            value = self._as_text(row.get(key))
            if value:
                return value
        return None

    def _extract_creator(self, value: Any) -> str:
        if isinstance(value, dict):
            for key in ("name", "handle", "username", "id"):
                text = self._as_text(value.get(key))
                if text:
                    return text
            return ""
        return self._as_text(value)

    def _as_text(self, value: Any, default: str = "") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text or default
