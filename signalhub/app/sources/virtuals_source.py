from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Sequence

import httpx

from signalhub.app.config import Settings


class VirtualsSource:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch_projects(self) -> Any:
        if self.settings.sample_mode or not self.settings.virtuals_endpoint:
            return self._load_sample_data()

        return await self._fetch_virtuals_api()

    async def fetch_project_details(self, project_ids: Sequence[str]) -> list[dict[str, Any]]:
        if self.settings.sample_mode or not self.settings.virtuals_endpoint:
            return []

        unique_ids = [project_id for project_id in dict.fromkeys(project_ids) if str(project_id).strip()]
        if not unique_ids:
            return []

        headers = self._build_headers()
        async with httpx.AsyncClient(
            timeout=self.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
        ) as client:
            tasks = [self._fetch_project_detail(client, project_id) for project_id in unique_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        details: list[dict[str, Any]] = []
        for result in results:
            if isinstance(result, dict):
                details.append(result)
        return details

    async def _fetch_virtuals_api(self) -> list[dict[str, Any]]:
        headers = self._build_headers()
        items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(
            timeout=self.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
        ) as client:
            for page in range(1, self.settings.virtuals_pages + 1):
                response = await client.get(
                    self.settings.virtuals_endpoint,
                    params=self._build_list_params(page),
                )
                response.raise_for_status()
                payload = response.json()

                for item in self._extract_items(payload):
                    item_id = str(item.get("id") or item.get("uid") or "")
                    if item_id and item_id in seen_ids:
                        continue
                    if item_id:
                        seen_ids.add(item_id)
                    items.append(item)

                page_count = (
                    payload.get("meta", {})
                    .get("pagination", {})
                    .get("pageCount")
                )
                if isinstance(page_count, int) and page >= page_count:
                    break

        return items

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/133.0.0.0 Safari/537.36"
            ),
            "referer": f"{self.settings.virtuals_app_base_url}/",
            "origin": self.settings.virtuals_app_base_url,
            "accept": "application/json, text/plain, */*",
        }
        headers.update(self.settings.virtuals_headers)
        return headers

    def _build_list_params(self, page: int) -> dict[str, str]:
        params: dict[str, str] = {
            "pagination[page]": str(page),
            "pagination[pageSize]": str(self.settings.virtuals_page_size),
            "sort": self.settings.virtuals_sort,
        }

        if self.settings.virtuals_populate:
            params["populate"] = ",".join(self.settings.virtuals_populate)

        if self.settings.virtuals_mode == "upcoming_launches":
            params["filters[launchedAt][$gt]"] = self._utc_now_iso()
            params["filters[tokenAddress][$null]"] = "true"
            if not self.settings.virtuals_statuses:
                params["filters[status][$in][0]"] = "UNDERGRAD"
                params["filters[status][$in][1]"] = "INITIALIZED"

        if self.settings.virtuals_chain:
            params["filters[chain][$eq]"] = self.settings.virtuals_chain.upper()

        for index, status in enumerate(self.settings.virtuals_statuses):
            params[f"filters[status][$in][{index}]"] = status

        for index, category in enumerate(self.settings.virtuals_categories):
            params[f"filters[category][$in][{index}]"] = category

        return params

    def _extract_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return [item for item in payload["data"] if isinstance(item, dict)]
        return []

    def merge_project_records(
        self,
        base_records: Sequence[dict[str, Any]],
        detail_records: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}

        for record in base_records:
            key = self._record_key(record)
            if not key:
                continue
            merged[key] = dict(record)

        for record in detail_records:
            key = self._record_key(record)
            if not key:
                continue
            merged[key] = {**merged.get(key, {}), **record}

        return list(merged.values())

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    async def _fetch_project_detail(
        self,
        client: httpx.AsyncClient,
        project_id: str,
    ) -> dict[str, Any] | None:
        try:
            response = await client.get(
                f"{self.settings.virtuals_endpoint.rstrip('/')}/{project_id}",
                params=self._build_detail_params(),
            )
            response.raise_for_status()
        except httpx.HTTPError:
            return None

        payload = response.json()
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload["data"]
        return None

    def _build_detail_params(self) -> dict[str, str]:
        if not self.settings.virtuals_populate:
            return {}
        return {"populate": ",".join(self.settings.virtuals_populate)}

    def _record_key(self, record: dict[str, Any]) -> str:
        for key in (
            "project_id",
            "projectId",
            "id",
            "uid",
            "slug",
            "tokenAddress",
            "preToken",
            "contract_address",
            "contractAddress",
        ):
            value = str(record.get(key) or "").strip()
            if value:
                return value
        return ""

    def _load_sample_data(self) -> Any:
        if not self.settings.sample_data_path.exists():
            return []
        return json.loads(self.settings.sample_data_path.read_text(encoding="utf-8"))
