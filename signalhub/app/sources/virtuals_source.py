from __future__ import annotations

import json
from typing import Any

import httpx

from signalhub.app.config import Settings


class VirtualsSource:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch_projects(self) -> Any:
        if self.settings.sample_mode or not self.settings.virtuals_endpoint:
            return self._load_sample_data()

        async with httpx.AsyncClient(
            timeout=self.settings.request_timeout_seconds,
            headers=self.settings.virtuals_headers,
        ) as client:
            response = await client.get(self.settings.virtuals_endpoint)
            response.raise_for_status()
            return response.json()

    def _load_sample_data(self) -> Any:
        if not self.settings.sample_data_path.exists():
            return []
        return json.loads(self.settings.sample_data_path.read_text(encoding="utf-8"))
