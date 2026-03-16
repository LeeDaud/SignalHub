from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from signalhub.app.config import Settings
from signalhub.app.database.models import utc_now_iso


LAUNCH_WINDOW_MINUTES = 100
LAUNCH_WINDOW_DELTA = timedelta(minutes=LAUNCH_WINDOW_MINUTES)
GET_RESERVES_SELECTOR = "0x0902f1ac"
MARKET_DATA_SOURCE = "chain_reserves+virtuals_fx"
MARKET_DATA_REQUEST_TIMEOUT_SECONDS = 3.0
MARKET_DATA_CONNECT_TIMEOUT_SECONDS = 1.5
MARKET_DATA_MAX_CONCURRENCY = 8
DEFAULT_RPC_ENDPOINTS = (
    "https://base.llamarpc.com",
    "https://mainnet.base.org",
)


class LaunchWindowMarketDataService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        timeout_seconds = min(
            float(self.settings.request_timeout_seconds),
            MARKET_DATA_REQUEST_TIMEOUT_SECONDS,
        )
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                timeout_seconds,
                connect=min(timeout_seconds, MARKET_DATA_CONNECT_TIMEOUT_SECONDS),
            ),
            follow_redirects=True,
        )
        self._request_semaphore = asyncio.Semaphore(MARKET_DATA_MAX_CONCURRENCY)
        self._fx_cache: tuple[float, Decimal] | None = None
        self._supply_cache: dict[str, tuple[float, int | None]] = {}
        self._market_cache: dict[str, tuple[float, dict[str, Any]]] = {}

    async def close(self) -> None:
        await self._client.aclose()

    async def enrich_internal_market_items(
        self,
        items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        enriched = [
            {
                **item,
                **self._build_market_payload(status="unavailable"),
            }
            for item in items
        ]
        launch_window_indexes = [
            index
            for index, item in enumerate(items)
            if self.is_launch_window(item.get("launch_time"), now=now)
        ]
        if not launch_window_indexes:
            return enriched

        try:
            virtual_usd = await self._get_virtual_usd()
        except Exception:
            updated_at = utc_now_iso()
            for index in launch_window_indexes:
                enriched[index].update(
                    self._build_market_payload(
                        status="error",
                        updated_at=updated_at,
                    )
                )
            return enriched

        tasks = [
            self._get_market_data(items[index], virtual_usd=virtual_usd, now=now)
            for index in launch_window_indexes
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for index, result in zip(launch_window_indexes, results):
            if isinstance(result, Exception):
                enriched[index].update(
                    self._build_market_payload(
                        status="error",
                        updated_at=utc_now_iso(),
                    )
                )
                continue
            enriched[index].update(result)
        return enriched

    @staticmethod
    def is_launch_window(
        launch_time: str | None,
        *,
        now: datetime | None = None,
    ) -> bool:
        launch_dt = LaunchWindowMarketDataService._parse_datetime(launch_time)
        if launch_dt is None:
            return False
        current = now or datetime.now(timezone.utc)
        return launch_dt <= current < launch_dt + LAUNCH_WINDOW_DELTA

    @staticmethod
    def decode_reserves_result(result: str) -> tuple[int, int]:
        raw = str(result or "").strip()
        if not raw.startswith("0x"):
            raise ValueError("Reserve payload must be hex-prefixed")
        encoded = raw[2:]
        if len(encoded) < 128 or len(encoded) % 64 != 0:
            raise ValueError("Reserve payload length is invalid")
        chunks = [encoded[index:index + 64] for index in range(0, len(encoded), 64)]
        if len(chunks) < 2:
            raise ValueError("Reserve payload is incomplete")
        return int(chunks[0], 16), int(chunks[1], 16)

    @staticmethod
    def calculate_price_and_fdv(
        *,
        token_reserve: int,
        virtual_reserve: int,
        virtual_usd: Decimal,
        total_supply: int,
    ) -> tuple[float, float]:
        if token_reserve <= 0 or virtual_reserve <= 0 or total_supply <= 0:
            raise ValueError("Reserves and total supply must be positive")
        price_virtual = Decimal(virtual_reserve) / Decimal(token_reserve)
        price_usd = price_virtual * virtual_usd
        fdv_usd = price_usd * Decimal(total_supply)
        return float(price_usd), float(fdv_usd)

    async def _get_market_data(
        self,
        item: dict[str, Any],
        *,
        virtual_usd: Decimal,
        now: datetime,
    ) -> dict[str, Any]:
        cache_key = self._market_cache_key(item)
        cached = self._get_cached_value(self._market_cache, cache_key)
        if cached is not None:
            return cached

        project_id = str(item.get("project_id") or "").strip()
        pool_address = str(
            item.get("pool_address")
            or item.get("internal_market_address")
            or ""
        ).strip()
        if not project_id or not pool_address:
            payload = self._build_market_payload(status="unavailable")
            self._set_cached_value(self._market_cache, cache_key, payload, ttl_seconds=15.0)
            return payload

        try:
            total_supply, reserves = await asyncio.gather(
                self._get_total_supply(project_id),
                self._get_pool_reserves(pool_address),
            )
        except Exception:
            payload = self._build_market_payload(
                status="error",
                updated_at=utc_now_iso(),
            )
            self._set_cached_value(self._market_cache, cache_key, payload, ttl_seconds=10.0)
            return payload

        if total_supply is None:
            payload = self._build_market_payload(
                status="unavailable",
                updated_at=utc_now_iso(),
            )
            self._set_cached_value(self._market_cache, cache_key, payload, ttl_seconds=15.0)
            return payload

        try:
            token_reserve, virtual_reserve = reserves
            price_usd, fdv_usd = self.calculate_price_and_fdv(
                token_reserve=token_reserve,
                virtual_reserve=virtual_reserve,
                virtual_usd=virtual_usd,
                total_supply=total_supply,
            )
        except Exception:
            payload = self._build_market_payload(
                status="error",
                updated_at=utc_now_iso(),
            )
            self._set_cached_value(self._market_cache, cache_key, payload, ttl_seconds=10.0)
            return payload

        payload = self._build_market_payload(
            status="ok",
            price_usd=price_usd,
            fdv_usd=fdv_usd,
            updated_at=utc_now_iso(),
        )
        self._set_cached_value(self._market_cache, cache_key, payload, ttl_seconds=15.0)
        return payload

    async def _get_virtual_usd(self) -> Decimal:
        now = time.monotonic()
        if self._fx_cache and self._fx_cache[0] > now:
            return self._fx_cache[1]

        async with self._request_semaphore:
            response = await self._client.get(
                f"{self._virtuals_api_base()}/api/dex/prices",
                headers=self._virtuals_headers(),
            )
        response.raise_for_status()
        payload = response.json()
        value = (
            payload.get("data", {})
            .get("BASE", {})
            .get("virtual")
        )
        try:
            virtual_usd = Decimal(str(value))
        except (InvalidOperation, TypeError) as exc:
            raise RuntimeError("Virtuals FX payload is invalid") from exc
        if virtual_usd <= 0:
            raise RuntimeError("Virtuals FX payload is missing BASE.virtual")
        self._fx_cache = (now + 30.0, virtual_usd)
        return virtual_usd

    async def _get_total_supply(self, project_id: str) -> int | None:
        cached = self._get_cached_value(self._supply_cache, project_id)
        if project_id in self._supply_cache and cached is None:
            return None
        if cached is not None:
            return cached

        async with self._request_semaphore:
            response = await self._client.get(
                f"{self.settings.virtuals_endpoint.rstrip('/')}/{project_id}",
                headers=self._virtuals_headers(),
            )
        response.raise_for_status()
        payload = response.json()
        detail = payload.get("data") if isinstance(payload, dict) else None
        total_supply = self._coerce_positive_int(
            detail.get("totalSupply") if isinstance(detail, dict) else None
        )
        self._set_cached_value(self._supply_cache, project_id, total_supply, ttl_seconds=600.0)
        return total_supply

    async def _get_pool_reserves(self, pool_address: str) -> tuple[int, int]:
        request_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": pool_address,
                    "data": GET_RESERVES_SELECTOR,
                },
                "latest",
            ],
        }
        last_error: Exception | None = None
        for rpc_url in self._rpc_urls():
            try:
                async with self._request_semaphore:
                    response = await self._client.post(rpc_url, json=request_payload)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("RPC payload is invalid")
                if payload.get("error"):
                    message = payload["error"].get("message") or "unknown rpc error"
                    raise RuntimeError(str(message))
                return self.decode_reserves_result(str(payload.get("result") or ""))
            except Exception as exc:
                last_error = exc
        raise RuntimeError("All Base RPC endpoints failed") from last_error

    def _virtuals_headers(self) -> dict[str, str]:
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

    def _virtuals_api_base(self) -> str:
        endpoint = str(self.settings.virtuals_endpoint or "").strip().rstrip("/")
        marker = "/api/virtuals"
        if marker in endpoint:
            return endpoint.split(marker, 1)[0]
        return endpoint

    def _rpc_urls(self) -> tuple[str, ...]:
        candidates = [*DEFAULT_RPC_ENDPOINTS, self.settings.chainstack_base_https_url]
        unique: list[str] = []
        for url in candidates:
            normalized = str(url or "").strip()
            if normalized and normalized not in unique:
                unique.append(normalized)
        return tuple(unique)

    def _market_cache_key(self, item: dict[str, Any]) -> str:
        return "|".join(
            [
                str(item.get("project_id") or "").strip(),
                str(item.get("pool_address") or item.get("internal_market_address") or "").strip(),
                str(item.get("launch_time") or "").strip(),
            ]
        )

    def _build_market_payload(
        self,
        *,
        status: str,
        price_usd: float | None = None,
        fdv_usd: float | None = None,
        updated_at: str | None = None,
    ) -> dict[str, Any]:
        return {
            "price_usd": price_usd,
            "fdv_usd": fdv_usd,
            "market_data_status": status,
            "market_data_source": MARKET_DATA_SOURCE,
            "market_data_updated_at": updated_at,
        }

    def _get_cached_value(
        self,
        cache: dict[str, tuple[float, Any]],
        key: str,
    ) -> Any | None:
        cached = cache.get(key)
        if cached is None:
            return None
        expires_at, value = cached
        if expires_at <= time.monotonic():
            cache.pop(key, None)
            return None
        return value

    def _set_cached_value(
        self,
        cache: dict[str, tuple[float, Any]],
        key: str,
        value: Any,
        *,
        ttl_seconds: float,
    ) -> None:
        cache[key] = (time.monotonic() + ttl_seconds, value)

    @staticmethod
    def _coerce_positive_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
