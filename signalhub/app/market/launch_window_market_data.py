from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from signalhub.app.config import Settings
from signalhub.app.database.models import utc_now_iso


LAUNCH_WINDOW_MINUTES = 100
LAUNCH_WINDOW_DELTA = timedelta(minutes=LAUNCH_WINDOW_MINUTES)
GET_RESERVES_SELECTOR = "0x0902f1ac"
DEFINED_API_URL = "https://www.defined.fi/api"
DEFINED_NETWORK_ID_FALLBACK = 8453
DEFINED_TOKEN_BATCH_SIZE = 25
DEFINED_BARS_RESOLUTION = "1"
DEFINED_BARS_LOOKBACK = timedelta(hours=6)
MARKET_DATA_REQUEST_TIMEOUT_SECONDS = 3.0
MARKET_DATA_CONNECT_TIMEOUT_SECONDS = 1.5
HOST_MAX_CONCURRENCY = {
    "defined": 4,
    "virtuals": 6,
    "rpc": 8,
}
FX_CACHE_TTL_SECONDS = 30.0
TOKEN_CACHE_TTL_SECONDS = 600.0
SUPPLY_CACHE_TTL_SECONDS = 600.0
MARKET_CACHE_TTL_SECONDS = 12.0
BARS_CACHE_TTL_SECONDS = 15.0
RESERVE_CACHE_TTL_SECONDS = 15.0
ERROR_CACHE_TTL_SECONDS = 8.0
DEFAULT_RPC_ENDPOINTS = (
    "https://base.llamarpc.com",
    "https://mainnet.base.org",
)
BOOTSTRAP_VIRTUAL_RESERVES = Decimal("5700")
BOOTSTRAP_TOKEN_RESERVES = Decimal("1000000000")
RAW_ERC20_SCALE = Decimal("1000000000000000000")
MARKET_DATA_SOURCE_UNAVAILABLE = "unavailable"

DEFINED_GET_TOKENS_QUERY = """
query GetTokens($ids: [TokenInput!]!) {
  tokens(ids: $ids) {
    address
    symbol
    info {
      circulatingSupply
      totalSupply
    }
    explorerData {
      tokenPriceUSD
    }
  }
}
""".strip()

DEFINED_GET_BARS_QUERY = """
query GetBars(
  $symbol: String!,
  $countback: Int,
  $from: Int!,
  $to: Int!,
  $resolution: String!,
  $currencyCode: String,
  $quoteToken: QuoteToken,
  $statsType: TokenPairStatisticsType,
  $removeLeadingNullValues: Boolean,
  $removeEmptyBars: Boolean
) {
  getBars(
    symbol: $symbol
    countback: $countback
    from: $from
    to: $to
    resolution: $resolution
    currencyCode: $currencyCode
    quoteToken: $quoteToken
    statsType: $statsType
    removeLeadingNullValues: $removeLeadingNullValues
    removeEmptyBars: $removeEmptyBars
  ) {
    s
    c
    t
  }
}
""".strip()


@dataclass(frozen=True, slots=True)
class DefinedTokenSnapshot:
    token_price_usd: Decimal | None
    total_supply: int | None
    circulating_supply: int | None


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
        self._host_semaphores = {
            host: asyncio.Semaphore(limit)
            for host, limit in HOST_MAX_CONCURRENCY.items()
        }
        self._fx_cache: dict[str, tuple[float, Decimal]] = {}
        self._supply_cache: dict[str, tuple[float, int | None]] = {}
        self._market_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._defined_token_cache: dict[str, tuple[float, DefinedTokenSnapshot | None]] = {}
        self._defined_bar_cache: dict[str, tuple[float, Decimal | None]] = {}
        self._official_reserve_cache: dict[str, tuple[float, tuple[Decimal, Decimal] | None]] = {}
        self._rpc_reserve_cache: dict[str, tuple[float, tuple[Decimal, Decimal] | None]] = {}

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

        launch_window_items = [items[index] for index in launch_window_indexes]
        token_lookup = await self._load_defined_tokens_for_items(launch_window_items)

        tasks = [
            self._get_market_data(
                items[index],
                now=now,
                defined_token=token_lookup.get(self._normalize_address(self._token_address(items[index]))),
            )
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
        token_reserve: Decimal,
        virtual_reserve: Decimal,
        virtual_usd: Decimal,
        total_supply: int | None,
    ) -> tuple[float, float | None]:
        if token_reserve <= 0 or virtual_reserve <= 0 or virtual_usd <= 0:
            raise ValueError("Reserves and FX must be positive")
        price_virtual = virtual_reserve / token_reserve
        price_usd = price_virtual * virtual_usd
        fdv_usd = (
            price_usd * Decimal(total_supply)
            if total_supply and total_supply > 0
            else None
        )
        return float(price_usd), float(fdv_usd) if fdv_usd is not None else None

    async def _get_market_data(
        self,
        item: dict[str, Any],
        *,
        now: datetime,
        defined_token: DefinedTokenSnapshot | None = None,
    ) -> dict[str, Any]:
        cache_key = self._market_cache_key(item)
        found, cached = self._get_cached_entry(self._market_cache, cache_key)
        if found:
            return cached

        project_id = str(item.get("project_id") or "").strip()
        pool_address = self._pool_address(item)
        updated_at = utc_now_iso()
        encountered_error = False

        total_supply = defined_token.total_supply if defined_token else None
        if total_supply is None and project_id:
            try:
                total_supply = await self._get_total_supply(project_id)
            except Exception:
                encountered_error = True

        live_price = defined_token.token_price_usd if defined_token else None
        if live_price and live_price > 0:
            payload = self._build_price_payload(
                price_usd=live_price,
                total_supply=total_supply,
                source="defined_tokens",
                mode="live",
                updated_at=updated_at,
            )
            self._set_cached_entry(
                self._market_cache,
                cache_key,
                payload,
                ttl_seconds=MARKET_CACHE_TTL_SECONDS,
            )
            return payload

        if pool_address:
            try:
                bar_price = await self._get_defined_bar_close_usd(
                    pool_address,
                    launch_time=item.get("launch_time"),
                    now=now,
                )
            except Exception:
                encountered_error = True
                bar_price = None
            if bar_price and bar_price > 0:
                payload = self._build_price_payload(
                    price_usd=bar_price,
                    total_supply=total_supply,
                    source="defined_bars",
                    mode="live",
                    updated_at=updated_at,
                )
                self._set_cached_entry(
                    self._market_cache,
                    cache_key,
                    payload,
                    ttl_seconds=MARKET_CACHE_TTL_SECONDS,
                )
                return payload

        if pool_address:
            try:
                official_reserves = await self._get_official_pool_reserves(pool_address)
            except Exception:
                encountered_error = True
                official_reserves = None
            if official_reserves is not None:
                try:
                    virtual_usd = await self._get_virtual_usd()
                    price_usd, fdv_usd = self.calculate_price_and_fdv(
                        token_reserve=official_reserves[0],
                        virtual_reserve=official_reserves[1],
                        virtual_usd=virtual_usd,
                        total_supply=total_supply,
                    )
                    mode = self._reserve_mode(
                        token_reserve=official_reserves[0],
                        virtual_reserve=official_reserves[1],
                        total_supply=total_supply,
                    )
                    payload = self._build_market_payload(
                        status="ok",
                        price_usd=price_usd,
                        fdv_usd=fdv_usd,
                        source="virtuals_token_reserves+virtuals_fx",
                        mode=mode,
                        updated_at=updated_at,
                    )
                    self._set_cached_entry(
                        self._market_cache,
                        cache_key,
                        payload,
                        ttl_seconds=MARKET_CACHE_TTL_SECONDS,
                    )
                    return payload
                except Exception:
                    encountered_error = True

        if pool_address:
            try:
                rpc_reserves = await self._get_pool_reserves(pool_address)
            except Exception:
                encountered_error = True
                rpc_reserves = None
            if rpc_reserves is not None:
                try:
                    virtual_usd = await self._get_virtual_usd()
                    price_usd, fdv_usd = self.calculate_price_and_fdv(
                        token_reserve=rpc_reserves[0],
                        virtual_reserve=rpc_reserves[1],
                        virtual_usd=virtual_usd,
                        total_supply=total_supply,
                    )
                    mode = self._reserve_mode(
                        token_reserve=rpc_reserves[0],
                        virtual_reserve=rpc_reserves[1],
                        total_supply=total_supply,
                    )
                    payload = self._build_market_payload(
                        status="ok",
                        price_usd=price_usd,
                        fdv_usd=fdv_usd,
                        source="chain_reserves+virtuals_fx",
                        mode=mode,
                        updated_at=updated_at,
                    )
                    self._set_cached_entry(
                        self._market_cache,
                        cache_key,
                        payload,
                        ttl_seconds=MARKET_CACHE_TTL_SECONDS,
                    )
                    return payload
                except Exception:
                    encountered_error = True

        payload = self._build_market_payload(
            status="error" if encountered_error else "unavailable",
            source=MARKET_DATA_SOURCE_UNAVAILABLE,
            mode="unavailable",
            updated_at=updated_at if encountered_error else None,
        )
        self._set_cached_entry(
            self._market_cache,
            cache_key,
            payload,
            ttl_seconds=ERROR_CACHE_TTL_SECONDS if encountered_error else MARKET_CACHE_TTL_SECONDS,
        )
        return payload

    async def _load_defined_tokens_for_items(
        self,
        items: list[dict[str, Any]],
    ) -> dict[str, DefinedTokenSnapshot | None]:
        token_addresses = [
            self._normalize_address(self._token_address(item))
            for item in items
            if self._normalize_address(self._token_address(item))
        ]
        if not token_addresses:
            return {}
        return await self._get_defined_tokens(token_addresses)

    async def _get_defined_tokens(
        self,
        token_addresses: list[str],
    ) -> dict[str, DefinedTokenSnapshot | None]:
        ordered = []
        seen = set()
        for token_address in token_addresses:
            normalized = self._normalize_address(token_address)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)

        snapshots: dict[str, DefinedTokenSnapshot | None] = {}
        missing: list[str] = []
        for token_address in ordered:
            found, cached = self._get_cached_entry(self._defined_token_cache, token_address)
            if found:
                snapshots[token_address] = cached
            else:
                missing.append(token_address)

        for batch in self._chunked(missing, DEFINED_TOKEN_BATCH_SIZE):
            variables = {
                "ids": [
                    {
                        "address": token_address,
                        "networkId": self._defined_network_id(),
                    }
                    for token_address in batch
                ]
            }
            payload = {
                "operationName": "GetTokens",
                "variables": variables,
                "query": DEFINED_GET_TOKENS_QUERY,
            }
            try:
                response_payload = await self._post_defined_graphql(payload)
            except Exception:
                continue

            returned: dict[str, DefinedTokenSnapshot | None] = {}
            data = response_payload.get("data") if isinstance(response_payload, dict) else None
            raw_tokens = data.get("tokens") if isinstance(data, dict) else None
            if isinstance(raw_tokens, list):
                for raw_token in raw_tokens:
                    if not isinstance(raw_token, dict):
                        continue
                    address = self._normalize_address(raw_token.get("address"))
                    if not address:
                        continue
                    info = raw_token.get("info") if isinstance(raw_token.get("info"), dict) else {}
                    explorer_data = (
                        raw_token.get("explorerData")
                        if isinstance(raw_token.get("explorerData"), dict)
                        else {}
                    )
                    snapshot = DefinedTokenSnapshot(
                        token_price_usd=self._coerce_positive_decimal(
                            explorer_data.get("tokenPriceUSD")
                        ),
                        total_supply=self._coerce_positive_int(info.get("totalSupply")),
                        circulating_supply=self._coerce_positive_int(info.get("circulatingSupply")),
                    )
                    returned[address] = snapshot
                    snapshots[address] = snapshot
                    self._set_cached_entry(
                        self._defined_token_cache,
                        address,
                        snapshot,
                        ttl_seconds=TOKEN_CACHE_TTL_SECONDS,
                    )

            for token_address in batch:
                if token_address in returned:
                    continue
                snapshots[token_address] = None
                self._set_cached_entry(
                    self._defined_token_cache,
                    token_address,
                    None,
                    ttl_seconds=TOKEN_CACHE_TTL_SECONDS,
                )

        return snapshots

    async def _get_defined_bar_close_usd(
        self,
        pool_address: str,
        *,
        launch_time: str | None,
        now: datetime,
    ) -> Decimal | None:
        pair_address = self._normalize_address(pool_address)
        if not pair_address:
            return None
        found, cached = self._get_cached_entry(self._defined_bar_cache, pair_address)
        if found:
            return cached

        launch_dt = self._parse_datetime(launch_time)
        now_ts = int(now.timestamp())
        lookback_start = now - DEFINED_BARS_LOOKBACK
        from_dt = max(lookback_start, launch_dt - timedelta(minutes=15)) if launch_dt else lookback_start
        from_ts = int(from_dt.timestamp())
        countback = max(min(int((now_ts - from_ts) / 60) + 5, 360), 30)
        payload = {
            "operationName": "GetBars",
            "variables": {
                "symbol": f"{pair_address}:{self._defined_network_id()}",
                "resolution": DEFINED_BARS_RESOLUTION,
                "from": from_ts,
                "to": now_ts,
                "countback": countback,
                "currencyCode": "USD",
                "statsType": "FILTERED",
                "quoteToken": "token0",
                "removeLeadingNullValues": True,
                "removeEmptyBars": True,
            },
            "query": DEFINED_GET_BARS_QUERY,
        }
        response_payload = await self._post_defined_graphql(payload)
        data = response_payload.get("data") if isinstance(response_payload, dict) else None
        bars = data.get("getBars") if isinstance(data, dict) else None
        if not isinstance(bars, dict) or str(bars.get("s") or "").lower() == "no_data":
            self._set_cached_entry(
                self._defined_bar_cache,
                pair_address,
                None,
                ttl_seconds=BARS_CACHE_TTL_SECONDS,
            )
            return None

        closes = bars.get("c")
        close_value: Decimal | None = None
        if isinstance(closes, list):
            for raw in reversed(closes):
                close_value = self._coerce_positive_decimal(raw)
                if close_value is not None:
                    break

        self._set_cached_entry(
            self._defined_bar_cache,
            pair_address,
            close_value,
            ttl_seconds=BARS_CACHE_TTL_SECONDS,
        )
        return close_value

    async def _post_defined_graphql(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        async with self._host_semaphores["defined"]:
            response = await self._client.post(
                DEFINED_API_URL,
                json=payload,
                headers=self._defined_headers(),
            )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError("Defined payload is invalid")
        if body.get("errors"):
            raise RuntimeError("Defined response contains errors")
        return body

    async def _get_virtual_usd(self) -> Decimal:
        found, cached = self._get_cached_entry(self._fx_cache, "virtual_usd")
        if found:
            return cached

        async with self._host_semaphores["virtuals"]:
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
        self._set_cached_entry(
            self._fx_cache,
            "virtual_usd",
            virtual_usd,
            ttl_seconds=FX_CACHE_TTL_SECONDS,
        )
        return virtual_usd

    async def _get_total_supply(self, project_id: str) -> int | None:
        found, cached = self._get_cached_entry(self._supply_cache, project_id)
        if found:
            return cached

        async with self._host_semaphores["virtuals"]:
            response = await self._client.get(
                f"{self.settings.virtuals_endpoint.rstrip('/')}/{project_id}",
                headers=self._virtuals_headers(),
            )
        response.raise_for_status()
        payload = response.json()
        detail = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
        total_supply = self._coerce_positive_int(
            detail.get("totalSupply") if isinstance(detail, dict) else None
        )
        self._set_cached_entry(
            self._supply_cache,
            project_id,
            total_supply,
            ttl_seconds=SUPPLY_CACHE_TTL_SECONDS,
        )
        return total_supply

    async def _get_official_pool_reserves(
        self,
        pool_address: str,
    ) -> tuple[Decimal, Decimal] | None:
        normalized = self._normalize_address(pool_address)
        if not normalized:
            return None

        found, cached = self._get_cached_entry(self._official_reserve_cache, normalized)
        if found:
            return cached

        async with self._host_semaphores["virtuals"]:
            response = await self._client.get(
                f"{self._virtuals_api_base()}/api/dex/token-reserves/{normalized}?chain=BASE",
                headers=self._virtuals_headers(),
            )
        if response.status_code in {204, 404}:
            self._set_cached_entry(
                self._official_reserve_cache,
                normalized,
                None,
                ttl_seconds=RESERVE_CACHE_TTL_SECONDS,
            )
            return None
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
        token_reserve = self._coerce_positive_decimal(
            data.get("tokenReserves") if isinstance(data, dict) else None
        )
        virtual_reserve = self._coerce_positive_decimal(
            data.get("virtualReserves") if isinstance(data, dict) else None
        )
        reserves = (
            (token_reserve, virtual_reserve)
            if token_reserve is not None and virtual_reserve is not None
            else None
        )
        self._set_cached_entry(
            self._official_reserve_cache,
            normalized,
            reserves,
            ttl_seconds=RESERVE_CACHE_TTL_SECONDS,
        )
        return reserves

    async def _get_pool_reserves(
        self,
        pool_address: str,
    ) -> tuple[Decimal, Decimal] | None:
        normalized = self._normalize_address(pool_address)
        if not normalized:
            return None

        found, cached = self._get_cached_entry(self._rpc_reserve_cache, normalized)
        if found:
            return cached

        request_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": normalized,
                    "data": GET_RESERVES_SELECTOR,
                },
                "latest",
            ],
        }
        last_error: Exception | None = None
        for rpc_url in self._rpc_urls():
            try:
                async with self._host_semaphores["rpc"]:
                    response = await self._client.post(rpc_url, json=request_payload)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("RPC payload is invalid")
                if payload.get("error"):
                    message = payload["error"].get("message") or "unknown rpc error"
                    raise RuntimeError(str(message))
                token_reserve, virtual_reserve = self.decode_reserves_result(
                    str(payload.get("result") or "")
                )
                reserves = (Decimal(token_reserve), Decimal(virtual_reserve))
                self._set_cached_entry(
                    self._rpc_reserve_cache,
                    normalized,
                    reserves,
                    ttl_seconds=RESERVE_CACHE_TTL_SECONDS,
                )
                return reserves
            except Exception as exc:
                last_error = exc

        raise RuntimeError("All Base RPC endpoints failed") from last_error

    def _defined_headers(self) -> dict[str, str]:
        return {
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/133.0.0.0 Safari/537.36"
            ),
            "origin": "https://www.defined.fi",
            "referer": "https://www.defined.fi/",
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
        }

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
                self._normalize_address(self._token_address(item)),
                self._normalize_address(self._pool_address(item)),
                str(item.get("launch_time") or "").strip(),
            ]
        )

    def _build_price_payload(
        self,
        *,
        price_usd: Decimal,
        total_supply: int | None,
        source: str,
        mode: str,
        updated_at: str,
    ) -> dict[str, Any]:
        fdv_usd = (
            float(price_usd * Decimal(total_supply))
            if total_supply and total_supply > 0
            else None
        )
        return self._build_market_payload(
            status="ok",
            price_usd=float(price_usd),
            fdv_usd=fdv_usd,
            source=source,
            mode=mode,
            updated_at=updated_at,
        )

    def _build_market_payload(
        self,
        *,
        status: str,
        price_usd: float | None = None,
        fdv_usd: float | None = None,
        source: str = MARKET_DATA_SOURCE_UNAVAILABLE,
        mode: str = "unavailable",
        updated_at: str | None = None,
    ) -> dict[str, Any]:
        return {
            "price_usd": price_usd,
            "fdv_usd": fdv_usd,
            "market_data_status": status,
            "market_data_source": source,
            "market_data_mode": mode,
            "market_data_updated_at": updated_at,
        }

    def _reserve_mode(
        self,
        *,
        token_reserve: Decimal,
        virtual_reserve: Decimal,
        total_supply: int | None,
    ) -> str:
        if self._is_bootstrap_reserve(
            token_reserve=token_reserve,
            virtual_reserve=virtual_reserve,
            total_supply=total_supply,
        ):
            return "bootstrap"
        return "derived"

    def _is_bootstrap_reserve(
        self,
        *,
        token_reserve: Decimal,
        virtual_reserve: Decimal,
        total_supply: int | None,
    ) -> bool:
        if (
            token_reserve == BOOTSTRAP_TOKEN_RESERVES
            and virtual_reserve == BOOTSTRAP_VIRTUAL_RESERVES
        ):
            return True
        if total_supply and total_supply > 0:
            total_supply_decimal = Decimal(total_supply)
            if (
                token_reserve == total_supply_decimal
                and virtual_reserve == BOOTSTRAP_VIRTUAL_RESERVES
            ):
                return True
            if (
                token_reserve == total_supply_decimal * RAW_ERC20_SCALE
                and virtual_reserve == BOOTSTRAP_VIRTUAL_RESERVES * RAW_ERC20_SCALE
            ):
                return True
        return False

    def _get_cached_entry(
        self,
        cache: dict[str, tuple[float, Any]],
        key: str,
    ) -> tuple[bool, Any]:
        cached = cache.get(key)
        if cached is None:
            return False, None
        expires_at, value = cached
        if expires_at <= time.monotonic():
            cache.pop(key, None)
            return False, None
        return True, value

    def _set_cached_entry(
        self,
        cache: dict[str, tuple[float, Any]],
        key: str,
        value: Any,
        *,
        ttl_seconds: float,
    ) -> None:
        cache[key] = (time.monotonic() + ttl_seconds, value)

    def _defined_network_id(self) -> int:
        return self._coerce_positive_int(self.settings.base_chain_id) or DEFINED_NETWORK_ID_FALLBACK

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
    def _coerce_positive_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        try:
            parsed = Decimal(str(value).strip())
        except (InvalidOperation, TypeError, ValueError):
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

    @staticmethod
    def _normalize_address(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return text.lower()

    @staticmethod
    def _chunked(values: list[str], size: int) -> list[list[str]]:
        if size <= 0:
            return [values]
        return [values[index:index + size] for index in range(0, len(values), size)]

    @staticmethod
    def _pool_address(item: dict[str, Any]) -> str:
        return str(
            item.get("pool_address")
            or item.get("internal_market_address")
            or ""
        ).strip()

    @staticmethod
    def _token_address(item: dict[str, Any]) -> str:
        return str(
            item.get("token_address")
            or item.get("contract_address")
            or ""
        ).strip()
