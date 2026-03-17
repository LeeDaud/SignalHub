from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from signalhub.app.config import load_settings
from signalhub.app.market.launch_window_market_data import (
    LaunchWindowMarketDataService,
    DefinedTokenSnapshot,
)


def make_item(
    *,
    project_id: str = "51777",
    launch_offset_minutes: int = -10,
) -> dict[str, str]:
    launch_time = (
        datetime.now(timezone.utc) + timedelta(minutes=launch_offset_minutes)
    ).isoformat().replace("+00:00", "Z")
    return {
        "project_id": project_id,
        "name": "Intel",
        "symbol": "INTEL",
        "launch_time": launch_time,
        "token_address": "0xAC72A5D184a4Bbc78a23e784d10A8d66105244d2",
        "contract_address": "0xAC72A5D184a4Bbc78a23e784d10A8d66105244d2",
        "pool_address": "0x231f996a8da2c855b6e4cfebad27b1b2465029d1",
        "internal_market_address": "0x231f996a8da2c855b6e4cfebad27b1b2465029d1",
    }


class LaunchWindowMarketDataServiceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.service = LaunchWindowMarketDataService(load_settings())

    async def asyncTearDown(self) -> None:
        await self.service.close()

    async def test_decode_reserves_result(self) -> None:
        payload = "0x" + f"{10:064x}" + f"{20:064x}" + f"{30:064x}"
        token_reserve, virtual_reserve = self.service.decode_reserves_result(payload)
        self.assertEqual(token_reserve, 10)
        self.assertEqual(virtual_reserve, 20)

    async def test_calculate_price_and_fdv(self) -> None:
        price_usd, fdv_usd = self.service.calculate_price_and_fdv(
            token_reserve=Decimal("1000000000"),
            virtual_reserve=Decimal("5700"),
            virtual_usd=Decimal("0.79"),
            total_supply=1_000_000_000,
        )
        self.assertAlmostEqual(price_usd, 0.000004503, places=12)
        self.assertAlmostEqual(fdv_usd or 0.0, 4503.0, places=6)

    async def test_enrich_internal_market_items_skips_non_launch_window(self) -> None:
        item = make_item(launch_offset_minutes=180)
        loader = AsyncMock(return_value={})
        self.service._load_defined_tokens_for_items = loader

        enriched = await self.service.enrich_internal_market_items([item])

        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched[0]["market_data_status"], "unavailable")
        self.assertEqual(enriched[0]["market_data_mode"], "unavailable")
        loader.assert_not_awaited()

    async def test_market_data_prefers_defined_token_price(self) -> None:
        item = make_item()
        defined_token = DefinedTokenSnapshot(
            token_price_usd=Decimal("0.00000321"),
            total_supply=1_000_000_000,
            circulating_supply=1_000_000_000,
        )
        self.service._get_defined_bar_close_usd = AsyncMock(return_value=Decimal("0.00000210"))
        self.service._get_official_pool_reserves = AsyncMock(return_value=(Decimal("900"), Decimal("5800")))
        self.service._get_pool_reserves = AsyncMock(return_value=(Decimal("900"), Decimal("5800")))

        payload = await self.service._get_market_data(
            item,
            now=datetime.now(timezone.utc),
            defined_token=defined_token,
        )

        self.assertEqual(payload["market_data_status"], "ok")
        self.assertEqual(payload["market_data_mode"], "live")
        self.assertEqual(payload["market_data_source"], "defined_tokens")
        self.assertAlmostEqual(payload["price_usd"] or 0.0, 0.00000321, places=12)
        self.assertAlmostEqual(payload["fdv_usd"] or 0.0, 3210.0, places=6)
        self.service._get_defined_bar_close_usd.assert_not_awaited()
        self.service._get_official_pool_reserves.assert_not_awaited()
        self.service._get_pool_reserves.assert_not_awaited()

    async def test_market_data_uses_defined_bars_when_direct_price_missing(self) -> None:
        item = make_item()
        defined_token = DefinedTokenSnapshot(
            token_price_usd=None,
            total_supply=1_000_000_000,
            circulating_supply=1_000_000_000,
        )
        self.service._get_defined_bar_close_usd = AsyncMock(return_value=Decimal("0.00000333"))
        self.service._get_official_pool_reserves = AsyncMock(return_value=(Decimal("900"), Decimal("5800")))
        self.service._get_pool_reserves = AsyncMock(return_value=(Decimal("900"), Decimal("5800")))

        payload = await self.service._get_market_data(
            item,
            now=datetime.now(timezone.utc),
            defined_token=defined_token,
        )

        self.assertEqual(payload["market_data_status"], "ok")
        self.assertEqual(payload["market_data_mode"], "live")
        self.assertEqual(payload["market_data_source"], "defined_bars")
        self.assertAlmostEqual(payload["price_usd"] or 0.0, 0.00000333, places=12)
        self.assertAlmostEqual(payload["fdv_usd"] or 0.0, 3330.0, places=6)
        self.service._get_official_pool_reserves.assert_not_awaited()
        self.service._get_pool_reserves.assert_not_awaited()

    async def test_market_data_marks_bootstrap_for_default_reserves(self) -> None:
        item = make_item()
        self.service._get_total_supply = AsyncMock(return_value=1_000_000_000)
        self.service._get_defined_bar_close_usd = AsyncMock(return_value=None)
        self.service._get_official_pool_reserves = AsyncMock(
            return_value=(Decimal("1000000000"), Decimal("5700"))
        )
        self.service._get_virtual_usd = AsyncMock(return_value=Decimal("0.79"))
        self.service._get_pool_reserves = AsyncMock(return_value=(Decimal("900"), Decimal("5800")))

        payload = await self.service._get_market_data(
            item,
            now=datetime.now(timezone.utc),
            defined_token=None,
        )

        self.assertEqual(payload["market_data_status"], "ok")
        self.assertEqual(payload["market_data_mode"], "bootstrap")
        self.assertEqual(payload["market_data_source"], "virtuals_token_reserves+virtuals_fx")
        self.assertAlmostEqual(payload["price_usd"] or 0.0, 0.000004503, places=12)
        self.assertAlmostEqual(payload["fdv_usd"] or 0.0, 4503.0, places=6)
        self.service._get_pool_reserves.assert_not_awaited()

    async def test_market_data_marks_error_when_all_sources_fail(self) -> None:
        item = make_item()
        self.service._get_total_supply = AsyncMock(side_effect=RuntimeError("supply"))
        self.service._get_defined_bar_close_usd = AsyncMock(side_effect=RuntimeError("bars"))
        self.service._get_official_pool_reserves = AsyncMock(side_effect=RuntimeError("official"))
        self.service._get_pool_reserves = AsyncMock(side_effect=RuntimeError("rpc"))

        payload = await self.service._get_market_data(
            item,
            now=datetime.now(timezone.utc),
            defined_token=None,
        )

        self.assertEqual(payload["market_data_status"], "error")
        self.assertEqual(payload["market_data_mode"], "unavailable")
        self.assertIsNone(payload["price_usd"])
        self.assertIsNone(payload["fdv_usd"])
