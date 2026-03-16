from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from signalhub.app.market import LaunchWindowMarketDataService


def make_settings() -> SimpleNamespace:
    return SimpleNamespace(
        request_timeout_seconds=5,
        virtuals_endpoint="https://api2.virtuals.io/api/virtuals",
        virtuals_app_base_url="https://app.virtuals.io",
        chainstack_base_https_url=None,
        virtuals_headers={},
    )


class StubLaunchWindowMarketDataService(LaunchWindowMarketDataService):
    async def _get_virtual_usd(self) -> Decimal:
        return Decimal("0.7628")

    async def _get_total_supply(self, project_id: str) -> int | None:
        return 1_000_000_000 if project_id else None

    async def _get_pool_reserves(self, pool_address: str) -> tuple[int, int]:
        return 10**27, 5700 * 10**18


class ErrorFxLaunchWindowMarketDataService(StubLaunchWindowMarketDataService):
    async def _get_virtual_usd(self) -> Decimal:
        raise RuntimeError("fx unavailable")


class LaunchWindowMarketDataServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.service = StubLaunchWindowMarketDataService(make_settings())

    async def asyncTearDown(self) -> None:
        await self.service.close()

    def test_decode_reserves_result_reads_first_two_words(self) -> None:
        token_reserve, virtual_reserve = LaunchWindowMarketDataService.decode_reserves_result(
            "0x"
            "0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000"
            "000000000000000000000000000000000000000000000134ff63f81b0e900000"
        )
        self.assertEqual(token_reserve, 10**27)
        self.assertEqual(virtual_reserve, 5700 * 10**18)

    def test_calculate_price_and_fdv_uses_virtual_quote(self) -> None:
        price_usd, fdv_usd = LaunchWindowMarketDataService.calculate_price_and_fdv(
            token_reserve=10**27,
            virtual_reserve=5700 * 10**18,
            virtual_usd=Decimal("0.7628"),
            total_supply=1_000_000_000,
        )
        self.assertAlmostEqual(price_usd, 0.00000434796, places=12)
        self.assertAlmostEqual(fdv_usd, 4347.96, places=6)

    async def test_enrich_internal_market_items_only_prices_launch_window(self) -> None:
        now = datetime.now(timezone.utc)
        items = [
            {
                "project_id": "launch-window",
                "launch_time": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                "pool_address": "0xpool",
                "internal_market_address": "0xpool",
            },
            {
                "project_id": "future",
                "launch_time": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                "pool_address": "0xfuture",
                "internal_market_address": "0xfuture",
            },
        ]

        enriched = await self.service.enrich_internal_market_items(items)

        self.assertEqual(enriched[0]["market_data_status"], "ok")
        self.assertGreater(enriched[0]["price_usd"], 0)
        self.assertGreater(enriched[0]["fdv_usd"], 0)
        self.assertEqual(enriched[1]["market_data_status"], "unavailable")
        self.assertIsNone(enriched[1]["price_usd"])
        self.assertIsNone(enriched[1]["fdv_usd"])

    async def test_enrich_internal_market_items_marks_launch_window_error_on_fx_failure(self) -> None:
        service = ErrorFxLaunchWindowMarketDataService(make_settings())
        try:
            now = datetime.now(timezone.utc)
            items = [
                {
                    "project_id": "launch-window",
                    "launch_time": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                    "pool_address": "0xpool",
                    "internal_market_address": "0xpool",
                }
            ]
            enriched = await service.enrich_internal_market_items(items)
            self.assertEqual(enriched[0]["market_data_status"], "error")
            self.assertIsNone(enriched[0]["price_usd"])
            self.assertIsNone(enriched[0]["fdv_usd"])
        finally:
            await service.close()
