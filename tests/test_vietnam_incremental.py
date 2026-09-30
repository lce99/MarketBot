import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.collectors.vietnam as vietnam_module
import src.database as database
from src.collection_failures import CollectionFailure
from src.collectors.vietnam import VietnamCollector, map_vietnam_sector


class VietnamIncrementalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tempdir.name) / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.summary_db_path = self.data_dir / "marketbot.db"
        self.raw_db_path = self.data_dir / "marketbot_raw.db"

        self.patchers = [
            patch.object(database, "DATA_DIR", self.data_dir),
            patch.object(database, "DB_PATH", self.summary_db_path),
            patch.object(database, "RAW_DB_PATH", self.raw_db_path),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tempdir.cleanup()

    def _seed_universe(self, rows: list[dict]) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_instrument_universe(conn, "VN", rows)
            conn.commit()
        finally:
            conn.close()

    def _seed_abnormals(self, rows: list[dict]) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_abnormal_stocks(conn, rows)
            conn.commit()
        finally:
            conn.close()

    def _load_checkpoint(
        self,
        requested_date: str,
        run_mode: str | None = None,
    ) -> dict | None:
        database.init_db()
        conn = database.get_connection()
        try:
            return database.get_collection_checkpoint(
                conn,
                "VN",
                requested_date=requested_date,
                run_mode=run_mode,
            )
        finally:
            conn.close()

    def test_maps_vnstock_english_and_vietnamese_industries(self) -> None:
        self.assertEqual(map_vietnam_sector("Banks"), "금융")
        self.assertEqual(map_vietnam_sector("Ngân hàng"), "금융")
        self.assertEqual(map_vietnam_sector("Công nghệ và thông tin"), "정보기술")
        self.assertEqual(map_vietnam_sector("Industrials"), "산업재")

    def test_normalizes_vci_icb_name_column(self) -> None:
        collector = VietnamCollector()
        listing = collector._normalize_listing_frame(
            pd.DataFrame(
                [
                    {
                        "symbol": "VCB",
                        "organ_name": "Vietcombank",
                        "icb_name": "Tài chính",
                    }
                ]
            )
        )

        self.assertEqual(listing.iloc[0]["industry"], "Tài chính")
        self.assertEqual(map_vietnam_sector(listing.iloc[0]["industry"]), "금융")

    def test_restore_checkpoint_repairs_legacy_other_sector(self) -> None:
        collector = VietnamCollector()
        restored = collector._restore_checkpoint_state(
            {
                "run_mode": "incremental",
                "next_index": 1,
                "payload": {
                    "listing_rows": [
                        {
                            "ticker": "FPT",
                            "name": "FPT Corp",
                            "industry": "Công nghệ và thông tin",
                        }
                    ],
                    "collected_rows": [
                        {"ticker": "FPT", "name": "FPT Corp", "sector": "기타"}
                    ],
                    "used_dates": ["2026-07-10"],
                },
            }
        )

        self.assertEqual(restored["rows"][0]["sector"], "정보기술")

    def test_rejects_all_other_sector_output(self) -> None:
        collector = VietnamCollector()
        frame = pd.DataFrame(
            [{"ticker": f"T{i:02d}", "sector": "기타"} for i in range(10)]
        )

        with self.assertRaises(CollectionFailure) as ctx:
            collector._validate_sector_coverage(frame)

        self.assertEqual(ctx.exception.failure_code, "sector_metadata_missing")

    def test_select_listing_candidates_uses_active_abnormal_and_large_caps(self) -> None:
        self._seed_universe(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "AAA",
                    "name": "Alpha",
                    "sector": "금융",
                    "market_cap": 400_000_000_000,
                    "close_price": 10.0,
                    "volume": 100_000,
                    "avg_volume_20d": 80_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "BBB",
                    "name": "Beta",
                    "sector": "금융",
                    "market_cap": 350_000_000_000,
                    "close_price": 9.0,
                    "volume": 90_000,
                    "avg_volume_20d": 75_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "CCC",
                    "name": "Gamma",
                    "sector": "정보기술",
                    "market_cap": 900_000_000_000,
                    "close_price": 12.0,
                    "volume": 70_000,
                    "avg_volume_20d": 60_000,
                    "is_filtered": 1,
                    "is_abnormal": 0,
                },
            ]
        )
        self._seed_abnormals(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "DDD",
                    "name": "Delta",
                    "country": "VN",
                    "sector": "산업재",
                    "market_cap": 250_000_000_000,
                    "close_price": 8.0,
                    "daily_return": 51.0,
                    "volume": 120_000,
                    "avg_volume_20d": 60_000,
                }
            ]
        )
        listing = pd.DataFrame(
            [
                {"ticker": "AAA"},
                {"ticker": "BBB"},
                {"ticker": "CCC"},
                {"ticker": "DDD"},
                {"ticker": "EEE"},
            ]
        )
        collector = VietnamCollector()

        with patch.dict(
            vietnam_module.VN_INCREMENTAL_FULL_REFRESH_WEEKDAY,
            {},
            clear=True,
        ):
            with patch.object(vietnam_module, "VN_INCREMENTAL_LARGE_CAP_COUNT", 1):
                with patch.object(vietnam_module, "VN_INCREMENTAL_MIN_CANDIDATES", 1):
                    result = collector._select_listing_candidates(listing, "2026-04-21")

        self.assertEqual(
            set(result["ticker"]),
            {"AAA", "BBB", "CCC", "DDD"},
        )

    def test_select_listing_candidates_returns_full_listing_on_refresh_day(self) -> None:
        self._seed_universe(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "AAA",
                    "name": "Alpha",
                    "sector": "금융",
                    "market_cap": 400_000_000_000,
                    "close_price": 10.0,
                    "volume": 100_000,
                    "avg_volume_20d": 80_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                }
            ]
        )
        listing = pd.DataFrame(
            [
                {"ticker": "AAA"},
                {"ticker": "BBB"},
            ]
        )
        collector = VietnamCollector()

        with patch.dict(
            vietnam_module.VN_INCREMENTAL_FULL_REFRESH_WEEKDAY,
            {"VN": 1},
            clear=True,
        ):
            result = collector._select_listing_candidates(listing, "2026-04-21")

        pd.testing.assert_frame_equal(result.reset_index(drop=True), listing)

    def test_apply_full_rebuild_guard_limits_seed_batch(self) -> None:
        collector = VietnamCollector()
        collector.run_mode = "full"
        collector._set_selection_context("full", "cache_missing")
        listing = pd.DataFrame(
            [
                {"ticker": "AAA", "market_cap": 100},
                {"ticker": "BBB", "market_cap": 50},
                {"ticker": "CCC", "market_cap": 300},
            ]
        )

        with patch.object(vietnam_module, "VN_FULL_REBUILD_MAX_TICKERS", 2):
            result = collector._apply_full_rebuild_guard(listing)

        self.assertEqual(list(result["ticker"]), ["CCC", "AAA"])
        self.assertEqual(collector.run_mode, "seed")

    def test_fetch_all_stocks_saves_checkpoint_on_rate_limit(self) -> None:
        collector = VietnamCollector()
        collector.configure_collection(mode="seed")
        listing = pd.DataFrame(
            [
                {"ticker": "AAA", "name": "Alpha", "sector": "금융", "market_cap": 100},
                {"ticker": "BBB", "name": "Beta", "sector": "금융", "market_cap": 90},
            ]
        )
        history = pd.DataFrame(
            [
                {"time": "2026-04-20", "close": 10.0, "volume": 1000},
                {"time": "2026-04-21", "close": 11.0, "volume": 1200},
            ]
        )
        rate_limited = CollectionFailure(
            message="vnstock rate limit",
            failure_code="provider_rate_limited",
            failure_stage="fetch_history",
            provider="vnstock",
            run_mode="seed",
        )

        with patch.object(collector, "_load_listing", return_value=listing):
            with patch.dict("sys.modules", {"vnstock": types.ModuleType("vnstock")}):
                with patch.object(
                    collector,
                    "_load_history",
                    side_effect=[history, rate_limited],
                ):
                    with self.assertRaises(CollectionFailure) as ctx:
                        collector.fetch_all_stocks("2026-04-21")

        self.assertEqual(ctx.exception.failure_code, "provider_rate_limited")
        checkpoint = self._load_checkpoint("2026-04-21", run_mode="seed")
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint["next_index"], 1)
        self.assertEqual(checkpoint["saved_rows"], 1)
        self.assertEqual(checkpoint["last_ticker"], "AAA")
        self.assertEqual(
            checkpoint["payload"]["collected_rows"][0]["ticker"],
            "AAA",
        )

    def test_fetch_all_stocks_resumes_from_checkpoint(self) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_collection_checkpoint(
                conn,
                "VN",
                "2026-04-21",
                "seed",
                status="pending",
                next_index=1,
                batch_number=1,
                last_ticker="AAA",
                saved_rows=1,
                total_tickers=2,
                payload={
                    "listing_rows": [
                        {
                            "ticker": "AAA",
                            "name": "Alpha",
                            "sector": "금융",
                            "market_cap": 100,
                        },
                        {
                            "ticker": "BBB",
                            "name": "Beta",
                            "sector": "금융",
                            "market_cap": 90,
                        },
                    ],
                    "collected_rows": [
                        {
                            "ticker": "AAA",
                            "name": "Alpha",
                            "sector": "금융",
                            "market_cap": 100.0,
                            "close_price": 11.0,
                            "daily_return": 10.0,
                            "weekly_return": None,
                            "volume": 1200.0,
                            "avg_volume_20d": 1100.0,
                        }
                    ],
                    "used_dates": ["2026-04-21"],
                    "selection_mode": "seed",
                    "selection_reason": "cache_missing",
                    "effective_date": "2026-04-21",
                },
            )
            conn.commit()
        finally:
            conn.close()

        collector = VietnamCollector()
        collector.configure_collection(mode="seed", resume_from_checkpoint=True)
        history = pd.DataFrame(
            [
                {"time": "2026-04-20", "close": 20.0, "volume": 2000},
                {"time": "2026-04-21", "close": 21.0, "volume": 2300},
            ]
        )

        with patch.dict("sys.modules", {"vnstock": types.ModuleType("vnstock")}):
            with patch.object(
                collector,
                "_load_listing",
                side_effect=AssertionError("listing should come from checkpoint"),
            ):
                with patch.object(
                    collector,
                    "_load_history",
                    return_value=history,
                ) as mock_history:
                    result = collector.fetch_all_stocks("2026-04-21")

        self.assertEqual(set(result["ticker"]), {"AAA", "BBB"})
        mock_history.assert_called_once()
        self.assertIsNone(self._load_checkpoint("2026-04-21", run_mode="seed"))

    def test_load_history_falls_back_to_next_source_on_rate_limit(self) -> None:
        collector = VietnamCollector()
        history = pd.DataFrame(
            [
                {"time": "2026-04-20", "close": 20.0, "volume": 2000},
                {"time": "2026-04-21", "close": 21.0, "volume": 2300},
            ]
        )

        class DummyQuote:
            def __init__(self, symbol: str, source: str) -> None:
                self.symbol = symbol
                self.source = source

        class DummyVnstock:
            def stock(self, symbol: str, source: str):
                raise AssertionError("legacy fallback should not run")

        dummy_module = types.ModuleType("vnstock")
        dummy_module.Quote = DummyQuote
        dummy_module.Vnstock = DummyVnstock
        call_order: list[str] = []

        def fake_call(func, *, stage, context_label, provider_label=None):
            call_order.append(provider_label)
            if provider_label == "vnstock:KBS":
                raise CollectionFailure(
                    message="rate limited",
                    failure_code="provider_rate_limited",
                    failure_stage=stage,
                    provider=provider_label,
                    run_mode="incremental",
                )
            return history

        with patch.dict("sys.modules", {"vnstock": dummy_module}):
            with patch.object(collector, "_throttle_requests", return_value=None):
                with patch.object(collector, "_call_provider", side_effect=fake_call):
                    result = collector._load_history(
                        "VCB",
                        "2026-04-01",
                        "2026-04-21",
                    )

        pd.testing.assert_frame_equal(result, history)
        self.assertEqual(call_order, ["vnstock:KBS", "vnstock:VCI"])
        self.assertIn("KBS", collector._blocked_sources_by_stage["fetch_history"])

    def test_prepare_target_listing_auto_mitigates_after_repeated_failures(self) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.log_collection(
                conn,
                "VN",
                "failed",
                error="rate limited",
                failure_code="provider_rate_limited",
                failure_stage="fetch_history",
                run_mode="seed",
                provider="vnstock:KBS",
                raw_error_excerpt="Rate limit",
            )
            database.log_collection(
                conn,
                "VN",
                "failed",
                error="rate limited again",
                failure_code="provider_rate_limited",
                failure_stage="fetch_history",
                run_mode="seed",
                provider="vnstock:KBS",
                raw_error_excerpt="Rate limit",
            )
            conn.commit()
        finally:
            conn.close()

        collector = VietnamCollector()
        listing = pd.DataFrame(
            [
                {"ticker": "AAA", "market_cap": 100},
                {"ticker": "BBB", "market_cap": 50},
                {"ticker": "CCC", "market_cap": 300},
            ]
        )

        def fake_select_listing(input_listing: pd.DataFrame, _: str) -> pd.DataFrame:
            collector._set_selection_context("full", "cache_missing")
            return input_listing

        with patch.object(vietnam_module, "VN_DEGRADED_MAX_TICKERS", 1):
            collector._recent_failure_policy = collector._load_recent_failure_policy()
        with patch.object(collector, "_select_listing_candidates", side_effect=fake_select_listing):
            with patch.object(vietnam_module, "VN_FULL_REBUILD_MAX_TICKERS", 10):
                result = collector._prepare_target_listing(listing, "2026-04-21")

        self.assertEqual(collector.run_mode, "seed")
        self.assertEqual(list(result["ticker"]), ["CCC"])
        self.assertIn("auto_mitigated", collector._selection_reason)
