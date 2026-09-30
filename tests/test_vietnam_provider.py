import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests

import src.database as database
from src.collection_failures import CollectionFailure
from src.collectors import vietnam_provider as provider
from src.collectors.vietnam import VietnamCollector, map_vietnam_sector


class VietnamProviderTests(unittest.TestCase):
    def response(self, data, status=200):
        response = Mock(status_code=status)
        response.json.return_value = data
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(response=response)
        return response

    def test_kbs_listing_retains_equities_and_sector_metadata(self):
        responses = [
            self.response([
                {"symbol": "VCB", "name": "Vietcombank", "type": "stock", "exchange": "HOSE"},
                {"symbol": "FPT", "name": "FPT", "type": "stock", "exchange": "HOSE"},
                {"symbol": "ABC", "name": "ABC", "type": "stock", "exchange": "HNX"},
                {"symbol": "DEF", "name": "DEF", "type": "stock", "exchange": "UPCOM"},
                {"symbol": "CV001", "name": "Warrant", "type": "warrant"},
            ]),
            self.response([{"code": 11, "name": "Ngân hàng"}, {"code": 6, "name": "Công nghệ và thông tin"}]),
            self.response({"stocks": [{"sb": "VCB"}, {"sb": "ABC"}, {"sb": "DEF"}]}),
            self.response({"stocks": [{"sb": "FPT"}]}),
        ]
        throttle = Mock()
        with patch.object(provider.requests, "request", side_effect=responses) as request:
            frame = provider.fetch_listing("KBS", before_request=throttle)
        self.assertEqual(list(frame.ticker), ["VCB", "FPT", "ABC", "DEF"])
        self.assertEqual(set(frame.exchange), {"HOSE", "HNX", "UPCOM"})
        self.assertEqual(map_vietnam_sector(frame.iloc[0].industry), "금융")
        self.assertEqual(map_vietnam_sector(frame.iloc[1].industry), "정보기술")
        self.assertEqual(throttle.call_count, 4)
        for call in request.call_args_list:
            self.assertEqual(call.kwargs["timeout"], (10, 30))

    def test_vci_listing_retains_broad_icb_fallback(self):
        response = self.response({"data": [{"code": "VCB", "name": "Vietcombank",
            "icbLv3": {"name": "Unknown detailed industry"}, "icbLv1": {"name": "Financials"}}]})
        with patch.object(provider.requests, "request", return_value=response):
            row = provider.fetch_listing("VCI", before_request=Mock()).iloc[0]
        self.assertEqual(map_vietnam_sector(row.industry, row.icb_name1), "금융")

    def test_kbs_prices_are_thousands_of_vnd_and_sorted(self):
        data = {"symbol": "VCB", "data_day": [
            {"t": "2026-09-25 07:00", "o": 58200, "h": 58500, "l": 57700, "c": 58000, "v": 4390100},
            {"t": "2026-09-24 07:00", "o": 58000, "h": 59000, "l": 57000, "c": 58200, "v": 1000000},
        ]}
        with patch.object(provider.requests, "request", return_value=self.response(data)) as request:
            frame = provider.fetch_history("KBS", "VCB", "2026-09-24", "2026-09-25", before_request=Mock())
        self.assertEqual(frame.close.tolist(), [58.2, 58.0])
        self.assertEqual(frame.volume.tolist(), [1000000, 4390100])
        self.assertEqual(request.call_args.kwargs["params"], {"sdate": "24-09-2026", "edate": "25-09-2026"})

    def test_vci_arrays_are_normalized_and_trimmed(self):
        times = [int(pd.Timestamp(day, tz="UTC").timestamp()) for day in ("2026-09-23", "2026-09-24", "2026-09-25")]
        data = [{"symbol": "VCB", "t": [str(t) for t in times], "o": [57000]*3,
                 "h": [59000]*3, "l": [56000]*3, "c": [57000, 58200, 58000], "v": [1, 2, 3]}]
        with patch.object(provider.requests, "request", return_value=self.response(data)) as request:
            frame = provider.fetch_history("VCI", "VCB", "2026-09-24", "2026-09-25", before_request=Mock())
        self.assertEqual(frame.close.tolist(), [58.2, 58.0])
        self.assertEqual(frame.time.dt.strftime("%Y-%m-%d").tolist(), ["2026-09-24", "2026-09-25"])
        self.assertEqual(request.call_args.kwargs["json"]["to"], int(pd.Timestamp("2026-09-26", tz="UTC").timestamp()))

    def test_provider_errors_remain_distinct_and_never_return_data(self):
        for status, code in ((429, "provider_rate_limited"), (403, "provider_error"), (503, "provider_error")):
            with self.subTest(status=status), patch.object(provider.requests, "request", return_value=self.response({}, status)):
                with self.assertRaises(CollectionFailure) as ctx:
                    provider.fetch_history("KBS", "VCB", "2026-09-24", "2026-09-25", before_request=Mock())
                self.assertEqual(ctx.exception.failure_code, code)
                self.assertEqual(ctx.exception.provider, "vietnam-http:KBS")
                self.assertEqual(ctx.exception.failure_stage, "fetch_history")

    def test_malformed_json_and_timeout_fail_explicitly(self):
        for exc in (ValueError("bad JSON"), requests.Timeout("timed out")):
            response = self.response({})
            response.json.side_effect = exc
            with patch.object(provider.requests, "request", return_value=response):
                with self.assertRaises(CollectionFailure):
                    provider.fetch_listing("VCI", before_request=Mock())

    def test_missing_columns_and_mismatched_bar_arrays_fail(self):
        for source, data in (("KBS", {"symbol": "VCB", "data_day": [{"t": "2026-09-24", "c": 58000}]}),
                             ("VCI", [{"t": [1, 2], "o": [1], "h": [1], "l": [1], "c": [1], "v": [1]}])):
            with self.subTest(source=source), patch.object(provider.requests, "request", return_value=self.response(data)):
                with self.assertRaises(CollectionFailure):
                    provider.fetch_history(source, "VCB", "2026-09-24", "2026-09-25", before_request=Mock())

    def test_empty_history_tries_fallback_and_both_failures_raise(self):
        collector = VietnamCollector()
        frame = pd.DataFrame([{"time": "2026-09-24", "close": 58.2, "volume": 1000}])
        with patch.object(provider, "fetch_history", side_effect=[pd.DataFrame(), frame]) as fetch:
            pd.testing.assert_frame_equal(collector._load_history("VCB", "2026-09-23", "2026-09-25"), frame)
        self.assertEqual([call.args[0] for call in fetch.call_args_list], ["KBS", "VCI"])
        failure = CollectionFailure("blocked", "provider_error", "fetch_history")
        with patch.object(provider, "fetch_history", side_effect=failure):
            with self.assertRaises(CollectionFailure):
                collector._load_history("VCB", "2026-09-23", "2026-09-25")

    def test_all_unknown_industries_are_a_collection_failure(self):
        with self.assertRaises(CollectionFailure) as ctx:
            VietnamCollector()._validate_sector_coverage(pd.DataFrame([{"sector": "기타"}]))
        self.assertEqual(ctx.exception.failure_code, "sector_metadata_missing")

    def test_transport_failure_checkpoints_and_stops_partial_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            with patch.object(database, "DATA_DIR", data_dir), patch.object(database, "DB_PATH", data_dir / "summary.db"):
                collector = VietnamCollector()
                collector.configure_collection(mode="seed")
                listing = pd.DataFrame([{"ticker": "VCB", "name": "VCB", "industry": "Ngân hàng"},
                                        {"ticker": "FPT", "name": "FPT", "industry": "Technology"}])
                history = pd.DataFrame([{"time": "2026-09-24", "close": 58.2, "volume": 1000}])
                failure = CollectionFailure("HTTP 403", "provider_error", "fetch_history", provider="vietnam-http:VCI")
                with patch.object(collector, "_load_listing", return_value=listing), \
                     patch.object(collector, "_load_history", side_effect=[history, failure]), \
                     patch("src.collectors.vietnam.time.sleep"):
                    with self.assertRaises(CollectionFailure):
                        collector.fetch_all_stocks("2026-09-24")
                conn = database.get_connection()
                try:
                    checkpoint = database.get_collection_checkpoint(conn, "VN", requested_date="2026-09-24", run_mode="seed")
                finally:
                    conn.close()
                self.assertEqual(checkpoint["next_index"], 1)
                self.assertEqual(checkpoint["saved_rows"], 1)


if __name__ == "__main__":
    unittest.main()
