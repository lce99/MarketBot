import unittest

import pandas as pd

from src.filter import apply_filters


class MarketCapFilterTests(unittest.TestCase):
    def test_known_small_cap_is_filtered(self) -> None:
        df = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB"],
                "daily_return": [1.0, 2.0],
                "market_cap": [1_000_000.0, 900_000_000_000.0],
                "volume": [1000.0, 2000.0],
            }
        )
        result = apply_filters(df, "KR")
        self.assertEqual(int(result.loc[0, "is_filtered"]), 1)
        self.assertEqual(int(result.loc[1, "is_filtered"]), 0)

    def test_unknown_market_cap_is_not_filtered(self) -> None:
        """시총 정보가 없는 시장(예: VN vnstock listing)이 전부 제외되면 안 된다."""
        df = pd.DataFrame(
            {
                "ticker": ["VNA", "VNB", "VNC"],
                "daily_return": [1.0, -0.5, 0.2],
                "market_cap": [None, None, None],
                "volume": [1000.0, 2000.0, 3000.0],
            }
        )
        result = apply_filters(df, "VN")
        # 시총 기준으로는 아무도 제외되지 않아야 한다 (거래량 하위 구간만 제외 가능).
        capped = result[result["market_cap"].notna()]
        self.assertTrue((capped["is_filtered"] == 0).all())
        self.assertLess(int(result["is_filtered"].sum()), len(result))


if __name__ == "__main__":
    unittest.main()
