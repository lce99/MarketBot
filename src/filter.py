"""종목 필터링 - 시총, 거래량, 비정상 급등/급락"""

import pandas as pd

from src.config import (
    FILTER_ABNORMAL_RETURN_THRESHOLD,
    FILTER_MARKET_CAP_MIN,
    FILTER_VOLUME_BOTTOM_PERCENTILE,
)


def apply_filters(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """전체 필터 적용. is_filtered, is_abnormal 컬럼 설정.

    Args:
        df: 컬럼 필수: ticker, daily_return, market_cap, volume
        country: 국가 코드 (US, KR, CN, ...)

    Returns:
        동일 DataFrame에 is_filtered, is_abnormal 컬럼 추가
    """
    df = df.copy()
    df["is_filtered"] = 0
    df["is_abnormal"] = 0

    # 1) 시가총액 필터
    min_cap = FILTER_MARKET_CAP_MIN.get(country, 0)
    if min_cap > 0 and "market_cap" in df.columns:
        mask = df["market_cap"].fillna(0) < min_cap
        df.loc[mask, "is_filtered"] = 1

    # 2) 거래량 필터 (하위 N% 제외, 필터링 안 된 종목 기준)
    if "volume" in df.columns:
        active = df[df["is_filtered"] == 0]
        if len(active) > 0:
            threshold = active["volume"].quantile(
                FILTER_VOLUME_BOTTOM_PERCENTILE / 100.0
            )
            mask = (df["is_filtered"] == 0) & (df["volume"].fillna(0) < threshold)
            df.loc[mask, "is_filtered"] = 1

    # 3) 비정상 급등/급락
    if "daily_return" in df.columns:
        mask = df["daily_return"].abs() > FILTER_ABNORMAL_RETURN_THRESHOLD
        df.loc[mask, "is_abnormal"] = 1

    return df
