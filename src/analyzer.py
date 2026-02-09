"""글로벌 섹터 트렌드 분석 엔진

- 각 국가의 섹터 성과를 기반으로 글로벌 트렌드 스코어 계산
- 섹터별 국가 확산도(breadth) 분석
- 모멘텀 시그널 생성
"""

import logging
from datetime import datetime

from src.config import (
    COUNTRIES,
    SECTORS,
    TREND_WEIGHT_BREADTH,
    TREND_WEIGHT_MOMENTUM,
    TREND_WEIGHT_RETURN,
)
from src.database import (
    get_connection,
    get_latest_sector_performance,
    init_db,
    upsert_trend_scores,
)

logger = logging.getLogger(__name__)


def compute_trend_scores(date: str | None = None):
    """모든 국가의 섹터 성과를 기반으로 글로벌 트렌드 스코어 계산.

    트렌드 스코어 = (평균수익률 정규화 × 0.4)
                  + (확산도 × 0.3)
                  + (주간모멘텀 × 0.3)
    """
    if date is None:
        date = datetime.utcnow().strftime("%Y-%m-%d")

    init_db()
    conn = get_connection()

    # 해당 날짜의 모든 국가 섹터 성과
    all_perf = get_latest_sector_performance(conn, date=date)
    if not all_perf:
        logger.warning(f"트렌드 스코어 계산 불가: {date} 데이터 없음")
        conn.close()
        return

    # 섹터별로 국가 데이터 그룹화
    sector_data: dict[str, list[dict]] = {}
    for row in all_perf:
        sector = row["sector"]
        if sector == "기타":
            continue
        if sector not in sector_data:
            sector_data[sector] = []
        sector_data[sector].append(row)

    trend_rows = []
    for sector, entries in sector_data.items():
        returns = [e["daily_return"] for e in entries if e["daily_return"] is not None]
        if not returns:
            continue

        # 1) 글로벌 평균 수익률
        avg_return = sum(returns) / len(returns)

        # 2) 확산도: 상승 국가 비율
        positive = sum(1 for r in returns if r > 0)
        negative = sum(1 for r in returns if r < 0)
        total_countries = len(returns)
        breadth = positive / total_countries if total_countries > 0 else 0.5

        # 3) 수익률 정규화 (-100 ~ +100)
        # 일간 수익률 ±5%를 ±100으로 매핑
        norm_return = max(min(avg_return / 5.0 * 100, 100), -100)

        # 4) 확산도 정규화 (0~1 → -100~+100)
        norm_breadth = (breadth - 0.5) * 200  # 50%=0, 100%=100, 0%=-100

        # 5) 주간 모멘텀 (weekly_return이 있으면 사용, 없으면 0)
        weekly_returns = [e["weekly_return"] for e in entries if e.get("weekly_return")]
        if weekly_returns:
            avg_weekly = sum(weekly_returns) / len(weekly_returns)
            norm_momentum = max(min(avg_weekly / 10.0 * 100, 100), -100)
        else:
            norm_momentum = 0

        # 6) 최종 트렌드 스코어
        trend_score = (
            norm_return * TREND_WEIGHT_RETURN
            + norm_breadth * TREND_WEIGHT_BREADTH
            + norm_momentum * TREND_WEIGHT_MOMENTUM
        )

        # 7) 시그널 분류
        if trend_score > 60:
            signal = "STRONG_UP"
        elif trend_score > 30:
            signal = "UP"
        elif trend_score > -30:
            signal = "NEUTRAL"
        elif trend_score > -60:
            signal = "DOWN"
        else:
            signal = "STRONG_DOWN"

        trend_rows.append({
            "date": date,
            "sector": sector,
            "trend_score": round(trend_score, 2),
            "countries_positive": positive,
            "countries_negative": negative,
            "global_avg_return": round(avg_return, 4),
            "global_breadth": round(breadth, 4),
            "momentum_signal": signal,
        })

    if trend_rows:
        upsert_trend_scores(conn, trend_rows)
        conn.commit()
        logger.info(f"트렌드 스코어 저장: {len(trend_rows)}개 섹터")

    conn.close()
    return trend_rows
