"""글로벌 자금 흐름(lead-lag) 분석 엔진.

가설: "빠른 나라가 먼저 움직이고, 느린 나라가 같은 섹터를 따라간다."

이 모듈은 세 가지를 수행한다.

1. 섹터별 국가 페어의 시차 상관(lead-lag correlation) 계산
   - 같은 캘린더 날짜라도 장 마감 순서(JP→KR→CN→VN→IN→DE→US)를 반영해서
     look-ahead 없는 시차만 허용한다.
2. 선행국의 큰 움직임으로부터 후행국의 다음 거래일 방향을 예측하는
   flow signal 생성
3. 후행국 데이터가 도착하면 과거 예측을 채점해서 가설 적중률을 누적
   (continuous hypothesis verification)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from itertools import permutations

import pandas as pd

from src.config import (
    COUNTRIES,
    LEADLAG_LOOKBACK_DAYS,
    LEADLAG_MAX_LAG,
    LEADLAG_MIN_CORRELATION,
    LEADLAG_MIN_OVERLAP,
    LEADLAG_SIGNAL_EXPIRE_AFTER_DAYS,
    LEADLAG_SIGNAL_MIN_LEADER_MOVE,
)
from src.database import (
    get_connection,
    get_flow_signals,
    get_lead_lag_scores,
    init_db,
    resolve_flow_signal,
    upsert_flow_signals,
    upsert_lead_lag_scores,
)

logger = logging.getLogger(__name__)


def _close_minutes(country: str) -> int:
    """국가 장 마감 시각(UTC)을 분 단위로 반환. 알 수 없으면 하루 끝 취급."""
    close_utc = COUNTRIES.get(country, {}).get("close_utc")
    if not close_utc:
        return 24 * 60
    try:
        hours, minutes = close_utc.split(":")
        return int(hours) * 60 + int(minutes)
    except ValueError:
        return 24 * 60


def _allowed_lags(leader: str, follower: str) -> list[int]:
    """look-ahead 없는 시차 목록.

    leader가 follower보다 같은 날 먼저 마감하면 lag 0(같은 날 추종)도
    유효한 선행 관계다. 반대 방향은 lag 1부터만 의미가 있다.
    """
    start = 0 if _close_minutes(leader) < _close_minutes(follower) else 1
    return list(range(start, LEADLAG_MAX_LAG + 1))


def _resolve_analysis_date(conn, date: str | None) -> str | None:
    if date is not None:
        return date
    row = conn.execute("SELECT MAX(date) FROM sector_performance").fetchone()
    return row[0] if row and row[0] else None


def _load_return_history(conn, end_date: str) -> pd.DataFrame:
    start_date = (
        datetime.strptime(end_date, "%Y-%m-%d")
        - timedelta(days=LEADLAG_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    rows = conn.execute(
        """
        SELECT date, country, sector, daily_return
        FROM sector_performance
        WHERE date BETWEEN ? AND ?
          AND sector != '기타'
          AND daily_return IS NOT NULL
        """,
        (start_date, end_date),
    ).fetchall()
    return pd.DataFrame([dict(row) for row in rows])


def _score_pair(
    pivot: pd.DataFrame,
    leader: str,
    follower: str,
) -> dict | None:
    """한 섹터에서 leader→follower 페어의 최적 시차와 상관을 계산."""
    aligned = pivot[[leader, follower]].dropna()
    if len(aligned) < LEADLAG_MIN_OVERLAP:
        return None

    leader_series = aligned[leader].to_numpy()
    follower_series = aligned[follower].to_numpy()

    best: dict | None = None
    for lag in _allowed_lags(leader, follower):
        if lag > 0:
            lead_vals = leader_series[:-lag]
            follow_vals = follower_series[lag:]
        else:
            lead_vals = leader_series
            follow_vals = follower_series
        if len(lead_vals) < LEADLAG_MIN_OVERLAP:
            continue

        pair = pd.DataFrame({"lead": lead_vals, "follow": follow_vals})
        correlation = pair["lead"].corr(pair["follow"])
        if correlation is None or pd.isna(correlation):
            continue

        nonzero = pair[(pair["lead"] != 0) & (pair["follow"] != 0)]
        if len(nonzero) > 0:
            agreement = float(
                ((nonzero["lead"] * nonzero["follow"]) > 0).mean()
            )
        else:
            agreement = None

        candidate = {
            "lag": lag,
            "correlation": round(float(correlation), 4),
            "direction_agreement": (
                round(agreement, 4) if agreement is not None else None
            ),
            "n_obs": len(pair),
        }
        if best is None or abs(candidate["correlation"]) > abs(best["correlation"]):
            best = candidate

    return best


def compute_lead_lag_scores(date: str | None = None) -> list[dict]:
    """섹터별 국가 페어 lead-lag 점수를 계산해 저장한다."""
    init_db()
    conn = get_connection()
    try:
        date = _resolve_analysis_date(conn, date)
        if date is None:
            logger.warning("lead-lag 계산 불가: 섹터 성과 데이터 없음")
            return []

        history = _load_return_history(conn, date)
        if history.empty:
            logger.warning(f"lead-lag 계산 불가: {date} 기준 데이터 없음")
            return []

        score_rows: list[dict] = []
        for sector, sector_frame in history.groupby("sector"):
            pivot = sector_frame.pivot_table(
                index="date",
                columns="country",
                values="daily_return",
            ).sort_index()
            countries = [c for c in pivot.columns if pivot[c].notna().sum() >= LEADLAG_MIN_OVERLAP]
            for leader, follower in permutations(countries, 2):
                scored = _score_pair(pivot, leader, follower)
                if scored is None:
                    continue
                score_rows.append(
                    {
                        "date": date,
                        "sector": sector,
                        "leader": leader,
                        "follower": follower,
                        **scored,
                    }
                )

        if score_rows:
            upsert_lead_lag_scores(conn, score_rows)
            conn.commit()
            logger.info(f"lead-lag 점수 저장: {len(score_rows)}개 페어 ({date})")
        return score_rows
    finally:
        conn.close()


def generate_flow_signals(date: str | None = None) -> list[dict]:
    """선행국의 큰 섹터 움직임에서 후행국 방향 예측 시그널을 만든다.

    예측 가능해야 하므로 lag >= 1 관계만 사용한다. lag 0(같은 날 추종)은
    리포트 시점에 이미 실현돼 있어 검증 대상이 아니다.
    """
    init_db()
    conn = get_connection()
    try:
        date = _resolve_analysis_date(conn, date)
        if date is None:
            return []

        pair_rows = get_lead_lag_scores(conn, date=date)
        candidates = [
            row
            for row in pair_rows
            if row["lag"] >= 1
            and row["correlation"] is not None
            and row["correlation"] >= LEADLAG_MIN_CORRELATION
            and (row["n_obs"] or 0) >= LEADLAG_MIN_OVERLAP
        ]
        if not candidates:
            return []

        leader_moves = {
            (row["country"], row["sector"]): row["daily_return"]
            for row in conn.execute(
                """
                SELECT country, sector, daily_return
                FROM sector_performance
                WHERE date = ? AND sector != '기타'
                """,
                (date,),
            ).fetchall()
            if row["daily_return"] is not None
        }

        signal_rows = []
        for row in candidates:
            leader_return = leader_moves.get((row["leader"], row["sector"]))
            if (
                leader_return is None
                or abs(leader_return) < LEADLAG_SIGNAL_MIN_LEADER_MOVE
            ):
                continue
            signal_rows.append(
                {
                    "created_date": date,
                    "sector": row["sector"],
                    "leader": row["leader"],
                    "follower": row["follower"],
                    "lag": row["lag"],
                    "leader_return": leader_return,
                    "predicted_direction": 1 if leader_return > 0 else -1,
                    "correlation": row["correlation"],
                }
            )

        if signal_rows:
            upsert_flow_signals(conn, signal_rows)
            conn.commit()
            logger.info(f"flow signal 생성: {len(signal_rows)}개 ({date})")
        return signal_rows
    finally:
        conn.close()


def verify_flow_signals() -> dict:
    """후행국 데이터가 도착한 pending 시그널을 채점한다."""
    init_db()
    conn = get_connection()
    verified = 0
    expired = 0
    try:
        pending = get_flow_signals(conn, status="pending")
        today = datetime.utcnow().date()
        for signal in pending:
            target_offset = max(int(signal.get("lag") or 1) - 1, 0)
            outcome = conn.execute(
                """
                SELECT date, daily_return
                FROM sector_performance
                WHERE country = ? AND sector = ? AND date > ?
                ORDER BY date ASC
                LIMIT 1 OFFSET ?
                """,
                (
                    signal["follower"],
                    signal["sector"],
                    signal["created_date"],
                    target_offset,
                ),
            ).fetchone()

            if outcome is not None and outcome["daily_return"] is not None:
                follower_return = float(outcome["daily_return"])
                hit = int(follower_return * signal["predicted_direction"] > 0)
                resolve_flow_signal(
                    conn,
                    signal["id"],
                    status="verified",
                    target_date=outcome["date"],
                    follower_return=follower_return,
                    hit=hit,
                )
                verified += 1
                continue

            try:
                created = datetime.strptime(
                    signal["created_date"], "%Y-%m-%d"
                ).date()
            except ValueError:
                continue
            if (today - created).days > LEADLAG_SIGNAL_EXPIRE_AFTER_DAYS:
                resolve_flow_signal(conn, signal["id"], status="expired")
                expired += 1

        if verified or expired:
            conn.commit()
            logger.info(
                f"flow signal 채점: verified={verified}, expired={expired}"
            )
        return {"verified": verified, "expired": expired}
    finally:
        conn.close()


def update_lead_lag(date: str | None = None) -> dict:
    """일일 파이프라인 진입점: 채점 → 점수 갱신 → 신규 시그널 생성."""
    outcomes = verify_flow_signals()
    scores = compute_lead_lag_scores(date=date)
    signals = generate_flow_signals(date=date)
    return {
        "outcomes": outcomes,
        "pairs_scored": len(scores),
        "signals_created": len(signals),
    }
