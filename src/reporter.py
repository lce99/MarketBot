"""텔레그램 리포트 포맷터."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from src.config import (
    COUNTRIES,
    LEADLAG_MIN_CORRELATION,
    LEADLAG_SCOREBOARD_WINDOW_DAYS,
    STATUS_STALE_AFTER_DAYS,
)
from src.database import (
    _table_exists,
    get_abnormal_stocks,
    get_connection,
    get_flow_signal_stats,
    get_flow_signals,
    get_latest_benchmarks,
    get_latest_sector_dates_by_country,
    get_latest_sector_performance,
    get_lead_lag_scores,
)
from src.watchlist import WatchItem, load_watchlist

logger = logging.getLogger(__name__)

COUNTRY_INDEX_LABELS = {
    "KR": "KOSPI",
    "CN": "CSI300",
    "JP": "Nikkei 225",
    "VN": "VN-Index",
    "IN": "NIFTY 50",
    "DE": "DAX",
}

COUNTRY_ORDER = ["US", "KR", "CN", "JP", "VN", "IN", "DE"]
UNSTABLE_COVERAGE_MARKETS = {"CN", "VN"}


def _resolve_report_date(conn, date: str | None) -> str:
    if date is not None:
        return date

    row = conn.execute("SELECT MAX(date) FROM sector_performance").fetchone()
    return row[0] if row and row[0] else datetime.utcnow().strftime("%Y-%m-%d")


def _build_benchmark_lookup(conn, date: str) -> dict[tuple[str, str | None], dict]:
    rows = get_latest_benchmarks(conn, date=date)
    return {(row["country"], row["sector"]): row for row in rows}


def _get_benchmark_row(
    lookup: dict[tuple[str, str | None], dict],
    country: str,
    sector: str,
) -> dict | None:
    return lookup.get((country, sector)) or lookup.get((country, None))


def _parse_top_gainers(value) -> list[dict]:
    if not value:
        return []
    try:
        return json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        return []


def _parse_report_date(value: str) -> datetime.date | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _country_label(code: str) -> str:
    info = COUNTRIES.get(code, {})
    flag = info.get("flag", "")
    name = info.get("name_kr", code)
    return f"{flag} {name}".strip()


def _format_country_list(codes: list[str]) -> str:
    return ", ".join(_country_label(code) for code in codes) if codes else "-"


def _benchmark_label(row: dict) -> str:
    if row.get("sector") is None:
        return COUNTRY_INDEX_LABELS.get(row["country"], row["ticker"].lstrip("^"))
    return row["ticker"].lstrip("^")


def _format_signed_pct(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:+.{decimals}f}%"


def _format_signed_number(value: float | None, decimals: int = 0) -> str:
    if value is None:
        return "-"
    return f"{value:+.{decimals}f}"


def _clamp(value: float, low: float, high: float) -> float:
    return max(min(value, high), low)


def _format_benchmark_return(row: dict, report_date: str) -> str:
    daily_return = row.get("daily_return")
    if daily_return is None:
        return ""

    text = _format_signed_pct(daily_return)
    benchmark_date = row.get("date")
    if benchmark_date and benchmark_date != report_date:
        text += f" ({benchmark_date[5:]})"
    return text


def _format_country_benchmark_summary(
    lookup: dict[tuple[str, str | None], dict],
    country: str,
    report_date: str,
) -> str:
    benchmark = lookup.get((country, None))
    if not benchmark or benchmark.get("daily_return") is None:
        return ""

    parts = [
        f"기준선 {_benchmark_label(benchmark)} "
        f"{_format_benchmark_return(benchmark, report_date)}"
    ]

    weekly_return = benchmark.get("weekly_return")
    if weekly_return is not None:
        parts.append(f"주간 {_format_signed_pct(weekly_return)}")

    return " · ".join(parts)


def _format_benchmark_comparison(
    row: dict,
    benchmark: dict | None,
    report_date: str,
) -> str:
    if not benchmark:
        return ""

    sector_return = row["daily_return"]
    benchmark_return = benchmark.get("daily_return")
    if sector_return is None or benchmark_return is None:
        return ""

    label = _benchmark_label(benchmark)
    alpha = sector_return - benchmark_return
    return (
        f"{label} {_format_benchmark_return(benchmark, report_date)}"
        f" · 대비 {_format_signed_pct(alpha)}"
    )


def _format_leader(value) -> str:
    gainers = _parse_top_gainers(value)
    if not gainers:
        return ""

    top = gainers[0]
    return f"대표 {top['name']} {_format_signed_pct(top['return'], decimals=1)}"


def _format_market_cap_short(row: dict) -> str:
    market_cap = row.get("market_cap")
    if market_cap is None or market_cap <= 0:
        return ""

    if row.get("country") == "KR":
        return f"시총 {market_cap / 1e8:,.0f}억"
    return f"시총 {market_cap / 1e6:,.0f}M"


def _format_sector_brief(
    row: dict,
    benchmark_lookup: dict[tuple[str, str | None], dict],
    report_date: str,
) -> str:
    ret = row.get("daily_return") or 0
    breadth_pct = (row.get("breadth") or 0) * 100
    stock_count = row.get("stock_count")

    lines = [f"• {row['sector']} {_format_signed_pct(ret)}"]

    detail_parts = []
    comparison = _format_benchmark_comparison(
        row,
        _get_benchmark_row(benchmark_lookup, row["country"], row["sector"]),
        report_date,
    )
    if comparison:
        detail_parts.append(comparison)
    if breadth_pct > 0:
        detail_parts.append(f"상승 {breadth_pct:.0f}%")
    if stock_count:
        detail_parts.append(f"{stock_count}종목")

    leader = _format_leader(row.get("top_gainers"))
    if leader:
        detail_parts.append(leader)

    if detail_parts:
        lines.append("  " + " · ".join(detail_parts))

    return "\n".join(lines)


def _build_data_quality_lines(
    conn,
    report_date: str,
    by_country: dict[str, list[dict]],
    *,
    include_auto_warning: bool,
) -> tuple[list[str], bool]:
    if not include_auto_warning:
        return [], False

    latest_dates = get_latest_sector_dates_by_country(conn)
    report_dt = _parse_report_date(report_date)
    age_days = None
    if report_dt:
        age_days = (datetime.utcnow().date() - report_dt).days

    available_codes = [code for code in COUNTRY_ORDER if code in by_country]
    lagging = [
        f"{code} {latest_dates[code][5:]}"
        for code in COUNTRY_ORDER
        if latest_dates.get(code) and latest_dates[code] != report_date
    ]
    missing = [
        code
        for code in COUNTRY_ORDER
        if code not in latest_dates
    ]

    issues = []
    if age_days is not None and age_days > STATUS_STALE_AFTER_DAYS:
        issues.append(f"기준일 {age_days}일 경과")
    if lagging:
        issues.append("이전 데이터 " + ", ".join(lagging))
    if missing:
        issues.append("미수집 " + ", ".join(missing))

    is_low_quality = bool(issues)
    quality = "낮음" if is_low_quality else "정상"
    lines = [f"데이터 신뢰도: {quality}"]
    if issues:
        lines[0] += f" ({'; '.join(issues)})"
    lines.append(f"최신 포함: {_format_country_list(available_codes)}")

    unstable_missing = [
        code
        for code in COUNTRY_ORDER
        if code in UNSTABLE_COVERAGE_MARKETS
        and latest_dates.get(code) != report_date
    ]
    if unstable_missing:
        lines.append(f"불안정 커버리지: {_format_country_list(unstable_missing)}")

    return lines, is_low_quality


def _format_trend_summary(rows: list[dict], *, reverse: bool = False, limit: int = 3) -> str:
    selected = list(rows)
    if reverse:
        selected = list(reversed(selected))

    parts = []
    for row in selected:
        score = row["trend_score"]
        if reverse and score >= 0:
            continue
        if not reverse and score <= 0:
            continue
        parts.append(f"{row['sector']} {_format_signed_number(score)}")
        if len(parts) >= limit:
            break
    return ", ".join(parts)


def _format_perf_summary(rows: list[dict], *, reverse: bool = False, limit: int = 3) -> str:
    filtered = [row for row in rows if row["sector"] != "기타"]
    filtered.sort(
        key=lambda row: (
            row.get("daily_return") if row.get("daily_return") is not None else -999,
            row.get("breadth") or 0,
        ),
        reverse=not reverse,
    )

    parts = []
    for row in filtered:
        ret = row.get("daily_return")
        if reverse and (ret is None or ret >= 0):
            continue
        if not reverse and (ret is None or ret <= 0):
            continue
        parts.append(
            f"{_country_label(row['country'])} {row['sector']} {_format_signed_pct(ret)}"
        )
        if len(parts) >= limit:
            break
    return ", ".join(parts)


def _build_takeaway_lines(
    trend_rows: list[dict],
    all_perf: list[dict],
    *,
    is_low_quality: bool,
) -> list[str]:
    lines = ["핵심 결론"]
    if is_low_quality:
        lines.append("데이터가 오래되었거나 일부 시장이 빠져 있어 신규 판단은 보수적으로 봐야 합니다.")

    strong = _format_trend_summary(trend_rows, limit=3) if trend_rows else ""
    weak = _format_trend_summary(trend_rows, reverse=True, limit=2) if trend_rows else ""
    if not strong:
        strong = _format_perf_summary(all_perf, limit=3)
    if not weak:
        weak = _format_perf_summary(all_perf, reverse=True, limit=2)

    if strong:
        lines.append(f"강한 축: {strong}")
    if weak:
        lines.append(f"약한 축: {weak}")
    if not strong and not weak:
        lines.append("아직 요약할 섹터 흐름이 없습니다.")

    return lines


def _score_watch_candidate(
    row: dict,
    leader: dict,
    benchmark_lookup: dict[tuple[str, str | None], dict],
    report_date: str,
) -> dict:
    sector_return = row.get("daily_return") or 0.0
    breadth = row.get("breadth") or 0.0
    weekly_return = row.get("weekly_return")
    stock_count = row.get("stock_count") or 0
    leader_return = leader.get("return")

    benchmark = _get_benchmark_row(benchmark_lookup, row["country"], row["sector"])
    benchmark_return = benchmark.get("daily_return") if benchmark else None
    alpha = None
    if benchmark_return is not None and row.get("daily_return") is not None:
        alpha = sector_return - benchmark_return

    score = 50.0
    score += _clamp(sector_return * 7.0, -22.0, 24.0)
    score += _clamp((breadth - 0.5) * 45.0, -20.0, 20.0)
    if weekly_return is not None:
        score += _clamp(weekly_return * 1.5, -12.0, 12.0)
    if alpha is not None:
        score += _clamp(alpha * 5.0, -12.0, 12.0)
    if leader_return is not None:
        score += _clamp(leader_return * 0.4, -5.0, 8.0)

    cautions = []
    exclude = False
    if sector_return <= 0:
        score -= 18.0
        exclude = True
        cautions.append("섹터 약세")
    if breadth < 0.4:
        score -= 10.0
        exclude = True
        cautions.append("상승 확산 약함")
    if stock_count and stock_count < 5:
        score -= 8.0
        exclude = True
        cautions.append("표본 적음")
    if leader_return is not None and leader_return >= 30:
        score -= 20.0
        exclude = True
        cautions.append("대표 종목 과열")
    elif leader_return is not None and leader_return >= 15:
        score -= 8.0
        cautions.append("대표 종목 급등")

    reasons = [
        f"섹터 {_format_signed_pct(sector_return)}",
        f"확산 {breadth * 100:.0f}%",
    ]
    if alpha is not None:
        reasons.append(f"벤치 대비 {_format_signed_pct(alpha)}")
    if weekly_return is not None:
        reasons.append(f"주간 {_format_signed_pct(weekly_return)}")
    if leader_return is not None:
        reasons.append(f"대표 {_format_signed_pct(leader_return, 1)}")

    return {
        "name": leader.get("name", ""),
        "country": row["country"],
        "sector": row["sector"],
        "score": round(_clamp(score, 0.0, 100.0)),
        "reasons": reasons,
        "cautions": cautions,
        "exclude": exclude,
        "sector_return": sector_return,
        "leader_return": leader_return,
    }


def _format_watch_candidate(candidate: dict) -> str:
    return (
        f"{_country_label(candidate['country'])} {candidate['name']} "
        f"관찰점수 {candidate['score']} | "
        + " · ".join(candidate["reasons"])
    )


def _format_caution_candidate(candidate: dict) -> str:
    caution_text = ", ".join(candidate["cautions"]) if candidate["cautions"] else "점수 낮음"
    return (
        f"{_country_label(candidate['country'])} {candidate['name']} "
        f"({candidate['sector']}) | {caution_text}"
    )


def _build_watch_sections(
    all_perf: list[dict],
    benchmark_lookup: dict[tuple[str, str | None], dict],
    report_date: str,
    limit: int = 5,
) -> tuple[list[str], list[str]]:
    rows = [row for row in all_perf if row["sector"] != "기타"]
    scored = []
    seen = set()
    for row in rows:
        gainers = _parse_top_gainers(row.get("top_gainers"))
        if not gainers:
            continue

        leader = gainers[0]
        name = leader.get("name")
        if not name:
            continue
        key = (row["country"], name)
        if key in seen:
            continue
        seen.add(key)
        scored.append(
            _score_watch_candidate(row, leader, benchmark_lookup, report_date)
        )

    scored.sort(
        key=lambda candidate: (
            candidate["score"],
            candidate["sector_return"],
            candidate["leader_return"] or 0,
        ),
        reverse=True,
    )

    watch_candidates = []
    caution_candidates = []
    for candidate in scored:
        if candidate["exclude"] or candidate["score"] < 55:
            if candidate["cautions"] and len(caution_candidates) < 3:
                caution_candidates.append(_format_caution_candidate(candidate))
            continue

        watch_candidates.append(_format_watch_candidate(candidate))
        if len(watch_candidates) >= limit:
            break

    if len(watch_candidates) < limit:
        for candidate in scored:
            if len(watch_candidates) >= limit:
                break
            if candidate["exclude"] or candidate["score"] < 50:
                continue
            line = _format_watch_candidate(candidate)
            if line not in watch_candidates:
                watch_candidates.append(line)

    return watch_candidates, caution_candidates


def _find_watch_snapshot(conn, item: WatchItem, report_date: str) -> dict | None:
    row = conn.execute(
        """
        SELECT *
        FROM instrument_universe
        WHERE country = ?
          AND ticker = ?
          AND last_seen_date <= ?
        ORDER BY last_seen_date DESC
        LIMIT 1
        """,
        (item.country, item.ticker, report_date),
    ).fetchone()
    if row:
        return dict(row)

    return None


def _find_sector_row(conn, report_date: str, country: str, sector: str | None) -> dict | None:
    if not sector:
        return None

    row = conn.execute(
        """
        SELECT *
        FROM sector_performance
        WHERE date = ? AND country = ? AND sector = ?
        LIMIT 1
        """,
        (report_date, country, sector),
    ).fetchone()
    return dict(row) if row else None


def _watch_signal(sector_row: dict | None) -> str:
    if not sector_row:
        return "데이터 없음"

    ret = sector_row.get("daily_return") or 0
    breadth = sector_row.get("breadth") or 0
    if ret > 0 and breadth >= 0.5:
        return "우호"
    if ret < 0 or breadth < 0.4:
        return "주의"
    return "중립"


def _format_watchlist_line(
    conn,
    item: WatchItem,
    report_date: str,
    benchmark_lookup: dict[tuple[str, str | None], dict],
) -> str:
    snapshot = _find_watch_snapshot(conn, item, report_date)
    name = item.name or (snapshot or {}).get("name") or item.ticker
    sector = item.sector or (snapshot or {}).get("sector")
    sector_row = _find_sector_row(conn, report_date, item.country, sector)

    details = []
    if sector_row:
        details.append(f"{sector} {_watch_signal(sector_row)}")
        details.append(f"섹터 {_format_signed_pct(sector_row.get('daily_return'))}")
        details.append(f"확산 {(sector_row.get('breadth') or 0) * 100:.0f}%")

        benchmark = _get_benchmark_row(benchmark_lookup, item.country, sector)
        if benchmark and benchmark.get("daily_return") is not None:
            alpha = (sector_row.get("daily_return") or 0) - benchmark["daily_return"]
            details.append(f"벤치 대비 {_format_signed_pct(alpha)}")
    elif sector:
        details.append(f"{sector} 데이터 없음")
    else:
        details.append("섹터 매칭 없음")

    if snapshot:
        snapshot_date = snapshot.get("last_seen_date")
        if snapshot_date and snapshot_date != report_date:
            details.append(f"스냅샷 {snapshot_date[5:]}")
        if int(snapshot.get("last_is_abnormal") or 0) == 1:
            details.append("최근 이상 변동")
        if int(snapshot.get("last_is_filtered") or 0) == 1:
            details.append("필터 제외권")
    else:
        details.append("종목 스냅샷 없음")

    if item.note:
        details.append(item.note)

    return (
        f"{_country_label(item.country)} {name} ({item.ticker}) | "
        + " · ".join(details)
    )


def _build_watchlist_lines(
    conn,
    report_date: str,
    benchmark_lookup: dict[tuple[str, str | None], dict],
    limit: int = 8,
) -> list[str]:
    items = load_watchlist()
    if not items:
        return []

    return [
        _format_watchlist_line(conn, item, report_date, benchmark_lookup)
        for item in items[:limit]
    ]


def _get_trend_rows(conn, report_date: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT sector, trend_score, countries_positive, countries_negative,
               global_avg_return, momentum_signal
        FROM trend_scores
        WHERE date = ?
        ORDER BY trend_score DESC
        """,
        (report_date,),
    ).fetchall()
    return [dict(row) for row in rows]


def _build_trend_section_lines(
    trend_rows: list[dict],
    *,
    strong_limit: int = 5,
    weak_limit: int = 3,
) -> list[str]:
    lines: list[str] = []
    if not trend_rows:
        lines.append("트렌드 점수 없음: 국가별 섹터 등락 기준으로 대체 요약합니다.")
        return lines

    lines.append("🔥 강한 흐름")
    strong_rows = [trend for trend in trend_rows if trend["trend_score"] > 0]
    for i, trend in enumerate(strong_rows[:strong_limit], start=1):
        total = trend["countries_positive"] + trend["countries_negative"]
        avg_return = trend["global_avg_return"]
        lines.append(f"{i}. {trend['sector']} {_format_signed_number(trend['trend_score'])}")
        lines.append(
            f"   평균 {_format_signed_pct(avg_return)}"
            f" · 상승 {trend['countries_positive']}/{total}개국"
        )

    if not strong_rows:
        lines.append("뚜렷한 강세 섹터가 없습니다.")

    weak_rows = [
        row
        for row in reversed(trend_rows)
        if row["trend_score"] < 0
    ][:weak_limit]
    if weak_rows:
        lines.extend(["", "🧊 약한 흐름"])
        for trend in weak_rows:
            total = trend["countries_positive"] + trend["countries_negative"]
            avg_return = trend["global_avg_return"]
            lines.append(f"• {trend['sector']} {_format_signed_number(trend['trend_score'])}")
            lines.append(
                f"  평균 {_format_signed_pct(avg_return)}"
                f" · 하락 {trend['countries_negative']}/{total}개국"
            )

    return lines


def _format_abnormal_stock_lines(abnormals: list[dict], limit: int = 10) -> list[str]:
    lines: list[str] = []
    for abnormal in abnormals[:limit]:
        info = COUNTRIES.get(abnormal["country"], {})
        flag = info.get("flag", "")
        lines.extend(
            [
                "",
                f"• {flag} {abnormal['name']} {_format_signed_pct(abnormal['daily_return'], 1)}",
            ]
        )
        detail_parts = [
            part
            for part in (
                abnormal.get("sector"),
                _format_market_cap_short(abnormal),
            )
            if part
        ]
        if detail_parts:
            lines.append("  " + " · ".join(detail_parts))
    return lines


def _format_signal_line(signal: dict, *, with_status: bool = False) -> str:
    direction = "상승" if signal["predicted_direction"] > 0 else "하락"
    arrow = "📈" if signal["predicted_direction"] > 0 else "📉"
    line = (
        f"{arrow} {signal['sector']}: "
        f"{_country_label(signal['leader'])} "
        f"{_format_signed_pct(signal['leader_return'])} → "
        f"{_country_label(signal['follower'])} {direction} 예상"
        f" (ρ{_format_signed_number(signal['correlation'], 2)})"
    )
    if with_status and signal.get("status") == "verified":
        mark = "✅ 적중" if signal.get("hit") else "❌ 빗나감"
        line += (
            f" → 실제 {_format_signed_pct(signal.get('follower_return'))} {mark}"
        )
    return line


def _scoreboard_verdict(stats: dict) -> str:
    total = stats["total"]
    if total < 10:
        return f"표본 부족 (검증 {total}건)"
    rate = stats["hit_rate"] or 0
    if rate >= 0.6:
        verdict = "가설 지지"
    elif rate >= 0.525:
        verdict = "약한 지지"
    else:
        verdict = "지지 안 됨"
    return f"{verdict} · 적중률 {rate * 100:.0f}% ({stats['hits']}/{total})"


def _build_flow_scoreboard_lines(conn) -> list[str]:
    window_start = (
        datetime.utcnow() - timedelta(days=LEADLAG_SCOREBOARD_WINDOW_DAYS)
    ).strftime("%Y-%m-%d")
    recent = get_flow_signal_stats(conn, since_date=window_start)
    all_time = get_flow_signal_stats(conn)

    lines = [f"가설 검증 (최근 {LEADLAG_SCOREBOARD_WINDOW_DAYS}일): {_scoreboard_verdict(recent)}"]
    if all_time["total"] > recent["total"]:
        lines.append(f"누적: {_scoreboard_verdict(all_time)}")
    if recent["up_total"] or recent["down_total"]:
        parts = []
        if recent["up_total"]:
            parts.append(
                f"상승 예측 {recent['up_hits']}/{recent['up_total']}"
            )
        if recent["down_total"]:
            parts.append(
                f"하락 예측 {recent['down_hits']}/{recent['down_total']}"
            )
        lines.append(" · ".join(parts))
    return lines


def _build_leader_ranking_lines(pair_rows: list[dict], limit: int = 4) -> list[str]:
    """강한 페어 기준으로 어느 나라가 주로 선행하는지 순위를 만든다."""
    strong = [
        row
        for row in pair_rows
        if row["correlation"] is not None
        and row["correlation"] >= LEADLAG_MIN_CORRELATION
    ]
    if not strong:
        return []

    counts: dict[str, int] = {}
    for row in strong:
        counts[row["leader"]] = counts.get(row["leader"], 0) + 1
        counts[row["follower"]] = counts.get(row["follower"], 0)

    ranked = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    lines = ["🏁 선행국 순위 (강한 페어에서 선행한 횟수)"]
    for country, count in ranked[:limit]:
        lines.append(f"• {_country_label(country)}: {count}회")
    return lines


def _build_flow_section_lines(conn, date: str) -> list[str]:
    """일간 리포트에 붙는 압축된 자금 흐름 섹션."""
    if not _table_exists(conn, "flow_signals"):
        return []

    lines: list[str] = []
    signals = get_flow_signals(conn, status="pending", created_date=date, limit=3)
    if signals:
        lines.append("🌊 자금 흐름 시그널 (다음 거래일)")
        for signal in signals:
            lines.append(f"• {_format_signal_line(signal)}")

    scoreboard = _build_flow_scoreboard_lines(conn)
    stats_total = get_flow_signal_stats(conn)["total"]
    if signals or stats_total:
        if not signals:
            lines.append("🌊 자금 흐름 시그널: 오늘은 임계값을 넘는 선행 신호 없음")
        lines.append(scoreboard[0])
        lines.append("자세히: /flow")
    return lines


def format_flow_report(date: str | None = None) -> str:
    """글로벌 자금 흐름 lead-lag 리포트 (/flow 명령)."""
    conn = get_connection()
    try:
        if not _table_exists(conn, "lead_lag_scores"):
            return "🌊 자금 흐름 데이터가 아직 없습니다. 다음 리포트 사이클 이후 다시 시도하세요."

        date = _resolve_report_date(conn, date)
        pair_rows = get_lead_lag_scores(conn, date=date)

        lines = [
            "🌊 글로벌 자금 흐름 (lead-lag)",
            f"기준일 {date}",
            "",
        ]
        lines.extend(_build_flow_scoreboard_lines(conn))

        if not pair_rows:
            lines.extend(["", "아직 계산된 lead-lag 페어가 없습니다."])
            return "\n".join(lines).strip()

        ranking = _build_leader_ranking_lines(pair_rows)
        if ranking:
            lines.extend(["", *ranking])

        strong_pairs = [
            row
            for row in pair_rows
            if row["correlation"] is not None
            and row["correlation"] >= LEADLAG_MIN_CORRELATION
        ][:7]
        if strong_pairs:
            lines.extend(["", "🔗 강한 선행 관계"])
            for row in strong_pairs:
                lag_label = "당일" if row["lag"] == 0 else f"+{row['lag']}일"
                agreement = row.get("direction_agreement")
                agreement_text = (
                    f" · 방향 일치 {agreement * 100:.0f}%" if agreement is not None else ""
                )
                lines.append(
                    f"• {row['sector']}: {_country_label(row['leader'])} → "
                    f"{_country_label(row['follower'])} {lag_label}"
                    f" ρ{_format_signed_number(row['correlation'], 2)}"
                    f"{agreement_text} (n={row['n_obs']})"
                )

        pending = get_flow_signals(conn, status="pending", created_date=date, limit=5)
        if pending:
            lines.extend(["", "📌 다음 거래일 주목 흐름"])
            for signal in pending:
                lines.append(f"• {_format_signal_line(signal)}")

        recent_verified = [
            signal
            for signal in get_flow_signals(conn, status="verified", limit=5)
        ]
        if recent_verified:
            lines.extend(["", "🧾 최근 검증 결과"])
            for signal in recent_verified:
                lines.append(
                    f"• {_format_signal_line(signal, with_status=True)}"
                )

        return "\n".join(lines).strip()
    finally:
        conn.close()


def format_daily_report(date: str | None = None) -> list[str]:
    """일간 종합 리포트 생성. 텔레그램 메시지 길이 제한 때문에 분할 반환."""
    conn = get_connection()
    try:
        requested_date = date is not None
        date = _resolve_report_date(conn, date)
        benchmark_lookup = _build_benchmark_lookup(conn, date)
        messages = []
        all_perf = get_latest_sector_performance(conn, date=date)
        by_country: dict[str, list[dict]] = defaultdict(list)
        for row in all_perf:
            by_country[row["country"]].append(row)

        trend_rows = _get_trend_rows(conn, date)

        quality_lines, is_low_quality = _build_data_quality_lines(
            conn,
            date,
            by_country,
            include_auto_warning=not requested_date,
        )

        header_lines = [
            "📊 글로벌 섹터 데일리 리포트",
            f"기준일 {date}",
            "",
        ]
        if quality_lines:
            header_lines.extend(quality_lines)
            header_lines.append("")

        header_lines.extend(
            _build_takeaway_lines(
                trend_rows,
                all_perf,
                is_low_quality=is_low_quality,
            )
        )

        flow_lines = _build_flow_section_lines(conn, date)
        if flow_lines:
            header_lines.extend(["", *flow_lines])

        watchlist_lines = _build_watchlist_lines(conn, date, benchmark_lookup)
        if watchlist_lines:
            header_lines.extend(["", "🎯 내 관심 종목"])
            for line in watchlist_lines:
                header_lines.append(f"• {line}")

        header_lines.extend(["", *_build_trend_section_lines(trend_rows)])

        watch_candidates, caution_candidates = _build_watch_sections(
            all_perf,
            benchmark_lookup,
            date,
        )
        if watch_candidates:
            header_lines.extend(["", "👀 관심 후보"])
            for candidate in watch_candidates:
                header_lines.append(f"• {candidate}")
        if caution_candidates:
            header_lines.extend(["", "⚠️ 제외/주의 신호"])
            for candidate in caution_candidates:
                header_lines.append(f"• {candidate}")

        messages.append("\n".join(header_lines).strip())

        for code in COUNTRY_ORDER:
            if code not in by_country:
                continue

            info = COUNTRIES.get(code, {})
            flag = info.get("flag", "")
            name = info.get("name_kr", code)
            entries = by_country[code]
            total_stocks = sum(entry.get("stock_count", 0) for entry in entries)

            msg_lines = [f"{flag} {name} · 분석 {total_stocks:,}종목" if total_stocks else f"{flag} {name}"]
            benchmark_summary = _format_country_benchmark_summary(
                benchmark_lookup, code, date
            )
            if benchmark_summary:
                msg_lines.append(benchmark_summary)

            sorted_entries = sorted(
                entries,
                key=lambda entry: entry.get("daily_return") or 0,
                reverse=True,
            )

            for entry in sorted_entries:
                if entry["sector"] == "기타":
                    continue
                msg_lines.extend(["", _format_sector_brief(entry, benchmark_lookup, date)])

            messages.append("\n".join(msg_lines).strip())

        abnormals = get_abnormal_stocks(conn, date=date)
        if abnormals:
            msg_lines = [f"⚠️ 비정상 급등/급락 {len(abnormals)}종목"]
            msg_lines.extend(_format_abnormal_stock_lines(abnormals))
            messages.append("\n".join(msg_lines).strip())

        return messages
    finally:
        conn.close()


def format_trending_report(date: str | None = None) -> str:
    """Concise global trend summary for the /trending command."""
    conn = get_connection()
    try:
        requested_date = date is not None
        date = _resolve_report_date(conn, date)
        all_perf = get_latest_sector_performance(conn, date=date)
        by_country: dict[str, list[dict]] = defaultdict(list)
        for row in all_perf:
            by_country[row["country"]].append(row)

        trend_rows = _get_trend_rows(conn, date)
        quality_lines, _ = _build_data_quality_lines(
            conn,
            date,
            by_country,
            include_auto_warning=not requested_date,
        )

        lines = [
            "🔥 글로벌 트렌딩 섹터",
            f"기준일 {date}",
            "",
        ]
        if quality_lines:
            lines.extend(quality_lines)
            lines.append("")
        lines.extend(_build_trend_section_lines(trend_rows))
        return "\n".join(lines).strip()
    finally:
        conn.close()


def format_watchlist_report(date: str | None = None) -> str:
    """Personal watchlist summary for the latest report date."""
    conn = get_connection()
    try:
        date = _resolve_report_date(conn, date)
        benchmark_lookup = _build_benchmark_lookup(conn, date)
        lines = _build_watchlist_lines(conn, date, benchmark_lookup)
        if not lines:
            return (
                "🎯 내 관심 종목\n"
                "설정된 watchlist가 없습니다.\n"
                "MARKETBOT_WATCHLIST 환경변수 또는 data/watchlist.json에 "
                "종목을 추가하세요."
            )

        msg_lines = ["🎯 내 관심 종목", f"기준일 {date}"]
        for line in lines:
            msg_lines.append(f"• {line}")
        return "\n".join(msg_lines)
    finally:
        conn.close()


def format_abnormal_report(date: str | None = None) -> str:
    """Concise abnormal mover summary for the /abnormal command."""
    conn = get_connection()
    try:
        date = _resolve_report_date(conn, date)
        abnormals = get_abnormal_stocks(conn, date=date)
        if not abnormals:
            return f"✅ 비정상 급등/급락 종목 없음\n기준일 {date}"

        lines = [
            f"⚠️ 비정상 급등/급락 {len(abnormals)}종목",
            f"기준일 {date}",
        ]
        lines.extend(_format_abnormal_stock_lines(abnormals))
        return "\n".join(lines).strip()
    finally:
        conn.close()


def format_sector_detail(sector_name: str, date: str | None = None) -> str:
    """특정 섹터의 국가별 상세 리포트."""
    conn = get_connection()
    try:
        date = _resolve_report_date(conn, date)
        benchmark_lookup = _build_benchmark_lookup(conn, date)

        rows = conn.execute(
            """
            SELECT * FROM sector_performance
            WHERE date = ? AND sector = ?
            ORDER BY daily_return DESC
            """,
            (date, sector_name),
        ).fetchall()

        if not rows:
            return f"❌ '{sector_name}' 섹터 데이터를 찾을 수 없습니다."

        msg_lines = [
            f"🔍 {sector_name} 섹터 상세",
            f"기준일 {date}",
        ]

        for row in rows:
            info = COUNTRIES.get(row["country"], {})
            flag = info.get("flag", "")
            name = info.get("name_kr", row["country"])
            breadth = (row["breadth"] or 0) * 100

            msg_lines.extend(
                [
                    "",
                    f"• {flag} {name} {_format_signed_pct(row['daily_return'])}",
                ]
            )

            detail_parts = []
            comparison = _format_benchmark_comparison(
                row,
                _get_benchmark_row(benchmark_lookup, row["country"], sector_name),
                date,
            )
            if comparison:
                detail_parts.append(comparison)
            if breadth > 0:
                detail_parts.append(f"상승 {breadth:.0f}%")
            detail_parts.append(f"{row['stock_count']}종목")
            msg_lines.append("  " + " · ".join(detail_parts))

            gainers = _parse_top_gainers(row["top_gainers"])[:3]
            if gainers:
                gainers_text = ", ".join(
                    f"{gainer['name']} {_format_signed_pct(gainer['return'], 1)}"
                    for gainer in gainers
                )
                msg_lines.append(f"  강세 {gainers_text}")

        return "\n".join(msg_lines)
    finally:
        conn.close()


def format_country_detail(country_code: str, date: str | None = None) -> str:
    """특정 국가의 섹터별 상세 리포트."""
    conn = get_connection()
    try:
        date = _resolve_report_date(conn, date)
        benchmark_lookup = _build_benchmark_lookup(conn, date)

        rows = get_latest_sector_performance(conn, date=date, country=country_code)
        if not rows:
            return f"❌ '{country_code}' 데이터를 찾을 수 없습니다."

        info = COUNTRIES.get(country_code, {})
        flag = info.get("flag", "")
        name = info.get("name_kr", country_code)

        msg_lines = [
            f"{flag} {name} 섹터 상세",
            f"기준일 {date}",
        ]

        benchmark_summary = _format_country_benchmark_summary(
            benchmark_lookup, country_code, date
        )
        if benchmark_summary:
            msg_lines.append(benchmark_summary)

        for row in rows:
            if row["sector"] == "기타":
                continue

            msg_lines.extend(["", _format_sector_brief(row, benchmark_lookup, date)])

        return "\n".join(msg_lines)
    finally:
        conn.close()
