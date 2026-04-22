"""텔레그램 리포트 포맷터."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime

from src.config import COUNTRIES
from src.database import (
    get_abnormal_stocks,
    get_connection,
    get_latest_benchmarks,
    get_latest_sector_performance,
)

logger = logging.getLogger(__name__)

COUNTRY_INDEX_LABELS = {
    "KR": "KOSPI",
    "CN": "CSI300",
    "JP": "Nikkei 225",
    "VN": "VN-Index",
    "IN": "NIFTY 50",
    "DE": "DAX",
}


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


def format_daily_report(date: str | None = None) -> list[str]:
    """일간 종합 리포트 생성. 텔레그램 메시지 길이 제한 때문에 분할 반환."""
    conn = get_connection()
    try:
        date = _resolve_report_date(conn, date)
        benchmark_lookup = _build_benchmark_lookup(conn, date)
        messages = []

        trend_rows = conn.execute(
            """
            SELECT sector, trend_score, countries_positive, countries_negative,
                   global_avg_return, momentum_signal
            FROM trend_scores
            WHERE date = ?
            ORDER BY trend_score DESC
            """,
            (date,),
        ).fetchall()

        header_lines = [
            "📊 글로벌 섹터 데일리 리포트",
            f"기준일 {date}",
            "",
        ]

        if trend_rows:
            header_lines.append("🔥 강한 흐름")
            for i, trend in enumerate(trend_rows[:5], start=1):
                total = trend["countries_positive"] + trend["countries_negative"]
                avg_return = trend["global_avg_return"]
                header_lines.append(
                    f"{i}. {trend['sector']} {_format_signed_number(trend['trend_score'])}"
                )
                header_lines.append(
                    f"   평균 {_format_signed_pct(avg_return)}"
                    f" · 상승 {trend['countries_positive']}/{total}개국"
                )

            weak_rows = [row for row in reversed(trend_rows[-3:]) if row["trend_score"] < 0]
            if weak_rows:
                header_lines.extend(["", "🧊 약한 흐름"])
                for trend in weak_rows:
                    total = trend["countries_positive"] + trend["countries_negative"]
                    avg_return = trend["global_avg_return"]
                    header_lines.append(
                        f"• {trend['sector']} {_format_signed_number(trend['trend_score'])}"
                    )
                    header_lines.append(
                        f"  평균 {_format_signed_pct(avg_return)}"
                        f" · 하락 {trend['countries_negative']}/{total}개국"
                    )
        else:
            header_lines.append("트렌드 스코어 데이터가 없습니다.")

        messages.append("\n".join(header_lines).strip())

        all_perf = get_latest_sector_performance(conn, date=date)
        by_country: dict[str, list[dict]] = defaultdict(list)
        for row in all_perf:
            by_country[row["country"]].append(row)

        country_order = ["US", "KR", "CN", "JP", "VN", "IN", "DE"]
        for code in country_order:
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
            for abnormal in abnormals[:10]:
                info = COUNTRIES.get(abnormal["country"], {})
                flag = info.get("flag", "")
                msg_lines.extend(
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
                    msg_lines.append("  " + " · ".join(detail_parts))

            messages.append("\n".join(msg_lines).strip())

        return messages
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
