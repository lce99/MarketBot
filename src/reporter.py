"""텔레그램 리포트 포맷터."""

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


def _format_benchmark_return(row: dict, report_date: str) -> str:
    daily_return = row.get("daily_return")
    if daily_return is None:
        return ""

    text = f"{daily_return:+.2f}%"
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

    line = (
        f"  기준선 {_benchmark_label(benchmark)} "
        f"{_format_benchmark_return(benchmark, report_date)}"
    )

    weekly_return = benchmark.get("weekly_return")
    if weekly_return is not None:
        line += f" | 주간 {weekly_return:+.2f}%"

    return line + "\n"


def _format_benchmark_comparison(
    row: dict,
    benchmark: dict | None,
    report_date: str,
    compact: bool = False,
) -> str:
    if not benchmark:
        return ""

    sector_return = row["daily_return"]
    benchmark_return = benchmark.get("daily_return")
    if sector_return is None or benchmark_return is None:
        return ""

    label = _benchmark_label(benchmark)
    alpha = sector_return - benchmark_return
    if compact:
        return f" | {label} 대비 {alpha:+.2f}%"

    return (
        f" | 벤치 {label} {_format_benchmark_return(benchmark, report_date)}"
        f" | 초과 {alpha:+.2f}%"
    )


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

        header = f"📊 글로벌 섹터 데일리 리포트 ({date})\n"
        header += "━" * 20 + "\n\n"

        if trend_rows:
            header += "🔥 글로벌 트렌드 섹터 TOP 5\n"
            for i, trend in enumerate(trend_rows[:5], start=1):
                arrow = "▲" if trend["trend_score"] > 0 else "▼"
                total = trend["countries_positive"] + trend["countries_negative"]
                header += (
                    f"  {i}. {trend['sector']} {arrow} | "
                    f"스코어 {trend['trend_score']:+.0f} | "
                    f"{trend['countries_positive']}/{total}개국 상승\n"
                )
            header += "\n"

            header += "❄️ 글로벌 약세 섹터\n"
            for trend in trend_rows[-3:]:
                if trend["trend_score"] < 0:
                    total = trend["countries_positive"] + trend["countries_negative"]
                    header += (
                        f"  ▼ {trend['sector']} | "
                        f"스코어 {trend['trend_score']:+.0f} | "
                        f"{trend['countries_negative']}/{total}개국 하락\n"
                    )
        else:
            header += "(트렌드 스코어 데이터 없음)\n"

        messages.append(header)

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

            msg = f"\n{flag} {name}"
            if total_stocks:
                msg += f" (분석 {total_stocks:,}종목)"
            msg += "\n"

            country_summary = _format_country_benchmark_summary(
                benchmark_lookup, code, date
            )
            if country_summary:
                msg += country_summary

            sorted_entries = sorted(
                entries, key=lambda entry: entry.get("daily_return") or 0, reverse=True
            )

            for entry in sorted_entries:
                if entry["sector"] == "기타":
                    continue

                ret = entry.get("daily_return") or 0
                arrow = "▲" if ret > 0 else ("▼" if ret < 0 else "■")
                breadth_pct = (entry.get("breadth") or 0) * 100

                line = f"  {arrow} {entry['sector']:6s} {ret:+.2f}%"

                benchmark = _get_benchmark_row(benchmark_lookup, code, entry["sector"])
                use_compact = bool(
                    country_summary
                    and benchmark
                    and benchmark.get("sector") is None
                    and benchmark.get("daily_return") is not None
                )
                line += _format_benchmark_comparison(
                    entry, benchmark, date, compact=use_compact
                )

                if breadth_pct > 0:
                    line += f" | 상승 {breadth_pct:.0f}%"

                gainers = _parse_top_gainers(entry.get("top_gainers"))
                if gainers:
                    top = gainers[0]
                    line += f" | {top['name']} {top['return']:+.1f}%"

                msg += line + "\n"

            messages.append(msg)

        abnormals = get_abnormal_stocks(conn, date=date)
        if abnormals:
            msg = f"\n⚠️ 비정상 급등/급락 ({len(abnormals)}종목)\n"
            for abnormal in abnormals[:10]:
                info = COUNTRIES.get(abnormal["country"], {})
                flag = info.get("flag", "")
                cap_str = ""
                if abnormal.get("market_cap") and abnormal["market_cap"] > 0:
                    if abnormal["country"] == "KR":
                        cap_str = f" (시총 {abnormal['market_cap']/1e8:,.0f}억)"
                    else:
                        cap_str = f" (시총 {abnormal['market_cap']/1e6:,.0f}M)"
                msg += (
                    f"  {flag} {abnormal['name']} "
                    f"{abnormal['daily_return']:+.1f}%{cap_str}\n"
                )
            messages.append(msg)

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

        msg = f"🔍 {sector_name} 섹터 상세 ({date})\n"
        msg += "━" * 20 + "\n\n"

        for row in rows:
            info = COUNTRIES.get(row["country"], {})
            flag = info.get("flag", "")
            name = info.get("name_kr", row["country"])
            ret = row["daily_return"] or 0
            arrow = "▲" if ret > 0 else ("▼" if ret < 0 else "■")
            breadth = (row["breadth"] or 0) * 100

            line = f"{flag} {name}: {arrow} {ret:+.2f}%"
            line += _format_benchmark_comparison(
                row,
                _get_benchmark_row(benchmark_lookup, row["country"], sector_name),
                date,
            )
            line += f" | 상승 {breadth:.0f}% | {row['stock_count']}종목\n"
            msg += line

            for gainer in _parse_top_gainers(row["top_gainers"])[:3]:
                msg += f"    ↑ {gainer['name']} {gainer['return']:+.1f}%\n"

        return msg
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

        msg = f"{flag} {name} 섹터 상세 ({date})\n"
        msg += "━" * 20 + "\n\n"

        country_summary = _format_country_benchmark_summary(
            benchmark_lookup, country_code, date
        )
        if country_summary:
            msg += country_summary + "\n"

        for row in rows:
            if row["sector"] == "기타":
                continue

            ret = row["daily_return"] or 0
            arrow = "▲" if ret > 0 else ("▼" if ret < 0 else "■")
            breadth = (row["breadth"] or 0) * 100

            benchmark = _get_benchmark_row(benchmark_lookup, country_code, row["sector"])
            use_compact = bool(
                country_summary
                and benchmark
                and benchmark.get("sector") is None
                and benchmark.get("daily_return") is not None
            )

            line = f"{arrow} {row['sector']:8s} {ret:+.2f}%"
            line += _format_benchmark_comparison(
                row, benchmark, date, compact=use_compact
            )
            line += f" | 상승 {breadth:.0f}% | {row['stock_count']}종목\n"
            msg += line

        return msg
    finally:
        conn.close()
