"""텔레그램 리포트 포맷팅 (한국어)"""

import json
import logging
from datetime import datetime

from src.config import COUNTRIES, SECTORS
from src.database import get_abnormal_stocks, get_connection, get_latest_sector_performance

logger = logging.getLogger(__name__)


def format_daily_report(date: str | None = None) -> list[str]:
    """일간 종합 리포트 생성. 텔레그램 메시지 길이 제한(4096자) 때문에 여러 메시지로 분할."""
    conn = get_connection()

    if date is None:
        # DB에서 가장 최근 날짜
        row = conn.execute(
            "SELECT MAX(date) FROM sector_performance"
        ).fetchone()
        date = row[0] if row and row[0] else datetime.utcnow().strftime("%Y-%m-%d")

    messages = []

    # ── 글로벌 트렌드 섹터 ──
    trend_rows = conn.execute("""
        SELECT sector, trend_score, countries_positive, countries_negative,
               global_avg_return, momentum_signal
        FROM trend_scores
        WHERE date = ?
        ORDER BY trend_score DESC
    """, (date,)).fetchall()

    header = f"\U0001f4ca 글로벌 섹터 데일리 리포트 ({date})\n"
    header += "\u2501" * 20 + "\n\n"

    if trend_rows:
        header += "\U0001f525 글로벌 트렌딩 섹터 TOP 5\n"
        for i, t in enumerate(trend_rows[:5]):
            arrow = "\u25b2" if t["trend_score"] > 0 else "\u25bc"
            total = t["countries_positive"] + t["countries_negative"]
            header += (
                f"  {i+1}. {t['sector']} {arrow} | "
                f"스코어 {t['trend_score']:+.0f} | "
                f"{t['countries_positive']}/{total}개국 상승\n"
            )
        header += "\n"

        header += "\u2744\ufe0f 글로벌 약세 섹터\n"
        for t in trend_rows[-3:]:
            if t["trend_score"] < 0:
                total = t["countries_positive"] + t["countries_negative"]
                header += (
                    f"  \u25bc {t['sector']} | "
                    f"스코어 {t['trend_score']:+.0f} | "
                    f"{t['countries_negative']}/{total}개국 하락\n"
                )
    else:
        header += "(트렌드 스코어 데이터 없음)\n"

    messages.append(header)

    # ── 국가별 섹터 성과 ──
    all_perf = get_latest_sector_performance(conn, date=date)

    # 국가별 그룹
    by_country: dict[str, list[dict]] = {}
    for row in all_perf:
        c = row["country"]
        if c not in by_country:
            by_country[c] = []
        by_country[c].append(row)

    # 국가 순서: US → KR → CN → JP → VN → IN → DE
    country_order = ["US", "KR", "CN", "JP", "VN", "IN", "DE"]
    for code in country_order:
        if code not in by_country:
            continue

        info = COUNTRIES.get(code, {})
        flag = info.get("flag", "")
        name = info.get("name_kr", code)
        entries = by_country[code]

        # 분석 대상 종목 수
        total_stocks = sum(e.get("stock_count", 0) for e in entries)

        msg = f"\n{flag} {name}"
        if total_stocks:
            msg += f" (분석 {total_stocks:,}종목)"
        msg += "\n"

        # 상승 순으로 정렬
        sorted_entries = sorted(
            entries, key=lambda x: x.get("daily_return") or 0, reverse=True
        )

        for e in sorted_entries:
            if e["sector"] == "기타":
                continue

            ret = e.get("daily_return") or 0
            arrow = "\u25b2" if ret > 0 else ("\u25bc" if ret < 0 else "\u25a0")
            breadth_pct = (e.get("breadth") or 0) * 100

            line = f"  {arrow} {e['sector']:6s} {ret:+.2f}%"
            if breadth_pct > 0:
                line += f" | 상승 {breadth_pct:.0f}%"

            # 탑 종목 표시 (있으면)
            if e.get("top_gainers"):
                try:
                    gainers = json.loads(e["top_gainers"]) if isinstance(e["top_gainers"], str) else e["top_gainers"]
                    if gainers and len(gainers) > 0:
                        top = gainers[0]
                        line += f" | {top['name']} {top['return']:+.1f}%"
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

            msg += line + "\n"

        messages.append(msg)

    # ── 비정상 급등/급락 ──
    abnormals = get_abnormal_stocks(conn, date=date)
    if abnormals:
        msg = f"\n\u26a0\ufe0f 비정상 급등/급락 ({len(abnormals)}종목)\n"
        for a in abnormals[:10]:  # 최대 10개
            info = COUNTRIES.get(a["country"], {})
            flag = info.get("flag", "")
            cap_str = ""
            if a.get("market_cap") and a["market_cap"] > 0:
                if a["country"] == "KR":
                    cap_str = f" (시총 {a['market_cap']/1e8:,.0f}억)"
                else:
                    cap_str = f" (시총 {a['market_cap']/1e6:,.0f}M)"
            msg += (
                f"  {flag} {a['name']} {a['daily_return']:+.1f}%{cap_str}\n"
            )
        messages.append(msg)

    conn.close()
    return messages


def format_sector_detail(sector_name: str, date: str | None = None) -> str:
    """특정 섹터의 국가별 상세 리포트."""
    conn = get_connection()

    if date is None:
        row = conn.execute("SELECT MAX(date) FROM sector_performance").fetchone()
        date = row[0] if row and row[0] else datetime.utcnow().strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT * FROM sector_performance
        WHERE date = ? AND sector = ?
        ORDER BY daily_return DESC
    """, (date, sector_name)).fetchall()

    if not rows:
        conn.close()
        return f"\u274c '{sector_name}' 섹터 데이터를 찾을 수 없습니다."

    msg = f"\U0001f50d {sector_name} 섹터 상세 ({date})\n"
    msg += "\u2501" * 20 + "\n\n"

    for r in rows:
        info = COUNTRIES.get(r["country"], {})
        flag = info.get("flag", "")
        name = info.get("name_kr", r["country"])
        ret = r["daily_return"] or 0
        arrow = "\u25b2" if ret > 0 else "\u25bc"
        breadth = (r["breadth"] or 0) * 100

        msg += f"{flag} {name}: {arrow} {ret:+.2f}% | 상승 {breadth:.0f}% | {r['stock_count']}종목\n"

        # 상위 상승 종목
        if r["top_gainers"]:
            try:
                gainers = json.loads(r["top_gainers"]) if isinstance(r["top_gainers"], str) else r["top_gainers"]
                for g in gainers[:3]:
                    msg += f"    \u2191 {g['name']} {g['return']:+.1f}%\n"
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

    conn.close()
    return msg


def format_country_detail(country_code: str, date: str | None = None) -> str:
    """특정 국가의 섹터별 상세 리포트."""
    conn = get_connection()

    if date is None:
        row = conn.execute("SELECT MAX(date) FROM sector_performance").fetchone()
        date = row[0] if row and row[0] else datetime.utcnow().strftime("%Y-%m-%d")

    rows = get_latest_sector_performance(conn, date=date, country=country_code)
    if not rows:
        conn.close()
        return f"\u274c '{country_code}' 데이터를 찾을 수 없습니다."

    info = COUNTRIES.get(country_code, {})
    flag = info.get("flag", "")
    name = info.get("name_kr", country_code)

    msg = f"{flag} {name} 섹터 상세 ({date})\n"
    msg += "\u2501" * 20 + "\n\n"

    for r in rows:
        if r["sector"] == "기타":
            continue
        ret = r["daily_return"] or 0
        arrow = "\u25b2" if ret > 0 else "\u25bc"
        breadth = (r["breadth"] or 0) * 100

        msg += f"{arrow} {r['sector']:8s} {ret:+.2f}% | 상승 {breadth:.0f}% | {r['stock_count']}종목\n"

    conn.close()
    return msg
