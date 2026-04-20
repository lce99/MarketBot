"""Operational status and admin alerts for MarketBot."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from telegram import Bot

from src.config import (
    COUNTRIES,
    STATUS_STALE_AFTER_DAYS,
    TELEGRAM_ALERT_CHAT_ID,
    TELEGRAM_BOT_TOKEN,
)
from src.database import (
    get_connection,
    get_latest_collection_log,
    get_latest_sector_dates_by_country,
    get_recent_collection_logs,
)

logger = logging.getLogger(__name__)


def _parse_date(value: str | None) -> datetime.date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def get_operational_status(
    as_of_date: str | None = None,
    stale_after_days: int = STATUS_STALE_AFTER_DAYS,
) -> dict:
    """Build an operational health snapshot from collection logs and DB dates."""
    reference_date = _parse_date(as_of_date) or datetime.utcnow().date()

    conn = get_connection()
    try:
        latest_dates = get_latest_sector_dates_by_country(conn)
        recent_failures = get_recent_collection_logs(conn, limit=5, status="failed")

        markets = []
        for code, info in COUNTRIES.items():
            latest_event = get_latest_collection_log(conn, code)
            latest_success = get_latest_collection_log(conn, code, status="success")
            latest_failure = get_latest_collection_log(conn, code, status="failed")
            latest_data_date = latest_dates.get(code)

            success_dt = _parse_timestamp(latest_success["timestamp"]) if latest_success else None
            failure_dt = _parse_timestamp(latest_failure["timestamp"]) if latest_failure else None
            latest_data_dt = _parse_date(latest_data_date)
            freshness_date = latest_data_dt or (success_dt.date() if success_dt else None)

            latest_status = latest_event["status"] if latest_event else None
            is_failing = latest_status == "failed" and (
                not success_dt or (
                    failure_dt is not None
                    and failure_dt >= success_dt
                )
            )

            age_days = None
            if freshness_date:
                age_days = (reference_date - freshness_date).days

            if is_failing:
                state = "ERROR"
            elif freshness_date is None:
                state = "NO_DATA"
            elif age_days is not None and age_days > stale_after_days:
                state = "STALE"
            else:
                state = "OK"

            markets.append({
                "code": code,
                "name_kr": info["name_kr"],
                "flag": info["flag"],
                "state": state,
                "latest_status": latest_status,
                "last_success_at": latest_success["timestamp"] if latest_success else None,
                "last_failure_at": latest_failure["timestamp"] if latest_failure else None,
                "last_failure_error": latest_failure["error_message"] if latest_failure else None,
                "latest_data_date": latest_data_date,
                "age_days": age_days,
            })

        counts = {
            "OK": sum(1 for market in markets if market["state"] == "OK"),
            "ERROR": sum(1 for market in markets if market["state"] == "ERROR"),
            "STALE": sum(1 for market in markets if market["state"] == "STALE"),
            "NO_DATA": sum(1 for market in markets if market["state"] == "NO_DATA"),
        }
    finally:
        conn.close()

    return {
        "as_of_date": reference_date.isoformat(),
        "stale_after_days": stale_after_days,
        "counts": counts,
        "markets": markets,
        "recent_failures": recent_failures,
    }


def format_status_report(
    as_of_date: str | None = None,
    markets: list[str] | None = None,
) -> str:
    """Render a concise operational status report for Telegram or console."""
    snapshot = get_operational_status(as_of_date=as_of_date)
    market_filter = {market.upper() for market in markets} if markets else None

    status_rows = [
        market
        for market in snapshot["markets"]
        if market_filter is None or market["code"] in market_filter
    ]

    lines = [
        f"\U0001f6e0 운영 상태 ({snapshot['as_of_date']} UTC)",
        "\u2501" * 20,
        (
            f"정상 {snapshot['counts']['OK']} | "
            f"실패 {snapshot['counts']['ERROR']} | "
            f"stale {snapshot['counts']['STALE']} | "
            f"미수집 {snapshot['counts']['NO_DATA']}"
        ),
        "",
    ]

    problem_markets = [
        market for market in status_rows if market["state"] in {"ERROR", "STALE", "NO_DATA"}
    ]
    if problem_markets:
        lines.append("\u26a0\ufe0f 주의 시장")
        for market in problem_markets:
            detail_parts = []
            if market["latest_data_date"]:
                detail_parts.append(f"마지막 데이터 {market['latest_data_date']}")
            if market["last_success_at"]:
                detail_parts.append(f"마지막 성공 {market['last_success_at'][:10]}")
            if market["age_days"] is not None:
                detail_parts.append(f"{market['age_days']}일 경과")
            if market["last_failure_at"] and market["state"] == "ERROR":
                detail_parts.append(f"최근 실패 {market['last_failure_at'][:10]}")
            detail = " | ".join(detail_parts) if detail_parts else "기록 없음"
            lines.append(
                f"  - {market['flag']} {market['name_kr']} ({market['code']}) "
                f"{market['state']} | {detail}"
            )
        lines.append("")

    if snapshot["recent_failures"] and market_filter is None:
        lines.append("\U0001f4db 최근 실패 로그")
        for row in snapshot["recent_failures"][:3]:
            error = row["error_message"] or "원인 미상"
            lines.append(
                f"  - {row['market']} | {row['timestamp'][:16]} UTC | {error[:80]}"
            )
        lines.append("")

    lines.append("\U0001f4ca 시장별 상태")
    state_icon = {
        "OK": "\u2705",
        "ERROR": "\u274c",
        "STALE": "\u26a0\ufe0f",
        "NO_DATA": "\u2753",
    }
    for market in status_rows:
        latest_data = market["latest_data_date"] or "-"
        last_success = market["last_success_at"][:10] if market["last_success_at"] else "-"
        lines.append(
            f"  {state_icon[market['state']]} {market['flag']} {market['code']}"
            f" | 데이터 {latest_data} | 성공 {last_success}"
        )

    return "\n".join(lines)


def format_failure_alert(
    failed_markets: list[str],
    as_of_date: str | None = None,
) -> str:
    """Render a failure alert for operators."""
    lines = [
        f"\u26a0\ufe0f MarketBot 수집 실패 ({as_of_date or datetime.utcnow().strftime('%Y-%m-%d')})",
        f"실패 시장: {', '.join(failed_markets)}",
        "",
        format_status_report(as_of_date=as_of_date, markets=failed_markets),
    ]
    return "\n".join(lines)


async def _send_message(token: str, chat_id: str, text: str) -> None:
    bot = Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=text)


def send_admin_alert(text: str) -> bool:
    """Send an operator alert to Telegram if admin chat is configured."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ALERT_CHAT_ID:
        logger.info("관리자 알림 스킵: TELEGRAM_ALERT_CHAT_ID 또는 BOT_TOKEN 미설정")
        return False

    try:
        asyncio.run(_send_message(TELEGRAM_BOT_TOKEN, TELEGRAM_ALERT_CHAT_ID, text))
        logger.info("관리자 알림 전송 완료")
        return True
    except Exception as e:
        logger.error(f"관리자 알림 전송 실패: {e}", exc_info=True)
        return False
