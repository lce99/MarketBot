"""Personal watchlist loading for report personalization."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from src.config import DATA_DIR


@dataclass(frozen=True)
class WatchItem:
    country: str
    ticker: str
    name: str | None = None
    sector: str | None = None
    note: str | None = None


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _infer_country(ticker: str) -> str:
    upper = ticker.upper()
    if upper.endswith((".KS", ".KQ")):
        return "KR"
    if upper.endswith(".T"):
        return "JP"
    if upper.endswith(".NS"):
        return "IN"
    if upper.endswith(".DE"):
        return "DE"
    return "US"


def _normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()


def _watch_item_from_dict(raw: dict) -> WatchItem | None:
    ticker = _clean(raw.get("ticker") or raw.get("symbol"))
    if not ticker:
        return None

    country = _clean(raw.get("country") or raw.get("market"))
    ticker = _normalize_ticker(ticker)
    return WatchItem(
        country=(country or _infer_country(ticker)).upper(),
        ticker=ticker,
        name=_clean(raw.get("name")),
        sector=_clean(raw.get("sector")),
        note=_clean(raw.get("note")),
    )


def _watch_item_from_text(raw: str) -> WatchItem | None:
    text = raw.strip()
    if not text:
        return None

    parts = [part.strip() for part in text.split(":")]
    if len(parts) >= 2:
        country, ticker = parts[0], parts[1]
        name = parts[2] if len(parts) >= 3 else None
        sector = parts[3] if len(parts) >= 4 else None
        return WatchItem(
            country=country.upper(),
            ticker=_normalize_ticker(ticker),
            name=_clean(name),
            sector=_clean(sector),
        )

    ticker = _normalize_ticker(text)
    return WatchItem(country=_infer_country(ticker), ticker=ticker)


def _load_items_from_payload(payload: object) -> list[WatchItem]:
    if isinstance(payload, dict):
        payload = payload.get("items") or payload.get("watchlist") or []

    items: list[WatchItem] = []
    if isinstance(payload, list):
        for raw_item in payload:
            item = None
            if isinstance(raw_item, dict):
                item = _watch_item_from_dict(raw_item)
            elif isinstance(raw_item, str):
                item = _watch_item_from_text(raw_item)
            if item:
                items.append(item)
    return _dedupe_items(items)


def _dedupe_items(items: list[WatchItem]) -> list[WatchItem]:
    seen = set()
    result = []
    for item in items:
        key = (item.country, item.ticker)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def load_watchlist() -> list[WatchItem]:
    """Load a personal watchlist from env JSON, env CSV, or data/watchlist.json."""
    raw_env = os.getenv("MARKETBOT_WATCHLIST", "").strip()
    if raw_env:
        try:
            return _load_items_from_payload(json.loads(raw_env))
        except json.JSONDecodeError:
            return _dedupe_items(
                [
                    item
                    for item in (_watch_item_from_text(part) for part in raw_env.split(","))
                    if item
                ]
            )

    path_value = os.getenv("MARKETBOT_WATCHLIST_PATH")
    path = Path(path_value) if path_value else DATA_DIR / "watchlist.json"
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8") as fp:
        return _load_items_from_payload(json.load(fp))
