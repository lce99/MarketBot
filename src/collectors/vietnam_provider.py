"""Small HTTP client for the public KBS/VCI listing and daily price APIs.

No vnstock code is imported or installed. Prices returned to the collector use
thousands of VND, matching its existing contract; volume remains shares.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Callable

import pandas as pd
import requests

from src.collection_failures import CollectionFailure

KBS_URL = "https://kbbuddywts.kbsec.com.vn/iis-server/investment"
VCI_LISTING_URL = "https://iq.vietcap.com.vn/api/iq-insight-service/v2/company/search-bar"
VCI_HISTORY_URL = "https://trading.vietcap.com.vn/api/chart/OHLCChart/gap-chart"
BAR_COLUMNS = {"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}


def _failure(source: str, stage: str, message: str, code: str = "provider_error") -> CollectionFailure:
    return CollectionFailure(message, code, stage, provider=f"vietnam-http:{source}")


def _request(source: str, stage: str, url: str, *, before_request: Callable,
             method: str = "GET", **kwargs):
    before_request()
    try:
        response = requests.request(method, url, timeout=(10, 30),
                                    headers={"User-Agent": "MarketBot/1.0", "Accept": "application/json"},
                                    **kwargs)
        if response.status_code == 429:
            raise _failure(source, stage, f"{source} HTTP 429 rate limit", "provider_rate_limited")
        response.raise_for_status()
        return response.json()
    except CollectionFailure:
        raise
    except (requests.RequestException, ValueError) as exc:
        # Response bodies are unnecessary diagnostics and may contain sensitive data.
        status = getattr(getattr(exc, "response", None), "status_code", None)
        detail = f"HTTP {status}" if status else type(exc).__name__
        raise _failure(source, stage, f"{source} request failed: {detail}") from exc


def _records(data, source: str, stage: str) -> list[dict]:
    if isinstance(data, dict):
        data = data.get("data")
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise _failure(source, stage, f"{source} response schema changed")
    return data


def fetch_listing(source: str, *, before_request: Callable) -> pd.DataFrame:
    """Keep all listed equities, including HOSE, HNX and UPCOM, with industries."""
    stage = "load_listing"
    if source == "VCI":
        companies = _records(_request(source, stage, VCI_LISTING_URL,
                                      before_request=before_request, params={"language": 2}), source, stage)
        rows = []
        for company in companies:
            if not company.get("code") or not company.get("name"):
                raise _failure(source, stage, "VCI listing missing symbol/name")
            # Prefer detailed ICB names but retain broad classifications for mapping.
            industries = [company.get(f"icbLv{level}") or {} for level in (3, 2, 1, 4)]
            rows.append({"ticker": company["code"], "name": company["name"],
                         "industry": next((i.get("name") for i in industries if i.get("name")), ""),
                         "icb_name2": (company.get("icbLv2") or {}).get("name", ""),
                         "icb_name1": (company.get("icbLv1") or {}).get("name", "")})
    elif source == "KBS":
        companies = _records(_request(source, stage, KBS_URL + "/stock/search/data",
                                      before_request=before_request), source, stage)
        sectors = _records(_request(source, stage, KBS_URL + "/sector/all",
                                    before_request=before_request), source, stage)
        industries = {}
        for sector in sectors:
            if not sector.get("name") or "code" not in sector:
                raise _failure(source, stage, "KBS sector missing name/code")
            data = _request(source, stage, KBS_URL + "/sector/stock", before_request=before_request,
                            params={"code": sector["code"], "l": 1})
            stocks = data.get("stocks") if isinstance(data, dict) else None
            if not isinstance(stocks, list) or any(not isinstance(s, dict) or not s.get("sb") for s in stocks):
                raise _failure(source, stage, "KBS sector membership schema changed")
            for stock in stocks:
                industries[stock["sb"]] = sector["name"]
        rows = []
        for company in companies:
            if "type" not in company:
                raise _failure(source, stage, "KBS listing missing security type")
            if company["type"] != "stock":
                continue
            if not company.get("symbol") or not company.get("name"):
                raise _failure(source, stage, "KBS listing missing symbol/name")
            rows.append({"ticker": company["symbol"], "name": company["name"],
                         "exchange": company.get("exchange"),
                         "industry": industries.get(company["symbol"], "")})
    else:
        raise ValueError(f"Unsupported Vietnam source: {source}")
    if not rows or not any(row.get("industry") for row in rows):
        raise _failure(source, stage, f"{source} listing has no equities/industry metadata")
    return pd.DataFrame(rows).drop_duplicates("ticker").reset_index(drop=True)


def fetch_history(source: str, ticker: str, start: str, end: str, *, before_request: Callable) -> pd.DataFrame:
    """Normalize daily bars to ascending dates, thousands of VND and shares."""
    stage = "fetch_history"
    start_day, end_day = datetime.fromisoformat(start), datetime.fromisoformat(end)
    if start_day > end_day:
        raise ValueError("History start must precede end")
    if source == "KBS":
        data = _request(source, stage, f"{KBS_URL}/stocks/{ticker}/data_day", before_request=before_request,
                        params={"sdate": start_day.strftime("%d-%m-%Y"), "edate": end_day.strftime("%d-%m-%Y")})
        if not isinstance(data, dict) or "data_day" not in data or data.get("symbol") != ticker:
            raise _failure(source, stage, "KBS daily history schema/symbol changed")
        rows = _records(data["data_day"], source, stage)
    elif source == "VCI":
        # UTC avoids dependence on the machine's local timezone. Include end day.
        until = (end_day + timedelta(days=1)).replace(tzinfo=timezone.utc)
        data = _records(_request(source, stage, VCI_HISTORY_URL, before_request=before_request, method="POST",
                                json={"timeFrame": "ONE_DAY", "symbols": [ticker], "to": int(until.timestamp()),
                                      "countBack": (end_day - start_day).days + 2}), source, stage)
        if not data:
            rows = []
        elif len(data) == 1 and isinstance(data[0].get("t"), list):
            bar = data[0]
            if bar.get("symbol") not in (None, ticker):
                raise _failure(source, stage, "VCI history symbol mismatch")
            if any(not isinstance(bar.get(k), list) or len(bar[k]) != len(bar["t"]) for k in BAR_COLUMNS):
                raise _failure(source, stage, "VCI daily bar arrays have different lengths")
            rows = [{k: bar[k][i] for k in BAR_COLUMNS} for i in range(len(bar["t"]))]
        else:
            rows = data
    else:
        raise ValueError(f"Unsupported Vietnam source: {source}")
    if not rows:
        return pd.DataFrame(columns=list(BAR_COLUMNS.values()))
    frame = pd.DataFrame(rows)
    if any(k not in frame for k in BAR_COLUMNS):
        raise _failure(source, stage, f"{source} daily bars missing required columns")
    frame = frame[list(BAR_COLUMNS)].rename(columns=BAR_COLUMNS)
    # KBS sends ISO timestamps; VCI sends Unix seconds (occasionally numeric strings).
    numeric_time = pd.to_numeric(frame["time"], errors="coerce")
    if numeric_time.notna().all():
        frame["time"] = pd.to_datetime(numeric_time, unit="s", utc=True, errors="coerce").dt.tz_localize(None)
    else:
        frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame.isna().any().any() or not frame[["open", "high", "low", "close", "volume"]].apply(
        lambda column: column.map(isfinite)
    ).all().all() or (frame["close"] <= 0).any() or (frame["volume"] < 0).any():
        raise _failure(source, stage, f"{source} daily bars contain invalid values")
    frame[["open", "high", "low", "close"]] /= 1000.0
    frame = frame[(frame["time"] >= start_day) & (frame["time"] < end_day + timedelta(days=1))]
    return frame.sort_values("time").drop_duplicates("time").reset_index(drop=True)
