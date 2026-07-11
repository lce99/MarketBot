"""베트남 시장 수집기 - vnstock 기반 HOSE/HNX 전종목 수집."""

from __future__ import annotations

import contextlib
import io
import logging
import time
from collections import deque
from datetime import datetime, timedelta

import pandas as pd

from src.collection_failures import CollectionFailure, summarize_raw_error
from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_period_return_from_closes
from src.config import (
    VN_CHECKPOINT_BATCH_SIZE,
    VN_DEGRADED_MAX_TICKERS,
    VN_FAILURE_POLICY_LOOKBACK_RUNS,
    VN_FAILURE_STREAK_THRESHOLD,
    VN_FULL_REBUILD_MAX_TICKERS,
    VN_INCREMENTAL_ABNORMAL_LOOKBACK_DAYS,
    VN_INCREMENTAL_FULL_REFRESH_WEEKDAY,
    VN_INCREMENTAL_LARGE_CAP_COUNT,
    VN_INCREMENTAL_MIN_CANDIDATES,
    VN_INCREMENTAL_STALE_AFTER_DAYS,
    VN_RATE_LIMIT_PER_MINUTE,
)
from src.database import (
    delete_collection_checkpoint,
    get_collection_checkpoint,
    get_connection,
    get_instrument_universe,
    get_recent_collection_logs,
    get_recent_abnormal_tickers,
    init_db,
    upsert_collection_checkpoint,
)

logger = logging.getLogger(__name__)

# vnstock 업종 → GICS 매핑 (영문 industry name 기반)
VN_SECTOR_MAP = {
    # ICB/GICS level-1 names returned by vnstock 4.x.
    "Financials": "금융",
    "Industrials": "산업재",
    "Materials": "소재",
    "Health Care": "헬스케어",
    "Communication Services": "커뮤니케이션",
    "Banks": "금융",
    "Financial Services": "금융",
    "Insurance": "금융",
    "Securities": "금융",
    "Real Estate": "부동산",
    "Construction": "산업재",
    "Building Materials": "소재",
    "Steel": "소재",
    "Chemicals": "소재",
    "Technology": "정보기술",
    "Information Technology": "정보기술",
    "Software": "정보기술",
    "Telecommunications": "커뮤니케이션",
    "Media": "커뮤니케이션",
    "Healthcare": "헬스케어",
    "Pharmaceuticals": "헬스케어",
    "Food & Beverage": "필수소비재",
    "Consumer Staples": "필수소비재",
    "Consumer Goods": "필수소비재",
    "Retail": "경기소비재",
    "Consumer Discretionary": "경기소비재",
    "Automobiles": "경기소비재",
    "Textiles": "경기소비재",
    "Oil & Gas": "에너지",
    "Energy": "에너지",
    "Electricity": "유틸리티",
    "Utilities": "유틸리티",
    "Water": "유틸리티",
    "Transportation": "산업재",
    "Logistics": "산업재",
    "Industrial": "산업재",
    "Agriculture": "필수소비재",
    "Aquaculture": "필수소비재",
    "Mining": "소재",
    "Rubber": "소재",
    "Plastics": "소재",
    # KBS ``symbols_by_industries`` returns Vietnamese labels.
    "Bán buôn": "산업재",
    "Bảo hiểm": "금융",
    "Bất động sản": "부동산",
    "Chứng khoán": "금융",
    "Công nghệ và thông tin": "정보기술",
    "Bán lẻ": "경기소비재",
    "Chăm sóc sức khỏe": "헬스케어",
    "Khai khoáng": "소재",
    "Ngân hàng": "금융",
    "Nông - Lâm - Ngư": "필수소비재",
    "SX Thiết bị, máy móc": "산업재",
    "SX Hàng gia dụng": "경기소비재",
    "Sản phẩm cao su": "소재",
    "SX Nhựa - Hóa chất": "소재",
    "Thực phẩm - Đồ uống": "필수소비재",
    "Chế biến Thủy sản": "필수소비재",
    "Vật liệu xây dựng": "소재",
    "Tiện ích": "유틸리티",
    "Vận tải - kho bãi": "산업재",
    "Xây dựng": "산업재",
    "Dịch vụ lưu trú, ăn uống, giải trí": "경기소비재",
    "SX Phụ trợ": "산업재",
    "Thiết bị điện": "산업재",
    "Dịch vụ tư vấn, hỗ trợ": "산업재",
    "Tài chính khác": "금융",
    # VCI level-1 Vietnamese ICB labels.
    "Tài chính": "금융",
    "Công nghiệp": "산업재",
    "Nguyên vật liệu": "소재",
    "Y tế": "헬스케어",
    "Công nghệ Thông tin": "정보기술",
    "Dịch vụ Viễn thông": "커뮤니케이션",
    "Hàng Tiêu dùng": "경기소비재",
    "Hàng Tiêu dùng thiết yếu": "필수소비재",
    "Dầu khí": "에너지",
    "Tiện ích Cộng đồng": "유틸리티",
}

_VN_SECTOR_MAP_NORMALIZED = {
    " ".join(name.split()).casefold(): sector
    for name, sector in VN_SECTOR_MAP.items()
}
_VN_CANONICAL_SECTORS = set(VN_SECTOR_MAP.values())


def map_vietnam_sector(*values: object) -> str:
    """Normalize vnstock English/Vietnamese industry labels to one sector."""
    for value in values:
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            continue
        label = " ".join(str(value).strip().split())
        if not label:
            continue
        if label in _VN_CANONICAL_SECTORS:
            return label
        mapped = _VN_SECTOR_MAP_NORMALIZED.get(label.casefold())
        if mapped:
            return mapped
    return "기타"


class VietnamCollector(BaseCollector):
    country_code = "VN"

    def __init__(self) -> None:
        self.run_mode = "incremental"
        self.mode_override: str | None = None
        self.max_tickers: int | None = None
        self.resume_from_checkpoint = False
        self._selection_mode = "incremental"
        self._selection_reason = "default"
        self._request_timestamps: deque[float] = deque()
        self._source_penalties: dict[tuple[str, str], int] = {}
        self._blocked_sources_by_stage: dict[str, set[str]] = {}
        self._recent_failure_policy: dict[str, object] = {}

    def configure_collection(
        self,
        *,
        mode: str | None = None,
        max_tickers: int | None = None,
        resume_from_checkpoint: bool = False,
    ) -> None:
        """Override one collection run from the CLI."""
        self.mode_override = mode
        self.max_tickers = max_tickers
        self.resume_from_checkpoint = resume_from_checkpoint

    def _set_selection_context(self, mode: str, reason: str) -> None:
        self._selection_mode = mode
        self._selection_reason = reason

    def _throttle_requests(self) -> None:
        limit = max(1, VN_RATE_LIMIT_PER_MINUTE)
        now = time.monotonic()
        while self._request_timestamps and now - self._request_timestamps[0] >= 60:
            self._request_timestamps.popleft()

        if len(self._request_timestamps) >= limit:
            sleep_for = 60 - (now - self._request_timestamps[0]) + 1
            logger.info(f"[VN] request throttle: sleep {sleep_for:.1f}s")
            time.sleep(max(sleep_for, 0.0))
            now = time.monotonic()
            while self._request_timestamps and now - self._request_timestamps[0] >= 60:
                self._request_timestamps.popleft()

        self._request_timestamps.append(time.monotonic())

    def _looks_like_rate_limit(
        self,
        exc: BaseException | None = None,
        captured_output: str | None = None,
    ) -> bool:
        text = " ".join(
            part
            for part in (
                str(exc) if exc is not None else "",
                captured_output or "",
            )
            if part
        ).lower()
        return any(
            token in text
            for token in (
                "rate limit",
                "limit exceeded",
                "maximum api request",
                "wait to retry",
                "giới hạn api",
                "requests/phút",
            )
        )

    def _call_provider(
        self,
        func,
        *,
        stage: str,
        context_label: str,
        provider_label: str | None = None,
    ):
        provider_name = provider_label or "vnstock"
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(stdout_buffer):
                with contextlib.redirect_stderr(stderr_buffer):
                    result = func()
        except SystemExit as exc:
            captured = summarize_raw_error(
                "\n".join(
                    part
                    for part in (stdout_buffer.getvalue(), stderr_buffer.getvalue())
                    if part
                )
            )
            if self._looks_like_rate_limit(exc, captured):
                raise CollectionFailure(
                    message="vnstock API 호출 한도를 초과했습니다.",
                    failure_code="provider_rate_limited",
                    failure_stage=stage,
                    provider=provider_name,
                    raw_error_excerpt=captured or str(exc),
                    run_mode=self.get_run_mode(),
                ) from None
            raise CollectionFailure(
                message=f"vnstock provider terminated during {context_label}",
                failure_code="provider_error",
                failure_stage=stage,
                provider=provider_name,
                raw_error_excerpt=captured or str(exc),
                run_mode=self.get_run_mode(),
            ) from None
        except Exception as exc:
            captured = summarize_raw_error(
                "\n".join(
                    part
                    for part in (
                        stdout_buffer.getvalue(),
                        stderr_buffer.getvalue(),
                        str(exc),
                    )
                    if part
                )
            )
            if self._looks_like_rate_limit(exc, captured):
                raise CollectionFailure(
                    message="vnstock API 호출 한도를 초과했습니다.",
                    failure_code="provider_rate_limited",
                    failure_stage=stage,
                    provider=provider_name,
                    raw_error_excerpt=captured or str(exc),
                    run_mode=self.get_run_mode(),
                ) from exc
            raise

        captured = summarize_raw_error(
            "\n".join(
                part
                for part in (stdout_buffer.getvalue(), stderr_buffer.getvalue())
                if part
            )
        )
        if self._looks_like_rate_limit(captured_output=captured):
            raise CollectionFailure(
                message="vnstock API 호출 한도를 초과했습니다.",
                failure_code="provider_rate_limited",
                failure_stage=stage,
                provider=provider_name,
                raw_error_excerpt=captured,
                run_mode=self.get_run_mode(),
            )
        return result

    def _extract_source(self, provider: str | None) -> str | None:
        if not provider:
            return None
        parts = provider.split(":")
        for token in reversed(parts):
            if token in {"KBS", "VCI"}:
                return token
        return None

    def _load_recent_failure_policy(self) -> dict[str, object]:
        init_db()
        conn = get_connection()
        try:
            logs = get_recent_collection_logs(
                conn,
                limit=VN_FAILURE_POLICY_LOOKBACK_RUNS,
                market=self.country_code,
            )
        finally:
            conn.close()

        consecutive_failures: list[dict] = []
        stage_source_penalties: dict[str, set[str]] = {}
        for row in logs:
            if row.get("status") == "success":
                break
            if row.get("status") != "failed":
                continue

            failure_code = row.get("failure_code")
            if failure_code not in {"provider_rate_limited", "provider_error"}:
                break

            consecutive_failures.append(row)
            source = self._extract_source(row.get("provider"))
            stage = row.get("failure_stage")
            if source and stage:
                stage_source_penalties.setdefault(stage, set()).add(source)

        degraded = len(consecutive_failures) >= VN_FAILURE_STREAK_THRESHOLD
        if not degraded:
            return {
                "degraded": False,
                "consecutive_failures": len(consecutive_failures),
                "avoid_sources": stage_source_penalties,
            }

        dominant_code = consecutive_failures[0].get("failure_code") or "provider_error"
        reason = f"repeated_{dominant_code}"
        logger.warning(
            f"[VN] auto mitigation armed after {len(consecutive_failures)} "
            f"consecutive failures ({dominant_code})"
        )
        return {
            "degraded": True,
            "consecutive_failures": len(consecutive_failures),
            "failure_code": dominant_code,
            "reason": reason,
            "safe_max_tickers": VN_DEGRADED_MAX_TICKERS,
            "avoid_sources": stage_source_penalties,
        }

    def _get_source_order(self, stage: str) -> tuple[str, ...]:
        default_order = ["KBS", "VCI"]
        blocked_sources = self._blocked_sources_by_stage.get(stage, set())
        available_sources = [s for s in default_order if s not in blocked_sources]
        if not available_sources:
            available_sources = list(default_order)

        avoid_sources = set()
        if self._recent_failure_policy:
            avoid_sources = set(
                self._recent_failure_policy.get("avoid_sources", {}).get(stage, set())
            )

        def sort_key(source: str) -> tuple[int, int]:
            penalty = self._source_penalties.get((stage, source), 0)
            recent_penalty = 1 if source in avoid_sources else 0
            default_rank = default_order.index(source)
            return (recent_penalty + penalty, default_rank)

        return tuple(sorted(available_sources, key=sort_key))

    def _note_source_failure(
        self,
        stage: str,
        source: str,
        failure: CollectionFailure,
    ) -> None:
        key = (stage, source)
        self._source_penalties[key] = self._source_penalties.get(key, 0) + 1
        if failure.failure_code == "provider_rate_limited":
            self._blocked_sources_by_stage.setdefault(stage, set()).add(source)

    def _apply_auto_mitigation(self, listing: pd.DataFrame) -> pd.DataFrame:
        if self.mode_override is not None:
            return listing.reset_index(drop=True)

        if not self._recent_failure_policy.get("degraded"):
            return listing.reset_index(drop=True)

        safe_max_tickers = int(
            self._recent_failure_policy.get("safe_max_tickers") or VN_DEGRADED_MAX_TICKERS
        )
        if self.max_tickers is None or self.max_tickers > safe_max_tickers:
            self.max_tickers = safe_max_tickers

        reason = str(self._recent_failure_policy.get("reason") or "repeated_failures")
        if self.run_mode != "incremental":
            self.run_mode = "seed"
            self._set_selection_context("seed", f"auto_mitigated:{reason}")
            logger.warning(
                f"[VN] auto mitigation: switch to seed (max_tickers={self.max_tickers})"
            )
            return self._apply_seed_limit(listing, reason=reason)

        self._set_selection_context("incremental", f"auto_mitigated:{reason}")
        logger.warning(
            f"[VN] auto mitigation: keep incremental and cap max_tickers={self.max_tickers}"
        )
        return listing.reset_index(drop=True)

    def _prioritize_listing(self, listing: pd.DataFrame) -> pd.DataFrame:
        prioritized = listing.copy()
        if "market_cap" not in prioritized.columns:
            return prioritized.reset_index(drop=True)

        prioritized["_market_cap_sort"] = pd.to_numeric(
            prioritized["market_cap"],
            errors="coerce",
        ).fillna(-1)
        prioritized = prioritized.sort_values(
            ["_market_cap_sort", "ticker"],
            ascending=[False, True],
        )
        return prioritized.drop(columns=["_market_cap_sort"]).reset_index(drop=True)

    def _apply_full_rebuild_guard(self, listing: pd.DataFrame) -> pd.DataFrame:
        if len(listing) <= VN_FULL_REBUILD_MAX_TICKERS:
            return listing.reset_index(drop=True)

        limited_listing = self._prioritize_listing(listing).head(
            VN_FULL_REBUILD_MAX_TICKERS
        )
        self.run_mode = "seed"
        self._set_selection_context("seed", self._selection_reason)
        logger.warning(
            f"[VN] full rebuild guard {len(listing)} -> {len(limited_listing)} "
            f"(reason={self._selection_reason})"
        )
        return limited_listing.reset_index(drop=True)

    def _apply_operator_limit(self, listing: pd.DataFrame) -> pd.DataFrame:
        if self.max_tickers is None or len(listing) <= self.max_tickers:
            return listing.reset_index(drop=True)

        limited_listing = self._prioritize_listing(listing).head(self.max_tickers)
        logger.info(
            f"[VN] operator max_tickers {len(listing)} -> {len(limited_listing)} "
            f"(mode={self.get_run_mode()})"
        )
        return limited_listing.reset_index(drop=True)

    def _apply_seed_limit(self, listing: pd.DataFrame, *, reason: str) -> pd.DataFrame:
        limit = self.max_tickers or VN_FULL_REBUILD_MAX_TICKERS
        if len(listing) <= limit:
            return listing.reset_index(drop=True)

        limited_listing = self._prioritize_listing(listing).head(limit)
        logger.info(
            f"[VN] seed limit {len(listing)} -> {len(limited_listing)} "
            f"(reason={reason})"
        )
        return limited_listing.reset_index(drop=True)

    def _prepare_target_listing(
        self,
        listing: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        if self.mode_override == "full":
            self.run_mode = "full"
            self._set_selection_context("full", "manual_full")
            return self._apply_operator_limit(listing)

        if self.mode_override == "seed":
            self.run_mode = "seed"
            self._set_selection_context("seed", "manual_seed")
            return self._apply_seed_limit(listing, reason="manual_seed")

        selected_listing = self._select_listing_candidates(listing, date)

        if self.mode_override == "incremental":
            if self._selection_mode == "incremental":
                self.run_mode = "incremental"
                return self._apply_operator_limit(selected_listing)

            logger.warning(
                f"[VN] incremental mode unavailable ({self._selection_reason}), "
                "switching to seed"
            )
            self.run_mode = "seed"
            self._set_selection_context(
                "seed",
                f"manual_incremental_fallback:{self._selection_reason}",
            )
            return self._apply_seed_limit(listing, reason=self._selection_reason)

        if self._selection_mode == "incremental":
            self.run_mode = "incremental"
            selected_listing = self._apply_auto_mitigation(selected_listing)
            return self._apply_operator_limit(selected_listing)

        self.run_mode = "full"
        selected_listing = self._apply_full_rebuild_guard(selected_listing)
        selected_listing = self._apply_auto_mitigation(selected_listing)
        return self._apply_operator_limit(selected_listing)

    def _serialize_records(self, rows: list[dict]) -> list[dict]:
        if not rows:
            return []
        frame = pd.DataFrame(rows)
        frame = frame.where(pd.notna(frame), None)
        return frame.to_dict("records")

    def _serialize_listing(self, listing: pd.DataFrame) -> list[dict]:
        if listing.empty:
            return []
        normalized = listing.copy()
        normalized = normalized.where(pd.notna(normalized), None)
        return normalized.to_dict("records")

    def _save_checkpoint(
        self,
        *,
        requested_date: str,
        listing: pd.DataFrame,
        next_index: int,
        last_ticker: str | None,
        rows: list[dict],
        used_dates: list[str],
    ) -> None:
        init_db()
        conn = get_connection()
        try:
            batch_size = max(1, VN_CHECKPOINT_BATCH_SIZE)
            upsert_collection_checkpoint(
                conn,
                self.country_code,
                requested_date,
                self.get_run_mode(),
                status="pending",
                next_index=next_index,
                batch_number=(next_index + batch_size - 1) // batch_size,
                last_ticker=last_ticker,
                saved_rows=len(rows),
                total_tickers=len(listing),
                payload={
                    "listing_rows": self._serialize_listing(listing),
                    "collected_rows": self._serialize_records(rows),
                    "used_dates": sorted(set(used_dates)),
                    "selection_mode": self._selection_mode,
                    "selection_reason": self._selection_reason,
                    "effective_date": max(used_dates) if used_dates else None,
                },
            )
            conn.commit()
        finally:
            conn.close()

    def _clear_checkpoint(
        self,
        requested_date: str,
        run_mode: str | None = None,
    ) -> None:
        init_db()
        conn = get_connection()
        try:
            delete_collection_checkpoint(
                conn,
                self.country_code,
                requested_date=requested_date,
                run_mode=run_mode or self.get_run_mode(),
            )
            conn.commit()
        finally:
            conn.close()

    def _load_pending_checkpoint(self, requested_date: str) -> dict | None:
        init_db()
        conn = get_connection()
        try:
            if self.resume_from_checkpoint:
                checkpoint = get_collection_checkpoint(
                    conn,
                    self.country_code,
                    requested_date=requested_date,
                    run_mode=self.mode_override,
                )
                if checkpoint is not None:
                    return checkpoint

            if self.mode_override == "seed":
                return get_collection_checkpoint(
                    conn,
                    self.country_code,
                    requested_date=requested_date,
                    run_mode="seed",
                )

            if self.mode_override is None:
                return get_collection_checkpoint(
                    conn,
                    self.country_code,
                    requested_date=requested_date,
                    run_mode="seed",
                )
            return None
        finally:
            conn.close()

    def _restore_checkpoint_state(
        self,
        checkpoint: dict,
    ) -> dict | None:
        payload = checkpoint.get("payload") or {}
        listing_rows = payload.get("listing_rows") or []
        if not listing_rows:
            logger.warning("[VN] checkpoint has no listing rows, starting fresh")
            return None

        listing = pd.DataFrame(listing_rows)
        next_index = int(checkpoint.get("next_index") or 0)
        rows = payload.get("collected_rows") or []
        listing_by_ticker = {
            str(row.get("ticker") or row.get("symbol") or ""): row
            for row in listing_rows
        }
        for row in rows:
            if row.get("sector") not in (None, "", "기타"):
                continue
            listing_row = listing_by_ticker.get(str(row.get("ticker") or ""), {})
            row["sector"] = map_vietnam_sector(
                listing_row.get("sector"),
                listing_row.get("industry"),
                listing_row.get("en_icb_name"),
                listing_row.get("icb_name"),
            )
        used_dates = payload.get("used_dates") or []

        self.run_mode = checkpoint.get("run_mode") or self.get_run_mode()
        self._set_selection_context(
            payload.get("selection_mode") or self.run_mode,
            payload.get("selection_reason") or "checkpoint_resume",
        )
        effective_date = payload.get("effective_date")
        if effective_date:
            self.effective_date = effective_date

        logger.info(
            f"[VN] resume checkpoint loaded: mode={self.run_mode}, "
            f"next_index={next_index}, saved_rows={len(rows)}, total={len(listing)}"
        )
        return {
            "listing": listing.reset_index(drop=True),
            "next_index": max(0, next_index),
            "rows": rows,
            "used_dates": used_dates,
        }

    def _load_listing_from_cached_universe(self) -> pd.DataFrame:
        """Fall back to the last cached universe when vnstock listing APIs fail."""
        try:
            conn = get_connection()
            try:
                rows = get_instrument_universe(conn, self.country_code)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"[VN] cached listing unavailable: {exc}")
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        listing = pd.DataFrame(
            [
                {
                    "ticker": row.get("ticker"),
                    "name": row.get("name") or row.get("ticker"),
                    "sector": row.get("sector"),
                    "market_cap": row.get("market_cap"),
                }
                for row in rows
                if row.get("ticker")
            ]
        )
        if listing.empty:
            return pd.DataFrame()

        listing = listing.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
        logger.warning(
            f"[VN] vnstock listing unavailable, using cached universe: {len(listing)} tickers"
        )
        return listing

    def _load_listing(self) -> pd.DataFrame:
        """Load the Vietnam listing table across supported vnstock versions."""
        errors: list[str] = []
        last_rate_limit: CollectionFailure | None = None

        try:
            from vnstock import Listing

            for source in self._get_source_order("load_listing"):
                try:
                    listing_client = Listing(source=source)
                    base_listing = self._call_provider(
                        lambda: listing_client.all_symbols(),
                        stage="load_listing",
                        context_label=f"Listing API ({source}) all_symbols",
                        provider_label=f"vnstock:{source}",
                    )
                    industries = self._call_provider(
                        lambda: listing_client.symbols_by_industries(),
                        stage="load_listing",
                        context_label=f"Listing API ({source}) symbols_by_industries",
                        provider_label=f"vnstock:{source}",
                    )
                    listing = self._merge_listing_frames(base_listing, industries)
                    if listing is not None and not listing.empty:
                        return listing
                except CollectionFailure as exc:
                    self._note_source_failure("load_listing", source, exc)
                    if exc.failure_code == "provider_rate_limited":
                        last_rate_limit = exc
                        logger.warning(
                            f"[VN] listing source {source} rate limited, trying fallback"
                        )
                    errors.append(f"Listing API ({source}): {exc}")
                except Exception as exc:
                    errors.append(f"Listing API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Listing import: {exc}")

        try:
            from vnstock import Quote

            for source in self._get_source_order("load_listing"):
                try:
                    listing_client = Quote(symbol="VCI", source=source)
                    base_listing = self._call_provider(
                        lambda: listing_client.listing.all_symbols(),
                        stage="load_listing",
                        context_label=f"Quote.listing API ({source}) all_symbols",
                        provider_label=f"vnstock:{source}",
                    )
                    industries = self._call_provider(
                        lambda: listing_client.listing.symbols_by_industries(),
                        stage="load_listing",
                        context_label=f"Quote.listing API ({source}) symbols_by_industries",
                        provider_label=f"vnstock:{source}",
                    )
                    listing = self._merge_listing_frames(base_listing, industries)
                    if listing is not None and not listing.empty:
                        return listing
                except CollectionFailure as exc:
                    self._note_source_failure("load_listing", source, exc)
                    if exc.failure_code == "provider_rate_limited":
                        last_rate_limit = exc
                        logger.warning(
                            f"[VN] quote listing source {source} rate limited, trying fallback"
                        )
                    errors.append(f"Quote.listing API ({source}): {exc}")
                except Exception as exc:
                    errors.append(f"Quote.listing API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Quote import: {exc}")

        try:
            from vnstock import Vnstock

            stock = Vnstock()
            legacy_listing = self._call_provider(
                lambda: stock.stock().listing.all_symbols(),
                stage="load_listing",
                context_label="Legacy stock.listing API",
                provider_label="vnstock:legacy",
            )
            listing = self._normalize_listing_frame(legacy_listing)
            if listing is not None and not listing.empty:
                return listing
        except CollectionFailure as exc:
            if exc.failure_code == "provider_rate_limited":
                last_rate_limit = exc
            errors.append(f"Legacy stock.listing API: {exc}")
        except Exception as exc:
            errors.append(f"Legacy stock.listing API: {exc}")

        cached_listing = self._load_listing_from_cached_universe()
        if not cached_listing.empty:
            return cached_listing

        if last_rate_limit is not None:
            raise last_rate_limit

        joined_errors = "; ".join(errors) if errors else "unknown error"
        raise RuntimeError(f"베트남 종목 리스트 조회 실패: {joined_errors}")

    def _load_history(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Load quote history across supported vnstock interfaces and sources."""
        errors: list[str] = []
        last_rate_limit: CollectionFailure | None = None

        try:
            from vnstock import Quote

            for source in self._get_source_order("fetch_history"):
                try:
                    self._throttle_requests()
                    quote = Quote(symbol=ticker, source=source)
                    history = self._call_provider(
                        lambda: quote.history(
                            start=start_date,
                            end=end_date,
                            interval="1D",
                        ),
                        stage="fetch_history",
                        context_label=f"Quote API ({source}) {ticker}",
                        provider_label=f"vnstock:{source}",
                    )
                    if history is not None:
                        return history
                except CollectionFailure as exc:
                    self._note_source_failure("fetch_history", source, exc)
                    if exc.failure_code == "provider_rate_limited":
                        last_rate_limit = exc
                        logger.warning(
                            f"[VN] history source {source} rate limited for {ticker}, "
                            "trying fallback"
                        )
                    errors.append(f"Quote API ({source}): {exc}")
                except Exception as exc:
                    errors.append(f"Quote API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Quote import: {exc}")

        try:
            from vnstock import Vnstock

            stock = Vnstock()
            for source in self._get_source_order("fetch_history"):
                try:
                    self._throttle_requests()
                    history = self._call_provider(
                        lambda: stock.stock(symbol=ticker, source=source).quote.history(
                            start=start_date,
                            end=end_date,
                        ),
                        stage="fetch_history",
                        context_label=f"Legacy quote API ({source}) {ticker}",
                        provider_label=f"vnstock:legacy:{source}",
                    )
                    if history is not None:
                        return history
                except CollectionFailure as exc:
                    self._note_source_failure("fetch_history", source, exc)
                    if exc.failure_code == "provider_rate_limited":
                        last_rate_limit = exc
                        logger.warning(
                            f"[VN] legacy history source {source} rate limited for {ticker}, "
                            "trying fallback"
                        )
                    errors.append(f"Legacy quote API ({source}): {exc}")
                except Exception as exc:
                    errors.append(f"Legacy quote API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Legacy Vnstock import: {exc}")

        if last_rate_limit is not None:
            raise last_rate_limit

        joined_errors = "; ".join(errors) if errors else "unknown error"
        raise RuntimeError(f"{ticker} 가격 이력 조회 실패: {joined_errors}")

    def _normalize_listing_frame(self, listing: pd.DataFrame) -> pd.DataFrame:
        """Normalize vnstock listing tables to the columns used by the collector."""
        if listing is None or listing.empty:
            return pd.DataFrame()

        listing = listing.copy()
        if "ticker" not in listing.columns and "symbol" in listing.columns:
            listing["ticker"] = listing["symbol"]
        if "name" not in listing.columns:
            for column in (
                "organ_name",
                "organ_short_name",
                "company_name",
                "short_name",
            ):
                if column in listing.columns:
                    listing["name"] = listing[column]
                    break
        if "industry" not in listing.columns:
            industry = None
            for column in (
                "en_icb_name",
                "industry_name",
                "icb_name",
                "icb_name3",
                "icb_name2",
                "sector",
            ):
                if column in listing.columns:
                    industry = listing[column]
                    break
            if industry is not None:
                listing["industry"] = industry

        return listing

    def _merge_listing_frames(
        self,
        base_listing: pd.DataFrame,
        industries: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge vnstock listing/name data with industry classifications."""
        base_listing = self._normalize_listing_frame(base_listing)
        industries = self._normalize_listing_frame(industries)

        if base_listing.empty and industries.empty:
            return pd.DataFrame()
        if base_listing.empty:
            return industries
        if industries.empty:
            return base_listing

        merged = base_listing.merge(
            industries.drop_duplicates(subset=["ticker"]),
            on="ticker",
            how="left",
            suffixes=("", "_industry"),
        )

        if "name" not in merged.columns or merged["name"].isna().all():
            fallback_name = merged.get("name_industry")
            if fallback_name is not None:
                merged["name"] = fallback_name
        else:
            fallback_name = merged.get("name_industry")
            if fallback_name is not None:
                merged["name"] = merged["name"].fillna(fallback_name)

        if "industry" not in merged.columns or merged["industry"].isna().all():
            fallback_industry = merged.get("industry_industry")
            if fallback_industry is not None:
                merged["industry"] = fallback_industry
        else:
            fallback_industry = merged.get("industry_industry")
            if fallback_industry is not None:
                merged["industry"] = merged["industry"].fillna(fallback_industry)

        return merged

    def _validate_sector_coverage(self, frame: pd.DataFrame) -> None:
        """Fail loudly when provider metadata would poison every VN sector row."""
        if frame.empty or "sector" not in frame.columns:
            return
        mapped = frame["sector"].fillna("기타").ne("기타")
        if len(frame) >= 10 and not bool(mapped.any()):
            raise CollectionFailure(
                message="vnstock 업종 메타데이터를 표준 섹터로 매핑하지 못했습니다.",
                failure_code="sector_metadata_missing",
                failure_stage="normalize_sector",
                provider="vnstock",
                raw_error_excerpt=f"mapped_sectors=0/{len(frame)}",
                run_mode=self.get_run_mode(),
            )

    def _select_listing_candidates(
        self,
        listing: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """Use the latest active universe on weekdays and full rebuild weekly."""
        ticker_col = "ticker" if "ticker" in listing.columns else "symbol"
        if ticker_col not in listing.columns:
            self._set_selection_context("full", "missing_ticker_column")
            return listing

        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            self._set_selection_context("full", "invalid_requested_date")
            return listing

        refresh_weekday = VN_INCREMENTAL_FULL_REFRESH_WEEKDAY.get(self.country_code)
        if (
            refresh_weekday is not None
            and requested_date.weekday() == refresh_weekday
        ):
            self._set_selection_context("full", "scheduled_full_refresh")
            logger.info("[VN] weekly full rebuild day, skipping incremental filter")
            return listing

        try:
            conn = get_connection()
            try:
                universe_rows = get_instrument_universe(conn, self.country_code)
                abnormal_tickers = set(
                    get_recent_abnormal_tickers(
                        conn,
                        self.country_code,
                        date,
                        lookback_days=VN_INCREMENTAL_ABNORMAL_LOOKBACK_DAYS,
                    )
                )
            finally:
                conn.close()
        except Exception as exc:
            self._set_selection_context("full", "cache_unavailable")
            logger.warning(f"[VN] incremental universe unavailable, use full rebuild: {exc}")
            return listing

        if not universe_rows:
            self._set_selection_context("full", "cache_missing")
            logger.info("[VN] cached universe missing, use full rebuild")
            return listing

        latest_seen_date = max(
            (row.get("last_seen_date") for row in universe_rows if row.get("last_seen_date")),
            default=None,
        )
        if latest_seen_date:
            try:
                stale_days = (
                    requested_date - datetime.strptime(latest_seen_date, "%Y-%m-%d")
                ).days
            except ValueError:
                stale_days = 0
            if stale_days > VN_INCREMENTAL_STALE_AFTER_DAYS:
                self._set_selection_context("full", "cache_stale")
                logger.info("[VN] cached universe is stale, use full rebuild")
                return listing

        active_tickers = {
            row["ticker"]
            for row in universe_rows
            if row.get("ticker")
            and int(row.get("last_is_filtered") or 0) == 0
            and int(row.get("last_is_abnormal") or 0) == 0
        }
        large_cap_tickers = {
            row["ticker"]
            for row in sorted(
                universe_rows,
                key=lambda row: float(row.get("market_cap") or 0.0),
                reverse=True,
            )[:VN_INCREMENTAL_LARGE_CAP_COUNT]
            if row.get("ticker")
        }
        candidate_tickers = active_tickers | abnormal_tickers | large_cap_tickers
        if not candidate_tickers:
            self._set_selection_context("full", "empty_incremental_candidates")
            logger.info("[VN] incremental universe empty, use full rebuild")
            return listing

        filtered_listing = listing[listing[ticker_col].isin(candidate_tickers)].copy()
        min_candidates = min(len(listing), VN_INCREMENTAL_MIN_CANDIDATES)
        if len(filtered_listing) < min_candidates:
            self._set_selection_context("full", "incremental_candidates_too_small")
            logger.info(
                f"[VN] incremental universe too small ({len(filtered_listing)}), use full rebuild"
            )
            return listing

        self._set_selection_context("incremental", "cached_universe")
        logger.info(
            f"[VN] incremental universe {len(listing)} -> {len(filtered_listing)} "
            f"(active={len(active_tickers)}, abnormal={len(abnormal_tickers)}, "
            f"large_cap={len(large_cap_tickers)})"
        )
        return filtered_listing

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """HOSE + HNX 전종목 수집 via vnstock."""
        try:
            import vnstock  # noqa: F401
        except ImportError:
            raise CollectionFailure(
                message="vnstock 미설치. pip install vnstock",
                failure_code="provider_error",
                failure_stage="import_provider",
                provider="vnstock",
                raw_error_excerpt="vnstock import failed",
                run_mode=self.get_run_mode(),
            )

        try:
            target_date = datetime.strptime(date, "%Y-%m-%d")
            start_date = (target_date - timedelta(days=14)).strftime("%Y-%m-%d")
            end_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
            self.run_mode = "incremental"
            self._set_selection_context("incremental", "default")
            self._source_penalties.clear()
            self._blocked_sources_by_stage = {}
            self._recent_failure_policy = self._load_recent_failure_policy()
            checkpoint_state = self._load_pending_checkpoint(date)
            if checkpoint_state is not None:
                restored = self._restore_checkpoint_state(checkpoint_state)
            else:
                restored = None

            if restored is not None:
                listing = restored["listing"]
                rows = list(restored["rows"])
                used_dates = list(restored["used_dates"])
                start_index = restored["next_index"]
            else:
                listing = self._load_listing()
                if listing is None or listing.empty:
                    logger.warning("[VN] 종목 리스트 없음")
                    return pd.DataFrame()

                logger.info(f"[VN] 전체 종목: {len(listing)}개")
                listing = self._prepare_target_listing(listing, date)
                rows = []
                used_dates = []
                start_index = 0

            if listing is None or listing.empty:
                logger.warning("[VN] 수집 대상이 비어 있음")
                return pd.DataFrame()

            logger.info(
                f"[VN] 수집 대상: {len(listing)}개 "
                f"(mode={self.get_run_mode()}, reason={self._selection_reason})"
            )

            if start_index >= len(listing):
                logger.info("[VN] checkpoint already reached the end, finalizing cached rows")
                df = pd.DataFrame(rows)
                self._validate_sector_coverage(df)
                if used_dates:
                    self.effective_date = max(used_dates)
                self._clear_checkpoint(date, self.get_run_mode())
                return df

            batch_size = max(1, VN_CHECKPOINT_BATCH_SIZE)
            last_processed_ticker: str | None = None
            processed_since_log = 0

            for position in range(start_index, len(listing)):
                info = listing.iloc[position]
                ticker = info.get("ticker") or info.get("symbol") or ""
                if not ticker:
                    last_processed_ticker = None
                    continue

                try:
                    hist = self._load_history(ticker, start_date, end_date)
                    hist = self._prepare_history(hist, target_date)
                    if not hist.empty:
                        latest = hist.iloc[-1]
                        close_price = (
                            float(latest["close"])
                            if pd.notna(latest.get("close"))
                            else None
                        )
                        volume = (
                            float(latest["volume"])
                            if pd.notna(latest.get("volume"))
                            else None
                        )
                        daily_return = self._compute_daily_return(hist)
                        weekly_return = self._compute_weekly_return(hist)
                        avg_volume_20d = self._compute_avg_volume(hist)

                        sector = map_vietnam_sector(
                            info.get("sector"),
                            info.get("industry"),
                            info.get("en_icb_name"),
                            info.get("icb_name"),
                        )

                        market_cap = info.get("market_cap")
                        if pd.isna(market_cap) or market_cap in ("", None):
                            market_cap = None
                        else:
                            try:
                                market_cap = float(market_cap)
                            except (TypeError, ValueError):
                                market_cap = None

                        if "_date" in hist.columns:
                            used_dates.append(
                                hist.iloc[-1]["_date"].strftime("%Y-%m-%d")
                            )

                        rows.append(
                            {
                                "ticker": ticker,
                                "name": info.get("name", ticker),
                                "sector": sector,
                                "market_cap": market_cap,
                                "close_price": close_price,
                                "daily_return": daily_return,
                                "weekly_return": weekly_return,
                                "volume": volume,
                                "avg_volume_20d": avg_volume_20d,
                            }
                        )

                    time.sleep(0.1)
                    last_processed_ticker = ticker
                except CollectionFailure as exc:
                    if exc.failure_code == "provider_rate_limited":
                        logger.warning(
                            f"[VN] rate limit encountered, checkpoint and stop "
                            f"(next_index={position}, saved_rows={len(rows)})"
                        )
                        self._save_checkpoint(
                            requested_date=date,
                            listing=listing,
                            next_index=position,
                            last_ticker=last_processed_ticker,
                            rows=rows,
                            used_dates=used_dates,
                        )
                        raise
                    logger.debug(f"[VN] {ticker} 스킵: {exc}")
                    last_processed_ticker = ticker
                except Exception as exc:
                    logger.debug(f"[VN] {ticker} 스킵: {exc}")
                    last_processed_ticker = ticker
                    continue

                processed_since_log += 1
                next_index = position + 1
                should_checkpoint = (
                    next_index == len(listing)
                    or next_index % batch_size == 0
                )
                if should_checkpoint:
                    self._save_checkpoint(
                        requested_date=date,
                        listing=listing,
                        next_index=next_index,
                        last_ticker=last_processed_ticker,
                        rows=rows,
                        used_dates=used_dates,
                    )
                    logger.info(
                        f"[VN] checkpoint saved: next_index={next_index}, "
                        f"saved_rows={len(rows)}, total={len(listing)}"
                    )

                if processed_since_log >= 100:
                    logger.info(f"[VN] {len(rows)}개 수집 중...")
                    processed_since_log = 0

            df = pd.DataFrame(rows)
            self._validate_sector_coverage(df)
            if used_dates:
                self.effective_date = max(used_dates)
            self._clear_checkpoint(date, self.get_run_mode())
            logger.info(f"[VN] 수집 완료: {len(df)}개 종목")
            return df

        except CollectionFailure:
            raise
        except Exception as exc:
            logger.error(f"[VN] 수집 실패: {exc}", exc_info=True)
            raise

    def _prepare_history(self, hist: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        """최근 구간에서 목표일 이전의 최신 거래일 데이터만 남긴다."""
        if hist is None or hist.empty:
            return pd.DataFrame()

        hist = hist.copy()
        date_col = next(
            (col for col in ("time", "date", "tradingDate") if col in hist.columns),
            None,
        )
        if date_col:
            hist["_date"] = pd.to_datetime(hist[date_col], errors="coerce")
            hist = hist.dropna(subset=["_date"])
            hist = hist[hist["_date"].dt.date <= target_date.date()]
            hist = hist.sort_values("_date")

        return hist.reset_index(drop=True)

    def _compute_daily_return(self, hist: pd.DataFrame) -> float | None:
        """종가 기준 일간 수익률을 계산한다."""
        if "close" not in hist.columns:
            return None

        valid = hist.dropna(subset=["close"])
        return compute_period_return_from_closes(valid["close"].tolist(), periods_back=1)

    def _compute_weekly_return(self, hist: pd.DataFrame) -> float | None:
        """종가 기준 주간 수익률을 계산한다."""
        if "close" not in hist.columns:
            return None

        valid = hist.dropna(subset=["close"])
        return compute_period_return_from_closes(valid["close"].tolist(), periods_back=5)

    def _compute_avg_volume(self, hist: pd.DataFrame) -> float | None:
        """최근 20거래일 평균 거래량을 계산한다."""
        if "volume" not in hist.columns:
            return None

        volumes = hist["volume"].dropna().tail(20)
        if len(volumes) == 0:
            return None

        return float(volumes.mean())
