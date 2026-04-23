"""Structured collection failure metadata shared across collectors."""

from __future__ import annotations

from dataclasses import dataclass


def summarize_raw_error(raw_error: str | None, limit: int = 400) -> str | None:
    """Normalize provider output into a compact single-line excerpt."""
    if raw_error is None:
        return None

    cleaned = " ".join(str(raw_error).split())
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3]}..."


@dataclass
class CollectionFailure(RuntimeError):
    """Structured failure that can be persisted to collection_log."""

    message: str
    failure_code: str
    failure_stage: str
    provider: str | None = None
    raw_error_excerpt: str | None = None
    run_mode: str | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)
        self.raw_error_excerpt = summarize_raw_error(
            self.raw_error_excerpt or self.message
        )

    def __str__(self) -> str:
        return self.message
