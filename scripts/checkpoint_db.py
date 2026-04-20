"""Checkpoint MarketBot databases and compact the Git-tracked summary DB."""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.database import checkpoint_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    result = checkpoint_db()
    logger.info(
        "SQLite checkpoint complete: summary=%s raw=%s",
        result["summary_db"],
        result["raw_db"],
    )
    logger.info(
        "Legacy migration: rows=%s abnormal_rows=%s vacuumed=%s",
        result["migrated_rows"],
        result["backfilled_abnormal_rows"],
        result["vacuumed_summary"],
    )


if __name__ == "__main__":
    main()
