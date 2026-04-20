"""SQLite WAL 내용을 메인 DB 파일로 반영한다."""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import DB_PATH
from src.database import checkpoint_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    checkpoint_db()
    logger.info(f"SQLite checkpoint 완료: {DB_PATH}")


if __name__ == "__main__":
    main()
