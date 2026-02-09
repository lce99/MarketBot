"""MarketBot 전역 설정 - 국가, 섹터, 필터, 벤치마크 정의"""

import os
from pathlib import Path

# ── 경로 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "marketbot.db"

# ── .env 파일 로딩 (로컬 개발용) ──
try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass  # python-dotenv 없으면 환경변수만 사용

# ── API 키 (환경변수) ──
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

# ── GICS 11개 섹터 (한글 ↔ 영문 매핑) ──
SECTORS = {
    "정보기술": "Information Technology",
    "금융": "Financials",
    "헬스케어": "Healthcare",
    "경기소비재": "Consumer Discretionary",
    "필수소비재": "Consumer Staples",
    "산업재": "Industrials",
    "에너지": "Energy",
    "소재": "Materials",
    "유틸리티": "Utilities",
    "부동산": "Real Estate",
    "커뮤니케이션": "Communication Services",
}

SECTOR_EN_TO_KR = {v: k for k, v in SECTORS.items()}

# ── 국가 설정 ──
COUNTRIES = {
    "US": {
        "name_kr": "미국",
        "flag": "\U0001f1fa\U0001f1f8",
        "collector": "finnhub",
        "exchange": "US",
        "currency": "USD",
        "close_utc": "21:00",
    },
    "KR": {
        "name_kr": "한국",
        "flag": "\U0001f1f0\U0001f1f7",
        "collector": "pykrx",
        "exchange": "KRX",
        "currency": "KRW",
        "close_utc": "06:30",
    },
    "CN": {
        "name_kr": "중국",
        "flag": "\U0001f1e8\U0001f1f3",
        "collector": "tushare",
        "exchange": "SSE",
        "currency": "CNY",
        "close_utc": "07:00",
    },
    "JP": {
        "name_kr": "일본",
        "flag": "\U0001f1ef\U0001f1f5",
        "collector": "finnhub",
        "exchange": "T",
        "currency": "JPY",
        "close_utc": "06:00",
    },
    "VN": {
        "name_kr": "베트남",
        "flag": "\U0001f1fb\U0001f1f3",
        "collector": "vnstock",
        "exchange": "HOSE",
        "currency": "VND",
        "close_utc": "08:00",
    },
    "IN": {
        "name_kr": "인도",
        "flag": "\U0001f1ee\U0001f1f3",
        "collector": "finnhub",
        "exchange": "NS",
        "currency": "INR",
        "close_utc": "10:00",
    },
    "DE": {
        "name_kr": "독일",
        "flag": "\U0001f1e9\U0001f1ea",
        "collector": "finnhub",
        "exchange": "DE",  # XETRA
        "currency": "EUR",
        "close_utc": "16:30",
    },
}

# ── 필터링 임계값 ──
FILTER_MARKET_CAP_MIN = {
    "US": 500_000_000,        # $500M
    "KR": 100_000_000_000,    # 1,000억원
    "CN": 5_000_000_000,      # 50억 위안
    "JP": 50_000_000_000,     # 500억엔
    "VN": 5_000_000_000_000,  # 5조 VND
    "IN": 50_000_000_000,     # 500억 INR
    "DE": 500_000_000,        # €500M
}

# 거래량 하위 N% 제외
FILTER_VOLUME_BOTTOM_PERCENTILE = 20

# 비정상 급등/급락 기준 (±%)
FILTER_ABNORMAL_RETURN_THRESHOLD = 50.0

# ── 벤치마크 티커 (yfinance) ──
BENCHMARK_TICKERS = {
    # 미국 섹터 ETF
    "US_IT": {"ticker": "XLK", "country": "US", "sector": "정보기술"},
    "US_FIN": {"ticker": "XLF", "country": "US", "sector": "금융"},
    "US_ENERGY": {"ticker": "XLE", "country": "US", "sector": "에너지"},
    "US_HEALTH": {"ticker": "XLV", "country": "US", "sector": "헬스케어"},
    "US_IND": {"ticker": "XLI", "country": "US", "sector": "산업재"},
    "US_CD": {"ticker": "XLY", "country": "US", "sector": "경기소비재"},
    "US_CS": {"ticker": "XLP", "country": "US", "sector": "필수소비재"},
    "US_MAT": {"ticker": "XLB", "country": "US", "sector": "소재"},
    "US_RE": {"ticker": "XLRE", "country": "US", "sector": "부동산"},
    "US_COMM": {"ticker": "XLC", "country": "US", "sector": "커뮤니케이션"},
    "US_UTIL": {"ticker": "XLU", "country": "US", "sector": "유틸리티"},
    # 인도 Nifty 섹터 인덱스
    "IN_IT": {"ticker": "^CNXIT", "country": "IN", "sector": "정보기술"},
    "IN_BANK": {"ticker": "^CNXBANK", "country": "IN", "sector": "금융"},
    "IN_PHARMA": {"ticker": "^CNXPHARMA", "country": "IN", "sector": "헬스케어"},
    "IN_AUTO": {"ticker": "^CNXAUTO", "country": "IN", "sector": "산업재"},
    "IN_METAL": {"ticker": "^CNXMETAL", "country": "IN", "sector": "소재"},
    "IN_FMCG": {"ticker": "^CNXFMCG", "country": "IN", "sector": "필수소비재"},
    "IN_ENERGY": {"ticker": "^CNXENERGY", "country": "IN", "sector": "에너지"},
    # 글로벌 섹터 ETF
    "GL_IT": {"ticker": "IXN", "country": "GL", "sector": "정보기술"},
    "GL_FIN": {"ticker": "IXG", "country": "GL", "sector": "금융"},
    "GL_HEALTH": {"ticker": "IXJ", "country": "GL", "sector": "헬스케어"},
    "GL_ENERGY": {"ticker": "IXC", "country": "GL", "sector": "에너지"},
    "GL_IND": {"ticker": "EXI", "country": "GL", "sector": "산업재"},
    "GL_MAT": {"ticker": "MXI", "country": "GL", "sector": "소재"},
    # 국가별 종합 인덱스
    "KR_KOSPI": {"ticker": "^KS11", "country": "KR", "sector": None},
    "CN_CSI300": {"ticker": "000300.SS", "country": "CN", "sector": None},
    "JP_N225": {"ticker": "^N225", "country": "JP", "sector": None},
    "VN_INDEX": {"ticker": "^VNINDEX", "country": "VN", "sector": None},
    "IN_NIFTY": {"ticker": "^NSEI", "country": "IN", "sector": None},
    "DE_DAX": {"ticker": "^GDAXI", "country": "DE", "sector": None},
}

# ── 한국 섹터 매핑 (pykrx 업종 → GICS 섹터) ──
# pykrx의 get_market_ticker_and_name()으로 얻는 업종을 GICS에 매핑
KR_SECTOR_MAP = {
    # 정보기술
    "반도체": "정보기술",
    "IT부품": "정보기술",
    "IT가전": "정보기술",
    "소프트웨어": "정보기술",
    "컴퓨터서비스": "정보기술",
    "통신장비": "정보기술",
    "전자장비와기기": "정보기술",
    "전자제품": "정보기술",
    "디스플레이": "정보기술",
    # 금융
    "은행": "금융",
    "증권": "금융",
    "보험": "금융",
    "금융": "금융",
    "기타금융": "금융",
    "카드": "금융",
    "캐피탈": "금융",
    # 헬스케어
    "제약": "헬스케어",
    "바이오": "헬스케어",
    "의료정밀": "헬스케어",
    "건강관리장비와용품": "헬스케어",
    # 경기소비재
    "자동차": "경기소비재",
    "자동차부품": "경기소비재",
    "호텔,레스토랑,레저": "경기소비재",
    "미디어": "경기소비재",
    "유통": "경기소비재",
    "섬유,의류,신발,호화품": "경기소비재",
    "교육서비스": "경기소비재",
    "내구소비재와의류": "경기소비재",
    # 필수소비재
    "음식료": "필수소비재",
    "담배": "필수소비재",
    "생활용품": "필수소비재",
    "식품": "필수소비재",
    "식품과생활용품소매": "필수소비재",
    # 산업재
    "기계": "산업재",
    "조선": "산업재",
    "건설": "산업재",
    "운수장비": "산업재",
    "운수창고": "산업재",
    "항공사": "산업재",
    "해운사": "산업재",
    "방위산업": "산업재",
    "우주항공과국방": "산업재",
    "전기장비": "산업재",
    # 에너지
    "에너지": "에너지",
    "석유와가스": "에너지",
    # 소재
    "철강": "소재",
    "비철금속": "소재",
    "화학": "소재",
    "종이와목재": "소재",
    "광업": "소재",
    "포장재": "소재",
    # 유틸리티
    "전기가스": "유틸리티",
    "유틸리티": "유틸리티",
    # 부동산
    "부동산": "부동산",
    # 커뮤니케이션
    "통신": "커뮤니케이션",
    "방송": "커뮤니케이션",
    "게임": "커뮤니케이션",
    "인터넷": "커뮤니케이션",
}

# ── 트렌드 스코어 가중치 ──
TREND_WEIGHT_RETURN = 0.4      # 평균 수익률
TREND_WEIGHT_BREADTH = 0.3     # 국가 확산도
TREND_WEIGHT_MOMENTUM = 0.3    # 주간 모멘텀
