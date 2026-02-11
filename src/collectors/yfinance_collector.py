"""yfinance 기반 수집기 - 일본, 독일, 인도 시장

Finnhub 무료 티어가 US만 지원하므로, JP/DE/IN은
주요 인덱스 구성종목 + yfinance로 수집.
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.config import SECTOR_EN_TO_KR

logger = logging.getLogger(__name__)

# yfinance sector → GICS 한글 매핑
YF_SECTOR_TO_GICS = {
    "Technology": "정보기술",
    "Financial Services": "금융",
    "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재",
    "Consumer Defensive": "필수소비재",
    "Industrials": "산업재",
    "Energy": "에너지",
    "Basic Materials": "소재",
    "Utilities": "유틸리티",
    "Real Estate": "부동산",
    "Communication Services": "커뮤니케이션",
    # 추가 변형
    "Information Technology": "정보기술",
    "Financials": "금융",
    "Health Care": "헬스케어",
    "Consumer Discretionary": "경기소비재",
    "Consumer Staples": "필수소비재",
    "Materials": "소재",
}

# ── 일본 Nikkei 225 구성종목 (yfinance 접미사: .T) ──
JP_TICKERS = [
    # 정보기술 / 전자
    "6758.T", "6861.T", "6857.T", "6146.T", "6920.T",
    "6762.T", "6981.T", "6723.T", "6645.T", "6526.T",
    "6753.T", "6752.T", "6501.T", "6503.T", "6504.T",
    "6506.T", "6702.T", "6701.T", "6479.T", "6472.T",
    "6367.T", "6361.T", "6302.T", "6301.T", "6103.T",
    "7735.T", "7733.T", "7752.T", "7751.T", "7741.T",
    # 자동차
    "7203.T", "7267.T", "7269.T", "7270.T", "7272.T",
    "7261.T", "7211.T", "7201.T", "4902.T",
    # 금융
    "8306.T", "8316.T", "8411.T", "8309.T", "8308.T",
    "8604.T", "8601.T", "8630.T", "8725.T", "8766.T",
    "8795.T", "8697.T",
    # 소매/소비재
    "9983.T", "3382.T", "2802.T", "2801.T", "2503.T",
    "2502.T", "2501.T", "2914.T", "2413.T", "2269.T",
    # 의약/헬스케어
    "4519.T", "4502.T", "4503.T", "4507.T", "4506.T",
    "4568.T", "4523.T", "4578.T", "4543.T", "4901.T",
    # 통신
    "9432.T", "9433.T", "9434.T", "4689.T", "4755.T",
    # 에너지/소재
    "5020.T", "5019.T", "5021.T", "5401.T", "5411.T",
    "5406.T", "5332.T", "5301.T", "5214.T", "5201.T",
    "5108.T", "3407.T", "3405.T", "3402.T", "3401.T",
    "4188.T", "4183.T", "4063.T", "4043.T", "4005.T",
    "4004.T", "4021.T", "4042.T", "4208.T", "4452.T",
    # 건설/부동산
    "1925.T", "1928.T", "1878.T", "1812.T", "1808.T",
    "1803.T", "1802.T", "1801.T", "8801.T", "8802.T",
    "8830.T",
    # 운송
    "9020.T", "9021.T", "9022.T", "9064.T", "9147.T",
    "9101.T", "9104.T", "9107.T",
    # 유틸리티
    "9501.T", "9502.T", "9503.T", "9531.T", "9532.T",
    # 기타 산업재/서비스
    "8035.T", "8015.T", "8002.T", "8001.T", "8031.T",
    "8053.T", "8058.T", "4324.T", "4307.T", "6098.T",
    "2175.T", "9602.T", "9735.T", "4661.T", "9766.T",
    "3659.T", "6954.T", "6952.T", "6902.T", "6856.T",
    "7731.T", "7762.T", "7832.T", "7911.T", "7912.T",
    "7951.T", "3086.T", "3099.T", "8233.T", "8252.T",
    "8267.T", "3289.T", "3861.T", "3863.T", "4151.T",
    "4631.T", "4911.T", "4927.T", "6326.T", "6471.T",
    "6473.T", "7003.T", "7004.T", "7011.T", "7012.T",
    "7013.T", "7186.T", "7202.T", "7205.T",
    "7259.T", "8303.T", "8304.T", "8331.T", "8354.T",
    "8355.T", "2768.T", "3105.T", "3436.T", "4324.T",
    "5713.T", "5711.T", "5706.T", "5703.T", "5541.T",
    "3103.T", "1963.T", "1944.T", "1911.T", "1605.T",
    "1332.T", "1333.T",
]

# ── 독일 DAX 40 + MDAX 주요 종목 (yfinance 접미사: .DE) ──
DE_TICKERS = [
    # DAX 40
    "SAP.DE", "SIE.DE", "ALV.DE", "DTE.DE", "AIR.DE",
    "MBG.DE", "DHL.DE", "MUV2.DE", "BAS.DE", "BMW.DE",
    "IFX.DE", "ADS.DE", "DB1.DE", "EOAN.DE", "MRK.DE",
    "HEN3.DE", "BEI.DE", "SHL.DE", "RWE.DE", "VNA.DE",
    "FRE.DE", "HEI.DE", "VOW3.DE", "DTG.DE", "P911.DE",
    "SRT3.DE", "1COV.DE", "MTX.DE", "ENR.DE", "FME.DE",
    "HNR1.DE", "BNR.DE", "QIA.DE", "RHM.DE", "SY1.DE",
    "ZAL.DE", "CBK.DE", "DBK.DE", "PAH3.DE", "BAYN.DE",
    # MDAX 주요 종목
    "LHA.DE", "TLX.DE", "EVK.DE", "LEG.DE", "PUM.DE",
    "GXI.DE", "KGX.DE", "AFX.DE", "NDA.DE", "DEQ.DE",
    "SZG.DE", "G1A.DE", "BOSS.DE", "UN01.DE", "AG1.DE",
    "WAF.DE", "COP.DE", "AIR.DE", "RAA.DE", "TEG.DE",
    "FPE3.DE", "EVD.DE", "KBX.DE", "AT1.DE", "O2D.DE",
    "SDF.DE", "NDX1.DE",
]

# ── 인도 Nifty 50 + Nifty Next 50 (yfinance 접미사: .NS) ──
IN_TICKERS = [
    # Nifty 50
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "BHARTIARTL.NS", "SBIN.NS", "ITC.NS", "LT.NS", "HINDUNILVR.NS",
    "AXISBANK.NS", "KOTAKBANK.NS", "BAJFINANCE.NS", "MARUTI.NS", "HCLTECH.NS",
    "NTPC.NS", "SUNPHARMA.NS", "TATAMOTORS.NS", "TITAN.NS", "ONGC.NS",
    "ADANIENT.NS", "ADANIPORTS.NS", "M&M.NS", "ASIANPAINT.NS", "WIPRO.NS",
    "ULTRACEMCO.NS", "POWERGRID.NS", "BAJAJFINSV.NS", "TECHM.NS", "NESTLEIND.NS",
    "JSWSTEEL.NS", "TATASTEEL.NS", "INDUSINDBK.NS", "COALINDIA.NS", "HDFCLIFE.NS",
    "SBILIFE.NS", "GRASIM.NS", "HINDALCO.NS", "DRREDDY.NS", "BAJAJ-AUTO.NS",
    "DIVISLAB.NS", "CIPLA.NS", "APOLLOHOSP.NS", "EICHERMOT.NS", "BRITANNIA.NS",
    "BPCL.NS", "TATACONSUM.NS", "HEROMOTOCO.NS", "LTIM.NS", "SHRIRAMFIN.NS",
    # Nifty Next 50
    "BANKBARODA.NS", "VEDL.NS", "IOC.NS", "PIDILITIND.NS", "SIEMENS.NS",
    "GODREJCP.NS", "DLF.NS", "HAVELLS.NS", "DABUR.NS", "AMBUJACEM.NS",
    "ABB.NS", "TRENT.NS", "ICICIPRULI.NS", "INDIGO.NS", "PNB.NS",
    "MARICO.NS", "COLPAL.NS", "BERGEPAINT.NS", "MCDOWELL-N.NS", "BOSCHLTD.NS",
    "NAUKRI.NS", "TORNTPHARM.NS", "SRF.NS", "MUTHOOTFIN.NS", "LUPIN.NS",
    "CANBK.NS", "HAL.NS", "IRCTC.NS", "ZOMATO.NS", "POLYCAB.NS",
    "JINDALSTEL.NS", "PAGEIND.NS", "CHOLAFIN.NS", "IDFCFIRSTB.NS", "INDUSTOWER.NS",
    "AUBANK.NS", "PIIND.NS", "PERSISTENT.NS", "TATAPOWER.NS", "TATAELXSI.NS",
    "ACC.NS", "PEL.NS", "MAXHEALTH.NS", "GAIL.NS", "CONCOR.NS",
    "OBEROIRLTY.NS", "HINDPETRO.NS", "LICI.NS", "ADANIGREEN.NS", "ADANITRANS.NS",
]


class YfinanceCollector(BaseCollector):
    """yfinance 기반 수집기. 사전 정의된 인덱스 구성종목을 수집."""

    def __init__(self, country_code: str, tickers: list[str]):
        self.country_code = country_code
        self._tickers = tickers
        self._sector_cache: dict[str, str] = {}

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """인덱스 구성종목의 가격 데이터를 yfinance로 배치 수집."""
        tickers = self._tickers
        logger.info(f"[{self.country_code}] yfinance 수집 시작: {len(tickers)}개 종목")

        dt = datetime.strptime(date, "%Y-%m-%d")
        start_date = (dt - timedelta(days=5)).strftime("%Y-%m-%d")
        end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

        batch_size = 200
        all_rows = []

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            batch_str = " ".join(batch)

            try:
                data = yf.download(
                    batch_str,
                    start=start_date,
                    end=end_date,
                    group_by="ticker",
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )

                if data.empty:
                    continue

                for ticker in batch:
                    try:
                        if len(batch) == 1:
                            ticker_data = data
                        else:
                            if ticker not in data.columns.get_level_values(0):
                                continue
                            ticker_data = data[ticker]

                        if ticker_data.empty or ticker_data["Close"].isna().all():
                            continue

                        # 최신 날짜 데이터
                        latest = ticker_data.dropna(subset=["Close"]).iloc[-1]
                        close_price = float(latest["Close"])
                        volume = float(latest["Volume"]) if pd.notna(latest["Volume"]) else 0

                        # 등락률 계산
                        daily_return = None
                        valid_data = ticker_data.dropna(subset=["Close"])
                        if len(valid_data) >= 2:
                            prev_close = float(valid_data.iloc[-2]["Close"])
                            if prev_close > 0:
                                daily_return = ((close_price - prev_close) / prev_close) * 100

                        all_rows.append({
                            "ticker": ticker,
                            "name": ticker.split(".")[0],
                            "sector": "기타",  # 나중에 보충
                            "market_cap": None,
                            "close_price": close_price,
                            "daily_return": daily_return,
                            "volume": volume,
                            "avg_volume_20d": None,
                        })
                    except Exception as e:
                        logger.debug(f"[{self.country_code}] {ticker} 처리 실패: {e}")
                        continue

            except Exception as e:
                logger.warning(f"[{self.country_code}] 배치 {i}-{i+batch_size} 실패: {e}")
                continue

            logger.info(f"[{self.country_code}] 배치 {min(i+batch_size, len(tickers))}/{len(tickers)} 완료")
            time.sleep(1)

        df = pd.DataFrame(all_rows)

        if not df.empty:
            df = self._add_sector_and_cap(df)

        logger.info(f"[{self.country_code}] 수집 완료: {len(df)}개 종목")
        return df

    def _add_sector_and_cap(self, df: pd.DataFrame) -> pd.DataFrame:
        """yfinance Ticker.info로 섹터와 시총 보충."""
        sectors = {}
        market_caps = {}

        for ticker in df["ticker"].tolist():
            try:
                info = yf.Ticker(ticker).info
                if not info:
                    continue

                # 섹터
                raw_sector = info.get("sector", "")
                if raw_sector:
                    gics = YF_SECTOR_TO_GICS.get(raw_sector, SECTOR_EN_TO_KR.get(raw_sector, "기타"))
                    sectors[ticker] = gics

                # 시총
                cap = info.get("marketCap")
                if cap:
                    market_caps[ticker] = float(cap)

                # 이름
                name = info.get("shortName") or info.get("longName")
                if name:
                    df.loc[df["ticker"] == ticker, "name"] = name

                time.sleep(0.2)  # 부하 방지
            except Exception as e:
                logger.debug(f"[{self.country_code}] {ticker} info 실패: {e}")
                continue

        if sectors:
            df["sector"] = df["ticker"].map(lambda t: sectors.get(t, "기타"))
        if market_caps:
            df["market_cap"] = df["ticker"].map(market_caps)

        return df


class JPCollector(YfinanceCollector):
    def __init__(self):
        super().__init__("JP", JP_TICKERS)


class DECollector(YfinanceCollector):
    def __init__(self):
        super().__init__("DE", DE_TICKERS)


class INCollector(YfinanceCollector):
    def __init__(self):
        super().__init__("IN", IN_TICKERS)
