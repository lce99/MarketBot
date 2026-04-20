# MarketBot

주요 글로벌 시장의 섹터 데이터를 수집하고, 요약 DB를 Git으로 관리하면서 텔레그램으로 일간 리포트를 보내는 Python 봇입니다.

지원 시장:

- `US` 미국
- `KR` 한국
- `CN` 중국
- `JP` 일본
- `VN` 베트남
- `IN` 인도
- `DE` 독일

## 구조

- `src/collectors/`: 국가별 수집기
- `src/analyzer.py`: 글로벌 트렌드 스코어 계산
- `src/reporter.py`: 텔레그램 리포트 포맷팅
- `src/bot.py`: 텔레그램 봇 명령과 자동 전송
- `src/monitor.py`: `/status` 상태 요약과 관리자 실패 알림
- `scripts/collect.py`: 시장 수집 진입점
- `scripts/report.py`: 리포트용 파생 데이터 계산과 전송
- `scripts/checkpoint_db.py`: DB 체크포인트와 summary/raw 분리 마이그레이션
- `data/marketbot.db`: Git에 커밋하는 summary DB
- `data/marketbot_raw.db`: 로컬과 GitHub artifact에만 남기는 raw DB

## 빠른 시작

### 1. 의존성 설치

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env.example`를 참고해서 루트에 `.env`를 만듭니다.

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
TELEGRAM_ALERT_CHAT_ID=your_alert_chat_id_here
FINNHUB_API_KEY=your_finnhub_api_key_here
TUSHARE_TOKEN=your_tushare_token_here
```

메모:

- `TELEGRAM_*`이 없으면 리포트는 콘솔로 출력됩니다.
- `TELEGRAM_ALERT_CHAT_ID`를 지정하면 수집 실패 알림을 별도 채팅으로 보낼 수 있습니다.
- `FINNHUB_API_KEY`는 미국/일본/인도/독일 수집에 필요합니다.
- `TUSHARE_TOKEN`은 중국 수집에 필요합니다.

### 3. 시장 수집

```bash
python -m scripts.collect --market KR --date 2026-04-20
python -m scripts.collect --market US
python -m scripts.collect --market ALL
python -m scripts.collect --market BENCHMARK
```

거래일 데이터가 없으면 최근 거래일을 fallback으로 사용하고, 실제 저장일은 해당 거래일 기준으로 기록됩니다.

### 4. 리포트 생성과 전송

파생 데이터만 계산:

```bash
python -m scripts.report --prepare-only --date 2026-04-20
```

이미 계산된 DB를 기준으로 전송:

```bash
python -m scripts.report --skip-analyze --date 2026-04-20
```

계산 후 바로 전송:

```bash
python -m scripts.report --date 2026-04-20
```

### 5. 운영 상태 확인

텔레그램 봇에서 아래 명령으로 상태를 볼 수 있습니다.

```text
/status
/status KR VN
```

표시 내용:

- 마지막 성공일
- 최근 실패 시장
- stale 시장
- 시장별 최신 데이터 날짜

## 저장 전략

이 프로젝트는 DB를 두 층으로 나눠 운영합니다.

- `data/marketbot.db`
  - Git에 커밋되는 summary DB
  - 포함 테이블: `sector_performance`, `abnormal_stock_summary`, `benchmark_daily`, `trend_scores`, `collection_log`
- `data/marketbot_raw.db`
  - Git에는 올리지 않는 raw DB
  - 포함 테이블: `stock_daily`
  - 로컬 캐시와 GitHub Actions artifact 용도

핵심 원칙:

- 리포트와 운영 상태 확인은 summary DB만으로 가능해야 합니다.
- 용량이 커지는 raw 종목 데이터는 Git에서 분리합니다.
- `scripts/checkpoint_db.py`는 legacy `stock_daily`가 summary DB에 남아 있으면 raw DB로 옮기고 summary DB를 compact합니다.

## GitHub Actions

현재 워크플로는 아래 순서로 동작합니다.

- `collect_market.yml`
  - 시장 수집
  - 벤치마크 수집
  - DB 체크포인트와 legacy raw 마이그레이션
  - `data/marketbot_raw.db` artifact 업로드
  - `data/marketbot.db`만 Git 커밋
- `daily_report.yml`
  - 리포트용 파생 데이터 계산
  - DB 체크포인트
  - `data/marketbot.db`만 Git 커밋
  - 텔레그램 전송
- `smoke_tests.yml`
  - 외부 API 없이 도는 기본 스모크 테스트 실행

## 체크포인트

Git에 커밋하기 전에 아래 명령으로 WAL과 summary/raw 분리를 정리할 수 있습니다.

```bash
python -m scripts.checkpoint_db
```

이 명령은 다음을 수행합니다.

- summary DB와 raw DB WAL 체크포인트
- legacy `stock_daily`를 raw DB로 이동
- summary DB의 비정상 종목 요약 backfill
- summary DB compact

## 테스트

```bash
python -m unittest discover -s tests -v
```

포함 범위:

- `scripts.collect` CLI 종료 코드
- `scripts.report` 파생 데이터 계산
- `src.reporter` 리포트 포맷
- 운영 상태 모니터링
- summary/raw 저장 전략 마이그레이션

## 로컬 봇 실행

텔레그램 봇을 polling 모드로 띄우려면:

```bash
python -m src.bot
```
