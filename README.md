# MarketBot

글로벌 주요 시장의 섹터 데이터를 수집하고, SQLite에 저장한 뒤, 텔레그램으로 일간 리포트를 보내는 Python 봇입니다.

현재 지원 시장:

- `US` 미국
- `KR` 한국
- `CN` 중국
- `JP` 일본
- `VN` 베트남
- `IN` 인도
- `DE` 독일

## 구조

- `src/collectors/`: 국가별 수집기
- `src/analyzer.py`: 국가별 섹터 데이터를 바탕으로 글로벌 트렌드 스코어 계산
- `src/reporter.py`: 텔레그램 메시지 포맷팅
- `src/bot.py`: 텔레그램 봇 명령어와 자동 발송
- `scripts/collect.py`: 시장 데이터 수집 진입점
- `scripts/report.py`: 리포트용 파생 데이터 계산 및 전송 진입점
- `scripts/checkpoint_db.py`: SQLite WAL 체크포인트
- `data/marketbot.db`: 저장소에 커밋되는 운영 DB 스냅샷

## 빠른 시작

### 1. 의존성 설치

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env.example`을 참고해 루트에 `.env` 파일을 만듭니다.

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
FINNHUB_API_KEY=your_finnhub_api_key_here
TUSHARE_TOKEN=your_tushare_token_here
```

메모:

- `TELEGRAM_*`이 없으면 리포트는 콘솔로 출력됩니다.
- `FINNHUB_API_KEY`는 미국 시장 수집에 필요합니다.
- `TUSHARE_TOKEN`은 중국 시장 수집에 필요합니다.
- 베트남 수집은 `vnstock` 라이브러리를 사용하므로 별도 키가 필요하지 않습니다.

### 3. 단일 시장 수집

```bash
python -m scripts.collect --market KR --date 2026-04-20
python -m scripts.collect --market US
python -m scripts.collect --market BENCHMARK
```

실제 거래일 데이터가 없으면 최근 거래일을 역탐색해서 사용하며, 사용한 날짜는 DB에 실제 날짜로 저장됩니다.

### 4. 리포트 생성/전송

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

### 5. SQLite 체크포인트

이 프로젝트는 SQLite `WAL` 모드를 사용합니다. DB를 Git에 커밋하기 전에 아래 명령으로 메인 DB 파일에 반영할 수 있습니다.

```bash
python -m scripts.checkpoint_db
```

## GitHub Actions

현재 워크플로는 아래 순서로 동작합니다.

- `collect_market.yml`: 시장 수집 -> 벤치마크 수집 -> SQLite 체크포인트 -> DB 커밋
- `daily_report.yml`: 리포트용 파생 데이터 계산 -> SQLite 체크포인트 -> DB 커밋 -> 텔레그램 전송
- `smoke_tests.yml`: 외부 API 없이 돌아가는 기본 스모크 테스트 실행

## 테스트

외부 API를 치지 않는 최소 스모크 테스트가 포함되어 있습니다.

```bash
python -m unittest discover -s tests -v
```

포함 범위:

- `scripts.collect` CLI의 실패/성공 종료 코드
- `scripts.report`의 트렌드 스코어 계산 경로
- `src.reporter`의 기본 메시지 포맷

## 운영 메모

- `data/marketbot.db`는 결과물 스냅샷이라서 파일 크기가 커질 수 있습니다.
- `*.db-wal`, `*.db-shm`은 Git에 올리지 않도록 제외되어 있습니다.
- Windows 콘솔에서 한글이 깨지면 `chcp 65001` 후 다시 실행해 보세요.

## 로컬 봇 실행

텔레그램 봇을 polling 모드로 띄우려면:

```bash
python -m src.bot
```
