# CN/VN 수집 안정화 SW 요구사항정의서

- 문서 버전: v0.1
- 작성일: 2026-04-23
- 대상 시스템: `MarketBot`
- 대상 범위: 중국(`CN`) / 베트남(`VN`) 시장 수집 파이프라인, 운영 모니터링, 장애 대응

## 1. 목적

중국과 베트남 시장 수집이 최근 스케줄 실행에서 반복 실패하고 있어, 원인을 운영 설정 문제와 구조적 수집 문제로 분리하고 안정화 요구사항을 정의한다. 본 문서는 구현 우선순위와 수용 기준의 기준 문서로 사용한다.

## 2. 현행 장애 사실

### 2.1 중국(`CN`)

- 2026-04-23 GitHub Actions `Collect Market Data` run `24826869328` 실패
- 실패 시각: 2026-04-23 09:10 UTC
- 실패 단계: `Validate required credentials`
- 확인된 직접 원인: `TUSHARE_TOKEN` 미설정
- 실제 로그 요약:
  - 요청 마켓은 `CN`
  - `TUSHARE_TOKEN` 값이 비어 있음
  - 워크플로가 `Missing required workflow secrets: - CN requires TUSHARE_TOKEN`으로 즉시 종료

정리하면 `CN` 실패는 데이터 소스 응답 문제가 아니라 운영 비밀값 관리 실패다.

### 2.2 베트남(`VN`)

- 2026-04-23 GitHub Actions `Collect Market Data` run `24829237458` 실패
- 실패 시각: 2026-04-23 10:06 UTC
- 실패 단계: `Collect market data`
- 확인된 직접 원인:
  - `VN` 캐시 유니버스가 없어 전체 종목 1,538개 대상 full rebuild 수행
  - `vnstock` Guest 제한인 분당 20회 요청 초과
  - 외부 라이브러리가 `Rate limit exceeded`를 출력하고 프로세스를 종료
- 실제 로그 요약:
  - `[VN] 전체 종목: 1538개`
  - `[VN] cached universe missing, use full rebuild`
  - `Giới hạn: 20 requests/phút`
  - `Đã sử dụng: 20/20`
  - `Chờ 46 giây để tiếp tục`

정리하면 `VN` 실패는 단순 일시 장애가 아니라 현재 수집 방식이 공급자 호출 한도와 구조적으로 맞지 않는 문제다.

### 2.3 로컬 운영 로그 관찰

- 로컬 `data/marketbot.db` 기준 최근 `CN` 성공/실패 로그는 2026-04-08 이후 갱신이 끊겨 있음
- 로컬 `data/marketbot.db` 기준 최근 `VN` 로그는 2026-04-20까지 반복 실패로 기록됨
- 현재 `collection_log.error_message`에는 주로 `데이터 없음`만 남아 실제 원인인 `missing secret`, `rate limit`, `provider terminated`가 손실됨

## 3. 문제 정의

현재 시스템은 다음 두 종류의 장애를 충분히 구분하지 못한다.

1. 운영 설정 장애
   - 예: 필수 secret 누락
2. 외부 공급자 제약 기반 장애
   - 예: 호출 한도, 차단, 응답 구조 변경

이로 인해 운영자는 같은 `데이터 없음` 메시지 아래에서 서로 다른 근본 원인을 수동으로 찾아야 하며, 자동 복구 또는 우회 전략도 작동하지 않는다.

## 4. 목표

### 4.1 비즈니스 목표

- `CN`과 `VN`의 평일 정기 수집을 사람이 매일 수동 개입하지 않아도 유지한다.
- 장애 발생 시 원인을 즉시 구분 가능한 형태로 기록하고 알림으로 전달한다.
- `VN`은 공급자 한도 안에서 점진적으로 수집하거나 안전하게 축소 운전할 수 있어야 한다.

### 4.2 시스템 목표

- `CN` secret 누락 장애를 수집 시작 전에 탐지하고 운영자에게 명확히 알린다.
- `VN` 수집이 외부 한도 초과로 즉시 죽지 않도록 요청 속도, 재시도, 캐시 활용, 부분 수집 전략을 갖춘다.
- `collection_log`와 관리자 알림이 실제 실패 원인을 보존한다.

## 5. 범위

### 5.1 포함 범위

- GitHub Actions 스케줄 수집 진입부
- `scripts.collect`
- `src.collectors.china`
- `src.collectors.vietnam`
- `src.collectors.base`
- `src.monitor`
- `collection_log` 기반 운영 상태 판단과 관리자 알림

### 5.2 제외 범위

- 일일 리포트 포맷 개편
- 다른 국가(`US`, `KR`, `JP`, `IN`, `DE`) 수집 방식 전면 개편
- 외부 유료 데이터 공급자 즉시 도입

## 6. 사용자 및 운영 시나리오

### 6.1 운영자

- 운영자는 매일 `CN`과 `VN` 수집이 정상 완료되었는지 확인하고 싶다.
- 장애가 발생하면 "왜 실패했는지"를 텔레그램 알림과 상태 보고서에서 즉시 알고 싶다.
- 수동 재실행 시 full rebuild와 incremental 실행을 선택할 수 있기를 원한다.

### 6.2 시스템

- 스케줄 실행 시 필요한 비밀값과 캐시 상태를 먼저 점검해야 한다.
- 외부 공급자 한도 초과가 감지되면 안전한 대기, 재시도, 축소 운전 또는 명시적 실패 분류가 가능해야 한다.

## 7. 기능 요구사항

### FR-1. 사전 점검 단계 분리

- 수집 시작 전에 시장별 preflight check를 수행해야 한다.
- preflight check는 최소 다음 항목을 검증해야 한다.
  - 필수 secret 존재 여부
  - 최근 유니버스 캐시 존재 여부
  - 수집 모드(full/incremental) 결정 가능 여부
- preflight 실패 시 실제 수집 단계로 진입하지 않아야 한다.
- preflight 실패 결과는 `collection_log`에 별도 원인 코드와 함께 남아야 한다.

### FR-2. 중국(`CN`) credential 관리

- `CN` 수집은 `TUSHARE_TOKEN` 누락 시 `missing_credentials`로 분류되어야 한다.
- `CN` secret 누락 시 관리자 알림에는 다음 정보가 포함되어야 한다.
  - 시장 코드
  - 실패 시각
  - 누락된 secret 이름
  - 최근 성공일
- 운영 상태 보고서에서 `CN`은 단순 `데이터 없음`이 아니라 `설정 오류`로 식별 가능해야 한다.

### FR-3. 베트남(`VN`) 수집 모드 제어

- `VN` 수집기는 `full`, `incremental`, `seed` 모드를 지원해야 한다.
- 기본 모드는 `incremental`이어야 한다.
- 캐시 유니버스가 없을 때만 `seed` 또는 제한된 `full` 모드로 진입해야 한다.
- `VN` 수집기는 1회 실행에서 처리할 최대 종목 수를 설정값으로 제한할 수 있어야 한다.
- `VN` 수집기는 요청 속도 제한값(requests per minute)을 설정으로 가져와야 한다.

### FR-4. 베트남(`VN`) rate limit 대응

- 외부 응답 또는 예외에서 rate limit 상황을 식별할 수 있어야 한다.
- rate limit 감지 시 시스템은 다음 중 하나를 수행해야 한다.
  - 남은 대기 시간만큼 sleep 후 재시도
  - 현재 실행을 checkpoint 후 중단하고 다음 실행에서 재개
  - 축소 대상만 수집하고 부분 성공으로 종료
- 어떤 전략을 택했는지 로그에 남겨야 한다.
- rate limit 실패는 `데이터 없음`으로 치환되면 안 된다.

### FR-5. 유니버스 캐시 및 seed 전략

- `VN` 유니버스 캐시가 없으면 최초 seed 전략이 동작해야 한다.
- seed 전략은 전체 1,500여 종목을 한 번에 조회하지 않고 배치 단위로 저장해야 한다.
- seed 중 중단되더라도 다음 실행에서 이어서 진행할 수 있어야 한다.
- 최근 성공한 유니버스가 있으면 우선 재사용해야 한다.
- 캐시 유니버스가 stale 상태여도 즉시 full rebuild로 전환하지 말고, 제한된 증분 보강을 우선 시도해야 한다.

### FR-6. 실패 분류 체계

- `collection_log`는 최소 다음 실패 코드를 구분해야 한다.
  - `missing_credentials`
  - `provider_rate_limited`
  - `provider_error`
  - `schema_changed`
  - `no_data`
  - `unexpected_exception`
- 사용자용 메시지와 내부 저장용 코드는 분리 가능해야 한다.
- 상태 보고서와 관리자 알림은 실패 코드 기준으로 요약되어야 한다.

### FR-7. 관리자 알림 강화

- 관리자 알림은 시장별 실패 원인과 최근 성공 시점을 함께 포함해야 한다.
- 동일 원인 반복 실패 시 중복 알림 폭주를 방지해야 한다.
- `CN` secret 누락과 `VN` rate limit은 서로 다른 알림 문구를 사용해야 한다.

### FR-8. 수동 운영 제어

- 수동 실행 시 운영자는 다음 옵션을 지정할 수 있어야 한다.
  - `--market`
  - `--date`
  - `--mode full|incremental|seed`
  - `--max-tickers`
  - `--resume-from-checkpoint`
- 수동 실행 결과는 스케줄 실행과 동일한 로그 체계를 사용해야 한다.

## 8. 비기능 요구사항

### NFR-1. 신뢰성

- `CN` 스케줄 실패 중 secret 누락 계열은 원인 식별 성공률 100%여야 한다.
- `VN` 스케줄 실패 중 rate limit 계열은 원인 식별 성공률 100%여야 한다.
- `VN` incremental 모드는 외부 한도 내에서 평일 자동 실행 성공률 95% 이상을 목표로 한다.

### NFR-2. 성능

- `CN` preflight는 10초 이내 완료되어야 한다.
- `VN` incremental 기본 실행은 15분 이내 완료를 목표로 한다.
- `VN` seed/full 실행은 장시간 실행 가능하지만, 배치 단위 checkpoint가 필수다.

### NFR-3. 관측 가능성

- 모든 실패는 단계, 원인 코드, 원문 에러, 시장 코드, 실행 모드를 포함해야 한다.
- GitHub Actions 로그만 보지 않아도 `collection_log`와 `/status`에서 주요 원인을 파악할 수 있어야 한다.

### NFR-4. 유지보수성

- 공급자별 rate limit 정책과 수집 모드는 코드 상수 하드코딩 대신 설정값으로 관리해야 한다.
- 새로운 공급자 fallback이 추가되더라도 `BaseCollector` 공통 로깅 체계는 유지되어야 한다.

## 9. 데이터 및 로그 요구사항

- `collection_log`는 다음 정보 확장을 지원해야 한다.
  - `failure_code`
  - `failure_stage`
  - `run_mode`
  - `provider`
  - `raw_error_excerpt`
- `VN` checkpoint 저장소는 다음 정보를 지원해야 한다.
  - 마지막 처리 ticker 또는 배치 번호
  - 누적 저장 건수
  - 다음 재개 위치

## 10. 수용 기준

### AC-1. 중국(`CN`)

- `TUSHARE_TOKEN`이 비어 있으면 워크플로는 수집 전에 실패한다.
- 실패 로그에는 `missing_credentials`와 `TUSHARE_TOKEN`이 남는다.
- 관리자 알림에서 `CN 설정 오류`가 식별된다.

### AC-2. 베트남(`VN`)

- 캐시 유니버스가 없는 상태에서도 전체 종목을 즉시 무제한 조회하지 않는다.
- rate limit 응답을 만나면 `provider_rate_limited`로 기록된다.
- 동일 실행 또는 후속 실행에서 checkpoint 기반 재개가 가능하다.
- 최소 한 번의 성공 실행 후에는 다음 평일 실행이 incremental 모드로 동작한다.

### AC-3. 운영 상태

- `/status` 또는 동등한 상태 보고서에서 `CN`과 `VN`의 실패 원인이 구분되어 보인다.
- 로컬 DB의 최근 실패 로그만 봐도 `데이터 없음` 외의 실제 원인을 확인할 수 있다.

## 11. 우선순위

### P0

- `CN` secret 누락 원인 보존 및 관리자 알림
- `VN` rate limit 원인 보존
- `VN` full rebuild 진입 차단 또는 제한

### P1

- `VN` checkpoint/resume
- `VN` seed 모드 도입
- `/status` 실패 분류 노출

### P2

- `VN` 추가 공급자 fallback 검토
- 반복 실패 자동 완화 정책

## 12. 제안 구현 단계

### 1단계. 운영 가시성 복구

- 실패 분류 코드 도입
- `collection_log` 확장
- 관리자 알림 개선
- `CN` secret 누락 명시화

### 2단계. `VN` 안전 운전 모드 도입

- 모드 분리(`full`, `incremental`, `seed`)
- 최대 종목 수 제한
- rate limit 감지 및 재시도/중단 전략 구현
- 캐시 없을 때 무제한 full rebuild 방지

### 3단계. `VN` 지속 실행 구조화

- checkpoint/resume
- seed 완료 후 incremental 자동 전환
- stale 캐시 점진 보강

## 13. 오픈 이슈

- `VN`에서 무료 API key 기반 Community 한도를 사용할지 여부
- `VN` fallback 공급자를 추가할지 여부
- `CN` secret 상태를 별도 heartbeat 또는 사전 진단 작업으로 분리할지 여부

## 14. 결론

현재 `CN` 장애는 운영 설정 복구가 우선이고, `VN` 장애는 수집 전략 자체를 한도 친화적으로 다시 설계해야 해결된다. 따라서 구현 우선순위는 "원인 보존 및 알림"을 먼저, "VN 안전 운전 및 재개 구조"를 다음으로 두는 것이 적절하다.
