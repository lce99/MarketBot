# Lead-lag validation through 2026-07-10

Source state: `report-data: 2026-07-10` (`cd9edf9`). The evaluation reads only
stored `flow_signals` with `status='verified'`.

## Collection coverage

| country | first date | latest date | dates | sector rows | 2026-07-10 state |
|---|---|---|---:|---:|---|
| CN | - | - | 0 | 0 | preflight failed: missing secret |
| VN | 2026-06-10 | 2026-07-09 | 22 | 22 | provider rate-limited; no commit |
| JP | 2026-04-21 | 2026-07-10 | 55 | 605 | success |
| KR | 2026-02-06 | 2026-07-10 | 66 | 716 | success |
| IN | 2026-02-12 | 2026-07-10 | 92 | 1,012 | success |
| DE | 2026-02-11 | 2026-07-10 | 102 | 1,020 | success |
| US | 2026-02-11 | 2026-07-10 | 78 | 544 | success |

Every stored VN sector row is `기타`, which `src.leadlag` intentionally excludes.
CN and VN therefore contribute zero predictions even though VN has successful
collection commits before July 10.

## Overall result

- Verified: **116** signals, **62** hits over 20 creation dates and 22 target dates.
- Accuracy: **53.4%**, 95% Wilson CI **44.4%–62.3%**.
- Naive baseline: always choose the ex-post majority realised direction in the
  evaluated slice; overall this is `up`, **59.5%**.
- Excess versus naive: **-6.0pp**.
- There are 106 distinct `(target_date, follower, sector)` outcomes for 116
  signals. Shared outcomes make signals correlated, so Wilson intervals are
  descriptive and may be too narrow.

## Country and pair bias

| slice | n | accuracy | 95% CI | naive | excess |
|---|---:|---:|---:|---:|---:|
| follower JP | 74 | 60.8% | 49.4%–71.1% | 63.5% | -2.7pp |
| follower KR | 16 | 25.0% | 10.2%–49.5% | 68.8% | -43.8pp |
| follower DE | 13 | 69.2% | 42.4%–87.3% | 53.8% | +15.4pp |
| follower IN | 13 | 30.8% | 12.7%–57.6% | 76.9% | -46.2pp |
| DE→JP | 51 | 58.8% | 45.2%–71.2% | 62.7% | -3.9pp |
| US→JP | 20 | 65.0% | 43.3%–81.9% | 65.0% | 0.0pp |
| US→KR | 10 | 30.0% | 10.8%–60.3% | 70.0% | -40.0pp |

JP is 63.8% of all follower observations and DE is 54.3% of leaders. This is a
coverage/selection bias, not evidence that those countries are structurally the
best leader or follower.

## Sector, lag, and direction

| slice | n | accuracy | 95% CI | naive | excess |
|---|---:|---:|---:|---:|---:|
| 산업재 | 27 | 59.3% | 40.7%–75.5% | 51.9% | +7.4pp |
| 헬스케어 | 21 | 23.8% | 10.6%–45.1% | 66.7% | -42.9pp |
| 유틸리티 | 12 | 41.7% | 19.3%–68.0% | 83.3% | -41.7pp |
| 경기소비재 | 11 | 54.5% | 28.0%–78.7% | 81.8% | -27.3pp |
| 소재 | 10 | 80.0% | 49.0%–94.3% | 50.0% | +30.0pp |
| 정보기술 | 10 | 40.0% | 16.8%–68.7% | 80.0% | -40.0pp |
| 필수소비재 | 10 | 90.0% | 59.6%–98.2% | 80.0% | +10.0pp |
| lag 1 | 85 | 56.5% | 45.9%–66.5% | 65.9% | -9.4pp |
| lag 2 | 31 | 45.2% | 29.2%–62.2% | 58.1% | -12.9pp |
| up prediction | 63 | 61.9% | 49.6%–72.9% | 61.9% | 0.0pp |
| down prediction | 53 | 43.4% | 31.0%–56.7% | 56.6% | -13.2pp |

No slice has a 95% Wilson lower bound above its naive baseline. Materially high
sector rates are still small-sample hypotheses, not validated edges. The current
stored evidence rejects the previous `weak support` scoreboard label.
