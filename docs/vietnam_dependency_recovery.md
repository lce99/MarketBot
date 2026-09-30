# Vietnam dependency outage, September 2026

The collection install in [run 36651461970](https://github.com/lce99/MarketBot/actions/runs/36651461970)
failed before collection: `vnstock>=0.3.0` had no available distribution.
[PyPI](https://pypi.org/project/vnstock/) marks the whole project quarantined on
September 24, 2026. Its published metadata requires Python >=3.10 and lists
Python 3.11 support. Changing the runner's Python version or pinning an older
vnstock release therefore does not fix availability. Do not install the
quarantined package from a direct wheel URL or Git checkout.

MarketBot now uses its own bounded HTTP client for the existing public KBS and
Vietcap endpoints. No vnstock code is installed or vendored. KBS listing covers
HOSE, HNX and UPCOM equities and supplies Vietnamese industries; warrants and
other security types are excluded. Daily prices are normalized from VND to the
existing thousands-of-VND collector contract; volume stays in shares. VCI's
English ICB names retain broad taxonomy fallback. Every HTTP call uses the
existing request throttle and a timeout.

Rate limits, HTTP errors and malformed responses raise structured failures.
Source fallback and cached listing metadata remain available, but cached prices
are never substituted for fresh history. If both sources fail for a ticker,
collection stops and saves its checkpoint rather than reporting a partial batch
as successful. An all-unmapped sector batch also fails. The existing production
workflow still skips later database commits on failure; durable checkpoint
recovery is separate work proposed in draft PR #3, which this fix does not edit.

## Validation and operation

`Smoke Tests` now includes a Python 3.11 production-requirements installation,
`pip check`, and imports of collection/reporting entrypoints without invoking
them. Unit tests use mocked provider responses and temporary databases.

Local read-only adapter checks on September 30 returned 1,522 KBS equities
across HOSE/HNX/UPCOM, including 697 mapped sector records. VCB history returned
17 September 1–25 bars with September 25 close 58.0 (thousands of VND) and volume
4,390,100 shares, matching the raw public response. Vietcap returned HTTP 403 from
the local environment; its fallback normalization is covered by fixtures, but
live access from GitHub runners remains unverified. No production collection,
report preparation or Telegram delivery was run as part of this fix.

After review and merge, verify the next scheduled production run's install and
per-market collection outcomes before treating data as recovered. China still
requires its existing credential; this change does not configure credentials.

A successful dashboard export only proves the snapshot was serialized. Its
`generatedAt` is export time. `latestDate`, each market's `latestDate`,
`calendarDaysOld`, and `stale` describe the underlying observations. In the
September 30 audit, US observations ended September 23, KR/JP/VN/IN/DE ended
September 24, and CN was absent; all seven markets were stale. The exporter
preserves those flags and prints the stale/missing market list even when the
export succeeds. Historical gaps need separate production recovery validation.
