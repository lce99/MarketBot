# Dashboard analytics refresh

`scripts.refresh_dashboard_analytics` computes local trend scores, lead-lag scores,
and signal outcomes. It imports no reporting, bot, email, model API, or messaging
module. It does not fetch prices or rerun collection. A successful collection now
refreshes analytics inside the existing dashboard export workflow, under the shared
database writer lock. An explicit export dispatch with `refresh_analytics=true`
also refreshes them. No notification credentials are supplied to this workflow.

The additive `analytics` object in the schema version 1 export contains:

- `latestRefresh`: actual refresh timestamp, observation-based `asOfDate`,
  `inputDates` by active market, `trendInputMarkets`, and computed row/outcome counts.
- `trends`, `leadLag`, `flowSignals`: actual row `latestDate`, `calendarDaysOld`,
  `stale`, `evaluatedThrough`, and `rowCount`.

`coverage.freshMarkets` describes collected sector observations only. Derived
sections have independent freshness. A current export timestamp does not reset
any observation or signal creation date. If no new signal qualifies, historical
signal rows retain their dates and stale flags while `evaluatedThrough` records
the current evaluation date. A zero new-signal count is a valid computation result.

The existing trend calculation uses same-date observations. For a September 30
refresh before the US and German sessions close, `trendInputMarkets` contains the
four Asian markets; `inputDates` explicitly retains US/Germany September 29.
Lead-lag calculations use available history. These counts do not imply all
securities, all sectors, or six simultaneous closing snapshots are covered.

`docs/recovery-provenance.json` identifies only the three audited September 30
Japan/India/Germany recovery log rows whose configured provider label incorrectly
said Finnhub. An explicit dispatch with `correct_recovery_provenance=true` applies
the precise, idempotent corrections. Future yfinance collections log their actual
provider directly. Other historical log rows and China history are preserved.
