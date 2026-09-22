# Investment Office public export

`python scripts/export_dashboard.py --source-commit FULL_SHA` reads the summary SQLite database in read-only mode and writes `docs/data/market-dashboard.json`. Only Python's standard library is required. No model API, notifications, raw stock table, watchlist, account data, environment values or error bodies are included.

The `Export Investment Office Dashboard` workflow runs after Collect Market Data and Send Daily Report complete (including failed runs, retaining stored observations), on exporter/workflow changes, or by manual dispatch. It checks out current main, shares the existing database writer lock and commits only the JSON. It does not modify the database.

The public Investment Office at https://investment-office-kikoho.lostvalue887885.chatgpt.site/#/market loads this feed. Each country retains its own observation date; missing/unclassified sectors and stale data remain visible. Sector returns are filtered-stock simple averages, not cap-weighted benchmarks, capital inflows or earnings revisions. Lead/lag and direction-hit statistics remain exploratory.

The Site keeps a fallback snapshot and its existing daily archive task refreshes both MarketBot and the report-collector archive. A browser refresh alone is not durable Site storage. Original source history stays in this repository.
