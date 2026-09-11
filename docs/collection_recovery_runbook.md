# CN/VN collection recovery runbook

This runbook never reads or prints credential values. Verify only secret names,
workflow outcomes, structured failure codes, and stored data dates.

## China: restore the missing credential

1. Set `TUSHARE_TOKEN` through GitHub's masked prompt. Do not put the value in a
   command argument, shell history, issue, log, or PR description.

   ```bash
   gh secret set TUSHARE_TOKEN --repo lce99/MarketBot
   ```

2. Verify presence by name only.

   ```bash
   gh secret list --repo lce99/MarketBot --json name \
     --jq '.[] | select(.name == "TUSHARE_TOKEN") | .name'
   ```

3. Backfill the missing date. Preflight reports only present/missing state; it
   must never echo the value.

   ```bash
   gh workflow run collect_market.yml --repo lce99/MarketBot \
     -f market=CN -f date=2026-07-10
   ```

4. Confirm a successful `data: CN ...` commit and non-`기타` CN rows for the
   requested date before regenerating report data.

## Vietnam: resume a rate-limited run

The workflow commits the summary DB even when collection fails. This preserves
the structured failure log and the `collection_checkpoint` payload while the
final step still marks the workflow failed.

1. Re-run the same requested date with checkpoint resume enabled.

   ```bash
   gh workflow run collect_market.yml --repo lce99/MarketBot \
     -f market=VN -f date=2026-07-10 \
     -f mode=auto -f resume_from_checkpoint=true
   ```

2. If the provider rate-limits again, repeat the same command. Each failed run
   should advance `next_index` and commit the updated checkpoint.
3. Do not change `mode` while resuming unless intentionally discarding the old
   selection strategy. The restored rows are re-normalized from checkpoint
   industry metadata so legacy `기타` values are repaired.
4. Treat a run with zero mapped sectors as failed even when prices were fetched.
   A VN row set containing only `기타` cannot participate in lead-lag analysis.

## Post-recovery checks

- `collection_log`: latest CN/VN entry is `success`; prior failures keep their
  distinct `missing_secret`, `provider_rate_limited`, or sector metadata code.
- `sector_performance`: requested date exists and contains mapped sectors.
- `collection_checkpoint`: successful VN completion removes the pending row.
- `python -m scripts.evaluate_leadlag --format markdown`: country coverage and
  stored-prediction metrics are refreshed after report preparation.
