# bioc-traffic

Everything downstream of the Logpush delivery of Bioconductor access logs. The producer — the
Worker's per-request access record — lives in `bioc-edge`; the record is the contract across
that seam. This repo is private: it names buckets, job IDs and secrets.

| Piece | What |
|---|---|
| `ANALYTICS.md` | What exists, where, and how to query it. Start here. |
| `check-logpush.sh` + `systemd/` | Daily gap alarm on yesterday's delivery (ADR 0003): every UTC hour present, plus a minimum object count. `./check-logpush.sh 20260928` checks any day. Timer is installed on onclappc02. |
| `cloudfront-logs-to-parquet.py` | The one-shot CloudFront-era mirror to Parquet, with `--verify`. |
| `cloudflare-logs-to-parquet.py` + `justfile` | Logpush records → hourly Parquet (local, then R2), with `--verify` (#8). `just --list`. |
| `sql/access.sql` | DuckDB `access` view: both eras, `era` and `client_id` columns. `just duckdb` (also loads `client_class.sql` and `downloads.sql`). |
| `sql/cloudflare_access.bq.sql` | The old BigQuery normalising view, kept for reference; BigQuery is no longer used (#7). |
| `download-stats.py` + `sql/downloads.sql` | Package download stats across both eras (#11): monthly client partitions, Parquet aggregates with fixed and human/automated columns, the `/packages/stats/` tree (ADR 0004). `just stats`; see `ANALYTICS.md`. |
| `sql/rollup_tier.sql`, `sql/rollup_overall.sql` | Dashboard rollups (#10): minute/6 h, hour/30 d, day/90 d by class, status, country, UA family, page, referrer, cache, package and `bioc_version`; plus the forever per-day overall series, both eras. Static Parquet + JSON in `$BIOC_ROLLUPS` (default `/data/davsean/bioc-traffic-rollups`), then R2 `rollups/`. `just rollup minute\|hour\|day`. |
| `sql/client_class.sql` | `client_class_v0(...)`: per-request traffic class for both eras (#5), DuckDB macros. Check: `duckdb -c ".read sql/client_class.sql" -c ".read sql/client_class_check.sql"`. |

Decisions are cross-repo and live in `bioc-infrastructure/adr` — 0002 (mirror unfiltered),
0003 (logging after the cutover), 0004 (stats are static files), 0012 (raw addresses, hashed in
views).

## Install the timers

```bash
ln -sf "$PWD"/systemd/bioc-logpush-check{,-stale}.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now bioc-logpush-check.timer
# Seed the dead-man stamp before arming its timer, or it alerts at the next noon.
systemctl --user start bioc-logpush-check.service
systemctl --user enable --now bioc-logpush-check-stale.timer
```

`bioc-logpush-check-stale` is the dead-man: it fails if the check has not passed in 36h
(`~/.local/state/bioc-logpush-check.ok`), whether the timer stopped or the check keeps failing.
`LOGPUSH_PREFIX` takes `gs://…` (gcloud) or an rclone `remote:path` such as `r2:…`.

The Parquet timers (every 15 min: current and previous UTC hour; 08:00 UTC: re-seal
yesterday) are installed on onclappc02:

```bash
ln -sf "$PWD"/systemd/bioc-cloudflare-parquet{,-reseal}.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now bioc-cloudflare-parquet.timer bioc-cloudflare-parquet-reseal.timer
```

The daily download-stats timer (09:30 UTC, after the re-seal) is **not installed yet**:

```bash
ln -sf "$PWD"/systemd/bioc-download-stats.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now bioc-download-stats.timer
```

The rollup timers (minute tier every 15 min, ~3 s; hour tier hourly, ~25 min; day tier and the
overall series at 10:30 UTC, ~50 min) are **not installed yet**:

```bash
ln -sf "$PWD"/systemd/bioc-rollup-{minute,hour,day}.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now bioc-rollup-minute.timer bioc-rollup-hour.timer bioc-rollup-day.timer
```

`bioc-notify@.service` is shared with the sync timers and is installed from `bioc-edge`.
