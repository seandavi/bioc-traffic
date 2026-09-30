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
| `sql/access.sql` | DuckDB `access` view: both eras, `era` and `client_id` columns. `just duckdb`. |
| `sql/cloudflare_access.bq.sql` | The BigQuery normalising view, as extracted with `bq show`. |
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

`bioc-notify@.service` is shared with the sync timers and is installed from `bioc-edge`.
