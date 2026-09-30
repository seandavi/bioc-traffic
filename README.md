# bioc-traffic

Everything downstream of the Logpush delivery of Bioconductor access logs. The producer — the
Worker's per-request access record — lives in `bioc-edge`; the record is the contract across
that seam. This repo is private: it names buckets, job IDs and secrets.

| Piece | What |
|---|---|
| `ANALYTICS.md` | What exists, where, and how to query it. Start here. |
| `check-logpush.sh` + `systemd/` | Daily gap alarm on yesterday's delivery (ADR 0003). Timer is installed on onclappc02. |
| `cloudfront-logs-to-parquet.py` | The one-shot CloudFront-era mirror to Parquet, with `--verify`. |
| `cloudflare-logs-to-parquet.py` + `justfile` | Logpush records → hourly Parquet (local, then R2), with `--verify` (#8). `just --list`. |
| `sql/access.sql` | DuckDB `access` view: both eras, `era` and `client_id` columns. `just duckdb`. |
| `sql/cloudflare_access.bq.sql` | The BigQuery normalising view, as extracted with `bq show`. |

Decisions are cross-repo and live in `bioc-infrastructure/adr` — 0002 (mirror unfiltered),
0003 (logging after the cutover), 0004 (stats are static files), 0012 (raw addresses, hashed in
views).

## Install the timers

```bash
ln -sf "$PWD"/systemd/bioc-logpush-check.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now bioc-logpush-check.timer
```

The Parquet timers (every 15 min: current and previous UTC hour; 08:00 UTC: re-seal
yesterday) are in `systemd/` but **not installed yet**. Same pattern:

```bash
ln -sf "$PWD"/systemd/bioc-cloudflare-parquet{,-reseal}.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now bioc-cloudflare-parquet.timer bioc-cloudflare-parquet-reseal.timer
```

`bioc-notify@.service` is shared with the sync timers and is installed from `bioc-edge`.
