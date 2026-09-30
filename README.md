# bioc-traffic

Everything downstream of the Logpush delivery of Bioconductor access logs. The producer — the
Worker's per-request access record — lives in `bioc-edge`; the record is the contract across
that seam. This repo is private: it names buckets, job IDs and secrets.

| Piece | What |
|---|---|
| `ANALYTICS.md` | What exists, where, and how to query it. Start here. |
| `check-logpush.sh` + `systemd/` | Daily gap alarm on yesterday's delivery (ADR 0003). Timer is installed on onclappc02. |
| `cloudfront-logs-to-parquet.py` | The one-shot CloudFront-era mirror to Parquet, with `--verify`. |
| `sql/cloudflare_access.bq.sql` | The BigQuery normalising view, as extracted with `bq show`. |
| `sql/client_class.sql` | `client_class_v0(...)`: per-request traffic class for both eras (#5), DuckDB macros. Check: `duckdb -c ".read sql/client_class.sql" -c ".read sql/client_class_check.sql"`. |

Decisions are cross-repo and live in `bioc-infrastructure/adr` — 0002 (mirror unfiltered),
0003 (logging after the cutover), 0004 (stats are static files), 0012 (raw addresses, hashed in
views).

## Install the timer

```bash
ln -sf "$PWD"/systemd/bioc-logpush-check.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now bioc-logpush-check.timer
```

`bioc-notify@.service` is shared with the sync timers and is installed from `bioc-edge`.
