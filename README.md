# bioc-traffic

Access-log analytics for [bioconductor.org](https://bioconductor.org): who downloads which
packages, how traffic splits between people, package clients, mirrors and crawlers, and how
both have changed since 2020.

bioconductor.org moved from AWS CloudFront to a Cloudflare Worker on 2026-09-28. This repo
treats the two eras as one dataset: about 7.5 billion CloudFront requests from 2020-01-01
through the cutover, and the Worker's per-request records since. Requests run around 6
million a day on the Worker as of late September 2026.

## Where it sits

```
bioc-edge (Worker)  ──Logpush──▶  raw records  ──▶  hourly Parquet  ──▶  access view (both eras)
                                                                            │
                         CloudFront logs (2020 → cutover) ──▶ monthly Parquet ┘
                                                                            │
                                    ┌───────────────────────────────────────┤
                                    ▼                                       ▼
                           package download stats                 dashboard rollups
                     (fixed + human/automated columns)    (minute / hour / day, by class,
                                                          status, country, page, package…)
```

- **The producer is [`bioc-edge`](https://github.com/seandavi/bioc-edge).** Its Worker writes
  one access record per request, using CloudFront's field names so the eras line up. That
  record is the contract between the two repos.
- **This repo owns everything after delivery:** the delivery gap check, conversion to
  Parquet, the unified view, traffic classification, and the aggregates.
- **Raw records are never edited** ([ADR 0002](https://github.com/seandavi/bioc-infrastructure/blob/main/adr/0002-mirror-access-logs-unfiltered.md)).
  Everything else is derived and can be rebuilt from them.
- **Outputs are static files**, regenerated on timers, not a live query service
  ([ADR 0004](https://github.com/seandavi/bioc-infrastructure/blob/main/adr/0004-download-statistics-are-generated-static-files.md)).
- How this fits with the package registry and the research-intelligence work:
  [bioc-infrastructure#67](https://github.com/seandavi/bioc-infrastructure/issues/67).

## Privacy

The logs carry client IP addresses, and they stay private. Everything published from here is
an aggregate.

- **Raw addresses are kept, hashed in views** ([ADR 0012](https://github.com/seandavi/bioc-infrastructure/blob/main/adr/0012-client-addresses-are-stored-raw-and-hashed-in-views.md)):
  `client_id = sha256(salt || c_ip)` with a secret salt, the same in both eras, so distinct
  clients can be counted across the cutover. The test vector is in `cloudflare-logs-to-parquet.py --self-check`.
- **Nothing leaves this pipeline with an address in it.** Aggregates carry counts of
  `client_id`s, never `c_ip`.
- No log data is in this repository.

## What's here

| Piece | What it does |
|---|---|
| `cloudflare-logs-to-parquet.py` | Worker records → one Parquet file per UTC hour, with `--verify` (raw count = Parquet count). Every 15 min, plus a nightly re-seal for late records. |
| `cloudfront-logs-to-parquet.py` | CloudFront logs → one Parquet file per month, with `--verify`. Also holds the published download definition. |
| `sql/access.sql` | The `access` view: both eras, one schema, with `era`, `client_id` and a `production` flag. |
| `sql/client_class.sql` | `client_class_v0`: versioned, rule-based traffic classes (human browser, package client, mirror, CI, search crawler, AI crawler, likely automated…). Check: `sql/client_class_check.sql`. |
| `download-stats.py`, `sql/downloads.sql` | Package downloads by package, release and category, 2020 →. Fixed-definition columns that match the published `/packages/stats/` figures, plus human / package-client / automated columns labelled with the classifier version. Regenerates the `/packages/stats/` file tree. |
| `sql/rollup_*.sql` | Dashboard rollups, incremental: minute (6 h), hour (30 d), day (90 d), and a daily overall series back to 2020. |
| `check-logpush.sh` | Daily alarm if any UTC hour of yesterday's delivery is missing. Logpush can't backfill, so a missed gap is permanent loss. |
| `systemd/` | The timers that run all of the above. |
| `justfile` | Common invocations: `just --list`. |

## Two things worth knowing about the numbers

- **Download counts and "who downloads" are separate questions.** The fixed columns count
  requests the same way the published statistics always have. The human/automated columns
  are an interpretation, versioned, and will change as the classifier improves.
- **The human/automated split jumps at the cutover** for a classifier reason, not a traffic
  one: only the Cloudflare era has network (ASN) and verified-bot information. The fixed
  columns run straight across it.

## Running it

It runs on one host with DuckDB, `uv` and `rclone`. It needs credentials for the log buckets
and the salt, which aren't public. Operator specifics (buckets, job IDs, measurements, the
timer install steps) are in `ANALYTICS.local.md`, which is not in the repository.

## Status

Working: hourly conversion, gap check, unified view, package stats, rollups. Next: network
and geolocation lookups for the CloudFront era, so classification is comparable across the
cutover ([#4](../../issues/4)), and a public traffic dashboard built from the rollups ([#6](../../issues/6)).

## License

[Apache 2.0](LICENSE).
